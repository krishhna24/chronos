# ADR 0007: Process stalls and coverage loss the store cannot record

Status: accepted (2026-11-25), after chaos testing (docs/reliability.md,
defects D1 and D2).

## Context
ADR 0004 promises that an answer labelled `observed` is never a guess.
Chaos testing found two ways to break that promise:

- **D1, stalled process.** Freezing Chronos (SIGSTOP for 120 s, the same
  as a VM pause or hard CPU throttling) left coverage continuous. When it
  resumed, the watch delivered the backlog, stamped up to two minutes late,
  so the history inside the window was wrong yet labelled `observed`, and
  it stayed wrong permanently.
- **D2, coverage loss that cannot be persisted.** With the disk full, the
  collector knew continuity was lost but could not write the End event.
  Queries read coverage only from the store, so they kept answering
  `observed` with thousands of changes missing.

## Decision
1. **Stall watchdog (collector).** A dedicated goroutine ticks every
   second on the injected clock and records each tick. If the gap between
   two ticks exceeds `--stall-timeout` (default 5 s), the process did not
   run for that long, so nothing received in or after that window can be
   trusted to have its real time. Treat it as a continuity loss for every
   resource: coverage ends at the last on-time tick, and every resource is
   relisted. The same applies when a heartbeat tick is late by more than
   the stall timeout. This reuses the existing lost/relist path; End must
   still be placed before the first record it cannot vouch for, so if
   records stamped after the last on-time tick were already written, End
   goes at the last record written before the stall (never earlier than
   the store allows, never later than the last on-time tick).
   - This detects stalls of the Chronos process (SIGSTOP, VM or container
     freeze, CPU starvation). It cannot detect a slow API server that
     delivers watch events late while Chronos runs normally; that remains
     the "watch delivery lag" accepted in ADR 0002 and is documented as a
     limitation.
2. **Live coverage overlay (collector → query).** The collector exposes,
   per resource, the time continuity was lost whenever that loss is not yet
   durable as an End event (`ResourceStatus.LostAt`, cleared once the End
   is committed). The query engine takes an optional live-coverage source
   and classifies any instant at or after `LostAt` as `gap` with reason
   `"continuity lost; not yet recorded"`, whatever the store says. The
   overlay only ever moves answers from `observed` to `gap`, never the
   other way, so it cannot fabricate continuity.
3. **Visibility.** Store write failures are already logged; in addition
   the collector status records the last write error and its time, and
   `/api/v1/resources` coverage for an affected resource shows the
   overlaid open gap. Readiness is unchanged (ADR 0006 and the reasoning
   in cmd/chronos): the API stays reachable and reports the gap.

## Consequences
- A host suspend or long GC/CPU starvation now produces an explicit gap
  and a relist instead of silently late timestamps.
- During a store outage, answers degrade to `gap`/`unknown` immediately
  instead of after the store recovers.
- Lists and diffs spanning the overlaid gap are `complete=false`.

## Addendum (2026-11-25): read-only mode when the store stops accepting writes
Re-testing D2 after the round-2 store fixes showed a third case: an
ambiguous commit failure (disk full) poisons the store, the collector
stops, and the process used to exit, taking the API down although the
history before the loss is still valid. Now `cmd/chronos` keeps serving
reads, overlays every resource as a gap from max(last heartbeat, latest
record) -- the crash-recovery bound of ADR 0004 -- and exits after
`--degraded-exit-after` (default 10m) so the pod restarts and recovery is
attempted. Measured: while the disk was full the API kept answering and
all 5,339 unrecorded writes fell inside the reported gap.
