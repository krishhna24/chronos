# ADR 0004: Coverage, gaps and missing history

Status: accepted (2026-11-24)

## Principle
Never present a guess as an observation. Missing data must be
distinguishable from "the object did not exist".

## Model
Per resource type the store keeps alternating coverage events.
- `Start` is written in the same batch as, and after, the records produced
  by applying a complete LIST. From `Start.At` Chronos holds a complete view.
- `End` is written when continuity is lost: the reflector had to relist
  (watch expired with 410 Gone, or the watch could not be resumed), on
  graceful shutdown, or — after a crash — at startup with
  `At = max(last heartbeat, last record time)`. A watch that resumes from
  its last resourceVersion without relisting is continuous (the API server
  replays missed events) and is not a gap.

## Classification of an instant T for resource R
1. `T < horizon` → `expired`.
2. Find the last coverage event with `At <= T`. None → `unobserved`.
   `Start` → `observed`. `End` → `gap`.
3. `T` in the future → request error.

## Existence
- `observed`: latest record ≤ T is non-delete → `present`; delete or none →
  `absent`.
- `gap`: `unknown`; the last known state (latest record ≤ T) is returned
  marked as such.
- `unobserved` / `expired`: `unknown`, no object.

## Relist diff (after a gap or restart)
Compare the LIST against the latest stored record of every key:
new key → resync `Added` (or `baseline` if the resource has no coverage
history at all); missing key → resync `Deleted`; different UID → resync
Deleted + Added; normalized hash differs → resync `Modified`; equal → no
record (but the gap remains: ABA changes inside it are unknowable).

## Diff completeness
`diff(T1, T2)` is `complete` only if R is `observed` over all of [T1, T2].
