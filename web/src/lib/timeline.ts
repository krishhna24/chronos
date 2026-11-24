// Pure geometry for time axes: coverage segments (observed vs gap /
// unobserved / expired shading), version marker placement and tick marks.
// Positions are percentages of the axis width so CSS does the layout and
// nothing depends on measuring the DOM.

import type { CoverageState, Interval, Time, Version } from '../api/types';
import { toMs } from './time';

export interface Domain {
  start: number;
  end: number;
}

export interface Segment {
  state: CoverageState;
  start: number;
  end: number;
  /** Left edge and width as percentages of the domain. */
  left: number;
  width: number;
  /** For gaps: why the preceding interval ended. */
  reason?: string;
}

/** Fraction (0..1, unclamped) of t within the domain. */
export function fraction(d: Domain, t: number): number {
  const span = d.end - d.start;
  if (!(span > 0)) return 0;
  return (t - d.start) / span;
}

export function percent(d: Domain, t: number): number {
  return Math.min(100, Math.max(0, fraction(d, t) * 100));
}

/** Inverse of fraction: the time at a 0..1 position on the axis. */
export function timeAt(d: Domain, f: number): number {
  return d.start + Math.min(1, Math.max(0, f)) * (d.end - d.start);
}

/**
 * Splits the domain into consecutive coverage segments: `observed` for each
 * interval, `gap` between intervals (and after a closed last interval),
 * `unobserved` before the first interval, and `expired` before the
 * retention horizon. Segments cover the whole domain with no overlap, so
 * every instant on the axis is visibly classified.
 */
export function coverageSegments(
  intervals: readonly Interval[],
  domain: Domain,
  opts: { horizon?: Time; now?: number } = {},
): Segment[] {
  const now = opts.now ?? domain.end;
  const horizon = opts.horizon ? toMs(opts.horizon) : Number.NaN;
  const raw: Array<{ state: CoverageState; start: number; end: number; reason?: string }> = [];
  const sorted = [...intervals]
    .map((iv) => ({ start: toMs(iv.start), end: iv.end ? toMs(iv.end) : now, reason: iv.endReason, open: !iv.end }))
    .filter((iv) => !Number.isNaN(iv.start))
    .sort((a, b) => a.start - b.start);

  let cursor = Number.NEGATIVE_INFINITY;
  let first = true;
  for (const iv of sorted) {
    if (iv.start > cursor) {
      raw.push({
        state: first ? 'unobserved' : 'gap',
        start: cursor,
        end: iv.start,
        reason: first ? undefined : raw[raw.length - 1]?.reason,
      });
    }
    raw.push({ state: 'observed', start: Math.max(iv.start, cursor), end: iv.end, reason: iv.reason });
    cursor = Math.max(cursor, iv.end);
    first = false;
  }
  raw.push({
    state: first ? 'unobserved' : 'gap',
    start: cursor,
    end: Number.POSITIVE_INFINITY,
    reason: first ? undefined : raw[raw.length - 1]?.reason,
  });

  // The reason carried on observed segments describes how they ended; only
  // gaps keep it.
  const classified = raw.map((s) => (s.state === 'gap' ? s : { ...s, reason: undefined }));

  // Everything before the horizon is expired, whatever was recorded.
  const withHorizon: typeof raw = [];
  for (const s of classified) {
    if (!Number.isNaN(horizon) && s.start < horizon) {
      withHorizon.push({ state: 'expired', start: s.start, end: Math.min(s.end, horizon) });
      if (s.end > horizon) withHorizon.push({ ...s, start: horizon });
    } else {
      withHorizon.push(s);
    }
  }

  const out: Segment[] = [];
  for (const s of withHorizon) {
    const start = Math.max(s.start, domain.start);
    const end = Math.min(s.end, domain.end);
    if (!(end > start)) continue;
    const prev = out[out.length - 1];
    if (prev && prev.state === 'expired' && s.state === 'expired' && prev.end === start) {
      prev.end = end;
      prev.width = percent(domain, end) - prev.left;
      continue;
    }
    const left = percent(domain, start);
    const seg: Segment = { state: s.state, start, end, left, width: percent(domain, end) - left };
    if (s.reason) seg.reason = s.reason;
    out.push(seg);
  }
  return out;
}

/** Coverage state of a single instant according to the same rules. */
export function stateAt(
  intervals: readonly Interval[],
  t: number,
  opts: { horizon?: Time; now?: number } = {},
): CoverageState {
  // An open interval runs up to and including the server's now, but
  // segments are half-open [start, end), so the instant `now` itself would
  // fall into the trailing segment and read as a gap while Chronos is
  // watching. Answer that instant directly.
  if (opts.now !== undefined && t === opts.now) {
    const expired = opts.horizon !== undefined && t < toMs(opts.horizon);
    if (!expired && intervals.some((iv) => !iv.end && toMs(iv.start) <= t)) return 'observed';
  }
  const segs = coverageSegments(intervals, { start: t - 1, end: t + 1 }, opts);
  // The segment containing t itself (segments are [start, end)).
  return segs.find((s) => s.start <= t && t < s.end)?.state ?? 'unobserved';
}

export interface Marker {
  version: Version;
  /** Percentage position on the axis (clamped to 0..100). */
  left: number;
  /** True when the version lies outside the domain (drawn at the edge). */
  clamped: boolean;
  /** Visual lane, so markers that would overlap stack vertically. */
  lane: number;
}

/**
 * Places version markers. Markers closer than `minGapPct` to a previous one
 * in the same lane move to the next lane (up to `maxLanes`, after which they
 * share the last lane) so each stays individually clickable.
 */
export function placeMarkers(
  versions: readonly Version[],
  domain: Domain,
  opts: { minGapPct?: number; maxLanes?: number } = {},
): Marker[] {
  const minGap = opts.minGapPct ?? 1.5;
  const maxLanes = Math.max(1, opts.maxLanes ?? 3);
  const laneLast: number[] = [];
  const sorted = [...versions].sort((a, b) => a.seq - b.seq);
  return sorted.map((v) => {
    const t = toMs(v.observedAt);
    const f = fraction(domain, t);
    const left = Math.min(100, Math.max(0, f * 100));
    let lane = laneLast.findIndex((last) => left - last >= minGap);
    if (lane === -1) lane = laneLast.length < maxLanes ? laneLast.length : maxLanes - 1;
    laneLast[lane] = left;
    return { version: v, left, clamped: f < 0 || f > 1, lane };
  });
}

const STEPS = [
  1_000, 5_000, 15_000, 30_000,
  60_000, 5 * 60_000, 15 * 60_000, 30 * 60_000,
  3_600_000, 3 * 3_600_000, 6 * 3_600_000, 12 * 3_600_000,
  86_400_000, 2 * 86_400_000, 7 * 86_400_000, 30 * 86_400_000,
];

/** Picks a round step giving at most `maxTicks` ticks and returns the tick
 * times inside the domain, aligned to multiples of the step (in UTC, plus
 * the local timezone offset for steps of an hour or more so labels read
 * as round local times). */
export function ticks(domain: Domain, maxTicks = 6, tzOffsetMin = 0): { step: number; times: number[] } {
  const span = domain.end - domain.start;
  if (!(span > 0) || maxTicks < 1) return { step: 0, times: [] };
  const step = STEPS.find((s) => span / s <= maxTicks) ?? STEPS[STEPS.length - 1]!;
  // Date#getTimezoneOffset is minutes *behind* UTC: local midnight is at
  // UTC midnight + offset.
  const shift = step >= 3_600_000 ? tzOffsetMin * 60_000 : 0;
  const first = Math.ceil((domain.start - shift) / step) * step + shift;
  const times: number[] = [];
  for (let t = first; t <= domain.end; t += step) times.push(t);
  return { step, times };
}
