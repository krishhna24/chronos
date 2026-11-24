import { describe, expect, it } from 'vitest';
import type { Interval, Version } from '../api/types';
import { coverageSegments, fraction, percent, placeMarkers, stateAt, ticks, timeAt } from './timeline';

const T = (min: number) => Date.UTC(2026, 10, 24, 12, 0) + min * 60_000;
const iso = (min: number) => new Date(T(min)).toISOString();

// Observed 10..40, gap 40..55 (watch expired), observed 55..open.
const intervals: Interval[] = [
  { start: iso(10), end: iso(40), endReason: '410 Gone' },
  { start: iso(55) },
];
const domain = { start: T(0), end: T(100) };
const r = (n: number) => Math.round(n * 1e6) / 1e6;

describe('coverageSegments', () => {
  it('classifies the whole domain: unobserved, observed, gap, observed', () => {
    const segs = coverageSegments(intervals, domain);
    expect(segs.map((s) => [s.state, r(s.left), r(s.width)])).toEqual([
      ['unobserved', 0, 10],
      ['observed', 10, 30],
      ['gap', 40, 15],
      ['observed', 55, 45],
    ]);
    expect(segs[2]!.reason).toBe('410 Gone');
    expect(segs[1]!.reason).toBeUndefined();
  });

  it('segments are contiguous and cover 100% of the axis', () => {
    const segs = coverageSegments(intervals, domain, { horizon: iso(5) });
    for (let i = 1; i < segs.length; i++) expect(segs[i]!.start).toBe(segs[i - 1]!.end);
    expect(segs.reduce((n, s) => n + s.width, 0)).toBeCloseTo(100);
  });

  it('marks everything before the horizon as expired', () => {
    const segs = coverageSegments(intervals, domain, { horizon: iso(20) });
    expect(segs.map((s) => [s.state, r(s.left), r(s.width)])).toEqual([
      ['expired', 0, 20],
      ['observed', 20, 20],
      ['gap', 40, 15],
      ['observed', 55, 45],
    ]);
  });

  it('shades a trailing gap when the last interval is closed', () => {
    const segs = coverageSegments([{ start: iso(0), end: iso(60), endReason: 'shutdown' }], domain);
    expect(segs.map((s) => s.state)).toEqual(['observed', 'gap']);
    expect(segs[1]).toMatchObject({ left: 60, width: 40, reason: 'shutdown' });
  });

  it('an open interval extends to now, not beyond', () => {
    const segs = coverageSegments([{ start: iso(0) }], domain, { now: T(80) });
    expect(segs.map((s) => [s.state, r(s.left), r(s.width)])).toEqual([
      ['observed', 0, 80],
      ['gap', 80, 20],
    ]);
  });

  it('with no coverage everything is unobserved', () => {
    expect(coverageSegments([], domain).map((s) => [s.state, s.width])).toEqual([['unobserved', 100]]);
  });

  it('clips intervals to the domain and handles unsorted input', () => {
    const segs = coverageSegments([intervals[1]!, intervals[0]!], { start: T(30), end: T(60) });
    expect(segs.map((s) => [s.state, Math.round(s.left), Math.round(s.width)])).toEqual([
      ['observed', 0, 33],
      ['gap', 33, 50],
      ['observed', 83, 17],
    ]);
  });
});

describe('stateAt', () => {
  it('follows ADR 0004 at boundaries (Start inclusive, End starts the gap)', () => {
    const opts = { horizon: iso(5), now: T(100) };
    expect(stateAt(intervals, T(2), opts)).toBe('expired');
    expect(stateAt(intervals, T(7), opts)).toBe('unobserved');
    expect(stateAt(intervals, T(10), opts)).toBe('observed');
    expect(stateAt(intervals, T(39), opts)).toBe('observed');
    expect(stateAt(intervals, T(40), opts)).toBe('gap');
    expect(stateAt(intervals, T(54), opts)).toBe('gap');
    expect(stateAt(intervals, T(55), opts)).toBe('observed');
  });

  // Regression: against the real server the Overview's "Now" column read
  // "gap" for every type while Chronos was watching, because the open
  // interval's segment ended exactly at now (half-open).
  it('an open interval is observed at exactly now, and not after it', () => {
    const opts = { horizon: iso(5), now: T(100) };
    expect(stateAt(intervals, T(100), opts)).toBe('observed');
    expect(stateAt(intervals, T(101), opts)).toBe('gap');
    const closed: Interval[] = [{ start: iso(10), end: iso(40) }];
    expect(stateAt(closed, T(100), opts)).toBe('gap');
  });
});

describe('placeMarkers', () => {
  const v = (seq: number, min: number): Version => ({
    seq,
    observedAt: iso(min),
    type: 'Modified',
    origin: 'watch',
    uid: 'u',
    resourceVersion: String(seq),
  });

  it('places markers proportionally', () => {
    const m = placeMarkers([v(1, 25), v(2, 50)], domain);
    expect(m.map((x) => [x.version.seq, x.left, x.lane, x.clamped])).toEqual([
      [1, 25, 0, false],
      [2, 50, 0, false],
    ]);
  });

  it('clamps out-of-range versions to the edges and flags them', () => {
    const m = placeMarkers([v(1, -10), v(2, 150)], domain);
    expect(m.map((x) => [x.left, x.clamped])).toEqual([
      [0, true],
      [100, true],
    ]);
  });

  it('stacks overlapping markers into lanes, capped at maxLanes', () => {
    const m = placeMarkers([v(1, 50), v(2, 50.5), v(3, 50.7), v(4, 50.9), v(5, 60)], domain, { minGapPct: 2, maxLanes: 3 });
    expect(m.map((x) => x.lane)).toEqual([0, 1, 2, 2, 0]);
  });

  it('orders by seq', () => {
    expect(placeMarkers([v(2, 50), v(1, 25)], domain).map((x) => x.version.seq)).toEqual([1, 2]);
  });
});

describe('axis helpers', () => {
  it('fraction / percent / timeAt are consistent', () => {
    expect(fraction(domain, T(25))).toBe(0.25);
    expect(percent(domain, T(-50))).toBe(0);
    expect(percent(domain, T(500))).toBe(100);
    expect(timeAt(domain, 0.5)).toBe(T(50));
    expect(fraction({ start: 5, end: 5 }, 5)).toBe(0);
  });

  it('picks round tick steps within the domain', () => {
    const { step, times } = ticks(domain, 6);
    expect(step).toBe(30 * 60_000);
    expect(times).toEqual([T(0), T(30), T(60), T(90)]);
  });

  it('aligns hour-or-larger ticks to local time', () => {
    const day = { start: Date.UTC(2026, 10, 24, 0, 0), end: Date.UTC(2026, 10, 27, 0, 0) };
    // UTC-5: local midnight is 05:00Z.
    const { step, times } = ticks(day, 3, 300);
    expect(step).toBe(86_400_000);
    expect(times.map((t) => new Date(t).toISOString())).toEqual([
      '2026-11-24T05:00:00.000Z',
      '2026-11-25T05:00:00.000Z',
      '2026-11-26T05:00:00.000Z',
    ]);
  });

  it('returns no ticks for an empty domain', () => {
    expect(ticks({ start: 1, end: 1 }).times).toEqual([]);
  });
});
