// Time helpers. The server speaks RFC 3339 with nanoseconds; JavaScript
// Dates hold milliseconds. Geometry and display use milliseconds, but times
// the server returned are passed back verbatim so no precision is lost (a
// version observed at .123456789 must not be queried as .123, which would be
// *before* the record).

import type { Time } from '../api/types';

const RFC3339 =
  /^(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?([Zz]|[+-]\d{2}:\d{2})$/;

export interface Instant {
  /** Milliseconds since the epoch (floor). */
  ms: number;
  /** Nanoseconds within the millisecond, 0..999999. */
  subNanos: number;
}

/** Parses an RFC 3339 timestamp; returns null when malformed. */
export function parseTime(s: string): Instant | null {
  const m = RFC3339.exec(s.trim());
  if (!m) return null;
  const [, y, mo, d, h, mi, se, frac = '', zone = 'Z'] = m;
  const nanos = Number(frac.padEnd(9, '0'));
  let ms = Date.UTC(Number(y), Number(mo) - 1, Number(d), Number(h), Number(mi), Number(se));
  if (Number.isNaN(ms)) return null;
  if (zone !== 'Z' && zone !== 'z') {
    const sign = zone.startsWith('-') ? -1 : 1;
    const zh = Number(zone.slice(1, 3));
    const zm = Number(zone.slice(4, 6));
    ms -= sign * (zh * 60 + zm) * 60_000;
  }
  return { ms: ms + Math.floor(nanos / 1e6), subNanos: nanos % 1e6 };
}

/** Milliseconds since the epoch, or NaN when malformed. */
export function toMs(s: Time): number {
  return parseTime(s)?.ms ?? Number.NaN;
}

/** Exact (nanosecond) comparison of two RFC 3339 times: <0, 0, >0. */
export function compareTime(a: Time, b: Time): number {
  const x = parseTime(a);
  const y = parseTime(b);
  if (!x || !y) return 0;
  return x.ms - y.ms || x.subNanos - y.subNanos;
}

/** RFC 3339 in UTC with millisecond precision. */
export function fromMs(ms: number): Time {
  return new Date(ms).toISOString();
}

const pad = (n: number, w = 2) => String(n).padStart(w, '0');

/** Value for <input type="datetime-local" step="1"> in local time. */
export function toDatetimeLocal(ms: number): string {
  const d = new Date(ms);
  return (
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}` +
    `T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
  );
}

/** Parses a datetime-local value (local time, seconds optional). */
export function fromDatetimeLocal(v: string): number | null {
  const m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?$/.exec(v);
  if (!m) return null;
  const [, y, mo, d, h, mi, se = '0', frac = '0'] = m;
  const date = new Date(
    Number(y),
    Number(mo) - 1,
    Number(d),
    Number(h),
    Number(mi),
    Number(se),
    Number(frac.padEnd(3, '0')),
  );
  const ms = date.getTime();
  return Number.isNaN(ms) ? null : ms;
}

const UNIT_MS: Record<string, number> = {
  s: 1000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
};

/** Parses a relative offset like "-5m", "-1h30m", "-2d" to a (negative)
 * millisecond offset. Only past offsets are accepted. */
export function parseRelative(s: string): number | null {
  const m = /^-((?:\d+[smhd])+)$/.exec(s.trim());
  if (!m || m[1] === undefined) return null;
  let total = 0;
  for (const part of m[1].matchAll(/(\d+)([smhd])/g)) {
    total += Number(part[1]) * (UNIT_MS[part[2] ?? ''] ?? 0);
  }
  return -total;
}

/** "2026-11-24 14:00:05.123" in local time; keeps sub-ms digits if given. */
export function formatTime(t: Time | number, opts: { utc?: boolean } = {}): string {
  let ms: number;
  let extra = '';
  if (typeof t === 'number') {
    ms = t;
  } else {
    const p = parseTime(t);
    if (!p) return t;
    ms = p.ms;
    const frac = /\.(\d+)/.exec(t)?.[1] ?? '';
    if (frac.length > 3) extra = frac.slice(3).replace(/0+$/, '');
  }
  const d = new Date(ms);
  const get = opts.utc
    ? [d.getUTCFullYear(), d.getUTCMonth() + 1, d.getUTCDate(), d.getUTCHours(), d.getUTCMinutes(), d.getUTCSeconds(), d.getUTCMilliseconds()]
    : [d.getFullYear(), d.getMonth() + 1, d.getDate(), d.getHours(), d.getMinutes(), d.getSeconds(), d.getMilliseconds()];
  const [y, mo, da, h, mi, se, msPart] = get as [number, number, number, number, number, number, number];
  const fraction = msPart !== 0 || extra ? `.${pad(msPart, 3)}${extra}` : '';
  return `${y}-${pad(mo)}-${pad(da)} ${pad(h)}:${pad(mi)}:${pad(se)}${fraction}${opts.utc ? 'Z' : ''}`;
}

/** Compact duration: "45s", "5m", "1h 20m", "3d 4h". */
export function formatDuration(ms: number): string {
  const s = Math.round(Math.abs(ms) / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m${s % 60 ? ` ${s % 60}s` : ''}`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h${m % 60 ? ` ${m % 60}m` : ''}`;
  const d = Math.floor(h / 24);
  return `${d}d${h % 24 ? ` ${h % 24}h` : ''}`;
}

/** "5m ago" / "just now" / "in 2m" relative to nowMs. */
export function formatAgo(ms: number, nowMs: number): string {
  const delta = nowMs - ms;
  if (Math.abs(delta) < 1000) return 'just now';
  return delta > 0 ? `${formatDuration(delta)} ago` : `in ${formatDuration(delta)}`;
}
