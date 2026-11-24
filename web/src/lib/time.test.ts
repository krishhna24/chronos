import { describe, expect, it } from 'vitest';
import {
  compareTime,
  formatAgo,
  formatDuration,
  formatTime,
  fromDatetimeLocal,
  fromMs,
  parseRelative,
  parseTime,
  toDatetimeLocal,
  toMs,
} from './time';

describe('parseTime', () => {
  it('parses RFC 3339 with nanoseconds', () => {
    expect(parseTime('2026-11-24T14:00:00.123456789Z')).toEqual({ ms: Date.UTC(2026, 10, 24, 14, 0, 0, 123), subNanos: 456789 });
  });
  it('parses without fraction and with offsets', () => {
    expect(parseTime('2026-11-24T14:00:00Z')?.ms).toBe(Date.UTC(2026, 10, 24, 14));
    expect(parseTime('2026-11-24T15:30:00+01:30')?.ms).toBe(Date.UTC(2026, 10, 24, 14));
    expect(parseTime('2026-11-24T09:00:00.5-05:00')).toEqual({ ms: Date.UTC(2026, 10, 24, 14, 0, 0, 500), subNanos: 0 });
  });
  it('rejects malformed input', () => {
    for (const s of ['', 'yesterday', '2026-11-24', '2026-11-24T14:00:00', '2026-11-24T14:00:00.1234567890Z']) {
      expect(parseTime(s)).toBeNull();
    }
    expect(Number.isNaN(toMs('nope'))).toBe(true);
  });
});

describe('compareTime', () => {
  it('orders by nanoseconds, not just milliseconds', () => {
    const a = '2026-11-24T14:00:00.123000001Z';
    const b = '2026-11-24T14:00:00.123000002Z';
    expect(toMs(a)).toBe(toMs(b));
    expect(compareTime(a, b)).toBeLessThan(0);
    expect(compareTime(b, a)).toBeGreaterThan(0);
    expect(compareTime(a, a)).toBe(0);
  });
  it('compares across time zones', () => {
    expect(compareTime('2026-11-24T15:00:00+01:00', '2026-11-24T14:00:00Z')).toBe(0);
  });
});

describe('relative times', () => {
  it('parses past offsets', () => {
    expect(parseRelative('-5m')).toBe(-300_000);
    expect(parseRelative('-1h')).toBe(-3_600_000);
    expect(parseRelative('-1d')).toBe(-86_400_000);
    expect(parseRelative('-1h30m')).toBe(-5_400_000);
    expect(parseRelative(' -45s ')).toBe(-45_000);
  });
  it('rejects future and malformed offsets', () => {
    for (const s of ['5m', '+5m', '-', '-5x', '-m', '']) expect(parseRelative(s)).toBeNull();
  });
});

describe('datetime-local conversion', () => {
  it('round-trips local time to the second', () => {
    const ms = new Date(2026, 10, 24, 9, 5, 7).getTime();
    expect(toDatetimeLocal(ms)).toBe('2026-11-24T09:05:07');
    expect(fromDatetimeLocal('2026-11-24T09:05:07')).toBe(ms);
  });
  it('accepts minute precision and rejects garbage', () => {
    expect(fromDatetimeLocal('2026-11-24T09:05')).toBe(new Date(2026, 10, 24, 9, 5).getTime());
    expect(fromDatetimeLocal('')).toBeNull();
    expect(fromDatetimeLocal('2026-11-24')).toBeNull();
  });
});

describe('formatting', () => {
  it('formats UTC with millisecond and sub-millisecond digits', () => {
    expect(formatTime('2026-11-24T14:00:05Z', { utc: true })).toBe('2026-11-24 14:00:05Z');
    expect(formatTime('2026-11-24T14:00:05.120Z', { utc: true })).toBe('2026-11-24 14:00:05.120Z');
    expect(formatTime('2026-11-24T14:00:05.123456789Z', { utc: true })).toBe('2026-11-24 14:00:05.123456789Z');
    expect(formatTime(Date.UTC(2026, 0, 2, 3, 4, 5), { utc: true })).toBe('2026-01-02 03:04:05Z');
  });
  it('formats local time without a zone suffix', () => {
    expect(formatTime(new Date(2026, 10, 24, 9, 5, 7).getTime())).toBe('2026-11-24 09:05:07');
  });
  it('returns unparseable input unchanged', () => {
    expect(formatTime('garbage')).toBe('garbage');
  });
  it('formats durations compactly', () => {
    expect(formatDuration(45_000)).toBe('45s');
    expect(formatDuration(5 * 60_000)).toBe('5m');
    expect(formatDuration(5 * 60_000 + 3000)).toBe('5m 3s');
    expect(formatDuration(80 * 60_000)).toBe('1h 20m');
    expect(formatDuration(76 * 3_600_000)).toBe('3d 4h');
  });
  it('formats relative to now', () => {
    expect(formatAgo(1000, 1500)).toBe('just now');
    expect(formatAgo(0, 300_000)).toBe('5m ago');
    expect(formatAgo(300_000, 0)).toBe('in 5m');
  });
  it('fromMs produces RFC 3339 UTC', () => {
    expect(fromMs(Date.UTC(2026, 10, 24, 14, 0, 0, 7))).toBe('2026-11-24T14:00:00.007Z');
  });
});
