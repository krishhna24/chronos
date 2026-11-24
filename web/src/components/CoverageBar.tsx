import type { Interval } from '../api/types';
import { formatDuration, formatTime } from '../lib/time';
import { coverageSegments, type Domain } from '../lib/timeline';

/** A bar over the retention window: observed vs gap / unobserved / expired. */
export function CoverageBar({ intervals, domain, horizon, now }: { intervals: readonly Interval[]; domain: Domain; horizon?: string; now: number }) {
  const segs = coverageSegments(intervals, domain, { horizon, now });
  const span = domain.end - domain.start;
  const observed = segs.filter((s) => s.state === 'observed').reduce((n, s) => n + (s.end - s.start), 0);
  const gaps = segs.filter((s) => s.state === 'gap');
  const ratio = span > 0 ? observed / span : 0;
  // A short observation in a long window would otherwise read as "0%".
  const pct = ratio > 0 && ratio < 0.01 ? '<1' : String(Math.round(ratio * 100));
  const summary = `Observed ${pct}% of the window; ${gaps.length} gap${gaps.length === 1 ? '' : 's'}${
    gaps.length ? ` totalling ${formatDuration(gaps.reduce((n, s) => n + s.end - s.start, 0))}` : ''
  }.`;
  return (
    <div className="covbar-wrap">
      <div className="covbar" role="img" aria-label={summary}>
        {segs.map((s) => (
          <div
            key={`${s.state}-${s.start}`}
            className={`seg seg-${s.state}`}
            style={{ left: `${s.left}%`, width: `${s.width}%` }}
            title={`${s.state}: ${formatTime(s.start)} – ${formatTime(s.end)} (${formatDuration(s.end - s.start)})${s.reason ? `, ${s.reason}` : ''}`}
          />
        ))}
      </div>
      <span className="covbar-summary">{summary}</span>
    </div>
  );
}
