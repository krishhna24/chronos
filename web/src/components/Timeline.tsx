import type { Interval, Version } from '../api/types';
import { formatDuration, formatTime } from '../lib/time';
import { coverageSegments, placeMarkers, ticks, type Domain, percent } from '../lib/timeline';

export interface TimelineProps {
  domain: Domain;
  versions: readonly Version[];
  coverage: readonly Interval[];
  horizon?: string;
  now: number;
  /** Selected instant (ms), drawn as a cursor. */
  cursor?: number;
  selectedSeq?: number;
  /** Seqs marked as diff endpoints. */
  diffSeqs?: readonly number[];
  onSelect: (v: Version) => void;
}

const STATE_LABEL = {
  observed: 'observed',
  gap: 'gap (not watching)',
  unobserved: 'not yet watched',
  expired: 'expired',
} as const;

function tickLabel(t: number, step: number): string {
  const s = formatTime(t);
  // Drop the date for sub-day steps, seconds for minute steps.
  if (step < 60_000) return s.slice(11, 19);
  if (step < 86_400_000) return s.slice(11, 16);
  return s.slice(0, 10);
}

/** Horizontal history axis: coverage shading behind version markers. Each
 * marker is a button, so the timeline is fully keyboard operable. */
export function Timeline({ domain, versions, coverage, horizon, now, cursor, selectedSeq, diffSeqs = [], onSelect }: TimelineProps) {
  const segments = coverageSegments(coverage, domain, { horizon, now });
  const markers = placeMarkers(versions, domain);
  const lanes = Math.max(1, ...markers.map((m) => m.lane + 1));
  const { step, times } = ticks(domain, 6, new Date(domain.start).getTimezoneOffset());

  return (
    <figure className="timeline" aria-label="History timeline">
      <div className="timeline-track" style={{ height: `${1.5 + lanes * 1.25}rem` }}>
        {segments.map((s) => (
          <div
            key={`${s.state}-${s.start}`}
            className={`seg seg-${s.state}`}
            style={{ left: `${s.left}%`, width: `${s.width}%` }}
            title={`${STATE_LABEL[s.state]}: ${formatTime(s.start)} – ${formatTime(s.end)} (${formatDuration(s.end - s.start)})${s.reason ? `, ${s.reason}` : ''}`}
          />
        ))}
        {cursor !== undefined && (
          <div className="cursor" style={{ left: `${percent(domain, cursor)}%` }} aria-hidden="true" />
        )}
        {markers.map((m) => {
          const v = m.version;
          const selected = v.seq === selectedSeq;
          const diff = diffSeqs.includes(v.seq);
          return (
            <button
              key={v.seq}
              type="button"
              className={`marker chg-${v.type} origin-${v.origin}${selected ? ' selected' : ''}${diff ? ' diff-end' : ''}`}
              style={{ left: `${m.left}%`, top: `${0.75 + m.lane * 1.25}rem` }}
              aria-pressed={selected}
              aria-label={`${v.type} (${v.origin}) at ${formatTime(v.observedAt)}, seq ${v.seq}${m.clamped ? ', outside the visible range' : ''}`}
              title={`${v.type} · ${v.origin} · ${formatTime(v.observedAt)} · seq ${v.seq}`}
              onClick={() => onSelect(v)}
            />
          );
        })}
      </div>
      <div className="timeline-axis" aria-hidden="true">
        {times.map((t) => (
          <span key={t} className="tick" style={{ left: `${percent(domain, t)}%` }}>
            {tickLabel(t, step)}
          </span>
        ))}
      </div>
      <figcaption className="legend">
        <span><i className="sw seg-observed" /> observed</span>
        <span><i className="sw seg-gap" /> gap</span>
        <span><i className="sw seg-unobserved" /> not yet watched</span>
        <span><i className="sw seg-expired" /> expired</span>
        <span><i className="dot chg-Added" /> Added</span>
        <span><i className="dot chg-Modified" /> Modified</span>
        <span><i className="dot chg-Deleted" /> Deleted</span>
        <span><i className="dot origin-baseline" /> baseline</span>
        <span><i className="dot origin-resync" /> resync (time inside gap unknown)</span>
      </figcaption>
    </figure>
  );
}
