import type { DiffResponse, ObjectState } from '../api/types';
import { classifyDiff, diffStat } from '../lib/diff';
import { formatTime } from '../lib/time';
import type { AsyncState } from '../lib/useAsync';
import { CoverageBadge, Empty, ErrorBox, ExistenceBadge, Loading } from './common';

function Side({ label, s }: { label: string; s: ObjectState }) {
  return (
    <div className="diff-side">
      <span className="diff-side-label">{label}</span> <time dateTime={s.at}>{formatTime(s.at)}</time>{' '}
      <ExistenceBadge existence={s.existence} />
      {s.coverage.state !== 'observed' && <CoverageBadge state={s.coverage.state} />}
      {s.version && <span className="muted"> uid {s.version.uid.slice(0, 8)}</span>}
    </div>
  );
}

export function DiffView({ state, onReload }: { state: AsyncState<DiffResponse>; onReload: () => void }) {
  if (state.status === 'idle') return null;
  if (state.status === 'loading') return <Loading label="Computing diff…" />;
  if (state.status === 'error') return <ErrorBox error={state.error} onRetry={onReload} />;
  const d = state.data;
  const lines = classifyDiff(d.diff);
  const { added, removed } = diffStat(lines);
  const notPresent = [d.from, d.to].filter((s) => s.existence !== 'present');

  return (
    <div className="diff">
      <div className="diff-sides">
        <Side label="From" s={d.from} />
        <Side label="To" s={d.to} />
      </div>
      {!d.complete && (
        <section className="banner banner-gap" role="status" aria-label="Incomplete diff">
          <p className="banner-title">
            <span aria-hidden="true">⚠ </span>Incomplete: Chronos was not watching for part of this range
          </p>
          <p className="banner-detail">
            Changes inside the unobserved part are unknown. Intermediate states may be missing
            {d.diff === '' ? ', and an empty diff does not prove nothing changed' : ''}.
          </p>
        </section>
      )}
      {d.recreated && (
        <section className="banner banner-recreated" role="status" aria-label="Object recreated">
          <p className="banner-title">
            <span aria-hidden="true">↻ </span>Deleted and recreated in between
          </p>
          <p className="banner-detail">
            These are two different objects with the same name (UID {d.from.version?.uid ?? '?'} → {d.to.version?.uid ?? '?'}).
          </p>
        </section>
      )}
      {notPresent.length > 0 && (
        <p className="note">
          {notPresent.map((s) => (s === d.from ? 'The “from”' : 'The “to”')).join(' and ')} side did not exist or is unknown
          and is rendered as empty.
        </p>
      )}
      {lines.length === 0 ? (
        <Empty>{d.complete ? 'No changes between these two points.' : 'No differences between the two states shown.'}</Empty>
      ) : (
        <>
          <p className="diff-stat">
            <span className="add">+{added}</span> <span className="del">−{removed}</span>
          </p>
          <pre className="diff-body" tabIndex={0} aria-label="Unified diff">
            {lines.map((l, i) => (
              <span key={i} className={`dl dl-${l.kind}`}>
                {l.text}
                {'\n'}
              </span>
            ))}
          </pre>
        </>
      )}
    </div>
  );
}
