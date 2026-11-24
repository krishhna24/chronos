import { useId, useState } from 'react';
import { client } from '../api/client';
import type { Version } from '../api/types';
import { ChangeBadge, Empty, ErrorBox, Loading } from '../components/common';
import { CoverageBanner } from '../components/CoverageBanner';
import { DiffView } from '../components/DiffView';
import { Link } from '../components/Link';
import { ObjectView } from '../components/ObjectView';
import { Timeline } from '../components/Timeline';
import { TimePicker } from '../components/TimePicker';
import { useChronos } from '../context';
import { compareTime, formatTime, toMs } from '../lib/time';
import type { Domain } from '../lib/timeline';
import { useAsync } from '../lib/useAsync';
import type { AppState } from '../lib/urlState';

type Zoom = 'all' | '1d' | '6h' | '1h' | 'around';
const ZOOMS: Array<[Zoom, string]> = [
  ['all', 'Whole history'],
  ['1d', 'Last day'],
  ['6h', 'Last 6 hours'],
  ['1h', 'Last hour'],
  ['around', '±30 min around selected time'],
];

export function ObjectDetail({ state, update }: { state: AppState; update: (p: Partial<AppState>, o?: { replace?: boolean }) => void }) {
  const { resources, resourceById, serverNow } = useChronos();
  const id = useId();
  const [zoom, setZoom] = useState<Zoom>('all');
  const res = resourceById(state.resource);
  const ready = !!(state.resource && state.name);
  const key = { resource: state.resource ?? '', namespace: state.namespace, name: state.name ?? '' };

  const [objState, reloadState] = useAsync(
    (signal) =>
      state.seq !== undefined ? client.object({ ...key, seq: state.seq }, signal) : client.state({ ...key, at: state.at }, signal),
    ready ? JSON.stringify([key, state.at, state.seq]) : null,
  );
  const [history, reloadHistory] = useAsync((signal) => client.history(key, signal), ready ? JSON.stringify(key) : null);

  const [from, to] =
    state.diffA && state.diffB ? (compareTime(state.diffA, state.diffB) <= 0 ? [state.diffA, state.diffB] : [state.diffB, state.diffA]) : [];
  const diffable = from !== undefined && to !== undefined && compareTime(from, to) < 0;
  const [diff, reloadDiff] = useAsync(
    (signal) => client.diff({ ...key, from: from!, to: to! }, signal),
    ready && diffable ? JSON.stringify([key, from, to]) : null,
  );

  if (!ready) return <Empty>No object selected. Pick one from the <Link to={{ view: 'browse' }}>resource browser</Link>.</Empty>;

  const horizon = resources.status === 'success' ? resources.data.horizon : undefined;
  const title = `${res?.kind ?? state.resource} ${state.namespace ? `${state.namespace}/` : ''}${state.name}`;
  const now = serverNow();
  const cursor = state.at ? toMs(state.at) : now;

  const domainFor = (versions: readonly Version[], coverageStart: number | undefined): Domain => {
    const firstKnown = Math.min(
      horizon ? toMs(horizon) : Number.POSITIVE_INFINITY,
      coverageStart ?? Number.POSITIVE_INFINITY,
      ...versions.map((v) => toMs(v.observedAt)),
    );
    const all = { start: Number.isFinite(firstKnown) ? firstKnown : now - 3_600_000, end: now };
    switch (zoom) {
      case 'all':
        return all;
      case '1d':
        return { start: Math.max(all.start, now - 86_400_000), end: now };
      case '6h':
        return { start: Math.max(all.start, now - 6 * 3_600_000), end: now };
      case '1h':
        return { start: Math.max(all.start, now - 3_600_000), end: now };
      case 'around':
        return { start: cursor - 30 * 60_000, end: Math.min(now, cursor + 30 * 60_000) };
    }
  };

  const selectVersion = (v: Version) => update({ seq: v.seq });
  const versionAt = (t: string | undefined) =>
    t && history.status === 'success' ? history.data.versions.find((v) => v.observedAt === t)?.seq : undefined;

  return (
    <section aria-labelledby={`${id}-title`}>
      <div className="section-head">
        <h2 id={`${id}-title`}>{title}</h2>
        {res?.kind === 'Deployment' && (
          <Link to={{ view: 'relations', resource: state.resource, namespace: state.namespace, name: state.name, at: state.at }}>
            ReplicaSets &amp; Pods at this time →
          </Link>
        )}
      </div>
      <div className="controls">
        <TimePicker at={state.at} now={serverNow} onChange={(at) => update({ at, seq: undefined })} />
      </div>

      <div className="grid-2">
        <section aria-labelledby={`${id}-state`} className="panel">
          <div className="section-head">
            <h3 id={`${id}-state`}>
              {state.seq !== undefined ? `Version seq ${state.seq}` : 'State at selected time'}
            </h3>
            {state.seq !== undefined && (
              <button type="button" onClick={() => update({ seq: undefined })}>
                Back to state at selected time
              </button>
            )}
          </div>
          {objState.status === 'loading' || objState.status === 'idle' ? (
            <Loading />
          ) : objState.status === 'error' ? (
            <ErrorBox error={objState.error} onRetry={reloadState} />
          ) : (
            <>
              <CoverageBanner coverage={objState.data.coverage} subject={res ? `${res.kind} objects` : undefined} horizon={horizon} />
              <ObjectView state={objState.data} resource={res} record={state.seq !== undefined} />
            </>
          )}
        </section>

        <section aria-labelledby={`${id}-hist`} className="panel">
          <div className="section-head">
            <h3 id={`${id}-hist`}>History</h3>
            <label>
              <span className="sr-only">Timeline range</span>
              <select value={zoom} onChange={(e) => setZoom(e.target.value as Zoom)}>
                {ZOOMS.map(([z, label]) => (
                  <option key={z} value={z}>
                    {label}
                  </option>
                ))}
              </select>
            </label>
          </div>
          {history.status === 'loading' || history.status === 'idle' ? (
            <Loading label="Loading history…" />
          ) : history.status === 'error' ? (
            <ErrorBox error={history.error} onRetry={reloadHistory} />
          ) : (
            <>
              <Timeline
                domain={domainFor(history.data.versions, history.data.coverage[0] ? toMs(history.data.coverage[0].start) : undefined)}
                versions={history.data.versions}
                coverage={history.data.coverage}
                horizon={horizon}
                now={now}
                cursor={cursor}
                selectedSeq={state.seq}
                diffSeqs={[versionAt(state.diffA), versionAt(state.diffB)].filter((s): s is number => s !== undefined)}
                onSelect={selectVersion}
              />
              {history.data.truncated && (
                <p className="note">History is truncated to the first {history.data.versions.length} versions.</p>
              )}
              {history.data.versions.length === 0 ? (
                <Empty>
                  Chronos has no records of this object in the retention window. Either it never existed while Chronos was watching, or
                  the name is wrong.
                </Empty>
              ) : (
                <VersionTable
                  versions={history.data.versions}
                  selectedSeq={state.seq}
                  diffA={state.diffA}
                  diffB={state.diffB}
                  onInspect={selectVersion}
                  onDiff={(a, b) => update({ diffA: a, diffB: b })}
                />
              )}
            </>
          )}
        </section>
      </div>

      <section aria-labelledby={`${id}-diff`} className="panel">
        <div className="section-head">
          <h3 id={`${id}-diff`}>Diff</h3>
          {(state.diffA || state.diffB) && (
            <button type="button" onClick={() => update({ diffA: undefined, diffB: undefined })}>
              Clear diff
            </button>
          )}
        </div>
        {!state.diffA || !state.diffB ? (
          <Empty>
            Choose two points: use <strong>A</strong> and <strong>B</strong> in the version list, or “vs previous”.
            {state.diffA && <> A is set to {formatTime(state.diffA)}.</>}
            {state.diffB && <> B is set to {formatTime(state.diffB)}.</>}
            {(state.diffA || state.at) && (
              <>
                {' '}
                <button type="button" onClick={() => update(state.diffA ? { diffB: state.at ?? new Date(now).toISOString() } : { diffA: state.at })}>
                  Use selected time as {state.diffA ? 'B' : 'A'}
                </button>
              </>
            )}
          </Empty>
        ) : !diffable ? (
          <Empty>A and B are the same instant; choose two different points.</Empty>
        ) : (
          <DiffView state={diff} onReload={reloadDiff} />
        )}
      </section>
    </section>
  );
}

function VersionTable({
  versions,
  selectedSeq,
  diffA,
  diffB,
  onInspect,
  onDiff,
}: {
  versions: readonly Version[];
  selectedSeq?: number;
  diffA?: string;
  diffB?: string;
  onInspect: (v: Version) => void;
  onDiff: (a: string | undefined, b: string | undefined) => void;
}) {
  const rows = [...versions].sort((a, b) => b.seq - a.seq);
  const asc = [...versions].sort((a, b) => a.seq - b.seq);
  return (
    <div className="table-wrap versions">
      <table>
        <thead>
          <tr>
            <th scope="col">Seq</th>
            <th scope="col">Observed</th>
            <th scope="col">Change</th>
            <th scope="col">UID</th>
            <th scope="col">Actions</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((v) => {
            const idx = asc.indexOf(v);
            const prev = idx > 0 ? asc[idx - 1] : undefined;
            const uidChanged = prev !== undefined && prev.uid !== v.uid;
            return (
              <tr key={v.seq} className={v.seq === selectedSeq ? 'selected' : undefined}>
                <td>{v.seq}</td>
                <td>
                  <time dateTime={v.observedAt}>{formatTime(v.observedAt)}</time>
                </td>
                <td>
                  <ChangeBadge type={v.type} origin={v.origin} />
                </td>
                <td>
                  <code title={v.uid}>{v.uid.slice(0, 8)}</code>
                  {uidChanged && <span className="badge badge-recreated" title="Different UID from the previous version: a new object with the same name">new instance</span>}
                </td>
                <td className="actions">
                  <button type="button" aria-pressed={v.seq === selectedSeq} onClick={() => onInspect(v)} aria-label={`Inspect seq ${v.seq}`}>
                    Inspect
                  </button>
                  <button type="button" aria-pressed={diffA === v.observedAt} onClick={() => onDiff(v.observedAt, diffB)} aria-label={`Set seq ${v.seq} as diff point A`}>
                    A
                  </button>
                  <button type="button" aria-pressed={diffB === v.observedAt} onClick={() => onDiff(diffA, v.observedAt)} aria-label={`Set seq ${v.seq} as diff point B`}>
                    B
                  </button>
                  {prev && (
                    <button type="button" onClick={() => onDiff(prev.observedAt, v.observedAt)} aria-label={`Diff seq ${v.seq} against the previous version`}>
                      vs previous
                    </button>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
