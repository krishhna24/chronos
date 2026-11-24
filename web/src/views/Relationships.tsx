import { useEffect, useId, useState, type FormEvent } from 'react';
import { client } from '../api/client';
import type { ListResponse, ObjectState, Resource } from '../api/types';
import { CoverageBadge, Empty, ErrorBox, ExistenceBadge, Loading } from '../components/common';
import { CoverageBanner } from '../components/CoverageBanner';
import { Link } from '../components/Link';
import { TimePicker } from '../components/TimePicker';
import { useChronos } from '../context';
import { buildDeploymentTree, uidOf } from '../lib/relations';
import { formatTime } from '../lib/time';
import { useAsync } from '../lib/useAsync';
import type { AppState } from '../lib/urlState';

const DEPLOY = 'apps/v1/deployments';
const RS = 'apps/v1/replicasets';
const POD = 'v1/pods';

interface Snapshot {
  deployment: ObjectState;
  replicaSets: ListResponse;
  pods: ListResponse;
}

export function Relationships({ state, update }: { state: AppState; update: (p: Partial<AppState>, o?: { replace?: boolean }) => void }) {
  const { resources, resourceById, serverNow } = useChronos();
  const id = useId();
  const [ns, setNs] = useState(state.namespace ?? '');
  const [name, setName] = useState(state.name ?? '');
  useEffect(() => setNs(state.namespace ?? ''), [state.namespace]);
  useEffect(() => setName(state.name ?? ''), [state.name]);

  const ready = !!(state.namespace && state.name);
  const [snap, reload] = useAsync<Snapshot>(async (signal) => {
    // Pin one instant for all three queries so the tree is consistent even
    // when "now" is selected.
    const at = state.at ?? new Date(serverNow()).toISOString();
    const [deployment, replicaSets, pods] = await Promise.all([
      client.state({ resource: DEPLOY, namespace: state.namespace, name: state.name!, at }, signal),
      client.list({ resource: RS, namespace: state.namespace, at }, signal),
      client.list({ resource: POD, namespace: state.namespace, at }, signal),
    ]);
    return { deployment, replicaSets, pods };
  }, ready ? JSON.stringify([state.namespace, state.name, state.at]) : null);

  const submit = (e: FormEvent) => {
    e.preventDefault();
    update({ resource: DEPLOY, namespace: ns.trim() || undefined, name: name.trim() || undefined });
  };

  const horizon = resources.status === 'success' ? resources.data.horizon : undefined;

  return (
    <section aria-labelledby={`${id}-title`}>
      <h2 id={`${id}-title`}>Relationships: Deployment → ReplicaSets → Pods</h2>
      <div className="controls">
        <form className="filters" onSubmit={submit} aria-label="Deployment">
          <label htmlFor={`${id}-ns`}>Namespace</label>
          <input id={`${id}-ns`} value={ns} onChange={(e) => setNs(e.target.value)} required autoComplete="off" spellCheck={false} />
          <label htmlFor={`${id}-name`}>Deployment</label>
          <input id={`${id}-name`} value={name} onChange={(e) => setName(e.target.value)} required autoComplete="off" spellCheck={false} />
          <button type="submit">Show</button>
        </form>
        <TimePicker at={state.at} now={serverNow} onChange={(at) => update({ at })} />
      </div>
      {!ready ? (
        <Empty>Enter a namespace and Deployment name, or open one from the resource browser.</Empty>
      ) : snap.status === 'loading' || snap.status === 'idle' ? (
        <Loading label="Loading Deployment, ReplicaSets and Pods…" />
      ) : snap.status === 'error' ? (
        <ErrorBox error={snap.error} onRetry={reload} />
      ) : (
        <Tree snap={snap.data} horizon={horizon} rsType={resourceById(RS)} podType={resourceById(POD)} at={state.at} />
      )}
    </section>
  );
}

function NodeLabel({ o, kind, at }: { o: ObjectState; kind: string; at?: string }) {
  return (
    <>
      <span className="kind">{kind}</span>{' '}
      <Link to={{ view: 'object', resource: o.key.resource, namespace: o.key.namespace, name: o.key.name, at }}>{o.key.name}</Link>{' '}
      <ExistenceBadge existence={o.existence} /> <span className="muted">uid {uidOf(o)?.slice(0, 8) ?? '?'}</span>
    </>
  );
}

function replicas(o: ObjectState): string | null {
  const spec = (o.object as { spec?: { replicas?: unknown } } | undefined)?.spec;
  return typeof spec?.replicas === 'number' ? `${spec.replicas} desired` : null;
}

function phase(o: ObjectState): string | null {
  const p = (o.object as { status?: { phase?: unknown } } | undefined)?.status?.phase;
  return typeof p === 'string' ? p : null;
}

function Tree({ snap, horizon, rsType, podType, at }: { snap: Snapshot; horizon?: string; rsType?: Resource; podType?: Resource; at?: string }) {
  const d = snap.deployment;
  const banners = (
    <>
      <CoverageBanner coverage={d.coverage} subject="Deployments" horizon={horizon} />
      <CoverageBanner coverage={snap.replicaSets.coverage} subject="ReplicaSets" horizon={horizon} />
      <CoverageBanner coverage={snap.pods.coverage} subject="Pods" horizon={horizon} />
    </>
  );
  if (!d.object) {
    return (
      <>
        {banners}
        <Empty>
          {d.existence === 'absent'
            ? `Deployment ${d.key.namespace}/${d.key.name} did not exist at ${formatTime(d.at)}.`
            : `Chronos does not know whether Deployment ${d.key.namespace}/${d.key.name} existed at ${formatTime(d.at)}.`}
        </Empty>
      </>
    );
  }
  const tree = buildDeploymentTree(d, snap.replicaSets.items, snap.pods.items);
  return (
    <>
      {banners}
      <p className="note">
        Snapshot at <time dateTime={d.at}>{formatTime(d.at)}</time>. Coverage: Deployments <CoverageBadge state={d.coverage.state} />,{' '}
        {rsType?.kind ?? 'ReplicaSet'}s <CoverageBadge state={snap.replicaSets.coverage.state} />, {podType?.kind ?? 'Pod'}s{' '}
        <CoverageBadge state={snap.pods.coverage.state} />. Ownership follows <code>metadata.ownerReferences</code> by UID.
      </p>
      <ul className="tree" aria-label="Ownership tree">
        <li>
          <NodeLabel o={d} kind="Deployment" at={at} /> {replicas(d) && <span className="muted">· {replicas(d)}</span>}
          {tree.replicaSets.length === 0 ? (
            <ul>
              <li className="muted">No ReplicaSets owned by this Deployment at this time{snap.replicaSets.coverage.state !== 'observed' ? ' (or Chronos cannot know)' : ''}.</li>
            </ul>
          ) : (
            <ul>
              {tree.replicaSets.map((n) => (
                <li key={n.rs.key.name}>
                  <NodeLabel o={n.rs} kind="ReplicaSet" at={at} /> {replicas(n.rs) && <span className="muted">· {replicas(n.rs)}</span>}
                  <ul>
                    {n.pods.length === 0 ? (
                      <li className="muted">no pods</li>
                    ) : (
                      n.pods.map((p) => (
                        <li key={p.key.name}>
                          <NodeLabel o={p} kind="Pod" at={at} /> {phase(p) && <span className={`phase phase-${phase(p)}`}>{phase(p)}</span>}
                        </li>
                      ))
                    )}
                  </ul>
                </li>
              ))}
            </ul>
          )}
        </li>
      </ul>
      {tree.formerReplicaSets.length > 0 && (
        <details className="note">
          <summary>
            {tree.formerReplicaSets.length} ReplicaSet{tree.formerReplicaSets.length === 1 ? '' : 's'} owned by an earlier Deployment with this name
          </summary>
          <ul>
            {tree.formerReplicaSets.map((rs) => (
              <li key={rs.key.name}>
                <NodeLabel o={rs} kind="ReplicaSet" at={at} />
              </li>
            ))}
          </ul>
        </details>
      )}
    </>
  );
}
