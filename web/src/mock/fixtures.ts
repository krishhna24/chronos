// Realistic fixture history for mock mode, anchored to the current time so
// that "now" and the quick relative buttons are meaningful.
//
// Timeline (T0 = now - 3h; horizon = T0 - 30m):
//   horizon..T0      unobserved (Chronos not running yet)
//   T0               first LIST: baseline records for everything
//   T0+20m           rollout of prod/api 1.4.2 -> 1.5.0 (new ReplicaSet, pods)
//   T0+40m           prod/api scaled 3 -> 4
//   T0+50m           prod/worker deleted ...
//   T0+60m           ... and recreated with a new UID
//   T0+70m           configmap prod/feature-flags deleted
//   T0+80m..T0+95m   GAP: watch expired (410 Gone), relist required
//   T0+95m           relist (coverage resumes 30s later, when applied): resync records for everything that changed in
//                    the gap (api rolled to 1.5.1, api-config edited, pods
//                    replaced) -- exact times inside the gap are unknown
//   v1/events coverage starts later (T0+30m), so it is unobserved earlier.

import type { ChangeType, Interval, KubeObject, Origin, Resource } from '../api/types';

export interface FixtureRecord {
  seq: number;
  observedAt: string;
  resource: string;
  namespace: string;
  name: string;
  uid: string;
  resourceVersion: string;
  type: ChangeType;
  origin: Origin;
  object: KubeObject;
}

export interface Fixture {
  now: number;
  horizon: string;
  resources: Resource[];
  records: FixtureRecord[];
}

const MIN = 60_000;

/** RFC 3339 with nanoseconds, so mock responses exercise precision. */
export function nanoTime(ms: number, salt: number): string {
  const iso = new Date(ms).toISOString(); // ...sss Z
  const extra = String((salt * 7919) % 1_000_000).padStart(6, '0');
  return iso.slice(0, -1) + extra + 'Z';
}

type Spec = Omit<Resource, 'id' | 'coverage'>;

const TYPES: Spec[] = [
  { group: '', version: 'v1', resource: 'pods', kind: 'Pod', namespaced: true, metadataOnly: false, shortNames: ['po'] },
  { group: 'apps', version: 'v1', resource: 'deployments', kind: 'Deployment', namespaced: true, metadataOnly: false, shortNames: ['deploy'] },
  { group: 'apps', version: 'v1', resource: 'replicasets', kind: 'ReplicaSet', namespaced: true, metadataOnly: false, shortNames: ['rs'] },
  { group: '', version: 'v1', resource: 'services', kind: 'Service', namespaced: true, metadataOnly: false, shortNames: ['svc'] },
  { group: '', version: 'v1', resource: 'configmaps', kind: 'ConfigMap', namespaced: true, metadataOnly: false, shortNames: ['cm'] },
  { group: '', version: 'v1', resource: 'events', kind: 'Event', namespaced: true, metadataOnly: false, shortNames: ['ev'] },
  { group: '', version: 'v1', resource: 'secrets', kind: 'Secret', namespaced: true, metadataOnly: true },
];

const id = (t: Spec) => (t.group ? `${t.group}/${t.version}/${t.resource}` : `${t.version}/${t.resource}`);

export const POD = 'v1/pods';
export const DEPLOY = 'apps/v1/deployments';
export const RS = 'apps/v1/replicasets';

export function buildFixture(now: number = Date.now()): Fixture {
  const T0 = Math.floor((now - 180 * MIN) / 1000) * 1000;
  const at = (minutes: number, seconds = 0) => T0 + minutes * MIN + seconds * 1000;
  const gapStart = at(80);
  const gapEnd = at(95);
  const reason = 'watch expired (410 Gone); relist required';

  const coverageFor = (resourceId: string): Interval[] => [
    { start: nanoTime(resourceId === 'v1/events' ? at(30) : T0, 1), end: nanoTime(gapStart, 2), endReason: reason },
    // The relist's resync records are written first, then Start.
    { start: nanoTime(gapEnd + 30_000, 3) },
  ];
  const resources: Resource[] = TYPES.map((t) => ({ ...t, id: id(t), coverage: coverageFor(id(t)) }));

  const raw: Array<Omit<FixtureRecord, 'seq' | 'observedAt' | 'resourceVersion'> & { ms: number }> = [];
  const add = (
    ms: number,
    resource: string,
    namespace: string,
    name: string,
    uid: string,
    type: ChangeType,
    origin: Origin,
    object: KubeObject,
  ) => raw.push({ ms, resource, namespace, name, uid, type, origin, object });

  const created = (ms: number) => new Date(ms - 3 * 86_400_000).toISOString().replace(/\.\d+Z$/, 'Z');
  const ts = (ms: number) => new Date(ms).toISOString().replace(/\.\d+Z$/, 'Z');

  const deployment = (ns: string, name: string, uid: string, image: string, replicas: number, gen: number, createdMs: number): KubeObject => ({
    apiVersion: 'apps/v1',
    kind: 'Deployment',
    metadata: {
      name,
      namespace: ns,
      uid,
      generation: gen,
      creationTimestamp: ts(createdMs),
      labels: { app: name, team: 'platform' },
      annotations: { 'deployment.kubernetes.io/revision': String(gen) },
    },
    spec: {
      replicas,
      selector: { matchLabels: { app: name } },
      strategy: { type: 'RollingUpdate', rollingUpdate: { maxSurge: '25%', maxUnavailable: '25%' } },
      template: {
        metadata: { labels: { app: name } },
        spec: {
          containers: [
            {
              name,
              image,
              ports: [{ containerPort: 8080, protocol: 'TCP' }],
              env: [{ name: 'LOG_FORMAT', value: 'json' }],
              resources: { requests: { cpu: '100m', memory: '128Mi' } },
            },
          ],
        },
      },
    },
    status: { observedGeneration: gen, replicas, readyReplicas: replicas, availableReplicas: replicas },
  });

  const replicaSet = (ns: string, name: string, uid: string, owner: { name: string; uid: string }, image: string, replicas: number, createdMs: number): KubeObject => {
    const hash = name.slice(name.lastIndexOf('-') + 1);
    return {
      apiVersion: 'apps/v1',
      kind: 'ReplicaSet',
      metadata: {
        name,
        namespace: ns,
        uid,
        creationTimestamp: ts(createdMs),
        labels: { app: owner.name, 'pod-template-hash': hash },
        ownerReferences: [{ apiVersion: 'apps/v1', kind: 'Deployment', name: owner.name, uid: owner.uid, controller: true, blockOwnerDeletion: true }],
      },
      spec: {
        replicas,
        selector: { matchLabels: { app: owner.name, 'pod-template-hash': hash } },
        template: { metadata: { labels: { app: owner.name, 'pod-template-hash': hash } }, spec: { containers: [{ name: owner.name, image }] } },
      },
      status: { replicas, readyReplicas: replicas },
    };
  };

  const pod = (ns: string, name: string, uid: string, rs: { name: string; uid: string }, app: string, image: string, phase: string, createdMs: number, node: string): KubeObject => ({
    apiVersion: 'v1',
    kind: 'Pod',
    metadata: {
      name,
      namespace: ns,
      uid,
      creationTimestamp: ts(createdMs),
      labels: { app, 'pod-template-hash': rs.name.slice(rs.name.lastIndexOf('-') + 1) },
      ownerReferences: [{ apiVersion: 'apps/v1', kind: 'ReplicaSet', name: rs.name, uid: rs.uid, controller: true, blockOwnerDeletion: true }],
    },
    spec: { nodeName: node, containers: [{ name: app, image }], restartPolicy: 'Always' },
    status: {
      phase,
      conditions: [{ type: 'Ready', status: phase === 'Running' ? 'True' : 'False' }],
      ...(phase === 'Running' ? { podIP: `10.244.${uid.length % 4}.${(uid.charCodeAt(uid.length - 1) % 200) + 10}` } : {}),
    },
  });

  const nodes = ['node-a', 'node-b', 'node-c'];
  const uid = (s: string) => {
    let h = 0;
    for (const c of s) h = (h * 31 + c.charCodeAt(0)) >>> 0;
    const hex = h.toString(16).padStart(8, '0');
    return `${hex}-${hex.slice(0, 4)}-4${hex.slice(1, 4)}-a${hex.slice(2, 5)}-${(hex + hex).slice(0, 12)}`;
  };

  // A ReplicaSet with pods and their lifecycle.
  const rollout = (
    ns: string,
    dep: { name: string; uid: string },
    rsName: string,
    image: string,
    life: { added: number; origin: Origin; replicas: Array<[number, number, Origin]>; podNames: string[]; podsGone?: [number, Origin]; rsGone?: [number, Origin]; createdMs: number },
  ) => {
    const rsUid = uid(`${ns}/${rsName}`);
    const rsRef = { name: rsName, uid: rsUid };
    const first = life.replicas[0]?.[1] ?? 1;
    add(life.added, RS, ns, rsName, rsUid, 'Added', life.origin, replicaSet(ns, rsName, rsUid, dep, image, first, life.createdMs));
    for (const [t, n, origin] of life.replicas.slice(1)) {
      add(t, RS, ns, rsName, rsUid, 'Modified', origin, replicaSet(ns, rsName, rsUid, dep, image, n, life.createdMs));
    }
    if (life.rsGone) {
      add(life.rsGone[0], RS, ns, rsName, rsUid, 'Deleted', life.rsGone[1], replicaSet(ns, rsName, rsUid, dep, image, 0, life.createdMs));
    }
    life.podNames.forEach((p, i) => {
      const pUid = uid(`${ns}/${p}`);
      const node = nodes[i % nodes.length]!;
      const podCreated = life.origin === 'baseline' ? life.createdMs : life.added;
      const startedLater = life.origin === 'watch';
      const addedAt = life.added + (i + 1) * 1000;
      add(addedAt, POD, ns, p, pUid, 'Added', life.origin, pod(ns, p, pUid, rsRef, dep.name, image, startedLater ? 'Pending' : 'Running', podCreated, node));
      if (startedLater) {
        add(addedAt + 20_000, POD, ns, p, pUid, 'Modified', 'watch', pod(ns, p, pUid, rsRef, dep.name, image, 'Running', podCreated, node));
      }
      if (life.podsGone) {
        add(life.podsGone[0] + i * 1000, POD, ns, p, pUid, 'Deleted', life.podsGone[1], pod(ns, p, pUid, rsRef, dep.name, image, 'Running', podCreated, node));
      }
    });
  };

  // prod/api: rollout at T0+20m, scale at T0+40m, rolled again inside the gap.
  const api = { name: 'api', uid: uid('prod/api') };
  const apiCreated = created(T0);
  const apiCreatedMs = Date.parse(apiCreated);
  add(T0, DEPLOY, 'prod', 'api', api.uid, 'Added', 'baseline', deployment('prod', 'api', api.uid, 'registry.example.com/api:1.4.2', 3, 7, apiCreatedMs));
  add(at(20), DEPLOY, 'prod', 'api', api.uid, 'Modified', 'watch', deployment('prod', 'api', api.uid, 'registry.example.com/api:1.5.0', 3, 8, apiCreatedMs));
  add(at(40), DEPLOY, 'prod', 'api', api.uid, 'Modified', 'watch', deployment('prod', 'api', api.uid, 'registry.example.com/api:1.5.0', 4, 9, apiCreatedMs));
  add(gapEnd, DEPLOY, 'prod', 'api', api.uid, 'Modified', 'resync', deployment('prod', 'api', api.uid, 'registry.example.com/api:1.5.1', 4, 10, apiCreatedMs));

  rollout('prod', api, 'api-6d4f8b7c9', 'registry.example.com/api:1.4.2', {
    added: T0, origin: 'baseline', createdMs: apiCreatedMs,
    replicas: [[T0, 3, 'baseline'], [at(21, 30), 0, 'watch']],
    podNames: ['api-6d4f8b7c9-2xkqp', 'api-6d4f8b7c9-8hztw', 'api-6d4f8b7c9-lm4vn'],
    podsGone: [at(21, 40), 'watch'],
  });
  rollout('prod', api, 'api-7b9c5d6f8', 'registry.example.com/api:1.5.0', {
    added: at(20, 1), origin: 'watch', createdMs: at(20, 1),
    replicas: [[at(20, 1), 3, 'watch'], [at(40, 1), 4, 'watch'], [gapEnd, 0, 'resync']],
    podNames: ['api-7b9c5d6f8-a1b2c', 'api-7b9c5d6f8-d3e4f', 'api-7b9c5d6f8-g5h6i'],
    podsGone: [gapEnd, 'resync'],
  });
  // The fourth pod from the scale-up at T0+40m.
  {
    const rsRef = { name: 'api-7b9c5d6f8', uid: uid('prod/api-7b9c5d6f8') };
    const p = 'api-7b9c5d6f8-j7k8l';
    const pUid = uid(`prod/${p}`);
    add(at(40, 2), POD, 'prod', p, pUid, 'Added', 'watch', pod('prod', p, pUid, rsRef, 'api', 'registry.example.com/api:1.5.0', 'Pending', at(40, 2), 'node-a'));
    add(at(40, 25), POD, 'prod', p, pUid, 'Modified', 'watch', pod('prod', p, pUid, rsRef, 'api', 'registry.example.com/api:1.5.0', 'Running', at(40, 2), 'node-a'));
    add(gapEnd + 5000, POD, 'prod', p, pUid, 'Deleted', 'resync', pod('prod', p, pUid, rsRef, 'api', 'registry.example.com/api:1.5.0', 'Running', at(40, 2), 'node-a'));
  }
  rollout('prod', api, 'api-58f7c9d4b', 'registry.example.com/api:1.5.1', {
    added: gapEnd, origin: 'resync', createdMs: at(86),
    replicas: [[gapEnd, 4, 'resync']],
    podNames: ['api-58f7c9d4b-qz7xm', 'api-58f7c9d4b-r2d2a', 'api-58f7c9d4b-t9k3p', 'api-58f7c9d4b-v8n4w'],
  });

  // prod/worker: deleted at T0+50m and recreated (new UID) at T0+60m.
  const worker1 = { name: 'worker', uid: uid('prod/worker#1') };
  const worker2 = { name: 'worker', uid: uid('prod/worker#2') };
  add(T0, DEPLOY, 'prod', 'worker', worker1.uid, 'Added', 'baseline', deployment('prod', 'worker', worker1.uid, 'registry.example.com/worker:2.3.0', 1, 3, apiCreatedMs));
  add(at(50), DEPLOY, 'prod', 'worker', worker1.uid, 'Deleted', 'watch', deployment('prod', 'worker', worker1.uid, 'registry.example.com/worker:2.3.0', 1, 3, apiCreatedMs));
  rollout('prod', worker1, 'worker-5c8d9b6f7', 'registry.example.com/worker:2.3.0', {
    added: T0, origin: 'baseline', createdMs: apiCreatedMs,
    replicas: [[T0, 1, 'baseline']],
    podNames: ['worker-5c8d9b6f7-hh2kd'],
    podsGone: [at(50, 3), 'watch'],
    rsGone: [at(50, 2), 'watch'],
  });
  add(at(60), DEPLOY, 'prod', 'worker', worker2.uid, 'Added', 'watch', deployment('prod', 'worker', worker2.uid, 'registry.example.com/worker:2.4.0', 2, 1, at(60)));
  rollout('prod', worker2, 'worker-7f6e5d4c3', 'registry.example.com/worker:2.4.0', {
    added: at(60, 1), origin: 'watch', createdMs: at(60, 1),
    replicas: [[at(60, 1), 2, 'watch']],
    podNames: ['worker-7f6e5d4c3-mx9qa', 'worker-7f6e5d4c3-pp4lz'],
  });

  // staging/web: steady.
  const web = { name: 'web', uid: uid('staging/web') };
  add(T0, DEPLOY, 'staging', 'web', web.uid, 'Added', 'baseline', deployment('staging', 'web', web.uid, 'registry.example.com/web:0.9.1', 2, 2, apiCreatedMs));
  rollout('staging', web, 'web-84bd7c9f5', 'registry.example.com/web:0.9.1', {
    added: T0, origin: 'baseline', createdMs: apiCreatedMs,
    replicas: [[T0, 2, 'baseline']],
    podNames: ['web-84bd7c9f5-4tq8w', 'web-84bd7c9f5-zc6rn'],
  });

  // Services, ConfigMaps, Secret metadata.
  const svc = (ns: string, name: string, ip: string): KubeObject => ({
    apiVersion: 'v1',
    kind: 'Service',
    metadata: { name, namespace: ns, uid: uid(`${ns}/svc/${name}`), labels: { app: name }, creationTimestamp: apiCreated },
    spec: { type: 'ClusterIP', clusterIP: ip, selector: { app: name }, ports: [{ name: 'http', port: 80, targetPort: 8080, protocol: 'TCP' }] },
  });
  add(T0, 'v1/services', 'prod', 'api', uid('prod/svc/api'), 'Added', 'baseline', svc('prod', 'api', '10.96.12.40'));
  add(T0, 'v1/services', 'staging', 'web', uid('staging/svc/web'), 'Added', 'baseline', svc('staging', 'web', '10.96.80.7'));

  const cm = (ns: string, name: string, data: Record<string, string>): KubeObject => ({
    apiVersion: 'v1',
    kind: 'ConfigMap',
    metadata: { name, namespace: ns, uid: uid(`${ns}/cm/${name}`), creationTimestamp: apiCreated },
    data,
  });
  const apiConfig = { LOG_LEVEL: 'info', 'config.yaml': 'server:\n  port: 8080\n  timeout: 30s\n' };
  add(T0, 'v1/configmaps', 'prod', 'api-config', uid('prod/cm/api-config'), 'Added', 'baseline', cm('prod', 'api-config', apiConfig));
  add(gapEnd, 'v1/configmaps', 'prod', 'api-config', uid('prod/cm/api-config'), 'Modified', 'resync',
    cm('prod', 'api-config', { ...apiConfig, LOG_LEVEL: 'debug', 'config.yaml': 'server:\n  port: 8080\n  timeout: 60s\n' }));
  add(T0, 'v1/configmaps', 'prod', 'feature-flags', uid('prod/cm/feature-flags'), 'Added', 'baseline', cm('prod', 'feature-flags', { newCheckout: 'true' }));
  add(at(70), 'v1/configmaps', 'prod', 'feature-flags', uid('prod/cm/feature-flags'), 'Deleted', 'watch', cm('prod', 'feature-flags', { newCheckout: 'true' }));

  add(T0, 'v1/secrets', 'prod', 'api-tls', uid('prod/secret/api-tls'), 'Added', 'baseline', {
    apiVersion: 'v1',
    kind: 'Secret',
    metadata: { name: 'api-tls', namespace: 'prod', uid: uid('prod/secret/api-tls'), creationTimestamp: apiCreated, labels: { app: 'api' } },
  });

  // Events (coverage for events starts at T0+30m).
  const event = (ns: string, name: string, reason: string, message: string, obj: { kind: string; name: string }, ms: number): KubeObject => ({
    apiVersion: 'v1',
    kind: 'Event',
    metadata: { name, namespace: ns, uid: uid(`${ns}/ev/${name}`), creationTimestamp: ts(ms) },
    involvedObject: { kind: obj.kind, name: obj.name, namespace: ns },
    reason,
    message,
    type: 'Normal',
    count: 1,
  });
  const ev = (ms: number, name: string, reason: string, message: string, obj: { kind: string; name: string }) =>
    add(ms, 'v1/events', 'prod', name, uid(`prod/ev/${name}`), 'Added', ms === at(30) ? 'baseline' : 'watch', event('prod', name, reason, message, obj, ms));
  ev(at(30), 'api.181a2b3c4d5e6f70', 'ScalingReplicaSet', 'Scaled down replica set api-6d4f8b7c9 to 0 from 1', { kind: 'Deployment', name: 'api' });
  ev(at(40, 1), 'api.181a2b3c4d5e6f71', 'ScalingReplicaSet', 'Scaled up replica set api-7b9c5d6f8 to 4 from 3', { kind: 'Deployment', name: 'api' });
  ev(at(60, 1), 'worker.181a2b3c4d5e6f72', 'ScalingReplicaSet', 'Scaled up replica set worker-7f6e5d4c3 to 2 from 0', { kind: 'Deployment', name: 'worker' });

  raw.sort((a, b) => a.ms - b.ms);
  // The store guarantees strictly increasing ObservedAt with Seq.
  let last = -1;
  const records: FixtureRecord[] = raw.map((r, i) => {
    const ms = Math.max(r.ms, last + 1);
    last = ms;
    const { ms: _ignored, ...rest } = r;
    void _ignored;
    const rv = String(184_000 + i * 3);
    const object = structuredClone(rest.object);
    const md = (object.metadata ?? {}) as Record<string, unknown>;
    md.resourceVersion = rv;
    object.metadata = md;
    return { ...rest, object, seq: 1000 + i, observedAt: nanoTime(ms, i + 11), resourceVersion: rv };
  });

  return { now, horizon: nanoTime(T0 - 30 * MIN, 5), resources, records };
}
