import { describe, expect, it } from 'vitest';
import type { ObjectState } from '../api/types';
import { buildDeploymentTree, controllerOf, ownerRefs, uidOf } from './relations';

type Ref = { kind: string; name: string; uid: string; controller?: boolean };

function obj(resource: string, name: string, uid: string, owners: Ref[] = [], extra: Record<string, unknown> = {}): ObjectState {
  return {
    key: { resource, namespace: 'prod', name },
    at: '2026-11-24T14:00:00Z',
    existence: 'present',
    coverage: { state: 'observed' },
    object: { metadata: { name, namespace: 'prod', uid, ownerReferences: owners, ...extra } },
  };
}

const dep = obj('apps/v1/deployments', 'api', 'dep-2');
const rsNew = obj('apps/v1/replicasets', 'api-new', 'rs-new', [{ kind: 'Deployment', name: 'api', uid: 'dep-2', controller: true }], {
  creationTimestamp: '2026-11-24T13:00:00Z',
});
const rsOld = obj('apps/v1/replicasets', 'api-old', 'rs-old', [{ kind: 'Deployment', name: 'api', uid: 'dep-2', controller: true }], {
  creationTimestamp: '2026-11-20T00:00:00Z',
});
// Owned by an earlier Deployment instance named "api" (deleted, recreated).
const rsFormer = obj('apps/v1/replicasets', 'api-former', 'rs-former', [{ kind: 'Deployment', name: 'api', uid: 'dep-1', controller: true }]);
const rsOther = obj('apps/v1/replicasets', 'worker-1', 'rs-w', [{ kind: 'Deployment', name: 'worker', uid: 'dep-w', controller: true }]);
const pod = (name: string, rsUid: string, rsName: string) =>
  obj('v1/pods', name, `pod-${name}`, [{ kind: 'ReplicaSet', name: rsName, uid: rsUid, controller: true }]);

describe('buildDeploymentTree', () => {
  const pods = [pod('api-new-b', 'rs-new', 'api-new'), pod('api-new-a', 'rs-new', 'api-new'), pod('api-old-a', 'rs-old', 'api-old'), pod('w-a', 'rs-w', 'worker-1')];
  const tree = buildDeploymentTree(dep, [rsOld, rsFormer, rsOther, rsNew], pods);

  it('links ReplicaSets to the Deployment by UID, newest first', () => {
    expect(tree.replicaSets.map((n) => n.rs.key.name)).toEqual(['api-new', 'api-old']);
  });
  it('links Pods to their ReplicaSet, sorted by name', () => {
    expect(tree.replicaSets[0]!.pods.map((p) => p.key.name)).toEqual(['api-new-a', 'api-new-b']);
    expect(tree.replicaSets[1]!.pods.map((p) => p.key.name)).toEqual(['api-old-a']);
  });
  it('does not attach ReplicaSets of a previous instance with the same name', () => {
    expect(tree.formerReplicaSets.map((r) => r.key.name)).toEqual(['api-former']);
  });
  it('ignores unrelated objects', () => {
    const names = tree.replicaSets.flatMap((n) => [n.rs.key.name, ...n.pods.map((p) => p.key.name)]);
    expect(names).not.toContain('worker-1');
    expect(names).not.toContain('w-a');
  });
  it('matches pods by owner UID even when names collide', () => {
    const impostor = pod('api-new-x', 'rs-something-else', 'api-new');
    const t = buildDeploymentTree(dep, [rsNew], [impostor]);
    expect(t.replicaSets[0]!.pods).toEqual([]);
  });
  it('returns an empty tree for a Deployment without known UID', () => {
    const noUid: ObjectState = { ...dep, object: { metadata: { name: 'api' } }, version: undefined };
    expect(buildDeploymentTree(noUid, [rsNew], []).replicaSets).toEqual([]);
  });
});

describe('owner reference helpers', () => {
  it('prefers the controller reference', () => {
    const o = obj('v1/pods', 'p', 'u', [
      { kind: 'ReplicaSet', name: 'a', uid: 'a' },
      { kind: 'ReplicaSet', name: 'b', uid: 'b', controller: true },
    ]);
    expect(controllerOf(o, 'ReplicaSet')?.uid).toBe('b');
  });
  it('falls back to a sole non-controller reference, not to an ambiguous one', () => {
    expect(controllerOf(obj('v1/pods', 'p', 'u', [{ kind: 'ReplicaSet', name: 'a', uid: 'a' }]), 'ReplicaSet')?.uid).toBe('a');
    const two = obj('v1/pods', 'p', 'u', [
      { kind: 'ReplicaSet', name: 'a', uid: 'a' },
      { kind: 'ReplicaSet', name: 'b', uid: 'b' },
    ]);
    expect(controllerOf(two, 'ReplicaSet')).toBeUndefined();
  });
  it('tolerates missing or malformed metadata', () => {
    const bare: ObjectState = { key: { resource: 'v1/pods', name: 'x' }, at: '', existence: 'unknown', coverage: { state: 'gap' } };
    expect(ownerRefs(bare)).toEqual([]);
    expect(uidOf(bare)).toBeUndefined();
    const bad = { ...bare, object: { metadata: { ownerReferences: [{ kind: 'ReplicaSet' }, 'junk', null] } } } as unknown as ObjectState;
    expect(ownerRefs(bad)).toEqual([]);
  });
  it('uidOf falls back to the version UID', () => {
    const s: ObjectState = {
      key: { resource: 'v1/pods', name: 'x' },
      at: '',
      existence: 'absent',
      coverage: { state: 'observed' },
      version: { seq: 1, observedAt: '', type: 'Deleted', origin: 'watch', uid: 'from-version', resourceVersion: '1' },
    };
    expect(uidOf(s)).toBe('from-version');
  });
});
