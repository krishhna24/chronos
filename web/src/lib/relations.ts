// Builds Deployment -> ReplicaSet -> Pod trees from metadata.ownerReferences,
// using list results taken at the same instant. Ownership is matched by UID
// (not name): a ReplicaSet owned by a deleted-and-recreated Deployment of the
// same name belongs to the old instance, not the new one.

import type { ObjectState } from '../api/types';

export interface OwnerRef {
  kind: string;
  name: string;
  uid: string;
  controller: boolean;
}

interface Meta {
  name?: string;
  namespace?: string;
  uid?: string;
  ownerReferences?: Array<{ kind?: unknown; name?: unknown; uid?: unknown; controller?: unknown }>;
}

export function meta(o: ObjectState): Meta {
  const m = (o.object as { metadata?: unknown } | undefined)?.metadata;
  return typeof m === 'object' && m !== null ? (m as Meta) : {};
}

/** UID of the object instance: from metadata, else from its version. */
export function uidOf(o: ObjectState): string | undefined {
  return meta(o).uid || o.version?.uid || undefined;
}

export function ownerRefs(o: ObjectState): OwnerRef[] {
  const refs = meta(o).ownerReferences;
  if (!Array.isArray(refs)) return [];
  return refs
    .filter((r) => typeof r?.uid === 'string' && typeof r.kind === 'string')
    .map((r) => ({
      kind: r.kind as string,
      name: typeof r.name === 'string' ? r.name : '',
      uid: r.uid as string,
      controller: r.controller === true,
    }));
}

/** The owner reference that controls the object, falling back to the only
 * reference of that kind when none is marked controller. */
export function controllerOf(o: ObjectState, kind: string): OwnerRef | undefined {
  const refs = ownerRefs(o).filter((r) => r.kind === kind);
  return refs.find((r) => r.controller) ?? (refs.length === 1 ? refs[0] : undefined);
}

export interface ReplicaSetNode {
  rs: ObjectState;
  pods: ObjectState[];
}

export interface DeploymentTree {
  deployment: ObjectState;
  replicaSets: ReplicaSetNode[];
  /** ReplicaSets that name this Deployment as owner but carry a different
   * UID: they belonged to an earlier instance with the same name. */
  formerReplicaSets: ObjectState[];
}

const byName = (a: ObjectState, b: ObjectState) => a.key.name.localeCompare(b.key.name);

export function buildDeploymentTree(
  deployment: ObjectState,
  replicaSets: readonly ObjectState[],
  pods: readonly ObjectState[],
): DeploymentTree {
  const depUid = uidOf(deployment);
  const depName = deployment.key.name;
  const podsByOwner = new Map<string, ObjectState[]>();
  for (const p of pods) {
    const owner = controllerOf(p, 'ReplicaSet');
    if (!owner) continue;
    const list = podsByOwner.get(owner.uid) ?? [];
    list.push(p);
    podsByOwner.set(owner.uid, list);
  }

  const nodes: ReplicaSetNode[] = [];
  const former: ObjectState[] = [];
  for (const rs of replicaSets) {
    const owner = controllerOf(rs, 'Deployment');
    if (!owner) continue;
    if (depUid && owner.uid === depUid) {
      const rsUid = uidOf(rs);
      nodes.push({ rs, pods: (rsUid ? podsByOwner.get(rsUid) ?? [] : []).sort(byName) });
    } else if (owner.name === depName) {
      former.push(rs);
    }
  }
  // Newest ReplicaSet first (by creationTimestamp, then name).
  const created = (o: ObjectState) =>
    String((o.object as { metadata?: { creationTimestamp?: unknown } } | undefined)?.metadata?.creationTimestamp ?? '');
  nodes.sort((a, b) => created(b.rs).localeCompare(created(a.rs)) || byName(a.rs, b.rs));
  return { deployment, replicaSets: nodes, formerReplicaSets: former.sort(byName) };
}
