import { describe, expect, it } from 'vitest';
import type { DiffResponse, HistoryResponse, ListResponse, ObjectState, ResourcesResponse } from '../api/types';
import { buildFixture, DEPLOY, POD } from './fixtures';
import { createMockServer } from './server';

// Fixed clock: T0 = NOW - 180m; gap from T0+80m to T0+95m(+30s).
const NOW = Date.UTC(2026, 10, 24, 15, 0, 0);
const T0 = NOW - 180 * 60_000;
const at = (min: number) => new Date(T0 + min * 60_000).toISOString();
const fx = buildFixture(NOW);
const handle = createMockServer(fx, () => NOW);
function get<T>(path: string, params: Record<string, string> = {}): { status: number; body: T } {
  const u = new URL(`http://x/api/v1/${path}`);
  for (const [k, v] of Object.entries(params)) u.searchParams.set(k, v);
  return handle(u) as { status: number; body: T };
}

describe('mock server fixtures', () => {
  it('lists resource types with a gap in their coverage', () => {
    const { body } = get<ResourcesResponse>('resources');
    const pods = body.resources.find((r) => r.id === POD)!;
    expect(pods.coverage).toHaveLength(2);
    expect(pods.coverage[0]!.endReason).toMatch(/410/);
    expect(body.horizon).toBeDefined();
  });

  it('classifies coverage: unobserved, observed, gap, expired', () => {
    const cov = (t: string) => get<ListResponse>('list', { resource: DEPLOY, at: t }).body.coverage.state;
    expect(cov(at(-10))).toBe('unobserved');
    expect(cov(at(10))).toBe('observed');
    expect(cov(at(85))).toBe('gap');
    expect(cov(at(-60))).toBe('expired');
    expect(cov(at(120))).toBe('observed');
  });

  it('returns last known state marked unknown during the gap', () => {
    const { body } = get<ListResponse>('list', { resource: DEPLOY, namespace: 'prod', at: at(85) });
    expect(body.items.length).toBeGreaterThan(0);
    expect(body.items.every((i) => i.existence === 'unknown' && i.object)).toBe(true);
    expect(body.coverage).toMatchObject({ state: 'gap', reason: expect.stringMatching(/410/) });
  });

  it('records a deletion and a recreation with a new UID', () => {
    const h = get<HistoryResponse>('history', { resource: DEPLOY, namespace: 'prod', name: 'worker' }).body;
    expect(h.versions.map((v) => v.type)).toEqual(['Added', 'Deleted', 'Added']);
    expect(h.versions[0]!.uid).toBe(h.versions[1]!.uid);
    expect(h.versions[2]!.uid).not.toBe(h.versions[0]!.uid);

    const between = get<ObjectState>('state', { resource: DEPLOY, namespace: 'prod', name: 'worker', at: at(55) }).body;
    expect(between.existence).toBe('absent');

    const d = get<DiffResponse>('diff', { resource: DEPLOY, namespace: 'prod', name: 'worker', from: at(10), to: at(65) }).body;
    expect(d.recreated).toBe(true);
    expect(d.complete).toBe(true);
    expect(d.diff).toMatch(/^--- /);
  });

  it('marks diffs across the gap incomplete and shows resync origin', () => {
    const d = get<DiffResponse>('diff', { resource: DEPLOY, namespace: 'prod', name: 'api', from: at(60), to: at(120) }).body;
    expect(d.complete).toBe(false);
    expect(d.diff).toMatch(/^\+\s+image: registry\.example\.com\/api:1\.5\.1$/m);
    const h = get<HistoryResponse>('history', { resource: DEPLOY, namespace: 'prod', name: 'api' }).body;
    expect(h.versions.at(-1)!.origin).toBe('resync');
  });

  it('supports includeDeleted and label selectors', () => {
    const plain = get<ListResponse>('list', { resource: 'v1/configmaps', namespace: 'prod', at: at(75) }).body;
    expect(plain.items.map((i) => i.key.name)).toEqual(['api-config']);
    const withDeleted = get<ListResponse>('list', { resource: 'v1/configmaps', namespace: 'prod', at: at(75), includeDeleted: 'true' }).body;
    expect(withDeleted.items.map((i) => [i.key.name, i.existence])).toEqual([
      ['api-config', 'present'],
      ['feature-flags', 'absent'],
    ]);
    const sel = get<ListResponse>('list', { resource: DEPLOY, at: at(10), labelSelector: 'app in (api,web)' }).body;
    expect(sel.items.map((i) => i.key.name)).toEqual(['api', 'web']);
  });

  it('opens an exact record by seq, preserving nanosecond times', () => {
    const h = get<HistoryResponse>('history', { resource: DEPLOY, namespace: 'prod', name: 'api' }).body;
    const v = h.versions[1]!;
    expect(v.observedAt).toMatch(/\.\d{9}Z$/);
    const o = get<ObjectState>('object', { resource: DEPLOY, namespace: 'prod', name: 'api', seq: String(v.seq) }).body;
    expect(o.version?.seq).toBe(v.seq);
    const s = get<ObjectState>('state', { resource: DEPLOY, namespace: 'prod', name: 'api', at: v.observedAt }).body;
    expect(s.version?.seq).toBe(v.seq);
  });

  it('rejects bad requests like the server', () => {
    expect(get('list', {}).status).toBe(400);
    expect(get('list', { resource: 'pods' }).body).toMatchObject({ code: 'bad_request' });
    expect(get('state', { resource: POD, name: 'x', at: new Date(NOW + 3_600_000).toISOString() }).status).toBe(400);
    expect(get('nope').status).toBe(404);
  });

  it('pages /list with continue tokens at one instant', () => {
    const paged = createMockServer(fx, () => NOW, { defaultListLimit: 3 });
    const call = (params: Record<string, string>) => {
      const u = new URL('http://x/api/v1/list');
      for (const [k, v] of Object.entries(params)) u.searchParams.set(k, v);
      return paged(u) as { status: number; body: ListResponse & { code?: string } };
    };
    const base = { resource: POD, at: at(120) };
    const unpaged = call({ ...base, limit: '5000' }).body.items.map((i) => `${i.key.namespace}/${i.key.name}`);
    expect(unpaged.length).toBeGreaterThan(3);
    const names: string[] = [];
    let res = call(base).body;
    let pages = 1;
    for (;;) {
      expect(res.items.length).toBeLessThanOrEqual(3);
      names.push(...res.items.map((i) => `${i.key.namespace}/${i.key.name}`));
      if (!res.continue) break;
      res = call({ ...base, continue: res.continue }).body;
      pages++;
    }
    expect(pages).toBe(Math.ceil(unpaged.length / 3));
    expect(names).toEqual(unpaged);

    const tok = call(base).body.continue!;
    expect(call({ resource: POD, continue: tok })).toMatchObject({ status: 400, body: { code: 'bad_request' } });
    expect(call({ resource: POD, at: at(121), continue: tok }).status).toBe(400);
    expect(call({ ...base, namespace: 'prod', continue: tok }).status).toBe(400);
    expect(call({ ...base, continue: 'garbage' }).status).toBe(400);
    for (const limit of ['0', '5001', 'x']) expect(call({ ...base, limit }).status).toBe(400);
  });

  it('never returns an object for a delete record', () => {
    const h = get<HistoryResponse>('history', { resource: DEPLOY, namespace: 'prod', name: 'worker' }).body;
    const del = h.versions.find((v) => v.type === 'Deleted')!;
    const o = get<ObjectState>('object', { resource: DEPLOY, namespace: 'prod', name: 'worker', seq: String(del.seq) }).body;
    expect(o.version?.type).toBe('Deleted');
    expect(o.object).toBeUndefined();
  });

  it('lists deleted keys as unknown, version only, during a gap', () => {
    const { body } = get<ListResponse>('list', { resource: 'v1/configmaps', namespace: 'prod', at: at(85), includeDeleted: 'true' });
    const ff = body.items.find((i) => i.key.name === 'feature-flags')!;
    expect(ff).toMatchObject({ existence: 'unknown', version: { type: 'Deleted' } });
    expect(ff.object).toBeUndefined();
  });
});
