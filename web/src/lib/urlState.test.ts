import { describe, expect, it } from 'vitest';
import { parseState, serializeState } from './urlState';

describe('URL state', () => {
  it('round-trips every field', () => {
    const s = {
      view: 'object' as const,
      resource: 'apps/v1/deployments',
      namespace: 'prod',
      name: 'api',
      at: '2026-11-24T14:00:00.123456789Z',
      labelSelector: 'app=api,tier!=db',
      includeDeleted: true,
      seq: 1042,
      diffA: '2026-11-24T13:00:00Z',
      diffB: '2026-11-24T14:00:00Z',
    };
    expect(parseState(serializeState(s))).toEqual(s);
  });
  it('defaults to the overview and omits empty values', () => {
    expect(serializeState({ view: 'overview' })).toBe('');
    expect(parseState('')).toMatchObject({ view: 'overview', at: undefined });
    expect(serializeState({ view: 'browse', resource: 'v1/pods', namespace: '', includeDeleted: false })).toBe('?view=browse&resource=v1%2Fpods');
  });
  it('ignores invalid values', () => {
    expect(parseState('?view=hack&seq=abc')).toMatchObject({ view: 'overview', seq: undefined });
  });
});
