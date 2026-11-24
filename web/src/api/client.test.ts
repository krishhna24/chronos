import { describe, expect, it, vi } from 'vitest';
import { ApiError, buildUrl, Client, isAbortError } from './client';

function respond(status: number, body: string, statusText = '') {
  return vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) => new Response(body, { status, statusText }));
}

describe('buildUrl', () => {
  it('drops empty parameters so server defaults apply', () => {
    expect(
      buildUrl('api/v1', 'list', { resource: 'v1/pods', namespace: '', at: undefined, includeDeleted: false, labelSelector: 'app=api,tier!=db' }),
    ).toBe('api/v1/list?resource=v1%2Fpods&labelSelector=app%3Dapi%2Ctier%21%3Ddb');
  });
  it('keeps the base relative and strips trailing slashes', () => {
    expect(buildUrl('api/v1/', 'resources')).toBe('api/v1/resources');
    expect(buildUrl('api/v1', 'list', { includeDeleted: true, limit: 0 })).toBe('api/v1/list?includeDeleted=true&limit=0');
  });
  it('passes nanosecond times through verbatim', () => {
    expect(buildUrl('api/v1', 'state', { at: '2026-11-24T14:00:00.123456789Z' })).toBe(
      'api/v1/state?at=2026-11-24T14%3A00%3A00.123456789Z',
    );
  });
});

describe('Client', () => {
  it('uses a relative base URL by default', async () => {
    const fetch = respond(200, '{"now":"2026-11-24T14:00:00Z","resources":[]}');
    await new Client({ fetch }).resources();
    expect(fetch).toHaveBeenCalledWith('api/v1/resources', expect.anything());
  });

  it('returns parsed JSON on success', async () => {
    const fetch = respond(200, '{"at":"2026-11-24T14:00:00Z","coverage":{"state":"gap"},"items":[]}');
    const res = await new Client({ baseUrl: 'api/v1', fetch }).list({ resource: 'v1/pods', namespace: 'prod' });
    expect(res.coverage.state).toBe('gap');
    expect(fetch.mock.calls[0]![0]).toBe('api/v1/list?resource=v1%2Fpods&namespace=prod');
  });

  it('maps an api.Error body to ApiError with code and message', async () => {
    const fetch = respond(400, '{"code":"bad_request","message":"at is in the future"}');
    const err = await new Client({ fetch }).state({ resource: 'v1/pods', name: 'x' }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect(err).toMatchObject({ status: 400, code: 'bad_request', message: 'at is in the future' });
  });

  it('maps 503 unavailable', async () => {
    const fetch = respond(503, '{"code":"unavailable","message":"store not open yet"}');
    await expect(new Client({ fetch }).resources()).rejects.toMatchObject({ status: 503, code: 'unavailable' });
  });

  it('handles non-JSON error bodies (e.g. from a proxy)', async () => {
    const fetch = respond(502, '<html>Bad Gateway</html>', 'Bad Gateway');
    await expect(new Client({ fetch }).resources()).rejects.toMatchObject({
      status: 502,
      code: 'http_502',
      message: 'HTTP 502 Bad Gateway: <html>Bad Gateway</html>',
    });
  });

  it('handles JSON error bodies that are not api.Error', async () => {
    const fetch = respond(403, '{"kind":"Status","message":"forbidden"}');
    await expect(new Client({ fetch }).resources()).rejects.toMatchObject({ status: 403, code: 'http_403' });
  });

  it('rejects a 2xx body that is not JSON', async () => {
    const fetch = respond(200, '<!doctype html><title>Chronos</title>');
    await expect(new Client({ fetch }).resources()).rejects.toMatchObject({ code: 'bad_response' });
  });

  it('reports network failures as ApiError code "network"', async () => {
    const fetch = vi.fn(async () => {
      throw new TypeError('Failed to fetch');
    });
    await expect(new Client({ fetch }).resources()).rejects.toMatchObject({ status: 0, code: 'network' });
  });

  it('propagates aborts unchanged so callers can ignore them', async () => {
    const ctl = new AbortController();
    const fetch = vi.fn(
      (_url: RequestInfo | URL, init?: RequestInit) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')));
        }),
    );
    const p = new Client({ fetch }).history({ resource: 'v1/pods', namespace: 'prod', name: 'x' }, ctl.signal);
    ctl.abort();
    const err = await p.catch((e: unknown) => e);
    expect(isAbortError(err)).toBe(true);
    expect(err).not.toBeInstanceOf(ApiError);
    expect(fetch.mock.calls[0]![1]?.signal).toBe(ctl.signal);
  });

  it('builds every endpoint path', async () => {
    const fetch = respond(200, '{}');
    const c = new Client({ fetch });
    await c.object({ resource: 'v1/pods', namespace: 'p', name: 'n', seq: 42 });
    await c.diff({ resource: 'v1/pods', namespace: 'p', name: 'n', from: 'A', to: 'B' });
    await c.changes({ from: 'A', to: 'B', limit: 10 });
    expect(fetch.mock.calls.map((c) => c[0])).toEqual([
      'api/v1/object?resource=v1%2Fpods&namespace=p&name=n&seq=42',
      'api/v1/diff?resource=v1%2Fpods&namespace=p&name=n&from=A&to=B',
      'api/v1/changes?from=A&to=B&limit=10',
    ]);
  });
});
