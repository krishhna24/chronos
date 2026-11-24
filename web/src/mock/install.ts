// Mock mode: intercepts fetch() calls to .../api/v1/* and answers them from
// fixture data in the browser, with a small latency so loading states are
// visible. Everything else goes to the real fetch.

import { buildFixture } from './fixtures';
import { createMockServer } from './server';

export function installMock(opts: { latencyMs?: number } = {}): void {
  const latency = opts.latencyMs ?? 250;
  const handle = createMockServer(buildFixture());
  const realFetch = globalThis.fetch.bind(globalThis);

  globalThis.fetch = (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const raw = input instanceof Request ? input.url : String(input);
    const url = new URL(raw, document.baseURI);
    if (!/\/api\/v1\//.test(url.pathname)) return realFetch(input, init);
    const signal = init?.signal ?? (input instanceof Request ? input.signal : undefined);
    return new Promise<Response>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const abort = () => {
        if (timer !== undefined) clearTimeout(timer);
        reject(new DOMException('The operation was aborted.', 'AbortError'));
      };
      if (signal?.aborted) return abort();
      timer = setTimeout(() => {
        signal?.removeEventListener('abort', abort);
        const { status, body } = handle(url);
        resolve(new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }));
      }, latency);
      signal?.addEventListener('abort', abort, { once: true });
    });
  };
  console.info('[chronos] mock mode: /api/v1 is served from in-browser fixtures');
}
