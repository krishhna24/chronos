// Typed client for the Chronos HTTP API.
//
// The base URL is relative ("api/v1", no leading slash) so that requests
// resolve against the page's own location. That keeps the dashboard working
// whether it is served at "/" (kubectl port-forward) or under the
// kube-apiserver service proxy prefix
// "/api/v1/namespaces/<ns>/services/<svc>:http/proxy/".

import type {
  ChangesResponse,
  DiffResponse,
  ErrorBody,
  HistoryResponse,
  ListResponse,
  ObjectState,
  ResourcesResponse,
  Time,
} from './types';

/** An API failure: a non-2xx response, an unreadable body, or no response. */
export class ApiError extends Error {
  /** HTTP status; 0 when no response was received. */
  readonly status: number;
  /** api.Error code (bad_request, not_found, internal, unavailable), or
   * "network" / "bad_response" / "http_<status>" for client-side failures. */
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
  }
}

/** True for the rejection produced by aborting a request. Callers that
 * cancel stale requests should ignore these rather than show an error. */
export function isAbortError(err: unknown): boolean {
  return (
    typeof err === 'object' &&
    err !== null &&
    'name' in err &&
    (err as { name: unknown }).name === 'AbortError'
  );
}

type Params = Record<string, string | number | boolean | undefined | null>;

export interface ClientOptions {
  /** Defaults to VITE_API_BASE or "api/v1" (relative to the page). */
  baseUrl?: string;
  /** Injectable for tests; defaults to the global fetch. */
  fetch?: typeof fetch;
}

export interface StateParams {
  resource: string;
  namespace?: string;
  name: string;
  at?: Time;
}

export interface ListParams {
  resource: string;
  namespace?: string;
  at?: Time;
  labelSelector?: string;
  includeDeleted?: boolean;
}

export interface DiffParams {
  resource: string;
  namespace?: string;
  name: string;
  from: Time;
  to: Time;
}

export interface HistoryParams {
  resource: string;
  namespace?: string;
  name: string;
  from?: Time;
  to?: Time;
  limit?: number;
}

export interface ChangesParams {
  from: Time;
  to: Time;
  resource?: string;
  namespace?: string;
  limit?: number;
}

export interface ObjectParams {
  resource: string;
  namespace?: string;
  name: string;
  seq: number;
}

/** Builds "<base>/<path>?<query>", dropping empty parameters so server
 * defaults (e.g. at = now) apply. */
export function buildUrl(baseUrl: string, path: string, params: Params = {}): string {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined || v === null || v === '' || v === false) continue;
    q.set(k, String(v));
  }
  const base = baseUrl.replace(/\/+$/, '');
  const qs = q.toString();
  return `${base}/${path}${qs ? `?${qs}` : ''}`;
}

function isErrorBody(v: unknown): v is ErrorBody {
  return (
    typeof v === 'object' &&
    v !== null &&
    typeof (v as ErrorBody).code === 'string' &&
    typeof (v as ErrorBody).message === 'string'
  );
}

export function defaultBaseUrl(): string {
  const env = import.meta.env.VITE_API_BASE;
  return typeof env === 'string' && env !== '' ? env : 'api/v1';
}

export class Client {
  private readonly baseUrl: string;
  private readonly fetchFn: typeof fetch;

  constructor(opts: ClientOptions = {}) {
    this.baseUrl = opts.baseUrl ?? defaultBaseUrl();
    // Bind lazily so a fetch interceptor installed later (mock mode) and
    // test stubs of globalThis.fetch are honoured.
    this.fetchFn = opts.fetch ?? ((input, init) => globalThis.fetch(input, init));
  }

  async get<T>(path: string, params: Params = {}, signal?: AbortSignal): Promise<T> {
    const url = buildUrl(this.baseUrl, path, params);
    let res: Response;
    try {
      res = await this.fetchFn(url, { signal, headers: { Accept: 'application/json' } });
    } catch (err) {
      if (isAbortError(err)) throw err;
      throw new ApiError(0, 'network', `Cannot reach the Chronos API: ${String((err as Error)?.message ?? err)}`);
    }
    let text: string;
    try {
      text = await res.text();
    } catch (err) {
      if (isAbortError(err)) throw err;
      throw new ApiError(res.status, 'bad_response', 'Failed to read the response body');
    }
    let body: unknown;
    try {
      body = text === '' ? undefined : JSON.parse(text);
    } catch {
      body = undefined;
    }
    if (!res.ok) {
      if (isErrorBody(body)) throw new ApiError(res.status, body.code, body.message);
      const snippet = text.trim().slice(0, 200);
      throw new ApiError(
        res.status,
        `http_${res.status}`,
        `HTTP ${res.status}${res.statusText ? ` ${res.statusText}` : ''}${snippet ? `: ${snippet}` : ''}`,
      );
    }
    if (body === undefined) {
      throw new ApiError(res.status, 'bad_response', 'The server returned a response that is not JSON');
    }
    return body as T;
  }

  resources(signal?: AbortSignal): Promise<ResourcesResponse> {
    return this.get('resources', {}, signal);
  }
  state(p: StateParams, signal?: AbortSignal): Promise<ObjectState> {
    return this.get('state', { ...p }, signal);
  }
  list(p: ListParams, signal?: AbortSignal): Promise<ListResponse> {
    return this.get('list', { ...p }, signal);
  }
  diff(p: DiffParams, signal?: AbortSignal): Promise<DiffResponse> {
    return this.get('diff', { ...p }, signal);
  }
  history(p: HistoryParams, signal?: AbortSignal): Promise<HistoryResponse> {
    return this.get('history', { ...p }, signal);
  }
  changes(p: ChangesParams, signal?: AbortSignal): Promise<ChangesResponse> {
    return this.get('changes', { ...p }, signal);
  }
  object(p: ObjectParams, signal?: AbortSignal): Promise<ObjectState> {
    return this.get('object', { ...p }, signal);
  }
}

export const client = new Client();
