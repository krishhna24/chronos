// Application state kept in the URL query string, so every view is a
// shareable link. Times are absolute RFC 3339; an absent `at` means "now"
// (the server's clock), which is the only relative value ever shared.

import { useCallback, useEffect, useState } from 'react';

export type View = 'overview' | 'browse' | 'object' | 'relations';
const VIEWS: readonly View[] = ['overview', 'browse', 'object', 'relations'];

export interface AppState {
  view: View;
  /** Canonical resource ID, e.g. "apps/v1/deployments". */
  resource?: string;
  namespace?: string;
  name?: string;
  /** Selected instant (RFC 3339); undefined = now. */
  at?: string;
  labelSelector?: string;
  includeDeleted?: boolean;
  /** History record being inspected in the object view. */
  seq?: number;
  /** Diff endpoints (RFC 3339), in either order. */
  diffA?: string;
  diffB?: string;
}

const KEYS = {
  view: 'view',
  resource: 'resource',
  namespace: 'ns',
  name: 'name',
  at: 'at',
  labelSelector: 'selector',
  includeDeleted: 'deleted',
  seq: 'seq',
  diffA: 'a',
  diffB: 'b',
} as const satisfies Record<keyof AppState, string>;

export function parseState(search: string): AppState {
  const q = new URLSearchParams(search);
  const str = (k: string) => {
    const v = q.get(k);
    return v === null || v === '' ? undefined : v;
  };
  const view = str(KEYS.view) as View | undefined;
  const seqRaw = str(KEYS.seq);
  const seq = seqRaw !== undefined && /^\d+$/.test(seqRaw) ? Number(seqRaw) : undefined;
  return {
    view: view && VIEWS.includes(view) ? view : 'overview',
    resource: str(KEYS.resource),
    namespace: str(KEYS.namespace),
    name: str(KEYS.name),
    at: str(KEYS.at),
    labelSelector: str(KEYS.labelSelector),
    includeDeleted: q.get(KEYS.includeDeleted) === '1' || undefined,
    seq,
    diffA: str(KEYS.diffA),
    diffB: str(KEYS.diffB),
  };
}

export function serializeState(s: AppState): string {
  const q = new URLSearchParams();
  for (const [field, key] of Object.entries(KEYS) as Array<[keyof AppState, string]>) {
    const v = s[field];
    if (v === undefined || v === '' || v === false) continue;
    if (field === 'view' && v === 'overview') continue;
    q.set(key, v === true ? '1' : String(v));
  }
  const out = q.toString();
  return out ? `?${out}` : '';
}

const EVENT = 'chronos:urlstate';

/** Reads and updates the URL-backed state. `push` creates a history entry
 * (navigation); `replace` is for typing-level edits. */
export function useAppState(): [AppState, (patch: Partial<AppState>, opts?: { replace?: boolean; reset?: boolean }) => void] {
  const [state, setState] = useState(() => parseState(window.location.search));
  useEffect(() => {
    const sync = () => setState(parseState(window.location.search));
    window.addEventListener('popstate', sync);
    window.addEventListener(EVENT, sync);
    return () => {
      window.removeEventListener('popstate', sync);
      window.removeEventListener(EVENT, sync);
    };
  }, []);
  const update = useCallback((patch: Partial<AppState>, opts: { replace?: boolean; reset?: boolean } = {}) => {
    const base = opts.reset ? { view: 'overview' as View } : parseState(window.location.search);
    navigate({ ...base, ...patch }, opts);
  }, []);
  return [state, update];
}

/** Navigates to a complete state (a new history entry unless replace). */
export function navigate(s: AppState, opts: { replace?: boolean } = {}): void {
  const url = window.location.pathname + serializeState(s) + window.location.hash;
  if (opts.replace) window.history.replaceState(null, '', url);
  else window.history.pushState(null, '', url);
  window.dispatchEvent(new Event(EVENT));
}

/** href for a state, for real links (open in new tab, copy link). */
export function hrefFor(s: AppState): string {
  return serializeState(s) || '?';
}
