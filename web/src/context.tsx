import { createContext, useCallback, useContext, useMemo, useRef, type ReactNode } from 'react';
import { client } from './api/client';
import type { Resource, ResourcesResponse } from './api/types';
import { toMs } from './lib/time';
import { useAsync, type AsyncState } from './lib/useAsync';

export interface ChronosContext {
  resources: AsyncState<ResourcesResponse>;
  reloadResources: () => void;
  /** Current time on the server's clock (client clock corrected by the skew
   * measured from /resources), so "-5m" never lands in the server's future. */
  serverNow: () => number;
  resourceById: (id: string | undefined) => Resource | undefined;
}

const Ctx = createContext<ChronosContext | null>(null);

export function ChronosProvider({ children }: { children: ReactNode }) {
  const skew = useRef(0);
  const [resources, reloadResources] = useAsync(async (signal) => {
    const res = await client.resources(signal);
    const server = toMs(res.now);
    if (!Number.isNaN(server)) skew.current = server - Date.now();
    return res;
  }, 'resources');
  const serverNow = useCallback(() => Date.now() + skew.current, []);
  const value = useMemo<ChronosContext>(() => {
    const list = resources.status === 'success' ? resources.data.resources : [];
    return {
      resources,
      reloadResources,
      serverNow,
      resourceById: (id) => list.find((r) => r.id === id),
    };
  }, [resources, reloadResources, serverNow]);
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useChronos(): ChronosContext {
  const c = useContext(Ctx);
  if (!c) throw new Error('useChronos outside ChronosProvider');
  return c;
}

/** Plural display name, e.g. "Deployments". */
export function kindPlural(r: Resource | undefined): string {
  if (!r) return 'objects';
  return r.kind.endsWith('s') ? `${r.kind}es` : `${r.kind}s`;
}
