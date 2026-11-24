import { useCallback, useEffect, useRef, useState } from 'react';
import { client as defaultClient, isAbortError, type Client, type ListParams } from '../api/client';
import type { ListResponse, ObjectState } from '../api/types';
import { useAsync, type AsyncState } from './useAsync';

export interface PagedListState {
  /** The first page with every loaded page's items appended, and the
   * continue token of the last loaded page. */
  state: AsyncState<ListResponse>;
  pages: number;
  loadMore: () => void;
  loadingMore: boolean;
  moreError: unknown;
  reload: () => void;
}

interface Extra {
  key: string;
  items: ObjectState[];
  continue: string | undefined;
  pages: number;
}

/**
 * Loads the first page of a listing whenever `key` changes; loadMore()
 * appends the next page, read at the first page's `at` so all pages
 * describe one instant. Changing `key` discards loaded pages and aborts an
 * in-flight loadMore.
 */
export function usePagedList(params: ListParams | null, key: string | null, client: Client = defaultClient): PagedListState {
  const [first, reloadFirst] = useAsync((signal) => client.list(params!, signal), key);
  const [extra, setExtra] = useState<Extra | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [moreError, setMoreError] = useState<unknown>(null);
  const inflight = useRef<AbortController | null>(null);

  const reset = useCallback(() => {
    inflight.current?.abort();
    inflight.current = null;
    setExtra(null);
    setLoadingMore(false);
    setMoreError(null);
  }, []);
  useEffect(() => reset, [key, reset]);
  // A new first page (key change or reload) starts the listing over.
  useEffect(() => {
    if (first.status === 'loading') reset();
  }, [first.status, reset]);

  const current = first.status === 'success' && extra && extra.key === key ? extra : null;
  const merged: AsyncState<ListResponse> =
    first.status === 'success'
      ? {
          status: 'success',
          data: current
            ? { ...first.data, items: [...first.data.items, ...current.items], continue: current.continue }
            : first.data,
        }
      : first;

  const loadMore = useCallback(() => {
    if (merged.status !== 'success' || !merged.data.continue || !params || key === null || inflight.current) return;
    const ctl = new AbortController();
    inflight.current = ctl;
    setLoadingMore(true);
    setMoreError(null);
    const prev = merged.data;
    client.listNext(params, prev, ctl.signal).then(
      (page) => {
        if (ctl.signal.aborted) return;
        inflight.current = null;
        setLoadingMore(false);
        setExtra((e) => {
          const base = e && e.key === key ? e : { key, items: [], continue: undefined, pages: 1 };
          return { key, items: [...base.items, ...page.items], continue: page.continue, pages: base.pages + 1 };
        });
      },
      (err: unknown) => {
        if (ctl.signal.aborted || isAbortError(err)) return;
        inflight.current = null;
        setLoadingMore(false);
        setMoreError(err);
      },
    );
  }, [merged, params, key, client]);

  return {
    state: merged,
    pages: first.status === 'success' ? (current?.pages ?? 1) : 0,
    loadMore,
    loadingMore,
    moreError,
    reload: reloadFirst,
  };
}
