import type { MouseEvent, ReactNode } from 'react';
import type { ListResponse, ObjectState, Resource } from '../api/types';
import { formatAgo, formatTime, toMs } from '../lib/time';
import type { AsyncState } from '../lib/useAsync';
import { ChangeBadge, Empty, ErrorBox, ExistenceBadge, Loading } from './common';
import { CoverageBanner } from './CoverageBanner';

export interface ObjectListProps {
  state: AsyncState<ListResponse>;
  resource: Resource | undefined;
  namespace?: string;
  labelSelector?: string;
  horizon?: string;
  onReload: () => void;
  /** Builds the link to an object's detail view. */
  hrefFor: (item: ObjectState) => string;
  onOpen: (item: ObjectState, e: MouseEvent) => void;
  /** Extra per-row actions (e.g. relationships for Deployments). */
  rowActions?: (item: ObjectState) => ReactNode;
  /** Paging controls; the list shows a "Load more" button while
   * state.data.continue is set. */
  paging?: { pages: number; loadingMore: boolean; error: unknown; onLoadMore: () => void };
}

function emptyMessage(res: ListResponse, kinds: string, where: string, selector?: string): string {
  const sel = selector ? ` matching “${selector}”` : '';
  switch (res.coverage.state) {
    case 'observed':
      return `No ${kinds}${sel} existed ${where} at this time.`;
    case 'gap':
      return `Chronos has no last known ${kinds}${sel} ${where} for this time, and it was not watching — it cannot say what existed.`;
    case 'unobserved':
    case 'expired':
      return `Chronos has no data for this time, so it cannot say which ${kinds} existed.`;
  }
}

export function ObjectList({ state, resource, namespace, labelSelector, horizon, onReload, hrefFor, onOpen, rowActions, paging }: ObjectListProps) {
  const kinds = resource ? `${resource.kind} objects` : 'objects';
  const where = namespace ? `in namespace “${namespace}”` : 'in any namespace';

  if (state.status === 'idle') return <Empty>Choose a resource type to list objects.</Empty>;
  if (state.status === 'loading') return <Loading label={`Loading ${kinds}…`} />;
  if (state.status === 'error') return <ErrorBox error={state.error} onRetry={onReload} />;

  const res = state.data;
  const now = Date.now();
  const more = !!res.continue;
  const loadMore = paging && more && (
    <div className="load-more">
      {paging.error ? <ErrorBox error={paging.error} onRetry={paging.onLoadMore} /> : null}
      <button type="button" onClick={paging.onLoadMore} disabled={paging.loadingMore} aria-describedby="list-count">
        {paging.loadingMore ? 'Loading…' : 'Load more'}
      </button>
    </div>
  );
  return (
    <>
      <CoverageBanner coverage={res.coverage} subject={resource ? `${resource.kind} objects` : undefined} horizon={horizon} />
      {res.items.length === 0 && !more ? (
        <Empty>{emptyMessage(res, kinds, where, labelSelector)}</Empty>
      ) : res.items.length === 0 ? (
        loadMore
      ) : (
        <div className="table-wrap">
          <table className="objects">
            <caption id="list-count" aria-live="polite">
              {more ? `${res.items.length} ${kinds} loaded so far` : `${res.items.length} ${kinds}`} at{' '}
              <time dateTime={res.at}>{formatTime(res.at)}</time>
              {res.coverage.state === 'gap' && ' (last known state before the gap)'}
              {more && ` · more available${paging && paging.pages > 1 ? ` · ${paging.pages} pages loaded` : ''}`}
            </caption>
            <thead>
              <tr>
                {!namespace && <th scope="col">Namespace</th>}
                <th scope="col">Name</th>
                <th scope="col">Existence</th>
                <th scope="col">Last change</th>
                <th scope="col">
                  <span className="sr-only">Actions</span>
                </th>
              </tr>
            </thead>
            <tbody>
              {res.items.map((item) => (
                <tr key={`${item.key.namespace ?? ''}/${item.key.name}`} className={`row-${item.existence}`}>
                  {!namespace && <td>{item.key.namespace}</td>}
                  <th scope="row">
                    <a href={hrefFor(item)} onClick={(e) => onOpen(item, e)}>
                      {item.key.name}
                    </a>
                  </th>
                  <td>
                    <ExistenceBadge existence={item.existence} />
                  </td>
                  <td>
                    {item.version ? (
                      <>
                        <ChangeBadge type={item.version.type} origin={item.version.origin} />{' '}
                        <time dateTime={item.version.observedAt} title={formatTime(item.version.observedAt)}>
                          {formatAgo(toMs(item.version.observedAt), now)}
                        </time>
                      </>
                    ) : (
                      '—'
                    )}
                  </td>
                  <td className="actions">{rowActions?.(item)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {loadMore}
        </div>
      )}
    </>
  );
}
