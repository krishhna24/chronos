import { useEffect, useId, useMemo, useRef, useState, type FormEvent } from 'react';
import type { ListParams } from '../api/client';
import type { ObjectState } from '../api/types';
import { Empty, ErrorBox, Loading } from '../components/common';
import { followLink, Link } from '../components/Link';
import { ObjectList } from '../components/ObjectList';
import { TimePicker } from '../components/TimePicker';
import { useChronos } from '../context';
import { usePagedList } from '../lib/usePagedList';
import { hrefFor, type AppState } from '../lib/urlState';

export function Browser({ state, update }: { state: AppState; update: (p: Partial<AppState>, o?: { replace?: boolean }) => void }) {
  const { resources, reloadResources, resourceById, serverNow } = useChronos();
  const id = useId();
  const res = resourceById(state.resource);
  const [ns, setNs] = useState(state.namespace ?? '');
  const [sel, setSel] = useState(state.labelSelector ?? '');
  const seenNamespaces = useRef(new Set<string>());

  useEffect(() => setNs(state.namespace ?? ''), [state.namespace]);
  useEffect(() => setSel(state.labelSelector ?? ''), [state.labelSelector]);

  const listKey = res ? JSON.stringify([res.id, state.namespace, state.at, state.labelSelector, state.includeDeleted]) : null;
  const listParams = useMemo<ListParams | null>(
    () =>
      res
        ? {
            resource: res.id,
            namespace: res.namespaced ? state.namespace : undefined,
            at: state.at,
            labelSelector: state.labelSelector,
            includeDeleted: state.includeDeleted,
          }
        : null,
    // listKey captures every field read here.
    [listKey],
  );
  const paged = usePagedList(listParams, listKey);
  const { state: list, reload } = paged;
  if (list.status === 'success') for (const it of list.data.items) if (it.key.namespace) seenNamespaces.current.add(it.key.namespace);

  if (resources.status === 'loading' || resources.status === 'idle') return <Loading label="Loading resource types…" />;
  if (resources.status === 'error') return <ErrorBox error={resources.error} onRetry={reloadResources} />;

  const applyFilters = (e: FormEvent) => {
    e.preventDefault();
    update({ namespace: ns.trim() || undefined, labelSelector: sel.trim() || undefined });
  };

  const detail = (item: ObjectState): AppState => ({
    view: 'object',
    resource: item.key.resource,
    namespace: item.key.namespace,
    name: item.key.name,
    at: state.at,
  });

  return (
    <section aria-labelledby={`${id}-title`}>
      <h2 id={`${id}-title`}>Browse {res ? `${res.kind} objects` : 'objects'}</h2>
      <div className="controls">
        <form className="filters" onSubmit={applyFilters} aria-label="Filters">
          <label htmlFor={`${id}-res`}>Resource</label>
          <select
            id={`${id}-res`}
            value={state.resource ?? ''}
            onChange={(e) => update({ resource: e.target.value || undefined })}
          >
            <option value="">Choose…</option>
            {resources.data.resources.map((r) => (
              <option key={r.id} value={r.id}>
                {r.kind} ({r.id})
              </option>
            ))}
          </select>
          {(!res || res.namespaced) && (
            <>
              <label htmlFor={`${id}-ns`}>Namespace</label>
              <input
                id={`${id}-ns`}
                value={ns}
                onChange={(e) => setNs(e.target.value)}
                placeholder="all namespaces"
                list={`${id}-nslist`}
                autoComplete="off"
                spellCheck={false}
              />
              <datalist id={`${id}-nslist`}>
                {[...seenNamespaces.current].sort().map((n) => (
                  <option key={n} value={n} />
                ))}
              </datalist>
            </>
          )}
          <label htmlFor={`${id}-sel`}>Label selector</label>
          <input
            id={`${id}-sel`}
            value={sel}
            onChange={(e) => setSel(e.target.value)}
            placeholder="app=api,tier!=db"
            autoComplete="off"
            spellCheck={false}
          />
          <button type="submit">Apply</button>
          <label className="checkbox">
            <input
              type="checkbox"
              checked={!!state.includeDeleted}
              onChange={(e) => update({ includeDeleted: e.target.checked || undefined })}
            />
            Include deleted
          </label>
        </form>
        <TimePicker at={state.at} now={serverNow} onChange={(at) => update({ at })} />
      </div>
      {!res && state.resource ? (
        <Empty>Unknown resource type “{state.resource}”. Choose one from the list.</Empty>
      ) : (
        <ObjectList
          state={list}
          resource={res}
          namespace={state.namespace}
          labelSelector={state.labelSelector}
          horizon={resources.data.horizon}
          onReload={reload}
          paging={{ pages: paged.pages, loadingMore: paged.loadingMore, error: paged.moreError, onLoadMore: paged.loadMore }}
          hrefFor={(item) => hrefFor(detail(item))}
          onOpen={(item, e) => followLink(e, detail(item))}
          rowActions={(item) =>
            res?.kind === 'Deployment' ? (
              <Link to={{ view: 'relations', resource: item.key.resource, namespace: item.key.namespace, name: item.key.name, at: state.at }}>
                ReplicaSets &amp; Pods
              </Link>
            ) : null
          }
        />
      )}
    </section>
  );
}
