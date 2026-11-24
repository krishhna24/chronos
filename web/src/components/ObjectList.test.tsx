import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../api/client';
import type { ListResponse, Resource } from '../api/types';
import type { AsyncState } from '../lib/useAsync';
import { ObjectList } from './ObjectList';

afterEach(cleanup);

const pods: Resource = {
  id: 'v1/pods', group: '', version: 'v1', resource: 'pods', kind: 'Pod', namespaced: true, metadataOnly: false, coverage: [],
};

function renderList(state: AsyncState<ListResponse>, extra: { namespace?: string; labelSelector?: string } = {}) {
  const onReload = vi.fn();
  render(
    <ObjectList state={state} resource={pods} onReload={onReload} hrefFor={() => '?'} onOpen={() => undefined} {...extra} />,
  );
  return { onReload };
}

const at = '2026-11-24T14:00:00Z';

describe('ObjectList states', () => {
  it('shows a loading state', () => {
    renderList({ status: 'loading' });
    expect(screen.getByRole('status').textContent).toContain('Loading Pod objects');
  });

  it('shows an API error with retry', () => {
    const { onReload } = renderList({ status: 'error', error: new ApiError(400, 'bad_request', 'invalid label selector') });
    const alert = screen.getByRole('alert');
    expect(alert.textContent).toContain('The request was rejected');
    expect(alert.textContent).toContain('invalid label selector');
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    expect(onReload).toHaveBeenCalledOnce();
  });

  it('shows a network error', () => {
    renderList({ status: 'error', error: new ApiError(0, 'network', 'Failed to fetch') });
    expect(screen.getByRole('alert').textContent).toContain('Cannot reach the Chronos API');
  });

  it('says nothing existed when observed and empty', () => {
    renderList({ status: 'success', data: { at, coverage: { state: 'observed' }, items: [] } }, { namespace: 'prod', labelSelector: 'app=x' });
    expect(screen.getByText(/No Pod objects matching “app=x” existed in namespace “prod” at this time\./)).toBeTruthy();
    expect(screen.queryByRole('status')).toBeNull(); // no coverage warning
  });

  it('does not claim absence when empty during a gap', () => {
    renderList({ status: 'success', data: { at, coverage: { state: 'gap', since: at }, items: [] } });
    expect(screen.getByRole('status').textContent).toContain('state unknown');
    expect(screen.getByText(/it cannot say what existed/)).toBeTruthy();
    expect(screen.queryByText(/existed in/)).toBeNull();
  });

  it('says it has no data when unobserved or expired', () => {
    renderList({ status: 'success', data: { at, coverage: { state: 'expired' }, items: [] } });
    expect(screen.getByText(/Chronos has no data for this time/)).toBeTruthy();
  });

  it('renders items with existence badges and a gap caption', () => {
    renderList({
      status: 'success',
      data: {
        at,
        coverage: { state: 'gap', since: at },
        items: [
          {
            key: { resource: 'v1/pods', namespace: 'prod', name: 'api-1' },
            at,
            existence: 'unknown',
            coverage: { state: 'gap' },
            version: { seq: 7, observedAt: at, type: 'Modified', origin: 'watch', uid: 'u', resourceVersion: '1' },
          },
        ],
      },
    });
    const row = screen.getByRole('row', { name: /api-1/ });
    expect(row.textContent).toContain('unknown');
    expect(row.textContent).toContain('Modified');
    expect(screen.getByRole('table').querySelector('caption')?.textContent).toContain('last known state before the gap');
  });

  it('prompts for a resource when idle', () => {
    renderList({ status: 'idle' });
    expect(screen.getByText('Choose a resource type to list objects.')).toBeTruthy();
  });
});
