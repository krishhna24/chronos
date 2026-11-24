// End-to-end-ish: the real App and views against the mock backend (the
// same fetch interceptor `pnpm dev:mock` uses), in jsdom.
import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';
import { App } from './App';
import { ChronosProvider } from './context';
import { installMock } from './mock/install';

beforeAll(() => installMock({ latencyMs: 0 }));
afterEach(cleanup);

function open(search: string) {
  window.history.replaceState(null, '', `/${search}`);
  return render(
    <ChronosProvider>
      <App />
    </ChronosProvider>,
  );
}

const ago = (min: number) => new Date(Date.now() - min * 60_000).toISOString();

describe('App in mock mode', () => {
  it('overview lists resource types with coverage bars', async () => {
    open('');
    expect(await screen.findByRole('heading', { name: 'Recorded resource types' })).toBeTruthy();
    const rows = screen.getAllByRole('row');
    expect(rows.length).toBe(8); // header + 7 types
    expect(screen.getAllByRole('img', { name: /Observed \d+% of the window; 1 gap/ }).length).toBe(7);
  });

  it('browser lists objects now, and warns during the gap', async () => {
    const { unmount } = open('?view=browse&resource=apps%2Fv1%2Fdeployments&ns=prod');
    expect(await screen.findByRole('link', { name: 'api' })).toBeTruthy();
    expect(screen.queryByRole('status', { name: 'Coverage warning' })).toBeNull();
    unmount();

    // T0 = now-180m; gap is T0+80m..T0+95m, i.e. 100..85 minutes ago.
    open(`?view=browse&resource=apps%2Fv1%2Fdeployments&ns=prod&at=${encodeURIComponent(ago(90))}`);
    const banner = await screen.findByRole('status', { name: 'Coverage warning' });
    expect(banner.textContent).toContain('Chronos was not watching at this time');
    const row = screen.getByRole('row', { name: /api/ });
    expect(within(row).getByText('unknown')).toBeTruthy();
  });

  it('quick time buttons move the selected time into the URL', async () => {
    open('?view=browse&resource=v1%2Fpods');
    await screen.findAllByRole('link', { name: /api-58f7c9d4b/ });
    await act(async () => fireEvent.click(screen.getByRole('button', { name: '-1d' })));
    expect(new URLSearchParams(window.location.search).get('at')).toMatch(/Z$/);
    const banner = await screen.findByRole('status', { name: 'Coverage warning' });
    expect(banner.textContent).toMatch(/retention window|not started watching/);
  });

  it('object view shows state, history markers, and a recreation diff', async () => {
    open(`?view=object&resource=apps%2Fv1%2Fdeployments&ns=prod&name=worker&a=${encodeURIComponent(ago(170))}&b=${encodeURIComponent(ago(115))}`);
    expect(await screen.findByRole('heading', { name: /Deployment prod\/worker/ })).toBeTruthy();
    const markers = await screen.findAllByRole('button', { name: /\(watch\)|\(baseline\)/ });
    expect(markers.length).toBe(3);
    expect(await screen.findByRole('status', { name: 'Object recreated' })).toBeTruthy();
    expect(screen.getByText('new instance')).toBeTruthy();

    await act(async () => fireEvent.click(screen.getAllByRole('button', { name: /^Deleted \(watch\)/ })[0]!));
    expect(await screen.findByRole('heading', { name: /Version seq \d+/ })).toBeTruthy();
    expect(await screen.findByText(/This record is a deletion/)).toBeTruthy();
  });

  it('object view flags a diff across the gap as incomplete', async () => {
    open(`?view=object&resource=apps%2Fv1%2Fdeployments&ns=prod&name=api&a=${encodeURIComponent(ago(120))}&b=${encodeURIComponent(ago(60))}`);
    expect(await screen.findByRole('status', { name: 'Incomplete diff' })).toBeTruthy();
    expect(screen.getByLabelText('Unified diff').textContent).toContain('api:1.5.1');
  });

  it('relationships view builds Deployment -> ReplicaSets -> Pods', async () => {
    open('?view=relations&resource=apps%2Fv1%2Fdeployments&ns=prod&name=api');
    const tree = await screen.findByRole('list', { name: 'Ownership tree' });
    expect(within(tree).getByRole('link', { name: 'api-58f7c9d4b' })).toBeTruthy();
    expect(within(tree).getAllByRole('link', { name: /^api-58f7c9d4b-/ }).length).toBe(4);
    // Old ReplicaSets are still owned (scaled to 0) and have no pods.
    expect(within(tree).getByRole('link', { name: 'api-6d4f8b7c9' })).toBeTruthy();
  });

  it('relationships for a recreated Deployment exclude the old instance', async () => {
    open(`?view=relations&resource=apps%2Fv1%2Fdeployments&ns=prod&name=worker&at=${encodeURIComponent(ago(110))}`);
    const tree = await screen.findByRole('list', { name: 'Ownership tree' });
    expect(within(tree).getByRole('link', { name: 'worker-7f6e5d4c3' })).toBeTruthy();
    expect(within(tree).queryByRole('link', { name: 'worker-5c8d9b6f7' })).toBeNull();
  });

  it('shows a server error from the API', async () => {
    open('?view=browse&resource=v1%2Fpods&selector=' + encodeURIComponent('app in (unterminated'));
    const alert = await screen.findByRole('alert');
    expect(alert.textContent).toContain('The request was rejected');
  });
});
