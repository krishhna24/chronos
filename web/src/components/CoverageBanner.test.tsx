import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import { CoverageBanner } from './CoverageBanner';

afterEach(cleanup);

describe('CoverageBanner', () => {
  it('renders nothing when observed', () => {
    const { container } = render(<CoverageBanner coverage={{ state: 'observed', since: '2026-11-24T10:00:00Z' }} />);
    expect(container.innerHTML).toBe('');
  });

  it('warns that state is unknown during a gap, with reason and bounds', () => {
    render(
      <CoverageBanner
        subject="Pod objects"
        coverage={{ state: 'gap', since: '2026-11-24T10:00:00Z', until: '2026-11-24T10:15:00Z', reason: 'watch expired (410 Gone)' }}
      />,
    );
    const banner = screen.getByRole('status', { name: 'Coverage warning' });
    expect(banner.textContent).toContain('Chronos was not watching at this time — state unknown');
    expect(banner.textContent).toContain('watch expired (410 Gone)');
    expect(banner.textContent).toContain('resumed at');
    expect(banner.textContent).toContain('last state Chronos saw');
    expect(banner.className).toContain('banner-gap');
  });

  it('says when a gap is still open', () => {
    render(<CoverageBanner coverage={{ state: 'gap', since: '2026-11-24T10:00:00Z' }} />);
    expect(screen.getByRole('status').textContent).toContain('has not resumed yet');
  });

  it('explains unobserved time with the recording start', () => {
    render(<CoverageBanner subject="Events" coverage={{ state: 'unobserved', until: '2026-11-24T10:00:00Z' }} />);
    const text = screen.getByRole('status').textContent ?? '';
    expect(text).toContain('Chronos had not started watching yet — state unknown');
    expect(text).toContain('Recording of Events began at');
  });

  it('explains unobserved time for a never-observed type', () => {
    render(<CoverageBanner subject="Events" coverage={{ state: 'unobserved' }} />);
    expect(screen.getByRole('status').textContent).toContain('never observed Events');
  });

  it('explains expired time with the horizon', () => {
    render(<CoverageBanner coverage={{ state: 'expired' }} horizon="2026-11-17T10:00:00Z" />);
    const text = screen.getByRole('status').textContent ?? '';
    expect(text).toContain('Outside the retention window — state unknown');
    expect(text).toMatch(/older than 2026-11-1[67]/);
  });
});
