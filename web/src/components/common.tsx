import type { ReactNode } from 'react';
import { ApiError } from '../api/client';
import type { ChangeType, CoverageState, Existence, Origin } from '../api/types';

export function Loading({ label = 'Loading…' }: { label?: string }) {
  return (
    <p className="state state-loading" role="status" aria-live="polite">
      <span className="spinner" aria-hidden="true" /> {label}
    </p>
  );
}

export function describeError(err: unknown): { title: string; detail: string } {
  if (err instanceof ApiError) {
    const titles: Record<string, string> = {
      bad_request: 'The request was rejected',
      not_found: 'Not found',
      internal: 'The Chronos server failed',
      unavailable: 'Chronos is not ready yet',
      network: 'Cannot reach the Chronos API',
      bad_response: 'Unexpected response from the server',
    };
    return {
      title: titles[err.code] ?? `Request failed (HTTP ${err.status})`,
      detail: `${err.message}${err.status ? ` [${err.code}, HTTP ${err.status}]` : ''}`,
    };
  }
  return { title: 'Something went wrong', detail: err instanceof Error ? err.message : String(err) };
}

export function ErrorBox({ error, onRetry }: { error: unknown; onRetry?: () => void }) {
  const { title, detail } = describeError(error);
  return (
    <div className="state state-error" role="alert">
      <p>
        <strong>{title}.</strong> {detail}
      </p>
      {onRetry && (
        <button type="button" onClick={onRetry}>
          Retry
        </button>
      )}
    </div>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return <div className="state state-empty">{children}</div>;
}

const COVERAGE_LABEL: Record<CoverageState, string> = {
  observed: 'observed',
  gap: 'gap: unknown',
  unobserved: 'not yet watched',
  expired: 'expired',
};

export function CoverageBadge({ state }: { state: CoverageState }) {
  return (
    <span className={`badge cov-${state}`} title={`Coverage: ${state}`}>
      {COVERAGE_LABEL[state]}
    </span>
  );
}

const EXISTENCE_TITLE: Record<Existence, string> = {
  present: 'Existed at this time (observed)',
  absent: 'Did not exist at this time (observed)',
  unknown: 'Unknown: Chronos was not watching, so it cannot say',
};

export function ExistenceBadge({ existence }: { existence: Existence }) {
  return (
    <span className={`badge ex-${existence}`} title={EXISTENCE_TITLE[existence]}>
      {existence === 'unknown' ? 'unknown' : existence}
    </span>
  );
}

const ORIGIN_TITLE: Record<Origin, string> = {
  watch: 'Seen live by the watch; time is accurate',
  baseline: 'Present in the first LIST; nothing is known before this time',
  resync: 'Inferred after a gap; happened at an unknown time inside the gap',
};

export function ChangeBadge({ type, origin }: { type: ChangeType; origin: Origin }) {
  return (
    <span className={`badge chg-${type} origin-${origin}`} title={ORIGIN_TITLE[origin]}>
      {type}
      <span className="badge-sub"> · {origin}</span>
    </span>
  );
}
