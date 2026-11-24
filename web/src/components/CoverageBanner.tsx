import type { Coverage } from '../api/types';
import { formatTime } from '../lib/time';

export interface CoverageBannerProps {
  coverage: Coverage;
  /** What the coverage is about, e.g. "Pods". */
  subject?: string;
  horizon?: string;
}

/**
 * The prominent warning shown whenever an answer is not based on continuous
 * observation (ADR 0004: never present a guess as an observation). Renders
 * nothing when coverage is "observed".
 */
export function CoverageBanner({ coverage, subject = 'this resource type', horizon }: CoverageBannerProps) {
  const t = (s: string | undefined) => (s ? formatTime(s) : undefined);
  let title: string;
  let detail: string;
  switch (coverage.state) {
    case 'observed':
      return null;
    case 'gap': {
      const since = t(coverage.since);
      const until = t(coverage.until);
      title = 'Chronos was not watching at this time — state unknown';
      detail =
        `Continuity for ${subject} was lost${since ? ` at ${since}` : ''}` +
        `${coverage.reason ? ` (${coverage.reason})` : ''}` +
        ` and ${until ? `resumed at ${until}` : 'has not resumed yet'}. ` +
        'Objects may have been created, changed or deleted during the gap without being recorded. ' +
        'Anything shown is the last state Chronos saw before the gap, for reference only.';
      break;
    }
    case 'unobserved': {
      const until = t(coverage.until);
      title = 'Chronos had not started watching yet — state unknown';
      detail = until
        ? `Recording of ${subject} began at ${until}. Nothing is known about earlier times.`
        : `Chronos has never observed ${subject}.`;
      break;
    }
    case 'expired': {
      const h = t(horizon ?? coverage.until);
      title = 'Outside the retention window — state unknown';
      detail = `History older than ${h ?? 'the retention horizon'} has been discarded.`;
      break;
    }
  }
  return (
    <section className={`banner banner-${coverage.state}`} role="status" aria-label="Coverage warning">
      <p className="banner-title">
        <span aria-hidden="true">⚠ </span>
        {title}
      </p>
      <p className="banner-detail">{detail}</p>
    </section>
  );
}
