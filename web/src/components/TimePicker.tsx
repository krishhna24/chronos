import { useEffect, useId, useState, type FormEvent } from 'react';
import { formatAgo, formatTime, fromDatetimeLocal, fromMs, parseRelative, toDatetimeLocal, toMs } from '../lib/time';

export interface TimePickerProps {
  /** Selected instant (RFC 3339) or undefined for "now". */
  at: string | undefined;
  /** The server's current time. */
  now: () => number;
  onChange: (at: string | undefined) => void;
}

const QUICK = ['-5m', '-1h', '-1d'] as const;

/** Absolute (datetime-local, local time zone) picker with quick relative
 * buttons. Relative choices are resolved against the server clock at click
 * time and stored absolute, so a shared link means the same instant. */
export function TimePicker({ at, now, onChange }: TimePickerProps) {
  const id = useId();
  const [draft, setDraft] = useState(() => toDatetimeLocal(at ? toMs(at) : now()));
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setDraft(toDatetimeLocal(at ? toMs(at) : now()));
    setError(null);
  }, [at, now]);

  const submit = (e: FormEvent) => {
    e.preventDefault();
    const ms = fromDatetimeLocal(draft);
    if (ms === null) {
      setError('Enter a complete date and time.');
      return;
    }
    if (ms > now()) {
      setError('That time is in the future; Chronos only knows the past.');
      return;
    }
    setError(null);
    onChange(fromMs(ms));
  };

  const atMs = at ? toMs(at) : undefined;
  return (
    <form className="timepicker" onSubmit={submit} aria-label="Point in time">
      <label htmlFor={`${id}-dt`}>Time</label>
      <input
        id={`${id}-dt`}
        type="datetime-local"
        step={1}
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        aria-describedby={`${id}-desc`}
        aria-invalid={error ? true : undefined}
      />
      <button type="submit">Go</button>
      <span className="btn-group" role="group" aria-label="Quick times">
        {QUICK.map((q) => (
          <button key={q} type="button" onClick={() => onChange(fromMs(now() + (parseRelative(q) ?? 0)))}>
            {q}
          </button>
        ))}
        <button type="button" aria-pressed={at === undefined} onClick={() => onChange(undefined)}>
          now
        </button>
      </span>
      <span id={`${id}-desc`} className="timepicker-desc">
        {atMs === undefined ? (
          <>Showing <strong>now</strong> (live)</>
        ) : (
          <>
            Showing <time dateTime={at}>{formatTime(at!)}</time> local ({formatAgo(atMs, now())})
          </>
        )}
      </span>
      {error && (
        <span className="field-error" role="alert">
          {error}
        </span>
      )}
    </form>
  );
}
