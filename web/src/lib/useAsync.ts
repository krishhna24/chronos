import { useCallback, useEffect, useState } from 'react';
import { isAbortError } from '../api/client';

export type AsyncState<T> =
  | { status: 'idle' }
  | { status: 'loading'; previous?: T }
  | { status: 'error'; error: unknown }
  | { status: 'success'; data: T };

/**
 * Runs `fn` whenever `key` changes (pass null to stay idle), aborting the
 * previous request so a slow stale response can never overwrite a newer
 * one. Returns the state and a reload function.
 */
export function useAsync<T>(fn: (signal: AbortSignal) => Promise<T>, key: string | null): [AsyncState<T>, () => void] {
  const [state, setState] = useState<AsyncState<T>>({ status: key === null ? 'idle' : 'loading' });
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    if (key === null) {
      setState({ status: 'idle' });
      return;
    }
    const ctl = new AbortController();
    setState((s) => ({ status: 'loading', previous: s.status === 'success' ? s.data : undefined }));
    fn(ctl.signal).then(
      (data) => {
        if (!ctl.signal.aborted) setState({ status: 'success', data });
      },
      (error: unknown) => {
        if (!ctl.signal.aborted && !isAbortError(error)) setState({ status: 'error', error });
      },
    );
    return () => ctl.abort();
    // fn is intentionally not a dependency: `key` captures everything it
    // reads, so callers can pass an inline closure.
  }, [key, nonce]);

  const reload = useCallback(() => setNonce((n) => n + 1), []);
  return [state, reload];
}
