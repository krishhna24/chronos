import type { MouseEvent, ReactNode } from 'react';
import { hrefFor, navigate, type AppState } from '../lib/urlState';

/** A real link (open in new tab and copy link work) that navigates in-app
 * on a plain click. */
export function Link({ to, children, className }: { to: AppState; children: ReactNode; className?: string }) {
  return (
    <a href={hrefFor(to)} className={className} onClick={(e) => followLink(e, to)}>
      {children}
    </a>
  );
}

export function followLink(e: MouseEvent, to: AppState): void {
  if (e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
  e.preventDefault();
  navigate(to);
  window.scrollTo?.(0, 0);
}
