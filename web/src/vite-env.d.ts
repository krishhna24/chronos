/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Override the API base URL (default "api/v1", relative to the page). */
  readonly VITE_API_BASE?: string;
  /** "1" serves fixture data in the browser instead of calling a server. */
  readonly VITE_MOCK?: string;
}
