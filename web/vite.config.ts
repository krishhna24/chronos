import react from '@vitejs/plugin-react';
import { loadEnv } from 'vite';
import { defineConfig } from 'vitest/config';

// `pnpm dev` proxies /api to a running Chronos server (default
// http://localhost:8080, e.g. `kubectl port-forward svc/chronos 8080`).
// Override with CHRONOS_API_URL in the environment or a .env.local file.
// `pnpm dev:mock` (mode "mock") serves fixture data in the browser instead.
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, '', '');
  const target = env.CHRONOS_API_URL || 'http://localhost:8080';
  return {
    // Relative asset URLs: the dashboard may be served under a proxy prefix
    // such as /api/v1/namespaces/chronos/services/chronos:http/proxy/.
    base: './',
    plugins: [
      react(),
      {
        // web/dist/.keep is tracked so go:embed compiles before the first
        // build; Vite empties dist, so emit it again (identical, empty).
        name: 'chronos-keep-placeholder',
        apply: 'build',
        generateBundle() {
          this.emitFile({ type: 'asset', fileName: '.keep', source: '' });
        },
      },
    ],
    server: {
      proxy: {
        '/api': { target, changeOrigin: true },
      },
    },
    build: {
      outDir: 'dist',
      emptyOutDir: true,
      sourcemap: false,
    },
    test: {
      environment: 'jsdom',
      include: ['src/**/*.test.{ts,tsx}'],
    },
  };
});
