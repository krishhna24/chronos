import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from './App';
import { ChronosProvider } from './context';
import './styles.css';

async function start() {
  // Mock mode (`pnpm dev:mock` or VITE_MOCK=1): answer /api/v1 from
  // in-browser fixtures. The condition is static, so production builds drop
  // the mock code entirely.
  if (import.meta.env.MODE === 'mock' || import.meta.env.VITE_MOCK === '1') {
    const { installMock } = await import('./mock/install');
    installMock();
  }
  createRoot(document.getElementById('root')!).render(
    <StrictMode>
      <ChronosProvider>
        <App />
      </ChronosProvider>
    </StrictMode>,
  );
}

void start();
