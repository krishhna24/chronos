import { Link } from './components/Link';
import { useChronos } from './context';
import { formatTime } from './lib/time';
import { useAppState, type AppState, type View } from './lib/urlState';
import { Browser } from './views/Browser';
import { ObjectDetail } from './views/ObjectDetail';
import { Overview } from './views/Overview';
import { Relationships } from './views/Relationships';

const NAV: Array<[View, string]> = [
  ['overview', 'Overview'],
  ['browse', 'Browse'],
  ['object', 'Object'],
  ['relations', 'Relationships'],
];

export function App() {
  const [state, update] = useAppState();
  const { resources } = useChronos();

  // Carry the context that makes sense across views.
  const navTarget = (view: View): AppState => {
    const common = { view, at: state.at, namespace: state.namespace };
    switch (view) {
      case 'overview':
        return { view };
      case 'browse':
        return { ...common, resource: state.resource, labelSelector: state.labelSelector, includeDeleted: state.includeDeleted };
      case 'object':
        return { ...common, resource: state.resource, name: state.name };
      case 'relations':
        return state.resource === 'apps/v1/deployments' ? { ...common, resource: state.resource, name: state.name } : common;
    }
  };

  return (
    <>
      <a className="skip" href="#main">
        Skip to content
      </a>
      <header className="app-header">
        <h1>
          <Link to={{ view: 'overview' }}>Chronos</Link>
        </h1>
        <nav aria-label="Views">
          <ul>
            {NAV.map(([view, label]) => (
              <li key={view}>
                <span aria-current={state.view === view ? 'page' : undefined}>
                  <Link to={navTarget(view)}>{label}</Link>
                </span>
              </li>
            ))}
          </ul>
        </nav>
        <p className="server-time">
          {resources.status === 'success' ? (
            <>
              Server time at load: <time dateTime={resources.data.now}>{formatTime(resources.data.now)}</time>
            </>
          ) : null}
          {import.meta.env.MODE === 'mock' || import.meta.env.VITE_MOCK === '1' ? <span className="badge badge-mock">mock data</span> : null}
        </p>
      </header>
      <main id="main" tabIndex={-1}>
        {state.view === 'overview' && <Overview />}
        {state.view === 'browse' && <Browser state={state} update={update} />}
        {state.view === 'object' && <ObjectDetail state={state} update={update} />}
        {state.view === 'relations' && <Relationships state={state} update={update} />}
      </main>
    </>
  );
}
