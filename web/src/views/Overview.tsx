import { Empty, ErrorBox, Loading, CoverageBadge } from '../components/common';
import { CoverageBar } from '../components/CoverageBar';
import { Link } from '../components/Link';
import { useChronos } from '../context';
import { formatAgo, formatDuration, formatTime, toMs } from '../lib/time';
import { stateAt } from '../lib/timeline';

export function Overview() {
  const { resources, reloadResources, serverNow } = useChronos();
  if (resources.status === 'loading' || resources.status === 'idle') return <Loading label="Loading resource types…" />;
  if (resources.status === 'error') return <ErrorBox error={resources.error} onRetry={reloadResources} />;

  const r = resources.data;
  const now = toMs(r.now);
  const horizon = r.horizon ? toMs(r.horizon) : undefined;
  const earliest = Math.min(
    ...r.resources.flatMap((x) => x.coverage.map((c) => toMs(c.start))).filter((n) => !Number.isNaN(n)),
    now,
  );
  // The window shown: the retention horizon (or first observation) to now.
  const domain = { start: horizon ?? earliest, end: now };

  return (
    <section aria-labelledby="ov-title">
      <div className="section-head">
        <h2 id="ov-title">Recorded resource types</h2>
        <button type="button" onClick={reloadResources}>Refresh</button>
      </div>
      <dl className="facts">
        <div>
          <dt>Server time</dt>
          <dd><time dateTime={r.now}>{formatTime(r.now)}</time></dd>
        </div>
        <div>
          <dt>Retention horizon</dt>
          <dd>
            {r.horizon ? (
              <>
                <time dateTime={r.horizon}>{formatTime(r.horizon)}</time> ({formatDuration(now - toMs(r.horizon))} of history kept)
              </>
            ) : (
              'none (nothing has expired yet)'
            )}
          </dd>
        </div>
        <div>
          <dt>Window shown</dt>
          <dd>
            {formatTime(domain.start)} – now ({formatAgo(domain.start, serverNow())} to now)
          </dd>
        </div>
      </dl>
      {r.resources.length === 0 ? (
        <Empty>The server reports no recorded resource types.</Empty>
      ) : (
        <div className="table-wrap">
          <table className="overview">
            <thead>
              <tr>
                <th scope="col">Kind</th>
                <th scope="col">Resource</th>
                <th scope="col">Now</th>
                <th scope="col" className="wide">Coverage over the window</th>
                <th scope="col">Intervals</th>
              </tr>
            </thead>
            <tbody>
              {r.resources.map((res) => {
                const current = stateAt(res.coverage, now, { horizon: r.horizon, now });
                return (
                  <tr key={res.id}>
                    <th scope="row">
                      <Link to={{ view: 'browse', resource: res.id }}>{res.kind}</Link>
                      {res.metadataOnly && <span className="badge badge-meta" title="Collected through the metadata API; contents are never recorded">metadata only</span>}
                    </th>
                    <td><code>{res.id}</code></td>
                    <td><CoverageBadge state={current} /></td>
                    <td className="wide">
                      <CoverageBar intervals={res.coverage} domain={domain} horizon={r.horizon} now={now} />
                    </td>
                    <td>
                      {res.coverage.length === 0 ? (
                        'never observed'
                      ) : (
                        <details>
                          <summary>{res.coverage.length} interval{res.coverage.length === 1 ? '' : 's'}</summary>
                          <ol className="intervals">
                            {res.coverage.map((iv) => (
                              <li key={iv.start}>
                                {formatTime(iv.start)} – {iv.end ? formatTime(iv.end) : 'now (watching)'}
                                {iv.endReason && <span className="muted"> · ended: {iv.endReason}</span>}
                              </li>
                            ))}
                          </ol>
                        </details>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
