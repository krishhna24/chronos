import { useState } from 'react';
import type { ObjectState, Resource } from '../api/types';
import { formatTime } from '../lib/time';
import { toYaml } from '../lib/yaml';
import { ChangeBadge, CoverageBadge, Empty, ExistenceBadge } from './common';

/** One object at one instant (or one specific record), as YAML or JSON. */
export function ObjectView({ state, resource, record }: { state: ObjectState; resource?: Resource; record?: boolean }) {
  const [format, setFormat] = useState<'yaml' | 'json'>('yaml');
  const v = state.version;
  let body: string | null = null;
  if (state.object) body = format === 'yaml' ? toYaml(state.object) : JSON.stringify(state.object, null, 2);

  let explanation: string;
  if (record && v) {
    explanation =
      v.type === 'Deleted'
        ? 'This record is a deletion, so it carries no object. Inspect the previous version for the last state Chronos saw.'
        : 'The object exactly as recorded by this version.';
  } else if (state.existence === 'present') {
    explanation = 'The object as it was at this time.';
  } else if (state.existence === 'absent') {
    explanation = v ? `The object did not exist at this time: it was deleted at ${formatTime(v.observedAt)}.` : 'The object did not exist at this time (Chronos was watching and has no record of it).';
  } else if (state.object) {
    explanation = 'Last known state before Chronos stopped watching. It may have changed or been deleted since.';
  } else {
    explanation = 'Chronos was not watching at this time and has no last known state.';
  }

  return (
    <div className="objectview">
      <dl className="facts">
        <div>
          <dt>{record ? 'Recorded' : 'At'}</dt>
          <dd>
            <time dateTime={record && v ? v.observedAt : state.at}>{formatTime(record && v ? v.observedAt : state.at)}</time>
          </dd>
        </div>
        <div>
          <dt>Existence</dt>
          <dd>
            <ExistenceBadge existence={state.existence} /> <CoverageBadge state={state.coverage.state} />
          </dd>
        </div>
        {v && (
          <>
            <div>
              <dt>Based on</dt>
              <dd>
                <ChangeBadge type={v.type} origin={v.origin} /> seq {v.seq} at <time dateTime={v.observedAt}>{formatTime(v.observedAt)}</time>
              </dd>
            </div>
            <div>
              <dt>UID</dt>
              <dd><code>{v.uid}</code></dd>
            </div>
            <div>
              <dt>resourceVersion</dt>
              <dd><code>{v.resourceVersion}</code></dd>
            </div>
          </>
        )}
      </dl>
      <p className="note">{explanation}</p>
      {resource?.metadataOnly && (
        <p className="note">
          {resource.kind} objects are collected through the metadata API: their contents are never recorded.
        </p>
      )}
      {body === null ? (
        <Empty>No object to show.</Empty>
      ) : (
        <>
          <div className="btn-group" role="group" aria-label="Format">
            <button type="button" aria-pressed={format === 'yaml'} onClick={() => setFormat('yaml')}>YAML</button>
            <button type="button" aria-pressed={format === 'json'} onClick={() => setFormat('json')}>JSON</button>
          </div>
          <pre className={`code${state.existence === 'present' || record ? '' : ' stale'}`} tabIndex={0} aria-label={`Object ${format.toUpperCase()}`}>
            {body}
          </pre>
        </>
      )}
    </div>
  );
}
