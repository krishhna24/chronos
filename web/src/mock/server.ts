// An in-browser implementation of the /api/v1 semantics (docs/api.md,
// ADR 0004) over fixture data, used only by mock mode and tests. It follows
// the server's rules closely enough that the UI can be built against it:
// coverage classification, existence, last-known state in gaps, diff
// completeness and recreation.

import type {
  Coverage,
  DiffResponse,
  ErrorBody,
  HistoryResponse,
  ListResponse,
  ObjectState,
  Resource,
  ResourcesResponse,
  Version,
} from '../api/types';
import { unifiedDiff } from '../lib/diff';
import { compareTime, parseTime } from '../lib/time';
import { toYaml } from '../lib/yaml';
import type { Fixture, FixtureRecord } from './fixtures';

export interface MockResponse {
  status: number;
  body: unknown;
}

class HttpError extends Error {
  constructor(
    readonly status: number,
    readonly code: ErrorBody['code'],
    message: string,
  ) {
    super(message);
  }
}

const bad = (msg: string) => new HttpError(400, 'bad_request', msg);

function coverageAt(r: Resource, at: string, horizon: string): Coverage {
  if (compareTime(at, horizon) < 0) return { state: 'expired', until: horizon };
  let last: { type: 'start' | 'end'; i: number } | null = null;
  r.coverage.forEach((iv, i) => {
    if (compareTime(iv.start, at) <= 0) last = { type: 'start', i };
    if (iv.end && compareTime(iv.end, at) <= 0) last = { type: 'end', i };
  });
  const first = r.coverage[0];
  if (last === null) return first ? { state: 'unobserved', until: first.start } : { state: 'unobserved' };
  const { type, i } = last as { type: 'start' | 'end'; i: number };
  const iv = r.coverage[i]!;
  if (type === 'start') {
    const c: Coverage = { state: 'observed', since: iv.start };
    if (iv.end) c.until = iv.end;
    return c;
  }
  const c: Coverage = { state: 'gap', since: iv.end! };
  const next = r.coverage[i + 1];
  if (next) c.until = next.start;
  if (iv.endReason) c.reason = iv.endReason;
  return c;
}

function versionOf(r: FixtureRecord): Version {
  return {
    seq: r.seq,
    observedAt: r.observedAt,
    type: r.type,
    origin: r.origin,
    uid: r.uid,
    resourceVersion: r.resourceVersion,
  };
}

function matchesSelector(labels: Record<string, string>, selector: string): boolean {
  // Split on commas outside parentheses.
  const reqs: string[] = [];
  let depth = 0;
  let cur = '';
  for (const ch of selector) {
    if (ch === '(') depth++;
    if (ch === ')') depth--;
    if (ch === ',' && depth === 0) {
      reqs.push(cur);
      cur = '';
    } else cur += ch;
  }
  reqs.push(cur);
  return reqs
    .map((r) => r.trim())
    .filter(Boolean)
    .every((req) => {
      let m: RegExpExecArray | null;
      if ((m = /^([\w./-]+)\s*(==|=|!=)\s*([\w.-]*)$/.exec(req))) {
        const has = labels[m[1]!] === m[3];
        return m[2] === '!=' ? !has : has;
      }
      if ((m = /^([\w./-]+)\s+(in|notin)\s+\(([^)]*)\)$/.exec(req))) {
        const vals = m[3]!.split(',').map((v) => v.trim());
        const v = labels[m[1]!];
        const inSet = v !== undefined && vals.includes(v);
        return m[2] === 'in' ? inSet : !inSet;
      }
      if ((m = /^!([\w./-]+)$/.exec(req))) return !(m[1]! in labels);
      if ((m = /^([\w./-]+)$/.exec(req))) return m[1]! in labels;
      throw bad(`invalid label selector: unable to parse requirement ${JSON.stringify(req)}`);
    });
}

export function createMockServer(fx: Fixture, clock: () => number = Date.now) {
  const byKey = new Map<string, FixtureRecord[]>();
  for (const r of fx.records) {
    const k = `${r.resource}\u0000${r.namespace}\u0000${r.name}`;
    const list = byKey.get(k) ?? [];
    list.push(r);
    byKey.set(k, list);
  }

  const resource = (q: URLSearchParams): Resource => {
    const id = q.get('resource');
    if (!id) throw bad('missing parameter "resource"');
    const r = fx.resources.find((x) => x.id === id);
    if (!r) throw bad(`unknown resource ${JSON.stringify(id)}; use the canonical ID from /api/v1/resources`);
    return r;
  };
  const time = (q: URLSearchParams, name: string, def?: string): string => {
    const v = q.get(name);
    if (v === null || v === '') {
      if (def !== undefined) return def;
      throw bad(`missing parameter "${name}"`);
    }
    const p = parseTime(v);
    if (!p) throw bad(`invalid ${name} ${JSON.stringify(v)}: want RFC 3339`);
    if (p.ms > clock() + 1000) throw bad(`${name} ${v} is in the future`);
    return v;
  };
  const nowStr = () => new Date(clock()).toISOString();
  const nameParam = (q: URLSearchParams) => {
    const n = q.get('name');
    if (!n) throw bad('missing parameter "name"');
    return n;
  };

  const latestAt = (records: FixtureRecord[] | undefined, at: string) => {
    let found: FixtureRecord | undefined;
    for (const r of records ?? []) if (compareTime(r.observedAt, at) <= 0) found = r;
    return found;
  };

  const stateOf = (res: Resource, namespace: string, name: string, at: string, rec?: FixtureRecord): ObjectState => {
    const coverage = coverageAt(res, at, fx.horizon);
    const st: ObjectState = { key: { resource: res.id, name }, at, existence: 'unknown', coverage };
    if (namespace) st.key.namespace = namespace;
    const latest = rec ?? latestAt(byKey.get(`${res.id}\u0000${namespace}\u0000${name}`), at);
    if (coverage.state === 'observed') {
      if (latest) st.version = versionOf(latest);
      if (latest && latest.type !== 'Deleted') {
        st.existence = 'present';
        st.object = latest.object;
      } else st.existence = 'absent';
    } else if (coverage.state === 'gap' && latest) {
      st.version = versionOf(latest);
      if (latest.type !== 'Deleted') st.object = latest.object;
    }
    return st;
  };

  const handlers: Record<string, (q: URLSearchParams) => unknown> = {
    resources: (): ResourcesResponse => ({ now: nowStr(), horizon: fx.horizon, resources: fx.resources }),

    state: (q) => {
      const res = resource(q);
      return stateOf(res, q.get('namespace') ?? '', nameParam(q), time(q, 'at', nowStr()));
    },

    object: (q) => {
      const res = resource(q);
      const ns = q.get('namespace') ?? '';
      const name = nameParam(q);
      const seq = Number(q.get('seq'));
      const rec = byKey.get(`${res.id}\u0000${ns}\u0000${name}`)?.find((r) => r.seq === seq);
      if (!rec) throw bad(`no record with seq ${q.get('seq')} for ${res.id}/${ns}/${name}`);
      const st = stateOf(res, ns, name, rec.observedAt, rec);
      // A specific record: show what it recorded even for deletes.
      st.object = rec.object;
      return st;
    },

    list: (q): ListResponse => {
      const res = resource(q);
      const at = time(q, 'at', nowStr());
      const ns = q.get('namespace') ?? '';
      const sel = q.get('labelSelector') ?? '';
      const includeDeleted = q.get('includeDeleted') === 'true';
      const coverage = coverageAt(res, at, fx.horizon);
      const items: ObjectState[] = [];
      if (coverage.state === 'observed' || coverage.state === 'gap') {
        for (const records of byKey.values()) {
          const first = records[0]!;
          if (first.resource !== res.id || (ns && first.namespace !== ns)) continue;
          const latest = latestAt(records, at);
          if (!latest) continue;
          if (latest.type === 'Deleted' && !(includeDeleted && coverage.state === 'observed')) continue;
          const labels = ((latest.object.metadata as { labels?: Record<string, string> } | undefined)?.labels ?? {});
          if (sel && !matchesSelector(labels, sel)) continue;
          items.push(stateOf(res, first.namespace, first.name, at, latest));
        }
      } else if (sel) {
        matchesSelector({}, sel); // still validate
      }
      items.sort((a, b) => (a.key.namespace ?? '').localeCompare(b.key.namespace ?? '') || a.key.name.localeCompare(b.key.name));
      return { at, coverage, items };
    },

    history: (q): HistoryResponse => {
      const res = resource(q);
      const ns = q.get('namespace') ?? '';
      const name = nameParam(q);
      const from = time(q, 'from', fx.horizon);
      const to = time(q, 'to', nowStr());
      const limit = Number(q.get('limit') ?? 1000);
      const all = (byKey.get(`${res.id}\u0000${ns}\u0000${name}`) ?? []).filter(
        (r) => compareTime(r.observedAt, from) >= 0 && compareTime(r.observedAt, to) <= 0,
      );
      const key = ns ? { resource: res.id, namespace: ns, name } : { resource: res.id, name };
      return { key, versions: all.slice(0, limit).map(versionOf), coverage: res.coverage, truncated: all.length > limit };
    },

    diff: (q): DiffResponse => {
      const res = resource(q);
      const ns = q.get('namespace') ?? '';
      const name = nameParam(q);
      const from = time(q, 'from');
      const to = time(q, 'to');
      if (compareTime(from, to) >= 0) throw bad('from must be before to');
      const a = stateOf(res, ns, name, from);
      const b = stateOf(res, ns, name, to);
      const render = (s: ObjectState) => (s.existence === 'present' && s.object ? toYaml(s.object, { sortKeys: true }) : '');
      const cov = coverageAt(res, from, fx.horizon);
      const complete = cov.state === 'observed' && (cov.until === undefined || compareTime(cov.until, to) >= 0);
      const recreated =
        a.existence === 'present' && b.existence === 'present' && !!a.version && !!b.version && a.version.uid !== b.version.uid;
      return { from: a, to: b, recreated, diff: unifiedDiff(render(a), render(b), from, to), complete };
    },
  };

  return function handle(url: URL): MockResponse {
    const m = /\/api\/v1\/([a-z]+)$/.exec(url.pathname);
    const h = m ? handlers[m[1]!] : undefined;
    if (!h) return { status: 404, body: { code: 'not_found', message: `no route for ${url.pathname}` } satisfies ErrorBody };
    try {
      return { status: 200, body: h(url.searchParams) };
    } catch (e) {
      if (e instanceof HttpError) return { status: e.status, body: { code: e.code, message: e.message } satisfies ErrorBody };
      return { status: 500, body: { code: 'internal', message: String(e) } satisfies ErrorBody };
    }
  };
}
