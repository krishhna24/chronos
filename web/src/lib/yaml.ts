// A minimal JSON -> YAML renderer for displaying Kubernetes objects. It only
// emits (never parses) YAML, so it needs no dependency: block mappings and
// sequences, with strings quoted whenever a plain scalar could be read back
// as something else. Keys keep their JSON order unless sortKeys is set
// (the server's diff renders with sorted keys).

const PLAIN_SAFE = /^[A-Za-z0-9_./][A-Za-z0-9_./ :@()+%=-]*$/;
const RESERVED = /^(?:~|null|Null|NULL|true|True|TRUE|false|False|FALSE|y|Y|yes|Yes|YES|n|N|no|No|NO|on|On|ON|off|Off|OFF|\.nan|\.NaN|\.NAN|[-+]?\.(?:inf|Inf|INF))$/;
const NUMBERISH = /^[-+]?(?:\d[\d_]*(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?$|^0[xob][0-9a-fA-F_]+$|^\d+(?::\d+)+$/;

export function yamlString(s: string): string {
  if (
    s !== '' &&
    PLAIN_SAFE.test(s) &&
    !RESERVED.test(s) &&
    !NUMBERISH.test(s) &&
    !s.endsWith(' ') &&
    !s.endsWith(':') &&
    !s.includes(': ') &&
    !s.includes(' #')
  ) {
    return s;
  }
  return JSON.stringify(s);
}

function scalar(v: unknown): string | null {
  if (v === null || v === undefined) return 'null';
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  if (typeof v === 'number') return Number.isFinite(v) ? String(v) : JSON.stringify(String(v));
  if (typeof v === 'string') return v.includes('\n') ? null : yamlString(v);
  if (Array.isArray(v)) return v.length === 0 ? '[]' : null;
  if (typeof v === 'object') return Object.keys(v as object).length === 0 ? '{}' : null;
  return JSON.stringify(String(v));
}

function blockString(s: string, indent: string): string {
  const chomp = s.endsWith('\n') ? (s.endsWith('\n\n') ? '+' : '') : '-';
  const body = s.replace(/\n$/, '').split('\n');
  // An indentation indicator is required when the first line starts with a space.
  const ind = body[0]?.startsWith(' ') ? '2' : '';
  return `|${ind}${chomp}\n` + body.map((l) => (l === '' ? '' : indent + l)).join('\n');
}

function render(v: unknown, indent: string, sortKeys: boolean): string {
  if (Array.isArray(v)) {
    return v
      .map((item) => {
        const s = scalar(item);
        if (s !== null) return `${indent}- ${s}`;
        if (typeof item === 'string') return `${indent}- ${blockString(item, indent + '  ')}`;
        const inner = render(item, indent + '  ', sortKeys);
        // Put the first line of the nested block on the dash line.
        return `${indent}- ${inner.slice(indent.length + 2)}`;
      })
      .join('\n');
  }
  const obj = v as Record<string, unknown>;
  const keys = Object.keys(obj);
  if (sortKeys) keys.sort();
  return keys
    .map((k) => {
      const key = yamlString(k);
      const val = obj[k];
      const s = scalar(val);
      if (s !== null) return `${indent}${key}: ${s}`;
      if (typeof val === 'string') return `${indent}${key}: ${blockString(val, indent + '  ')}`;
      // Kubernetes style: sequences under a mapping key are not indented.
      const childIndent = Array.isArray(val) ? indent : indent + '  ';
      return `${indent}${key}:\n${render(val, childIndent, sortKeys)}`;
    })
    .join('\n');
}

/** Renders a JSON value as a YAML document (no leading "---"). */
export function toYaml(v: unknown, opts: { sortKeys?: boolean } = {}): string {
  const s = scalar(v);
  if (s !== null) return s + '\n';
  if (typeof v === 'string') return blockString(v, '  ') + '\n';
  return render(v, '', opts.sortKeys ?? false) + '\n';
}
