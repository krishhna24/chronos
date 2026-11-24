// Classification of unified diff lines for rendering, and a small line diff
// (LCS) that produces unified diffs. The server computes real diffs; the
// generator is used by mock mode so fixtures behave like the server.

export type DiffLineKind = 'header' | 'hunk' | 'add' | 'del' | 'context' | 'meta';

export interface DiffLine {
  kind: DiffLineKind;
  text: string;
  /** Line numbers in the old/new file, where meaningful. */
  oldNo?: number;
  newNo?: number;
}

const HUNK = /^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@/;

/**
 * Classifies each line of a unified diff. `---`/`+++` are file headers only
 * before the first hunk; inside a hunk a line "--- x" is a removed line
 * whose content starts with "-- ".
 */
export function classifyDiff(diff: string): DiffLine[] {
  if (diff === '') return [];
  const lines = diff.replace(/\n$/, '').split('\n');
  const out: DiffLine[] = [];
  let inHunk = false;
  let oldNo = 0;
  let newNo = 0;
  for (const text of lines) {
    const h = HUNK.exec(text);
    if (h) {
      inHunk = true;
      oldNo = Number(h[1]);
      newNo = Number(h[3]);
      out.push({ kind: 'hunk', text });
      continue;
    }
    if (!inHunk) {
      out.push({ kind: text.startsWith('--- ') || text.startsWith('+++ ') ? 'header' : 'meta', text });
      continue;
    }
    switch (text[0]) {
      case '+':
        out.push({ kind: 'add', text, newNo: newNo++ });
        break;
      case '-':
        out.push({ kind: 'del', text, oldNo: oldNo++ });
        break;
      case ' ':
      case undefined: // some generators drop the space on empty context lines
        out.push({ kind: 'context', text, oldNo: oldNo++, newNo: newNo++ });
        break;
      case '\\':
        out.push({ kind: 'meta', text });
        break;
      default:
        // Not part of the hunk grammar (e.g. a "diff --git" line of a
        // following file): treat as metadata and leave the hunk.
        inHunk = false;
        out.push({ kind: text.startsWith('--- ') || text.startsWith('+++ ') ? 'header' : 'meta', text });
    }
  }
  return out;
}

/** Counts added and removed lines. */
export function diffStat(lines: readonly DiffLine[]): { added: number; removed: number } {
  let added = 0;
  let removed = 0;
  for (const l of lines) {
    if (l.kind === 'add') added++;
    else if (l.kind === 'del') removed++;
  }
  return { added, removed };
}

type Op = { op: ' ' | '-' | '+'; line: string };

function lineOps(a: readonly string[], b: readonly string[]): Op[] {
  const n = a.length;
  const m = b.length;
  // lcs[i][j] = LCS length of a[i:], b[j:]
  const lcs: number[][] = Array.from({ length: n + 1 }, () => new Array<number>(m + 1).fill(0));
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      lcs[i]![j] = a[i] === b[j] ? lcs[i + 1]![j + 1]! + 1 : Math.max(lcs[i + 1]![j]!, lcs[i]![j + 1]!);
    }
  }
  const ops: Op[] = [];
  let i = 0;
  let j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j]) {
      ops.push({ op: ' ', line: a[i]! });
      i++;
      j++;
    } else if (lcs[i + 1]![j]! >= lcs[i]![j + 1]!) {
      ops.push({ op: '-', line: a[i++]! });
    } else {
      ops.push({ op: '+', line: b[j++]! });
    }
  }
  while (i < n) ops.push({ op: '-', line: a[i++]! });
  while (j < m) ops.push({ op: '+', line: b[j++]! });
  return ops;
}

/** Unified diff of two texts with `context` lines around changes; empty
 * string when equal. */
export function unifiedDiff(oldText: string, newText: string, oldName: string, newName: string, context = 3): string {
  const split = (s: string) => (s === '' ? [] : s.replace(/\n$/, '').split('\n'));
  const ops = lineOps(split(oldText), split(newText));
  if (ops.every((o) => o.op === ' ')) return '';

  // Index ranges of ops to include, merged when contexts overlap.
  const ranges: Array<[number, number]> = [];
  ops.forEach((o, idx) => {
    if (o.op === ' ') return;
    const lo = Math.max(0, idx - context);
    const hi = Math.min(ops.length - 1, idx + context);
    const last = ranges[ranges.length - 1];
    if (last && lo <= last[1] + 1) last[1] = Math.max(last[1], hi);
    else ranges.push([lo, hi]);
  });

  const out = [`--- ${oldName}`, `+++ ${newName}`];
  let oldLine = 1;
  let newLine = 1;
  let pos = 0;
  for (const [lo, hi] of ranges) {
    for (; pos < lo; pos++) {
      oldLine++;
      newLine++;
    }
    const body: string[] = [];
    let oldCount = 0;
    let newCount = 0;
    const oldStart = oldLine;
    const newStart = newLine;
    for (; pos <= hi; pos++) {
      const o = ops[pos]!;
      body.push(o.op + o.line);
      if (o.op !== '+') {
        oldCount++;
        oldLine++;
      }
      if (o.op !== '-') {
        newCount++;
        newLine++;
      }
    }
    const fmt = (start: number, count: number) => (count === 0 ? `${start - 1},0` : count === 1 ? `${start}` : `${start},${count}`);
    out.push(`@@ -${fmt(oldStart, oldCount)} +${fmt(newStart, newCount)} @@`, ...body);
  }
  return out.join('\n') + '\n';
}
