import { describe, expect, it } from 'vitest';
import { classifyDiff, diffStat, unifiedDiff } from './diff';

const sample = [
  '--- 2026-11-24T14:00:00Z',
  '+++ 2026-11-24T14:05:00Z',
  '@@ -3,4 +3,4 @@',
  ' spec:',
  '-  replicas: 3',
  '+  replicas: 4',
  '--- this removed line starts with dashes',
  '+++ this added line starts with pluses',
  '',
  '\\ No newline at end of file',
  '',
].join('\n');

describe('classifyDiff', () => {
  const lines = classifyDiff(sample);

  it('classifies headers, hunks, additions, deletions and context', () => {
    expect(lines.map((l) => l.kind)).toEqual(['header', 'header', 'hunk', 'context', 'del', 'add', 'del', 'add', 'context', 'meta']);
  });
  it('treats ---/+++ inside a hunk as content, not headers', () => {
    expect(lines[6]).toMatchObject({ kind: 'del', text: '--- this removed line starts with dashes' });
    expect(lines[7]).toMatchObject({ kind: 'add' });
  });
  it('tracks line numbers from the hunk header', () => {
    expect(lines[3]).toMatchObject({ oldNo: 3, newNo: 3 });
    expect(lines[4]).toMatchObject({ oldNo: 4 });
    expect(lines[5]).toMatchObject({ newNo: 4 });
    expect(lines[6]).toMatchObject({ oldNo: 5 });
    expect(lines[8]).toMatchObject({ oldNo: 6, newNo: 6 });
  });
  it('handles hunk headers without counts', () => {
    expect(classifyDiff('--- a\n+++ b\n@@ -1 +1 @@\n-x\n+y\n').map((l) => l.kind)).toEqual(['header', 'header', 'hunk', 'del', 'add']);
  });
  it('returns nothing for an empty diff', () => {
    expect(classifyDiff('')).toEqual([]);
  });
  it('counts changes', () => {
    expect(diffStat(lines)).toEqual({ added: 2, removed: 2 });
  });
});

describe('unifiedDiff', () => {
  it('is empty for equal input', () => {
    expect(unifiedDiff('a\nb\n', 'a\nb\n', 'x', 'y')).toBe('');
  });
  it('produces a hunk with context that classifyDiff understands', () => {
    const a = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'].join('\n') + '\n';
    const b = ['a', 'b', 'c', 'd', 'E', 'f', 'g', 'h', 'i'].join('\n') + '\n';
    const d = unifiedDiff(a, b, 'from', 'to');
    expect(d).toBe(['--- from', '+++ to', '@@ -2,7 +2,7 @@', ' b', ' c', ' d', '-e', '+E', ' f', ' g', ' h', ''].join('\n'));
    expect(diffStat(classifyDiff(d))).toEqual({ added: 1, removed: 1 });
  });
  it('renders creation from empty and deletion to empty', () => {
    expect(unifiedDiff('', 'x\ny\n', 'a', 'b')).toBe('--- a\n+++ b\n@@ -0,0 +1,2 @@\n+x\n+y\n');
    expect(unifiedDiff('x\n', '', 'a', 'b')).toBe('--- a\n+++ b\n@@ -1 +0,0 @@\n-x\n');
  });
  it('splits distant changes into separate hunks', () => {
    const a = Array.from({ length: 20 }, (_, i) => `l${i}`).join('\n');
    const b = a.replace('l1\n', 'L1\n').replace('l18', 'L18');
    const hunks = classifyDiff(unifiedDiff(a, b, 'a', 'b')).filter((l) => l.kind === 'hunk');
    expect(hunks.map((h) => h.text)).toEqual(['@@ -1,5 +1,5 @@', '@@ -16,5 +16,5 @@']);
  });
});
