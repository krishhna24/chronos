import { describe, expect, it } from 'vitest';
import { toYaml, yamlString } from './yaml';

describe('toYaml', () => {
  it('renders a Kubernetes-style object', () => {
    const obj = {
      apiVersion: 'apps/v1',
      kind: 'Deployment',
      metadata: { name: 'api', labels: { app: 'api' }, annotations: {} },
      spec: {
        replicas: 3,
        paused: false,
        template: { spec: { containers: [{ name: 'api', image: 'registry.example.com/api:1.5.0', args: ['--port', '8080'] }] } },
      },
      status: null,
    };
    expect(toYaml(obj)).toBe(
      [
        'apiVersion: apps/v1',
        'kind: Deployment',
        'metadata:',
        '  name: api',
        '  labels:',
        '    app: api',
        '  annotations: {}',
        'spec:',
        '  replicas: 3',
        '  paused: false',
        '  template:',
        '    spec:',
        '      containers:',
        '      - name: api',
        '        image: registry.example.com/api:1.5.0',
        '        args:',
        '        - "--port"',
        '        - "8080"',
        'status: null',
        '',
      ].join('\n'),
    );
  });

  it('quotes strings that would be misread', () => {
    for (const s of ['true', 'no', 'null', '~', '8080', '1.5', '0x1F', '1e3', '', ' lead', 'trail ', 'a: b', 'x #c', '-dash', '*star', '{a}', 'key:']) {
      expect(yamlString(s)).toBe(JSON.stringify(s));
    }
    for (const s of ['api', 'apps/v1', 'registry.example.com/api:1.5.0', '100m', '25%', 'a=b', 'deployment.kubernetes.io/revision']) {
      expect(yamlString(s)).toBe(s);
    }
  });

  it('renders multi-line strings as literal blocks', () => {
    expect(toYaml({ data: { 'config.yaml': 'server:\n  port: 8080\n' } })).toBe('data:\n  config.yaml: |\n    server:\n      port: 8080\n');
    expect(toYaml({ s: 'a\nb' })).toBe('s: |-\n  a\n  b\n');
  });

  it('sorts keys when asked', () => {
    expect(toYaml({ b: 1, a: { d: 1, c: 2 } }, { sortKeys: true })).toBe('a:\n  c: 2\n  d: 1\nb: 1\n');
  });

  it('handles nested and empty sequences', () => {
    expect(toYaml({ a: [], b: [[1, 2], { x: 1 }] })).toBe('a: []\nb:\n- - 1\n  - 2\n- x: 1\n');
  });
});
