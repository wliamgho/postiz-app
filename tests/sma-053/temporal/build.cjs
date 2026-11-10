'use strict';
// Builds the SMA-053 Temporal bundle from the real sources: the V112 workflow
// (CommonJS, with three non-SDK imports redirected to ./shims) and the
// transpiled PostActivity. Needs the typescript package. Output: ./bundle
const ts = require('typescript');
const fs = require('node:fs');
const path = require('node:path');

const root = path.join(__dirname, '../../..');
const { execFileSync } = require('node:child_process');
const out = path.join(__dirname, 'bundle');
const opts = {
  compilerOptions: {
    module: ts.ModuleKind.CommonJS,
    target: ts.ScriptTarget.ES2020,
    esModuleInterop: true,
    experimentalDecorators: true,
  },
};
fs.mkdirSync(path.join(out, 'workflow'), { recursive: true });

const WF = 'apps/orchestrator/src/workflows/post-workflows/post.workflow.v1.1.2.ts';
const shimmed = (src) => {
  let wf = ts.transpileModule(src, opts).outputText;
  for (const mod of [
    'lodash',
    '@gitroom/nestjs-libraries/services/make.is',
    '@gitroom/nestjs-libraries/temporal/temporal.search.attribute',
  ]) {
    wf = wf.split(`require("${mod}")`).join('require("./shims")');
  }
  const left = [...wf.matchAll(/require\("([^"]+)"\)/g)]
    .map((m) => m[1])
    .filter((m) => !['@temporalio/workflow', '@temporalio/common', 'dayjs', './shims'].includes(m));
  if (left.length) throw new Error('unexpected workflow imports: ' + left.join(', '));
  return wf;
};
fs.writeFileSync(
  path.join(out, 'workflow/post.workflow.v1.1.2.js'),
  shimmed(fs.readFileSync(path.join(root, WF), 'utf8'))
);
// Older workflows for replay: the unmodified upstream workflow production ran
// before the SMA-053 marker, and the workflow production runs now (its
// compiled output is byte-identical to this commit's).
const BASE = process.env.SMA053_BASE_COMMIT || '0d486f75';
const PROD = process.env.SMA053_PROD_COMMIT || 'a740a809';
for (const [dirName, commit] of [['workflow-orig', BASE], ['workflow-prod', PROD]]) {
  fs.mkdirSync(path.join(out, dirName), { recursive: true });
  fs.writeFileSync(
    path.join(out, dirName, 'post.workflow.v1.1.2.js'),
    shimmed(execFileSync('git', ['show', `${commit}:${WF}`], { cwd: root, encoding: 'utf8' }))
  );
}
for (const d of ['workflow', 'workflow-orig', 'workflow-prod'])
fs.writeFileSync(
  path.join(out, d, 'shims.js'),
  `'use strict';
const { defineSearchAttributeKey } = require('@temporalio/common');
exports.capitalize = (s) => (s ? String(s)[0].toUpperCase() + String(s).slice(1).toLowerCase() : '');
exports.sortBy = (list, fn) => [...list].sort((a, b) => (fn(a) > fn(b) ? 1 : fn(a) < fn(b) ? -1 : 0));
exports.makeId = (n) => Array.from({ length: n }, () => 'x').join('');
exports.postId = defineSearchAttributeKey('postId', 'KEYWORD');
`
);

const activity = ts.transpileModule(
  fs.readFileSync(path.join(root, 'apps/orchestrator/src/activities/post.activity.ts'), 'utf8'),
  opts
).outputText;
fs.writeFileSync(path.join(out, 'post.activity.js'), activity);
console.log('bundle written to', out);
