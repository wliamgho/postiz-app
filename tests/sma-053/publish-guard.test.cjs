'use strict';
// SMA-053 publish guard: transpiles the real post.activity.ts and runs it
// against fake services. No Temporal, database or provider is contacted.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const ts = require('typescript');

const file = path.join(
  __dirname,
  '../../apps/orchestrator/src/activities/post.activity.ts'
);
const js = ts.transpileModule(fs.readFileSync(file, 'utf8'), {
  compilerOptions: {
    module: ts.ModuleKind.CommonJS,
    target: ts.ScriptTarget.ES2020,
    experimentalDecorators: true,
  },
}).outputText;

class BadBody extends Error {
  constructor(identifier, json, body, message = '') {
    super(message);
    this.type = 'bad_body';
    this.identifier = identifier;
  }
}
const decorator = () => () => undefined;
const stubs = {
  '@nestjs/common': { Injectable: decorator },
  'nestjs-temporal-core': {
    Activity: decorator,
    ActivityMethod: decorator,
    TemporalService: class {},
  },
  '@prisma/client': {
    State: { QUEUE: 'QUEUE', PUBLISHED: 'PUBLISHED', ERROR: 'ERROR', DRAFT: 'DRAFT' },
  },
  '@gitroom/helpers/utils/strip.html.validation': {
    stripHtmlValidation: (_e, content) => content,
  },
  '@gitroom/nestjs-libraries/temporal/temporal.heartbeat': {
    setHeartbeatDetails: () => undefined,
    withHeartbeat: (fn) => fn(),
  },
  '@gitroom/nestjs-libraries/integrations/social.abstract': {
    BadBody,
    Disconnect: class Disconnect extends Error {},
    PublishedWithError: class PublishedWithError extends Error {},
  },
  '@temporalio/common': { TypedSearchAttributes: class {} },
  '@gitroom/nestjs-libraries/temporal/temporal.search.attribute': {
    organizationId: 'organizationId',
    postId: 'postId',
  },
};
const sandboxModule = { exports: {} };
vm.runInNewContext(js, {
  module: sandboxModule,
  exports: sandboxModule.exports,
  require: (name) => stubs[name] || {},
  Buffer,
  process: { env: {} },
  JSON,
  Promise,
  Date,
  Math,
  Error,
});
const { PostActivity } = sandboxModule.exports;

const integration = {
  providerIdentifier: 'threads',
  organizationId: 'org',
  internalId: 'internal',
  token: 'token',
};
const post = (id, state, parentPostId = null, extra = {}) => ({
  id,
  state,
  parentPostId,
  organizationId: 'org',
  deletedAt: null,
  content: id,
  settings: '{}',
  image: '[]',
  ...extra,
});

function build(rows) {
  const calls = { post: 0, pending: 0, comment: 0, finalize: 0, error: [], state: [] };
  const provider = {
    editor: 'normal',
    postPending: async (_i, _t, mapped) => {
      calls.pending++;
      return mapped.map((m) => ({
        id: m.id,
        status: 'pending',
        pendingData: { step: 'container', containerId: 'c-' + m.id },
      }));
    },
    post: async (_i, _t, mapped) => {
      calls.post++;
      return mapped.map((m) => ({ id: m.id, status: 'success', postId: 'p-' + m.id }));
    },
    comment: async (_i, _p, _l, _t, mapped) => {
      calls.comment++;
      return mapped.map((m) => ({
        id: m.id,
        status: 'pending',
        pendingData: { step: 'container', containerId: 'c-' + m.id },
      }));
    },
    // a provider that drops unknown keys, as threads does on the carousel step
    checkPostStatus: async (_t, pd) => ({
      status: 'ready',
      pendingData: { step: pd.step, containerId: pd.containerId },
    }),
    finalizePost: async (_t, pd) => {
      calls.finalize++;
      return { status: 'completed', postId: 'live-' + pd.containerId };
    },
  };
  const postService = {
    getPostById: async (id, org) =>
      rows[id] && rows[id].organizationId === org ? rows[id] : null,
    updateTags: async (_org, posts) => posts,
    updateMedia: async () => [],
    markErrorUnlessSettled: async (id, err) => {
      calls.error.push(id);
      return !['DRAFT', 'PUBLISHED'].includes(rows[id]?.state);
    },
    changeState: async (id, state) => calls.state.push([id, state]),
  };
  const activity = new PostActivity(
    postService,
    {},
    { getSocialIntegration: () => provider },
    {},
    {},
    {},
    { client: { getRawClient: () => ({ workflow: { start: async () => undefined } }) } },
    {}
  );
  return { activity, calls, rows };
}

const isGuardStop = (err) =>
  err instanceof BadBody && err.type === 'bad_body' && /SMA-053 guard/.test(err.message);

const cases = [
  ['queued root reaches provider and carries guard', async () => {
    const { activity, calls } = build({ root: post('root', 'QUEUE') });
    const [result] = await activity.postSocialPending(integration, [post('root', 'QUEUE')]);
    assert.equal(calls.pending, 1);
    // objects come from the vm realm: compare their JSON
    assert.equal(JSON.stringify(result.pendingData.__sma053Guard), JSON.stringify({ orgId: 'org', postIds: ['root'] }));
  }],
  ['draft root never reaches provider', async () => {
    const { activity, calls } = build({ root: post('root', 'DRAFT') });
    await assert.rejects(activity.postSocialPending(integration, [post('root', 'QUEUE')]), isGuardStop);
    assert.equal(calls.pending, 0);
  }],
  ['deleted root never reaches provider (blocking post path)', async () => {
    const { activity, calls } = build({ root: post('root', 'QUEUE', null, { deletedAt: new Date() }) });
    await assert.rejects(activity.postSocial(integration, [post('root', 'QUEUE')]), isGuardStop);
    assert.equal(calls.post, 0);
  }],
  ['post from another organization is rejected', async () => {
    const { activity, calls } = build({ root: post('root', 'QUEUE', null, { organizationId: 'other' }) });
    await assert.rejects(activity.postSocialPending(integration, [post('root', 'QUEUE')]), isGuardStop);
    assert.equal(calls.pending, 0);
  }],
  ['reply is rejected when the root became draft', async () => {
    const { activity, calls } = build({ root: post('root', 'DRAFT'), reply: post('reply', 'QUEUE', 'root') });
    await assert.rejects(activity.postComment('live-root', undefined, integration, [post('reply', 'QUEUE')]), isGuardStop);
    assert.equal(calls.comment, 0);
  }],
  ['reply is rejected when the reply itself became draft', async () => {
    const { activity, calls } = build({ root: post('root', 'PUBLISHED'), reply: post('reply', 'DRAFT', 'root') });
    await assert.rejects(activity.postComment('live-root', undefined, integration, [post('reply', 'QUEUE')]), isGuardStop);
    assert.equal(calls.comment, 0);
  }],
  ['queued reply under published root reaches provider', async () => {
    const { activity, calls } = build({ root: post('root', 'PUBLISHED'), reply: post('reply', 'QUEUE', 'root') });
    const [result] = await activity.postComment('live-root', undefined, integration, [post('reply', 'QUEUE')]);
    assert.equal(calls.comment, 1);
    assert.deepEqual(result.pendingData.__sma053Guard.postIds, ['reply']);
  }],
  ['status check keeps guard when provider returns new pendingData', async () => {
    const { activity } = build({ root: post('root', 'QUEUE') });
    const guard = { orgId: 'org', postIds: ['root'] };
    const result = await activity.checkPostStatus(integration, { step: 'container', containerId: 'c', __sma053Guard: guard });
    assert.deepEqual(result.pendingData.__sma053Guard, guard);
  }],
  ['finalize publishes a still-queued post', async () => {
    const { activity, calls } = build({ root: post('root', 'QUEUE') });
    const result = await activity.finalizePost(integration, { step: 'container', containerId: 'c', __sma053Guard: { orgId: 'org', postIds: ['root'] } });
    assert.equal(calls.finalize, 1);
    assert.equal(result.status, 'completed');
  }],
  ['finalize is stopped when the post became draft after the container', async () => {
    const { activity, calls } = build({ root: post('root', 'DRAFT') });
    await assert.rejects(activity.finalizePost(integration, { step: 'container', containerId: 'c', __sma053Guard: { orgId: 'org', postIds: ['root'] } }), isGuardStop);
    assert.equal(calls.finalize, 0);
  }],
  ['legacy pendingData without guard fails closed', async () => {
    const { activity, calls } = build({ root: post('root', 'QUEUE') });
    await assert.rejects(activity.finalizePost(integration, { step: 'container', containerId: 'c' }), isGuardStop);
    assert.equal(calls.finalize, 0);
  }],
  ['database failure in guard propagates and blocks provider', async () => {
    const { activity, calls } = build({});
    activity._postService.getPostById = async () => { throw new Error('database unavailable'); };
    await assert.rejects(activity.postSocialPending(integration, [post('root', 'QUEUE')]), /database unavailable/);
    assert.equal(calls.pending, 0);
  }],
  ['ERROR goes through the conditional write, other states do not', async () => {
    const { activity, calls } = build({ root: post('root', 'DRAFT') });
    await activity.changeState('root', 'ERROR', 'x', []);
    await activity.changeState('root', 'PUBLISHED');
    assert.deepEqual(calls.error, ['root']);
    assert.deepEqual(calls.state, [['root', 'PUBLISHED']]);
  }],
];

(async () => {
  let failed = 0;
  for (const [name, fn] of cases) {
    try {
      await fn();
      console.log('PASS', name);
    } catch (err) {
      failed++;
      console.error('FAIL', name, '\n', err && err.stack);
    }
  }
  console.log(`${cases.length - failed}/${cases.length} passed`);
  process.exitCode = failed ? 1 : 0;
})();
