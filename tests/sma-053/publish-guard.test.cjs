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
  fs: { existsSync: (p) => holdFiles.has(p) },
};
// kill switch inputs, shared with the sandbox
const sandboxProcess = { env: {} };
const holdFiles = new Set();
const sandboxModule = { exports: {} };
vm.runInNewContext(js, {
  module: sandboxModule,
  exports: sandboxModule.exports,
  require: (name) => stubs[name] || {},
  Buffer,
  process: sandboxProcess,
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
  const calls = { post: 0, pending: 0, comment: 0, finalize: 0, lock: 0, plug: 0, error: [], state: [], notes: [] };
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
    // like threads: the reply's publish request runs inside the guard
    comment: async (_i, _p, _l, _t, mapped, _int, publishGuard) => {
      const publish = async () => {
        calls.comment++;
        return mapped.map((m) => ({ id: m.id, status: 'success', postId: 'r-' + m.id, releaseURL: 'https://example.invalid/r-' + m.id }));
      };
      return publishGuard ? publishGuard(publish) : publish();
    },
    // a provider that drops unknown keys, as threads does on the carousel step
    checkPostStatus: async (_t, pd) => ({
      status: 'ready',
      pendingData: { step: pd.step, containerId: pd.containerId },
    }),
    // like threads: only the publish request runs inside the guard
    finalizePost: async (_t, pd, _i, publishGuard) => {
      const publish = async () => {
        calls.finalize++;
        if (hooks.duringPublish) await hooks.duringPublish();
        return { id: 'live-' + pd.containerId };
      };
      const { id } = await (publishGuard ? publishGuard(publish) : publish());
      return { status: 'completed', postId: id, releaseURL: 'https://example.invalid/' + id };
    },
  };
  const hooks = {};
  const postService = {
    getPostById: async (id, org) =>
      rows[id] && rows[id].organizationId === org ? rows[id] : null,
    // in-memory stand-in for the row lock: hooks.beforeLock runs where a
    // concurrent change could still land; the real lock is tested in ./pg
    withPostChainLocked: async (org, ids, check, publish) => {
      calls.lock++;
      if (hooks.lockError) {
        const e = new Error('lock timeout'); e.name = 'PostChainLockError'; throw e;
      }
      if (hooks.beforeLock) await hooks.beforeLock();
      const chain = [];
      const visit = (id) => {
        const r = rows[id];
        if (!r || r.organizationId !== org || chain.some((c) => c.id === id)) return;
        chain.push({ id: r.id, state: r.state, deletedAt: r.deletedAt, parentPostId: r.parentPostId });
        if (r.parentPostId) visit(r.parentPostId);
      };
      ids.forEach(visit);
      check(chain);
      return publish();
    },
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
    { inAppNotification: async (org, subject, message) => calls.notes.push(subject + ' | ' + message) },
    { getSocialIntegration: () => provider },
    { processPlugs: async () => { calls.plug++; }, processInternalPlug: async () => { calls.plug++; } },
    {},
    {},
    { client: { getRawClient: () => ({ workflow: { start: async () => undefined } }) } },
    {}
  );
  return { activity, calls, rows, hooks };
}

const isGuardStop = (err) =>
  err instanceof BadBody &&
  err.type === 'bad_body' &&
  /SMA-053 guard/.test(err.message) &&
  /Nothing was published/.test(err.message);
const guardData = (id = 'root') => ({ step: 'container', containerId: 'c', __sma053Guard: { orgId: 'org', postIds: [id] } });

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
  ['errored root may reach the provider (the workflow retries after an unknown error)', async () => {
    const { activity, calls } = build({ root: post('root', 'ERROR') });
    await activity.postSocialPending(integration, [post('root', 'QUEUE')]);
    assert.equal(calls.pending, 1);
  }],
  ['published root may repeat (postNow chain)', async () => {
    const { activity, calls } = build({ root: post('root', 'PUBLISHED') });
    await activity.postSocialPending(integration, [post('root', 'QUEUE')]);
    assert.equal(calls.pending, 1);
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
    assert.equal(result.status, 'success');
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
  ['finalize runs the publish request inside the row-lock guard', async () => {
    const { activity, calls } = build({ root: post('root', 'QUEUE') });
    const result = await activity.finalizePost(integration, guardData());
    assert.equal(calls.lock, 1);
    assert.equal(calls.finalize, 1);
    assert.equal(result.status, 'completed');
  }],
  ['DRAFT that lands after the pre-check is caught under the lock', async () => {
    const { activity, calls, rows, hooks } = build({ root: post('root', 'QUEUE') });
    hooks.beforeLock = async () => { rows.root = { ...rows.root, state: 'DRAFT' }; };
    await assert.rejects(activity.finalizePost(integration, guardData()), isGuardStop);
    assert.equal(calls.finalize, 0);
  }],
  ['ancestor turned DRAFT under the lock stops a reply publish', async () => {
    const { activity, calls, rows, hooks } = build({ root: post('root', 'PUBLISHED'), reply: post('reply', 'QUEUE', 'root') });
    hooks.beforeLock = async () => { rows.root = { ...rows.root, state: 'DRAFT' }; };
    await assert.rejects(activity.finalizePost(integration, guardData('reply')), isGuardStop);
    assert.equal(calls.finalize, 0);
  }],
  ['lock failure sends nothing and stays retryable (not a bad_body stop)', async () => {
    const { activity, calls, hooks } = build({ root: post('root', 'QUEUE') });
    hooks.lockError = true;
    await assert.rejects(activity.finalizePost(integration, guardData()), (e) => !(e instanceof BadBody) && /not confirmed yet/.test(e.message));
    assert.equal(calls.finalize, 0);
  }],
  ['reply turned DRAFT under the lock is not published', async () => {
    const { activity, calls, rows, hooks } = build({ root: post('root', 'PUBLISHED'), reply: post('reply', 'QUEUE', 'root') });
    hooks.beforeLock = async () => { rows.reply = { ...rows.reply, state: 'DRAFT' }; };
    await assert.rejects(activity.postComment('live-root', undefined, integration, [post('reply', 'QUEUE')]), isGuardStop);
    assert.equal(calls.lock, 1);
    assert.equal(calls.comment, 0);
  }],
  ['queued reply is published inside the lock', async () => {
    const { activity, calls } = build({ root: post('root', 'PUBLISHED'), reply: post('reply', 'QUEUE', 'root') });
    const [r] = await activity.postComment('live-root', undefined, integration, [post('reply', 'QUEUE')]);
    assert.equal(calls.lock, 1);
    assert.equal(calls.comment, 1);
    assert.equal(r.status, 'success');
  }],
  ['kill switch also stops plugs', async () => {
    const { activity, calls } = build({});
    sandboxProcess.env.PUBLISH_HOLD = '1';
    try {
      await assert.rejects(activity.processPlug({ plugId: 'p', postId: 'x', delay: 0, totalRuns: 1, currentRun: 1 }), (e) => e instanceof BadBody && /PUBLISH_HOLD/.test(e.message));
      await assert.rejects(activity.processInternalPlug({ post: 'x', originalIntegration: 'a', integration: 'b', plugName: 'n', orgId: 'org', delay: 0, information: {} }), (e) => e instanceof BadBody);
    } finally {
      delete sandboxProcess.env.PUBLISH_HOLD;
    }
    assert.equal(calls.plug, 0);
    await activity.processPlug({ plugId: 'p', postId: 'x', delay: 0, totalRuns: 1, currentRun: 1 });
    assert.equal(calls.plug, 1);
  }],
  ['kill switch by environment stops every provider call', async () => {
    const { activity, calls } = build({ root: post('root', 'QUEUE') });
    sandboxProcess.env.PUBLISH_HOLD = 'true';
    try {
      await assert.rejects(activity.postSocialPending(integration, [post('root', 'QUEUE')]), (e) => isGuardStop(e) && /PUBLISH_HOLD/.test(e.message));
      await assert.rejects(activity.finalizePost(integration, guardData()), isGuardStop);
    } finally {
      delete sandboxProcess.env.PUBLISH_HOLD;
    }
    assert.equal(calls.pending + calls.finalize, 0);
  }],
  ['kill switch by file stops calls and clears without a restart', async () => {
    const { activity, calls } = build({ root: post('root', 'QUEUE') });
    holdFiles.add('/tmp/postiz-publish-hold');
    await assert.rejects(activity.finalizePost(integration, guardData()), isGuardStop);
    holdFiles.delete('/tmp/postiz-publish-hold');
    await activity.finalizePost(integration, guardData());
    assert.equal(calls.finalize, 1);
  }],
  ['alert when the post is a draft right after it went live', async () => {
    const { activity, calls, rows, hooks } = build({ root: post('root', 'QUEUE') });
    hooks.duringPublish = async () => { rows.root = { ...rows.root, state: 'DRAFT' }; };
    const result = await activity.finalizePost(integration, guardData());
    assert.equal(result.status, 'completed');
    assert.equal(calls.notes.length, 1);
    assert.match(calls.notes[0], /went live after it was changed to draft/);
    assert.match(calls.notes[0], /example\.invalid/);
  }],
  ['no alert when the post is still queued after publishing', async () => {
    const { activity, calls } = build({ root: post('root', 'QUEUE') });
    await activity.finalizePost(integration, guardData());
    assert.equal(calls.notes.length, 0);
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
