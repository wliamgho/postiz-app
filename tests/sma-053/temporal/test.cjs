'use strict';
// SMA-053: the V112 workflow compiled from source runs on a real Temporal dev
// server with the real (transpiled) PostActivity, backed by a real PostgreSQL
// table and a fake Threads-like provider (container first, publish at
// finalize). Synthetic data, network none. Never point this at live systems.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const { Client: Pg, Pool } = require('pg');
const { Worker, NativeConnection } = require('@temporalio/worker');
const { Connection, Client } = require('@temporalio/client');
const { ApplicationFailure } = require('@temporalio/common');

const dir = __dirname;
const workflowsPath = path.join(dir, 'bundle/workflow/post.workflow.v1.1.2.js');

class BadBody extends ApplicationFailure {
  constructor(_identifier, _json, _body, message = '') {
    super(message, 'bad_body', true);
  }
}
const decorator = () => () => undefined;
const stubs = {
  '@nestjs/common': { Injectable: decorator },
  'nestjs-temporal-core': { Activity: decorator, ActivityMethod: decorator, TemporalService: class {} },
  '@prisma/client': { State: { QUEUE: 'QUEUE', PUBLISHED: 'PUBLISHED', ERROR: 'ERROR', DRAFT: 'DRAFT' } },
  '@gitroom/helpers/utils/strip.html.validation': { stripHtmlValidation: (_e, c) => c },
  '@gitroom/nestjs-libraries/temporal/temporal.heartbeat': { setHeartbeatDetails: () => undefined, withHeartbeat: (fn) => fn() },
  '@gitroom/nestjs-libraries/integrations/social.abstract': {
    BadBody,
    Disconnect: class Disconnect extends Error {},
    PublishedWithError: class PublishedWithError extends Error {},
  },
  '@temporalio/common': { TypedSearchAttributes: class {} },
  '@gitroom/nestjs-libraries/temporal/temporal.search.attribute': { organizationId: 'organizationId', postId: 'postId' },
  fs,
};
const mod = { exports: {} };
vm.runInThisContext(`(function (module, exports, require) {\n${fs.readFileSync(path.join(dir, 'bundle/post.activity.js'), 'utf8')}\n})`)(
  mod,
  mod.exports,
  (name) => stubs[name] || {}
);
const { PostActivity } = mod.exports;

const ORG = 'org-synthetic';
const integration = {
  id: 'int-synthetic',
  organizationId: ORG,
  providerIdentifier: 'threads',
  internalId: 'internal',
  token: 'token',
  name: 'synthetic',
  refreshNeeded: false,
  disabled: false,
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  const db = new Pg({ host: '127.0.0.1', port: 5432, user: 'postgres', password: 'test', database: 'guard' });
  await db.connect();
  // separate connections for the publish lock and for concurrent owner writes
  const pool = new Pool({ host: '127.0.0.1', port: 5432, user: 'postgres', password: 'test', database: 'guard', max: 6 });
  await db.query(`CREATE TABLE posts (
    id text primary key, state text not null, parent text, deleted_at timestamptz,
    publish_date timestamptz not null, position int not null default 0, error text)`);

  const row = async (id) => (await db.query('SELECT * FROM posts WHERE id=$1', [id])).rows[0];
  const asPost = (r) =>
    r && {
      id: r.id,
      state: r.state,
      organizationId: ORG,
      parentPostId: r.parent,
      deletedAt: r.deleted_at,
      publishDate: r.publish_date.toISOString(),
      content: r.id,
      settings: '{}',
      image: '[]',
      delay: 0,
      integration,
    };
  const setState = (id, state) => db.query('UPDATE posts SET state=$2 WHERE id=$1', [id, state]);

  let counts, hooks;
  const reset = () => {
    counts = { container: 0, comment: 0, finalize: 0, notifications: [], alerts: [] };
    hooks = {};
  };
  const provider = {
    editor: 'normal',
    postPending: async (_i, _t, mapped) => {
      counts.container++;
      return mapped.map((m) => ({ id: m.id, status: 'pending', pendingData: { step: 'container', containerId: 'c-' + m.id } }));
    },
    comment: async (_i, _p, _l, _t, mapped) => {
      counts.comment++;
      return mapped.map((m) => ({ id: m.id, status: 'pending', pendingData: { step: 'container', containerId: 'c-' + m.id } }));
    },
    // returns a fresh pendingData object, as threads does on the carousel step
    checkPostStatus: async (_t, pd) => {
      if (hooks.onCheck) await hooks.onCheck(pd);
      return { status: 'ready', pendingData: { step: pd.step, containerId: pd.containerId } };
    },
    // like threads: only the publish request runs inside the publish guard
    finalizePost: async (_t, pd, _i, publishGuard) => {
      const publish = async () => {
        counts.finalize++;
        if (hooks.onFinalize) await hooks.onFinalize(pd);
        return { id: 'live-' + pd.containerId };
      };
      const { id } = await (publishGuard ? publishGuard(publish) : publish());
      return { status: 'completed', postId: id, releaseURL: 'https://example.invalid/' + pd.containerId };
    },
  };
  const postService = {
    getPostById: async (id, org) => (org === ORG ? asPost(await row(id)) : null),
    updateTags: async (_org, posts) => posts,
    updateMedia: async () => [],
    updatePost: async (id) => setState(id, 'PUBLISHED'),
    changeState: async (id, state, err) => db.query('UPDATE posts SET state=$2, error=$3 WHERE id=$1', [id, state, err ? String(err) : null]),
    // same predicate as PostsRepository.markErrorUnlessSettled (tested on Prisma in ../pg)
    markErrorUnlessSettled: async (id, err) =>
      (await db.query("UPDATE posts SET state='ERROR', error=$2 WHERE id=$1 AND state NOT IN ('DRAFT','PUBLISHED')", [id, err ? String(err).slice(0, 500) : null])).rowCount > 0,
    // same SQL and phases as PostsRepository.withPostChainLocked (the Prisma
    // version is tested in ../pg), on this test's table
    withPostChainLocked: async (org, ids, check, publish, opts = {}) => {
      const c = await pool.connect();
      const st = { phase: 'lock' };
      try {
        await c.query('BEGIN');
        await c.query(`SET LOCAL lock_timeout = '${Math.floor(opts.lockTimeoutMs || 3000)}ms'`);
        const { rows } = await c.query(
          `WITH RECURSIVE chain AS (
             SELECT id, parent, 1 AS depth FROM posts WHERE id = ANY($1::text[])
             UNION
             SELECT p.id, p.parent, ch.depth + 1 FROM posts p JOIN chain ch ON p.id = ch.parent WHERE ch.depth < 50)
           SELECT p.id, p.state, p.deleted_at AS "deletedAt", p.parent AS "parentPostId"
           FROM posts p WHERE p.id IN (SELECT id FROM chain) ORDER BY p.id FOR SHARE OF p`,
          [ids]
        );
        st.phase = 'check';
        check(org === ORG ? rows : []);
        st.phase = 'publish';
        try {
          st.outcome = { ok: true, value: await publish() };
        } catch (error) {
          st.outcome = { ok: false, error };
        }
        await c.query('COMMIT');
      } catch (e) {
        await c.query('ROLLBACK').catch(() => undefined);
        if (!st.outcome) {
          if (st.phase === 'lock') {
            const err = new Error('could not lock the post rows: ' + e.message);
            err.name = 'PostChainLockError';
            throw err;
          }
          throw e;
        }
      } finally {
        c.release();
      }
      if (!st.outcome.ok) throw st.outcome.error;
      return st.outcome.value;
    },
  };
  const activity = new PostActivity(
    postService,
    { inAppNotification: async (_o, subject) => counts.alerts.push(subject) },
    { getSocialIntegration: () => provider },
    {},
    {},
    {},
    { client: { getRawClient: () => ({ workflow: { start: async () => undefined } }) } },
    {}
  );
  const real = (name) => (...args) => activity[name](...args);
  const activities = {
    // workflow-side reads from the same table (not part of the guard)
    getPost: async (_org, id) => asPost(await row(id)),
    getPostsList: async (_org, id) =>
      (await db.query('SELECT * FROM posts WHERE id=$1 OR parent=$1 ORDER BY position', [id])).rows.map(asPost),
    isCommentable: real('isCommentable'),
    // hooks.before* run at the activity boundary, i.e. after the workflow's own
    // state check and immediately before the real (guarded) activity
    postSocialPending: async (...a) => { if (hooks.beforePost) await hooks.beforePost(); return activity.postSocialPending(...a); },
    postComment: async (...a) => { if (hooks.beforeComment) await hooks.beforeComment(); return activity.postComment(...a); },
    checkPostStatus: real('checkPostStatus'),
    finalizePost: async (...a) => { if (hooks.beforeFinalize) await hooks.beforeFinalize(); return activity.finalizePost(...a); },
    changeState: real('changeState'),
    updatePost: real('updatePost'),
    inAppNotification: async (_o, subject) => counts.notifications.push(subject),
    sendWebhooks: async () => undefined,
    internalPlugs: async () => [],
    globalPlugs: async () => [],
  };

  const native = await NativeConnection.connect({ address: '127.0.0.1:7233' });
  const connection = await Connection.connect({ address: '127.0.0.1:7233' });
  const client = new Client({ connection });
  const queue = 'sma053-guard';
  const newWorker = () => Worker.create({ connection: native, taskQueue: queue, workflowsPath, activities });

  const seed = async (posts) => {
    await db.query('DELETE FROM posts');
    for (const [i, p] of posts.entries()) {
      await db.query('INSERT INTO posts (id, state, parent, publish_date, position) VALUES ($1,$2,$3,$4,$5)', [
        p.id, p.state, p.parent || null, new Date(Date.now() + (p.inMs || 0)), i,
      ]);
    }
  };
  const start = (name) =>
    client.workflow.start('postWorkflowV112', {
      taskQueue: queue,
      workflowId: `sma053-guard-${name}-${Date.now()}`,
      args: [{ taskQueue: queue, postId: 'root', organizationId: ORG }],
    });
  const waitForTimer = async (handle) => {
    for (let i = 0; i < 200; i++) {
      const h = await handle.fetchHistory();
      if (h.events.some((e) => e.timerStartedEventAttributes)) return;
      await sleep(50);
    }
    throw new Error('no durable timer observed');
  };

  const cases = [
    ['control: queued root is published once', async () => {
      await seed([{ id: 'root', state: 'QUEUE', inMs: 1500 }]);
      const worker = await newWorker();
      await worker.runUntil(async () => (await start('control')).result());
      assert.equal(counts.container, 1);
      assert.equal(counts.finalize, 1);
      assert.equal((await row('root')).state, 'PUBLISHED');
    }],
    ['root set to DRAFT during the scheduling sleep: nothing reaches the provider', async () => {
      await seed([{ id: 'root', state: 'QUEUE', inMs: 4000 }]);
      const worker = await newWorker();
      await worker.runUntil(async () => {
        const handle = await start('draft-sleep');
        await waitForTimer(handle);
        await setState('root', 'DRAFT');
        await handle.result();
      });
      assert.equal(counts.container + counts.finalize, 0);
      assert.equal((await row('root')).state, 'DRAFT');
    }],
    ['worker restart: history resumed after DRAFT does not publish', async () => {
      await seed([{ id: 'root', state: 'QUEUE', inMs: 5000 }]);
      let handle;
      const first = await newWorker();
      await first.runUntil(async () => {
        handle = await start('restart');
        await waitForTimer(handle);
      });
      await setState('root', 'DRAFT');
      const second = await newWorker();
      await second.runUntil(() => handle.result());
      assert.equal(counts.container + counts.finalize, 0);
      assert.equal((await row('root')).state, 'DRAFT');
    }],
    ['DRAFT between container and finalize: the Threads publish step is stopped', async () => {
      await seed([{ id: 'root', state: 'QUEUE', inMs: 1000 }]);
      hooks.onCheck = async () => setState('root', 'DRAFT');
      const worker = await newWorker();
      await worker.runUntil(async () => (await start('finalize')).result());
      assert.equal(counts.container, 1, 'container is created (not public)');
      assert.equal(counts.finalize, 0);
      assert.equal((await row('root')).state, 'DRAFT');
    }],
    ['reply set to DRAFT after the root went live: reply is not published', async () => {
      await seed([
        { id: 'root', state: 'QUEUE', inMs: 1000 },
        { id: 'reply', state: 'QUEUE', parent: 'root', inMs: 1000 },
      ]);
      hooks.onFinalize = async (pd) => {
        if (pd.containerId === 'c-root') await setState('reply', 'DRAFT');
      };
      const worker = await newWorker();
      await worker.runUntil(async () => (await start('reply')).result());
      assert.equal(counts.finalize, 1, 'only the root was published');
      assert.equal(counts.comment, 0);
      assert.equal((await row('reply')).state, 'DRAFT');
      // owner decision 2026-11-09: a live root stays PUBLISHED
      assert.equal((await row('root')).state, 'PUBLISHED');
    }],
    ['DRAFT flipped at the activity boundary (after the workflow check): root container never created', async () => {
      await seed([{ id: 'root', state: 'QUEUE', inMs: 800 }]);
      hooks.beforePost = async () => setState('root', 'DRAFT');
      const worker = await newWorker();
      await worker.runUntil(async () => (await start('boundary-root')).result());
      assert.equal(counts.container + counts.finalize, 0);
      assert.equal((await row('root')).state, 'DRAFT');
    }],
    ['DRAFT flipped at the activity boundary: the Threads publish step is stopped by the activity', async () => {
      await seed([{ id: 'root', state: 'QUEUE', inMs: 800 }]);
      hooks.beforeFinalize = async () => setState('root', 'DRAFT');
      const worker = await newWorker();
      await worker.runUntil(async () => (await start('boundary-finalize')).result());
      assert.equal(counts.container, 1, 'container is created (not public)');
      assert.equal(counts.finalize, 0);
      assert.equal((await row('root')).state, 'DRAFT');
    }],
    ['reply set to DRAFT at the activity boundary: reply not published, root stays PUBLISHED', async () => {
      await seed([
        { id: 'root', state: 'QUEUE', inMs: 800 },
        { id: 'reply', state: 'QUEUE', parent: 'root', inMs: 800 },
      ]);
      hooks.beforeComment = async () => setState('reply', 'DRAFT');
      const worker = await newWorker();
      await worker.runUntil(async () => (await start('boundary-reply')).result());
      assert.equal(counts.finalize, 1);
      assert.equal(counts.comment, 0);
      assert.equal((await row('root')).state, 'PUBLISHED');
      assert.equal((await row('reply')).state, 'DRAFT');
    }],
    ['histories recorded with and without the SMA-053 marker replay on this workflow', async () => {
      const origPath = path.join(dir, 'bundle/workflow-orig/post.workflow.v1.1.2.js');
      const replay = async (history, id) => {
        const r = await Worker.runReplayHistory({ workflowsPath }, history, id);
        return r;
      };
      const record = async (label, wfPath, finish) => {
        await seed([{ id: 'root', state: 'QUEUE', inMs: finish ? 800 : 6000 }]);
        const w = await Worker.create({ connection: native, taskQueue: queue, workflowsPath: wfPath, activities });
        let handle;
        await w.runUntil(async () => {
          handle = await start(label);
          if (finish) await handle.result();
          else await waitForTimer(handle);
        });
        const history = await handle.fetchHistory();
        // do not leave a sleeping execution behind: it would wake up under the
        // next case's worker and publish that case's synthetic post
        if (!finish) await handle.terminate('SMA-053 test: history captured');
        return { history, id: handle.workflowId };
      };
      const stats = [];
      for (const [label, wfPath, finish] of [
        ['orig-timer', origPath, false],
        ['orig-done', origPath, true],
        ['marker-timer', workflowsPath, false],
        ['marker-done', workflowsPath, true],
      ]) {
        const { history, id } = await record(label, wfPath, finish);
        // the SDK records a patch as a core_patch marker event whose id is the payload
        const marked = history.events.some((e) => e.markerRecordedEventAttributes && e.markerRecordedEventAttributes.markerName === 'core_patch');
        await replay(history, id);
        stats.push(`${label}:${marked ? 'marker' : 'no-marker'}:ok`);
      }
      assert(stats.some((x) => x.includes(':marker:')) && stats.some((x) => x.includes(':no-marker:')));
      console.log('  replayed', stats.join(' '));
    }],
    ['row lock: owner DRAFT sent during threads_publish waits until the request returns', async () => {
      await seed([{ id: 'root', state: 'QUEUE', inMs: 800 }]);
      let ownerWrite;
      let ownerDoneAt = 0;
      let publishDoneAt = 0;
      hooks.onFinalize = async () => {
        ownerWrite = pool.query("UPDATE posts SET state='DRAFT' WHERE id='root'").then(() => { ownerDoneAt = Date.now(); });
        await sleep(700);
        publishDoneAt = Date.now();
      };
      const worker = await newWorker();
      await worker.runUntil(async () => (await start('lock-wait')).result());
      await ownerWrite;
      assert.equal(counts.finalize, 1);
      assert(ownerDoneAt >= publishDoneAt, `owner write landed ${publishDoneAt - ownerDoneAt}ms inside the publish request`);
      console.log('  note: root state after workflow =', (await row('root')).state, '; alerts =', counts.alerts.length);
    }],
    ['row lock: a lock held by someone else fails closed before publishing', async () => {
      await seed([{ id: 'root', state: 'QUEUE', inMs: 800 }]);
      let holder;
      hooks.beforeFinalize = async () => {
        if (holder) return;
        const c = await pool.connect();
        await c.query('BEGIN');
        await c.query("SELECT id FROM posts WHERE id='root' FOR UPDATE");
        holder = sleep(5000).then(() => c.query('COMMIT')).finally(() => c.release());
      };
      const worker = await newWorker();
      await worker.runUntil(async () => (await start('lock-timeout')).result());
      await holder;
      assert.equal(counts.container, 1);
      assert.equal(counts.finalize, 0);
    }],
    ['kill switch file stops the publish and clears without a restart', async () => {
      const file = '/tmp/postiz-publish-hold';
      await seed([{ id: 'root', state: 'QUEUE', inMs: 800 }]);
      fs.writeFileSync(file, '');
      try {
        const worker = await newWorker();
        await worker.runUntil(async () => (await start('hold')).result());
      } finally {
        fs.rmSync(file, { force: true });
      }
      assert.equal(counts.container + counts.finalize, 0);
      // stopped posts end as ERROR and must be rescheduled
      assert.equal((await row('root')).state, 'ERROR');
      reset();
      await seed([{ id: 'root', state: 'QUEUE', inMs: 800 }]);
      const worker = await newWorker();
      await worker.runUntil(async () => (await start('hold-cleared')).result());
      assert.equal(counts.finalize, 1);
    }],
  ];

  let failed = 0;
  for (const [name, fn] of cases) {
    reset();
    try {
      await fn();
      console.log('PASS', name, JSON.stringify({ ...counts, notifications: counts.notifications.length }));
    } catch (err) {
      failed++;
      console.error('FAIL', name, JSON.stringify(counts), '\n', err && err.stack);
    }
  }
  console.log(`${cases.length - failed}/${cases.length} passed`);
  await native.close();
  await connection.close();
  await db.end();
  await pool.end();
  process.exit(failed ? 1 : 0);
})().catch((e) => {
  console.error(e && e.stack);
  process.exit(1);
});
