'use strict';
// SMA-053: runs the extracted PostsRepository.markErrorUnlessSettled against a
// real PostgreSQL through Prisma. Synthetic data, network none.
const assert = require('node:assert/strict');
const { PrismaClient } = require('@prisma/client');
const Repo = require('./repo-method.js');

const a = new PrismaClient();
const b = new PrismaClient();
const repoOn = (client) => {
  const r = new Repo();
  r._post = { model: client };
  r._errors = { model: client };
  return r;
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function reset() {
  await a.errors.deleteMany();
  await a.post.deleteMany();
  await a.integration.deleteMany();
  await a.integration.create({ data: { id: 'int', providerIdentifier: 'threads' } });
}
const mk = (id, state, parentPostId = null, organizationId = 'org') =>
  a.post.create({ data: { id, state, parentPostId, organizationId, integrationId: 'int' } });
const allow = (rows) => {
  for (const r of rows) {
    if (r.deletedAt || !['QUEUE', 'PUBLISHED'].includes(r.state)) throw new Error('stop: ' + r.id + ' is ' + r.state);
  }
};
const { PostChainLockError } = Repo;

const cases = [
  ['QUEUE post is marked ERROR with message and error row', async () => {
    await mk('p', 'QUEUE');
    assert.equal(await repoOn(a).markErrorUnlessSettled('p', 'boom', [{ id: 'p' }]), true);
    const row = await a.post.findUnique({ where: { id: 'p' } });
    assert.equal(row.state, 'ERROR');
    assert.equal(row.error, 'boom');
    assert.equal(await a.errors.count({ where: { postId: 'p' } }), 1);
  }],
  ['DRAFT post is left untouched, no error row', async () => {
    await mk('p', 'DRAFT');
    assert.equal(await repoOn(a).markErrorUnlessSettled('p', 'boom', [{ id: 'p' }]), false);
    const row = await a.post.findUnique({ where: { id: 'p' } });
    assert.equal(row.state, 'DRAFT');
    assert.equal(row.error, null);
    assert.equal(await a.errors.count(), 0);
  }],
  ['missing post returns false', async () => {
    assert.equal(await repoOn(a).markErrorUnlessSettled('nope', 'boom', []), false);
  }],
  ['object error is stored as JSON, no body means no error row', async () => {
    await mk('p', 'ERROR');
    assert.equal(await repoOn(a).markErrorUnlessSettled('p', { code: 1 }), true);
    const row = await a.post.findUnique({ where: { id: 'p' } });
    assert.equal(row.state, 'ERROR');
    assert.equal(row.error, '{"code":1}');
    assert.equal(await a.errors.count(), 0);
  }],
  ['PUBLISHED post stays PUBLISHED, no error row', async () => {
    await mk('p', 'PUBLISHED');
    assert.equal(await repoOn(a).markErrorUnlessSettled('p', 'comment failed', [{ id: 'p' }]), false);
    const row = await a.post.findUnique({ where: { id: 'p' } });
    assert.equal(row.state, 'PUBLISHED');
    assert.equal(row.error, null);
    assert.equal(await a.errors.count(), 0);
  }],
  ['concurrent switch to DRAFT wins over a waiting ERROR write', async () => {
    await mk('p', 'QUEUE');
    let released;
    const holding = a.$transaction(async (tx) => {
      await tx.post.update({ where: { id: 'p' }, data: { state: 'DRAFT' } });
      await new Promise((r) => (released = r));
    }, { timeout: 20000 });
    await sleep(300);
    const write = repoOn(b).markErrorUnlessSettled('p', 'late error', [{ id: 'p' }]);
    await sleep(700);
    released();
    await holding;
    assert.equal(await write, false);
    const row = await a.post.findUnique({ where: { id: 'p' } });
    assert.equal(row.state, 'DRAFT');
    assert.equal(await a.errors.count(), 0);
  }],
  ['control: the old unconditional update overwrites DRAFT', async () => {
    await mk('p', 'DRAFT');
    await a.post.update({ where: { id: 'p' }, data: { state: 'ERROR', error: 'x' } });
    assert.equal((await a.post.findUnique({ where: { id: 'p' } })).state, 'ERROR');
  }],
  ['lock: owner DRAFT update waits until the publish request returns', async () => {
    await mk('p', 'QUEUE');
    let publishReturnedAt = 0;
    let ownerDoneAt = 0;
    let owner;
    const value = await repoOn(a).withPostChainLocked('org', ['p'], allow, async () => {
      owner = b.post.update({ where: { id: 'p' }, data: { state: 'DRAFT' } }).then(() => { ownerDoneAt = Date.now(); });
      await sleep(800);
      publishReturnedAt = Date.now();
      return 'live';
    });
    await owner;
    assert.equal(value, 'live');
    assert(ownerDoneAt >= publishReturnedAt, `owner write landed ${publishReturnedAt - ownerDoneAt}ms before publish returned`);
    assert.equal((await a.post.findUnique({ where: { id: 'p' } })).state, 'DRAFT');
  }],
  ['lock: DRAFT committed first is seen and publish never runs', async () => {
    await mk('p', 'QUEUE');
    await b.post.update({ where: { id: 'p' }, data: { state: 'DRAFT' } });
    let published = 0;
    await assert.rejects(
      repoOn(a).withPostChainLocked('org', ['p'], allow, async () => { published++; }),
      (e) => /stop: p is DRAFT/.test(e.message) && !(e instanceof PostChainLockError)
    );
    assert.equal(published, 0);
  }],
  ['lock: ancestors are read and locked with the reply', async () => {
    await mk('root', 'PUBLISHED');
    await mk('reply', 'QUEUE', 'root');
    let seen = [];
    let done = false;
    let w;
    await repoOn(a).withPostChainLocked('org', ['reply'], (rows) => { seen = rows.map((r) => `${r.id}:${r.state}`).sort(); }, async () => {
      // the ancestor is locked too: an update to it must not complete now
      w = b.post.update({ where: { id: 'root' }, data: { state: 'DRAFT' } }).then(() => { done = true; });
      await sleep(500);
      assert.equal(done, false, 'ancestor update did not wait for the lock');
    });
    await w;
    assert.equal(done, true);
    assert.deepEqual(seen, ['reply:QUEUE', 'root:PUBLISHED']);
  }],
  ['lock: posts of another organization are not returned', async () => {
    await mk('p', 'QUEUE', null, 'other-org');
    let seen;
    await repoOn(a).withPostChainLocked('org', ['p'], (rows) => { seen = rows; }, async () => 'x');
    assert.equal(seen.length, 0);
  }],
  ['lock: a held row lock times out and fails closed before publishing', async () => {
    await mk('p', 'QUEUE');
    let release;
    const holder = b.$transaction(async (tx) => {
      await tx.$queryRaw`SELECT id FROM "Post" WHERE id = 'p' FOR UPDATE`;
      await new Promise((r) => (release = r));
    }, { timeout: 20000 });
    await sleep(300);
    let published = 0;
    const started = Date.now();
    await assert.rejects(
      repoOn(a).withPostChainLocked('org', ['p'], allow, async () => { published++; }, { lockTimeoutMs: 400 }),
      (e) => e instanceof PostChainLockError
    );
    const waited = Date.now() - started;
    release();
    await holder;
    assert.equal(published, 0);
    assert(waited < 5000, 'lock wait was not bounded: ' + waited + 'ms');
  }],
  ['lock: publish result survives a transaction timeout', async () => {
    await mk('p', 'QUEUE');
    const value = await repoOn(a).withPostChainLocked('org', ['p'], allow, async () => { await sleep(1500); return 'live'; }, { transactionTimeoutMs: 500 });
    assert.equal(value, 'live');
    await b.post.update({ where: { id: 'p' }, data: { state: 'DRAFT' } });
  }],
  ['lock: a publish error propagates and the lock is released', async () => {
    await mk('p', 'QUEUE');
    await assert.rejects(
      repoOn(a).withPostChainLocked('org', ['p'], allow, async () => { throw new Error('platform 500'); }),
      /platform 500/
    );
    const started = Date.now();
    await b.post.update({ where: { id: 'p' }, data: { state: 'DRAFT' } });
    assert(Date.now() - started < 1000, 'row stayed locked');
  }],
];

(async () => {
  let failed = 0;
  for (const [name, fn] of cases) {
    await reset();
    try {
      await fn();
      console.log('PASS', name);
    } catch (err) {
      failed++;
      console.error('FAIL', name, '\n', err && err.stack);
    }
  }
  console.log(`${cases.length - failed}/${cases.length} passed`);
  await a.$disconnect();
  await b.$disconnect();
  process.exitCode = failed ? 1 : 0;
})();
