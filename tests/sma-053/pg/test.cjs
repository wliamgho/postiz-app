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
const mk = (id, state) =>
  a.post.create({ data: { id, state, organizationId: 'org', integrationId: 'int' } });

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
