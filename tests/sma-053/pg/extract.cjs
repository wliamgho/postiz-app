'use strict';
// Writes repo-method.js: PostChainLockError and the SMA-053 methods
// markErrorUnlessSettled and withPostChainLocked, extracted from the real
// PostsRepository source, for test.cjs. Needs the typescript package.
const ts = require('typescript');
const fs = require('node:fs');
const path = require('node:path');
const p = path.join(__dirname, '../../../libraries/nestjs-libraries/src/database/prisma/posts/posts.repository.ts');
const ast = ts.createSourceFile(p, fs.readFileSync(p, 'utf8'), ts.ScriptTarget.ES2022, true);
const cls = ast.statements.find((s) => ts.isClassDeclaration(s) && s.name?.text === 'PostsRepository');
const lockError = ast.statements.find((s) => ts.isClassDeclaration(s) && s.name?.text === 'PostChainLockError');
if (!cls || !lockError) throw new Error('PostsRepository or PostChainLockError missing');
const methods = ['markErrorUnlessSettled', 'withPostChainLocked'].map((name) => {
  const m = cls.members.find((x) => x.name?.getText(ast) === name);
  if (!m) throw new Error(name + ' missing');
  return m.getText(ast);
});
const source = `${lockError.getText(ast).replace(/^export\s+/, '')}
class Repo { ${methods.join('\n')} }
module.exports = Repo;
module.exports.PostChainLockError = PostChainLockError;`;
const js = ts.transpileModule(source, {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
}).outputText;
fs.writeFileSync(path.join(__dirname, 'repo-method.js'), js);
console.log('wrote repo-method.js');
