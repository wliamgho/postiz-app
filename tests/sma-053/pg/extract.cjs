'use strict';
// Writes repo-method.js: PostsRepository.markErrorUnlessDraft extracted from
// the real source, for test.cjs. Needs the typescript package.
const ts = require('typescript');
const fs = require('node:fs');
const path = require('node:path');
const p = path.join(__dirname, '../../../libraries/nestjs-libraries/src/database/prisma/posts/posts.repository.ts');
const ast = ts.createSourceFile(p, fs.readFileSync(p, 'utf8'), ts.ScriptTarget.ES2022, true);
const cls = ast.statements.find((s) => ts.isClassDeclaration(s) && s.name?.text === 'PostsRepository');
const m = cls && cls.members.find((x) => x.name?.getText(ast) === 'markErrorUnlessDraft');
if (!m) throw new Error('markErrorUnlessDraft missing');
const js = ts.transpileModule(`class Repo { ${m.getText(ast)} }\nmodule.exports = Repo;`, {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
}).outputText;
fs.writeFileSync(path.join(__dirname, 'repo-method.js'), js);
console.log('wrote repo-method.js');
