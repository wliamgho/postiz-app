# SMA-053 conditional ERROR write — PostgreSQL test

1. `node tests/sma-053/pg/extract.cjs` (needs `typescript`) writes `repo-method.js`.
2. Copy this folder to `/tmp/sma053-guard-pg` on a Docker host that has
   `postgres:16-alpine` and `sma053-prisma-isolated:6.5.0`, then run
   `sh /tmp/sma053-guard-pg/run.sh`.

Both containers run with network none and synthetic data; the script removes
them and the folder afterwards. 2026-11-09 on coder-dsh: 7/7 passed, including
a concurrent DRAFT switch that wins over a waiting ERROR write and a PUBLISHED
post that is not overwritten.
