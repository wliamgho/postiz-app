# SMA-053 publish guard — real Temporal test

1. `node tests/sma-053/temporal/build.cjs` (needs `typescript`) compiles the
   V112 workflow and `PostActivity` from source into `bundle/`.
2. Copy `bundle/`, `test.cjs` and `run.sh` to `/tmp/sma053-guard-temporal` on a
   Docker host with `temporalio/temporal:latest`, `postgres:16-alpine` and
   `sma053-isolated-runner:with-workflow`, then run
   `sh /tmp/sma053-guard-temporal/run.sh`.

All containers use network none and synthetic data; the provider is a fake
Threads-like provider (container, then publish at finalize). The script removes
the containers and the folder afterwards.

2026-11-09 on coder-dsh: 5/5 passed — control publish; DRAFT during the
scheduling sleep; DRAFT then worker restart (resumed history); DRAFT between
container and finalize; reply set to DRAFT after the root went live (the root
stays PUBLISHED).
