# SMA-053 guard on the production source base — 2026-11-09 (draft, do not deploy)

Branch `fix/sma-053-prod-base` starts at upstream `0d486f75` (2026-10-09), whose workflow, activity, posts service, posts repository and schema match the five source files captured from the running production image byte for byte. Three commits sit on it:

1. The compiled production workflow guard, reconstructed in source: `patched('sma053-draft-state-guard-v1')`, post-sleep state checks, per-step `sma053CanProceed`, and the `sma053-revoked` result. The activity gets the unused third `postSocialPending` argument the compiled workflow passes. This is a reconstruction of observed behaviour, not proof of equivalence with the compiled output.
2. The activity publish guard: before each provider request in `postSocial`/`postSocialPending`, `postComment` and `finalizePost`, the post and its ancestors must exist in the organization and be QUEUE or PUBLISHED; otherwise a non-retryable `bad_body` failure stops it. Pending data carries the post ids; data without them fails closed. Workflow ERROR writes use `markErrorUnlessSettled`, a single conditional update that skips DRAFT and PUBLISHED.
3. Tests.

Evidence (coder-dsh, disposable containers with network none, synthetic data): fake-service activity tests 15/15; PostgreSQL 16 through Prisma 6.5 7/7; real Temporal 9/9, which adds three cases that flip the post to DRAFT at the activity boundary, after the workflow's own check (root container, Threads finalize, reply), and a replay of four histories (with and without the patch marker, pending timer and completed) on this workflow. Replay covers the workflow only; activity code is not part of history. A fresh clone typechecks and `nest build` exits 0 for orchestrator and backend; the compiled workflow keeps the patch marker and the compiled activity the guard.

Typecheck note: `tsc -p tsconfig.json` reports one more implicit-any error (autopost.service.ts:179, langgraph state) than the pure base (4 against 3). It disappears when either the workflow or the activity change is reverted, so it is an inference interaction, not a semantic one; `nest build` is unaffected. The frontend was not built.

Still open: the milliseconds between the guard read and the provider request, and requests already in flight; reconciliation of old or unknown pending outcomes; a build identical to the production compiled output (this branch is a source reconstruction); independent review; owner decisions. The earlier branch `fix/sma-053-safe-drafts` is upstream-newer than production and keeps the V113 and cancellation experiments.

## Publish lock, hold switch and alert — 2026-11-10 (owner chose the low-cost package plus C)

- **Row lock around the public request (C + D).** Providers receive an optional `publishGuard` in `finalizePost`; Threads wraps only `threads_publish` with it. The activity implements it with `PostsRepository.withPostChainLocked`: a transaction that sets `lock_timeout` (3 s), reads the post and its ancestors `FOR SHARE`, re-checks them, then runs the request. An owner update to any of those rows waits until the request returns. A lock or read failure stops the publish before anything is sent; once the request ran, its result is returned even if the transaction fails afterwards. Not covered: the old blocking `post()` path used by workflows before V1.0.6, and providers other than Threads, which keep only the activity pre-check.
- **Kill switch (F).** `PUBLISH_HOLD=true` in the worker environment, or the file `PUBLISH_HOLD_FILE` (default `/tmp/postiz-publish-hold`) inside the worker container, stops every guarded provider call. The file needs no restart. Stopped posts end as ERROR and must be rescheduled.
- **Post-publish alert (G).** After a successful publish the activity re-reads the posts; if one is now DRAFT or deleted it sends a failure notification with the release URL. Best-effort.
- **Stop text.** Guard stops now read "Publication stopped on purpose (SMA-053 guard): … Nothing was published."

Evidence (coder-dsh, network none, synthetic data): activity tests with fake services 23/23; PostgreSQL 16 with Prisma 6.5 14/14, including an owner DRAFT update that waits for the request, a lock held elsewhere timing out without a publish, ancestors locked with a reply, and a result surviving a transaction timeout; real Temporal 12/12, including the lock wait end to end (alert sent), a lock held elsewhere, and the hold file. `tsc` now matches the pure base exactly (orchestrator 3, backend 4 pre-existing errors); `nest build` exits 0 for both and the compiled output contains the lock, the guard and `FOR SHARE`.

Behaviour to know: when the owner's DRAFT waits behind the lock, the post is live and the workflow then records it as PUBLISHED; the alert tells the owner to correct it on the platform. Two publishes of the same post can still run in parallel (no idempotency ledger; option B was deferred).

## Parity with the production worker — 2026-11-10

Production runs image `postiz:sma053-worker-v2-20261105`. Its compiled files were hashed read-only inside the running container on schedule-post and match the captures used here. A `nest build` of the pure base `0d486f75` on coder-dsh reproduces production byte for byte for `posts.repository.js`, `posts.service.js`, `social.abstract.js` and `threads.provider.js`, so the build is reproducible and production differs from upstream only in the workflow and the activity.

- **Workflow:** after bracing four guard returns (`a740a809`), the branch compiles to `post.workflow.v1.1.2.js` SHA-256 `c84c5194e6252b72d1ecd07157761026cbc185b8ad6ee39e5fdddf4cdafd6454`, identical to production. Deploying this branch does not change the workflow code, so running and recorded histories replay on exactly the code they ran on.
- **Activity:** production's compiled activity is the base plus one root re-read before the provider call that throws a plain `Error` named `ApplicationFailure` with `nonRetryable` set as a property. Temporal treats a plain `Error` as retryable, so that stop is retried rather than stopping at once. This branch replaces it with the chain guard and a real non-retryable `bad_body` failure.
- **Libraries:** repository, posts service, `social.abstract` and the Threads provider differ from production only by this branch's additions.

Not done in this step: building a full Postiz image (the type-guard derived image and this compiled orchestrator would be combined on the production host at deploy time, as on 2026-11-05) and running the draft-safety verifier against it.
