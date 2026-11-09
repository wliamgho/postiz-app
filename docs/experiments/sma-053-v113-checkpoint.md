# SMA-053 V113 experimental checkpoint — do not deploy

This preserves an incomplete preflight experiment, not a fix. No caller is routed to V113. The activity and workflow verify current root/target state before a provider operation. A state change after the check can still race the provider call. Atomic state changes, attempt fencing, cancellation, unknown/outcome reconciliation and migration of existing histories remain unresolved. The checkout's V112 source does not match all safeguards in the compiled production worker. Do not merge, deploy or lift the Postiz hold on this basis.

The activity's extracted method was exercised with a fake PostsService for queued root, published parent, revoked draft, wrong tenant or parent, and database failure. TypeScript transpile syntax checks passed in a disposable container. Full project typecheck, Temporal integration, production-schema Prisma integration, UI and provider behavior were not tested for this candidate. The separate approved-slide storage test verifies only backend stream bytes, not this workflow or browser upload.

## Draft cancellation reporting experiment

The draft path surfaces Temporal list/describe/terminate errors or an unavailable client as cancellation unconfirmed, including in the status, create and date callers. The draft database update can already have happened when the error is returned. A provider request may already be in flight. List/terminate may hang indefinitely; caller coverage, timeout semantics, partial batch writes, application error presentation, and Temporal/PostgreSQL integration are not qualified. The checkout source differs from the production compiled worker; do not deploy.

The focused method test `tests/sma-053/draft-cancellation-prototype.test.cjs` uses TypeScript to extract the actual source methods, a fake repository and fake Temporal client. It covers list, describe, terminate, missing client and successful cancellation; it does not exercise all changed callers or the actual Temporal SDK.

## Activity publish guard — 2026-11-09

Owner chose the minimal guard. `PostActivity` now re-reads each post and its ancestors (tenant-scoped) immediately before the provider request in `postSocial`/`postSocialPending`, `postComment` and `finalizePost`, and stops with a non-retryable `bad_body` failure when any is missing, deleted, from another organization or DRAFT. Activity code is not part of workflow history, so running V112 (and older) executions are covered without rerouting. Pending results carry the guard ids in `pendingData.__sma053Guard`; `checkPostStatus` re-attaches them. Pending data recorded before this change has no ids, so `finalizePost` fails closed for it. `changeState(..., 'ERROR')` now goes through `markErrorUnlessDraft`, a single conditional update that never overwrites DRAFT.

Evidence: `tests/sma-053/publish-guard.test.cjs` runs the transpiled real activity with fake services, 13/13. `tests/sma-053/pg/` runs the extracted repository method on real PostgreSQL 16 through Prisma 6.5 on coder-dsh, 6/6, including a concurrent DRAFT switch that wins over a waiting ERROR write.

`tests/sma-053/temporal/` runs the V112 workflow compiled from source on a real Temporal dev server with the transpiled activity, a real PostgreSQL table and a fake Threads-like provider: 5/5 — control publish; DRAFT during the scheduling sleep; DRAFT then worker restart; DRAFT between container and finalize; reply set to DRAFT after the root went live (the live root then ends as ERROR, existing V112 behaviour on a comment failure).

Not covered: a state change in the milliseconds between the guard read and the provider request; requests already in flight; full project typecheck and build; the production compiled V112 (this run uses the fork source); owner notification text on a guard stop (it reports a posting error). Deploy precondition: no pending executions from before this change, or accept that they stop as errors.
