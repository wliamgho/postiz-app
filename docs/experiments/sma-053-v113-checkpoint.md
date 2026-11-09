# SMA-053 V113 experimental checkpoint — do not deploy

This preserves an incomplete preflight experiment, not a fix. No caller is routed to V113. The activity and workflow verify current root/target state before a provider operation. A state change after the check can still race the provider call. Atomic state changes, attempt fencing, cancellation, unknown/outcome reconciliation and migration of existing histories remain unresolved. The checkout's V112 source does not match all safeguards in the compiled production worker. Do not merge, deploy or lift the Postiz hold on this basis.

The activity's extracted method was exercised with a fake PostsService for queued root, published parent, revoked draft, wrong tenant or parent, and database failure. TypeScript transpile syntax checks passed in a disposable container. Full project typecheck, Temporal integration, production-schema Prisma integration, UI and provider behavior were not tested for this candidate. The separate approved-slide storage test verifies only backend stream bytes, not this workflow or browser upload.

## Draft cancellation reporting experiment

The draft path surfaces Temporal list/describe/terminate errors or an unavailable client as cancellation unconfirmed, including in the status, create and date callers. The draft database update can already have happened when the error is returned. A provider request may already be in flight. List/terminate may hang indefinitely; caller coverage, timeout semantics, partial batch writes, application error presentation, and Temporal/PostgreSQL integration are not qualified. The checkout source differs from the production compiled worker; do not deploy.

The focused method test `tests/sma-053/draft-cancellation-prototype.test.cjs` uses TypeScript to extract the actual source methods, a fake repository and fake Temporal client. It covers list, describe, terminate, missing client and successful cancellation; it does not exercise all changed callers or the actual Temporal SDK.
