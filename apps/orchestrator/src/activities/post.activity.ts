import { Injectable } from '@nestjs/common';
import {
  Activity,
  ActivityMethod,
  TemporalService,
} from 'nestjs-temporal-core';
import { PostsService } from '@gitroom/nestjs-libraries/database/prisma/posts/posts.service';
import {
  NotificationService,
  NotificationType,
} from '@gitroom/nestjs-libraries/database/prisma/notifications/notification.service';
import { Integration, Post, State } from '@prisma/client';
import { stripHtmlValidation } from '@gitroom/helpers/utils/strip.html.validation';
import { IntegrationManager } from '@gitroom/nestjs-libraries/integrations/integration.manager';
import {
  AuthTokenDetails,
  PublishGuard,
} from '@gitroom/nestjs-libraries/integrations/social/social.integrations.interface';
import type { LockedPostRow } from '@gitroom/nestjs-libraries/database/prisma/posts/posts.repository';
import { existsSync } from 'fs';
import { RefreshIntegrationService } from '@gitroom/nestjs-libraries/integrations/refresh.integration.service';
import { timer } from '@gitroom/helpers/utils/timer';
import { IntegrationService } from '@gitroom/nestjs-libraries/database/prisma/integrations/integration.service';
import { WebhooksService } from '@gitroom/nestjs-libraries/database/prisma/webhooks/webhooks.service';
import { getSsrfSafeDispatcher } from '@gitroom/nestjs-libraries/dtos/webhooks/ssrf.safe.dispatcher';
import { TypedSearchAttributes } from '@temporalio/common';
import {
  organizationId,
  postId as postIdSearchParam,
} from '@gitroom/nestjs-libraries/temporal/temporal.search.attribute';
import { SubscriptionService } from '@gitroom/nestjs-libraries/database/prisma/subscriptions/subscription.service';
import {
  setHeartbeatDetails,
  withHeartbeat,
} from '@gitroom/nestjs-libraries/temporal/temporal.heartbeat';
import {
  BadBody,
  Disconnect,
} from '@gitroom/nestjs-libraries/integrations/social.abstract';

// Drops fields the workflow and downstream activities never read — biggest wins are `error` (grows per retry) and `childrenPost` (Prisma side-loads it on every recursive row).
function slimPost(post: any) {
  if (!post) return post;
  const {
    error,
    childrenPost,
    tags,
    description,
    title,
    submittedForOrderId,
    submittedForOrganizationId,
    submittedForOrder,
    submittedForOrganization,
    lastMessageId,
    parentPostId,
    approvedSubmitForOrder,
    deletedAt,
    createdAt,
    updatedAt,
    payoutProblems,
    comments,
    errors,
    ...rest
  } = post;
  return rest;
}

// A genuinely missed occurrence (dead workflow) is only recovered by the
// missing-posts sweep, which runs every hour - so anything later than the
// sweep period plus retry margin is a stale anchor, not a missed publish.
const REANCHOR_GRACE_MS = 2 * 60 * 60 * 1000;

// A repeat post keeps its original anchor publishDate forever (updatePost only
// flips the state, the calendar expands occurrences virtually). If the workflow
// gets that raw past date, any (re)start - an accidental edit resetting the
// state to QUEUE, or a missing-posts sweep poke - sleeps 0 and publishes
// instantly, machine-gunning the channel. Roll the returned date forward to the
// next occurrence on the anchor grid instead, so fresh starts wait for the next
// real occurrence. Only for the initial QUEUE run: repeat chain children
// (postNow) run against a PUBLISHED post and must keep publishing immediately.
// Occurrences missed within the grace window still catch up and post.
function reanchorInterval(post: any) {
  if (!post?.intervalInDays || post.state !== State.QUEUE) {
    return post;
  }

  const interval = post.intervalInDays * 24 * 60 * 60 * 1000;
  const late = Date.now() - new Date(post.publishDate).getTime();
  if (late <= REANCHOR_GRACE_MS) {
    return post;
  }

  const next =
    new Date(post.publishDate).getTime() +
    Math.ceil(late / interval) * interval;

  return { ...post, publishDate: new Date(next) };
}

// SMA-053: a post switched back to DRAFT (or deleted) while its workflow slept
// still reached the provider, because the workflow only checks the state
// before the scheduling sleep. Activity code is not part of workflow history,
// so this guard also covers executions already running on older workflow
// versions. It narrows the window to the moment before the provider request;
// it cannot recall a request that is already in flight.
const PUBLISH_GUARD_KEY = '__sma053Guard';
const MAX_PARENT_DEPTH = 50;
const PUBLISHABLE_STATES: string[] = [State.QUEUE, State.PUBLISHED, State.ERROR];

type GuardIds = { orgId: string; postIds: string[] };

// SMA-053 kill switch: PUBLISH_HOLD=true in the worker environment, or the
// file PUBLISH_HOLD_FILE (default /tmp/postiz-publish-hold) existing inside
// the worker container, makes every guarded provider call stop. The file can
// be created or removed without a restart. Stopped posts end as ERROR and
// must be rescheduled by the owner.
function publishHoldActive(): boolean {
  const flag = String(process.env.PUBLISH_HOLD || '').toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(flag)) {
    return true;
  }
  try {
    return existsSync(
      process.env.PUBLISH_HOLD_FILE || '/tmp/postiz-publish-hold'
    );
  } catch (e) {
    return false;
  }
}

function withPublishGuard(result: any, guard: GuardIds) {
  if (!result || typeof result !== 'object' || !result.pendingData) {
    return result;
  }
  return {
    ...result,
    pendingData: { ...result.pendingData, [PUBLISH_GUARD_KEY]: guard },
  };
}

@Injectable()
@Activity()
export class PostActivity {
  constructor(
    private _postService: PostsService,
    private _notificationService: NotificationService,
    private _integrationManager: IntegrationManager,
    private _integrationService: IntegrationService,
    private _refreshIntegrationService: RefreshIntegrationService,
    private _webhookService: WebhooksService,
    private _temporalService: TemporalService,
    private _subscriptionService: SubscriptionService
  ) {}

  @ActivityMethod()
  async getIntegrationById(orgId: string, id: string) {
    return this._integrationService.getIntegrationById(orgId, id);
  }

  @ActivityMethod()
  async searchForMissingThreeHoursPosts() {
    const list = await this._postService.searchForMissingThreeHoursPosts();
    for (const post of list) {
      await this._temporalService.client
        .getRawClient()
        .workflow.signalWithStart('postWorkflowV112', {
          workflowId: `post_${post.id}`,
          taskQueue: 'main',
          signal: 'poke',
          workflowIdConflictPolicy: 'USE_EXISTING',
          signalArgs: [],
          args: [
            {
              taskQueue: post.integration.providerIdentifier
                .split('-')[0]
                .toLowerCase(),
              postId: post.id,
              organizationId: post.organizationId,
            },
          ],
          typedSearchAttributes: new TypedSearchAttributes([
            {
              key: postIdSearchParam,
              value: post.id,
            },
            {
              key: organizationId,
              value: post.organizationId,
            },
          ]),
        });
    }
  }

  @ActivityMethod()
  async updatePost(id: string, postId: string, releaseURL: string) {
    await this._postService.updatePost(id, postId, releaseURL);
  }

  @ActivityMethod()
  async getPost(orgId: string, postId: string) {
    if (process.env.STRIPE_SECRET_KEY) {
      const subscription = await this._subscriptionService.getSubscription(
        orgId
      );
      if (!subscription) {
        return false;
      }
    }
    const post = await this._postService.getPostById(postId, orgId);
    if (post.deletedAt) {
      return false;
    }

    return reanchorInterval(post);
  }

  @ActivityMethod()
  async getPostsList(orgId: string, postId: string) {
    if (process.env.STRIPE_SECRET_KEY) {
      const subscription = await this._subscriptionService.getSubscription(
        orgId
      );
      if (!subscription) {
        return [];
      }
    }

    const getPosts = await this._postService.getPostsRecursively(
      postId,
      true,
      orgId
    );
    if (!getPosts || getPosts.length === 0 || getPosts[0].parentPostId) {
      return [];
    }

    // only the root drives the pre-publish sleep and the repeat schedule,
    // the rest are comments
    const [root, ...comments] = getPosts.map(slimPost);
    return [reanchorInterval(root), ...comments];
  }

  // Throws a non-retryable bad_body failure when any post, or any of its
  // ancestors, is missing, deleted, owned by another organization or DRAFT.
  // Every workflow version stops on bad_body without re-running the publish.
  private async assertPublishable(
    integration: Integration,
    orgId: string | undefined,
    postIds: string[]
  ) {
    const reject = (reason: string) => this.guardStop(integration, reason);

    if (publishHoldActive()) {
      throw reject('publishing is on hold (PUBLISH_HOLD)');
    }
    if (!orgId || !postIds.length) {
      throw reject('the post could not be identified');
    }

    for (const postId of postIds) {
      let currentId: string | null | undefined = postId;
      for (let depth = 0; currentId; depth++) {
        if (depth > MAX_PARENT_DEPTH) {
          throw reject('the post chain is too deep');
        }
        const current: any = await this._postService.getPostById(
          currentId,
          orgId
        );
        if (!current || current.deletedAt) {
          throw reject('the post no longer exists');
        }
        // DRAFT stops it. ERROR may proceed: the workflow marks the post ERROR
        // after an unknown error and then retries the publish, and the
        // workflow itself decides at start whether an ERROR post may run.
        if (!PUBLISHABLE_STATES.includes(current.state)) {
          throw reject(`the post is ${String(current.state).toLowerCase()}`);
        }
        currentId = current.parentPostId;
      }
    }
  }

  // Plugs publish follow-up posts (for example an auto-reply) after the main
  // post; the kill switch stops them too. bad_body ends only that plug.
  private assertNoPublishHold(kind: string) {
    if (publishHoldActive()) {
      throw new BadBody(
        kind,
        JSON.stringify({}),
        Buffer.from('{}'),
        `Publication stopped on purpose (SMA-053 guard): publishing is on hold (PUBLISH_HOLD), ${kind} skipped. Nothing was published.`
      );
    }
  }

  // The text reaches the owner through the workflow's failure notification.
  private guardStop(integration: Integration, reason: string) {
    return new BadBody(
      integration.providerIdentifier,
      JSON.stringify({}),
      Buffer.from('{}'),
      `Publication stopped on purpose (SMA-053 guard): ${reason}. Nothing was published.`
    );
  }

  // Same rules as assertPublishable, applied to the rows read under the
  // publish lock, so the decision and the platform request see one state.
  private checkLockedRows(
    integration: Integration,
    postIds: string[],
    rows: LockedPostRow[]
  ) {
    const reject = (reason: string) => this.guardStop(integration, reason);
    if (publishHoldActive()) {
      throw reject('publishing is on hold (PUBLISH_HOLD)');
    }
    const byId = new Map((rows || []).map((row) => [row.id, row]));
    for (const postId of postIds) {
      let current = byId.get(postId);
      if (!current) {
        throw reject('the post no longer exists');
      }
      for (let depth = 0; current; depth++) {
        if (depth > MAX_PARENT_DEPTH) {
          throw reject('the post chain is too deep');
        }
        if (current.deletedAt) {
          throw reject('the post no longer exists');
        }
        if (!PUBLISHABLE_STATES.includes(current.state)) {
          throw reject(`the post is ${String(current.state).toLowerCase()}`);
        }
        if (!current.parentPostId) {
          break;
        }
        const parent = byId.get(current.parentPostId);
        if (!parent) {
          throw reject('the post no longer exists');
        }
        current = parent;
      }
    }
  }

  // SMA-053: passed to providers that publish in finalizePost. The provider
  // calls it around the one request that makes the post public; the post
  // rows stay locked from the state check until that request returns, so an
  // owner switching the post to DRAFT either lands first (and stops it) or
  // waits until the request is done.
  private publishGuardFor(
    integration: Integration,
    ids: GuardIds
  ): PublishGuard {
    return <T>(publish: () => Promise<T>) =>
      this._postService
        .withPostChainLocked(
          ids.orgId,
          ids.postIds,
          (rows) => this.checkLockedRows(integration, ids.postIds, rows),
          publish
        )
        .catch((err: any) => {
          // Nothing was sent. A plain (retryable) error lets the workflow's
          // own retry run the publish again instead of losing the post.
          if (err && err.name === 'PostChainLockError') {
            throw new Error(
              `SMA-053 publish guard: post state not confirmed yet (${err.message}); nothing was published, retrying`
            );
          }
          throw err;
        });
  }

  // SMA-053: if a post went live but is now a draft or deleted (the owner's
  // change waited behind the publish lock, or the provider has no guard),
  // tell the owner right away so it can be corrected on the platform.
  // Best-effort: never fails the activity after a successful publish.
  private async alertIfRevokedAfterPublish(
    integration: Integration,
    ids: GuardIds | undefined,
    results: any[]
  ) {
    try {
      if (!ids) {
        return;
      }
      const live = (results || []).filter(
        (r) => r && (r.status === 'completed' || r.status === 'success')
      );
      if (!live.length) {
        return;
      }
      for (const postId of ids.postIds) {
        const post: any = await this._postService.getPostById(
          postId,
          ids.orgId
        );
        if (post && !post.deletedAt && post.state !== State.DRAFT) {
          continue;
        }
        const urls = live
          .map((r) => r.releaseURL)
          .filter(Boolean)
          .join(', ');
        await this._notificationService.inAppNotification(
          ids.orgId,
          `A post went live after it was changed to draft`,
          `A post on ${integration.providerIdentifier} for ${
            integration.name
          } was published but is now ${
            !post || post.deletedAt ? 'deleted' : 'a draft'
          }. Check it on the platform${urls ? `: ${urls}` : ''}. (SMA-053)`,
          true,
          false,
          'fail'
        );
        return;
      }
    } catch (e) {
      /**empty**/
    }
  }

  @ActivityMethod()
  async isCommentable(integration: Integration) {
    const getIntegration = this._integrationManager.getSocialIntegration(
      integration.providerIdentifier
    );

    return !!getIntegration.comment;
  }

  @ActivityMethod()
  async postComment(
    postId: string,
    lastPostId: string | undefined,
    integration: Integration,
    posts: Post[]
  ) {
    // kept only for in-flight postWorkflowV108 runs, which set a
    // heartbeatTimeout on this activity - removing the sender would kill
    // them. Under V109+ (no heartbeatTimeout) this is a no-op and can be
    // dropped once all V108 executions have drained
    return withHeartbeat(() =>
      this.handleDisconnect(integration, async () => {
        const getIntegration = this._integrationManager.getSocialIntegration(
          integration.providerIdentifier
        );

        const newPosts = await this._postService.updateTags(
          integration.organizationId,
          posts
        );

        const guard: GuardIds = {
          orgId: integration.organizationId,
          postIds: (posts || []).map((p) => p.id),
        };
        const comments = await Promise.all(
          (newPosts || []).map(async (p) => ({
            id: p.id,
            message: stripHtmlValidation(
              getIntegration.editor,
              p.content,
              true,
              false,
              !/<\/?[a-z][\s\S]*>/i.test(p.content),
              getIntegration.mentionFormat
            ),
            settings: JSON.parse(p.settings || '{}'),
            media: await this._postService.updateMedia(
              p.id,
              JSON.parse(p.image || '[]'),
              getIntegration?.convertToJPEG || false
            ),
          }))
        );

        // last check before the provider request (SMA-053)
        await this.assertPublishable(integration, guard.orgId, guard.postIds);

        const results = await getIntegration.comment(
          integration.internalId,
          postId,
          lastPostId,
          integration.token,
          comments,
          integration,
          this.publishGuardFor(integration, guard)
        );
        await this.alertIfRevokedAfterPublish(integration, guard, results);
        return (results || []).map((r: any) => withPublishGuard(r, guard));
      })
    );
  }

  @ActivityMethod()
  async postSocial(integration: Integration, posts: Post[]) {
    return this.postSocialInternal(integration, posts, false);
  }

  // Used by postWorkflowV106 and up: providers that implement `postPending`
  // return a `pending` response the workflow resolves via checkPostStatus /
  // finalizePost. Older workflow versions keep calling `postSocial` and get
  // the old blocking behavior.
  @ActivityMethod()
  // The third argument is passed by the compiled SMA-053 workflow guard
  // (repeat run) and is accepted for signature compatibility only: the
  // activity-side guard reads the post state itself.
  async postSocialPending(
    integration: Integration,
    posts: Post[],
    _sma053Repeat?: boolean
  ) {
    return this.postSocialInternal(integration, posts, true);
  }

  // A Disconnect error means the platform will keep rejecting this channel no
  // matter how many token refreshes (e.g. TikTok's daily active user cap):
  // mark the channel as needing a re-connect and notify the user, then rethrow
  // as BadBody so every workflow version - frozen once on main - treats it as
  // a terminal error without needing a new workflow.
  private async handleDisconnect<T>(
    integration: Integration,
    func: () => Promise<T>
  ): Promise<T> {
    try {
      return await func();
    } catch (err) {
      if (err instanceof Disconnect) {
        try {
          await this._integrationService.disconnectChannel(
            integration.organizationId,
            integration,
            err.message
          );
        } catch (e) {
          /**empty**/
        }

        throw new BadBody(
          integration.providerIdentifier,
          JSON.stringify({}),
          Buffer.from('{}'),
          err.message
        );
      }

      throw err;
    }
  }

  private async postSocialInternal(
    integration: Integration,
    posts: Post[],
    allowPending: boolean
  ) {
    // kept only for in-flight postWorkflowV108 runs, which set a
    // heartbeatTimeout on this activity - removing the sender would kill
    // them. Under V109+ (no heartbeatTimeout) this is a no-op and can be
    // dropped once all V108 executions have drained
    return withHeartbeat(() =>
      this.handleDisconnect(integration, () =>
        this.postSocialBody(integration, posts, allowPending)
      )
    );
  }

  private async postSocialBody(
    integration: Integration,
    posts: Post[],
    allowPending: boolean
  ) {
    // Stage markers: whatever ran last is what a timed-out activity reports.
    // Providers that go through this.fetch overwrite these with the exact URL;
    // the ones on their own HTTP client (x, youtube, bluesky) are still
    // narrowed down to the step they hung on.
    setHeartbeatDetails('subscription lookup');
    if (process.env.STRIPE_SECRET_KEY) {
      const subscription = await this._subscriptionService.getSubscription(
        integration.organizationId
      );

      if (!subscription) {
        throw new Error('No active subscription found for this organization.');
      }
    }

    const getIntegration = this._integrationManager.getSocialIntegration(
      integration.providerIdentifier
    );

    setHeartbeatDetails('update tags');
    const newPosts = await this._postService.updateTags(
      integration.organizationId,
      posts
    );

    setHeartbeatDetails('resolve media');
    const mappedPosts = await Promise.all(
      (newPosts || []).map(async (p) => ({
        id: p.id,
        message: stripHtmlValidation(
          getIntegration.editor,
          p.content,
          true,
          false,
          !/<\/?[a-z][\s\S]*>/i.test(p.content),
          getIntegration.mentionFormat,
          !!getIntegration.inlineImages?.(JSON.parse(p.settings || '{}'))
        ),
        settings: JSON.parse(p.settings || '{}'),
        media: await this._postService.updateMedia(
          p.id,
          JSON.parse(p.image || '[]'),
          getIntegration?.convertToJPEG || false
        ),
      }))
    );

    // last check before the provider request (SMA-053)
    setHeartbeatDetails('publish guard');
    const guard: GuardIds = {
      orgId: integration.organizationId,
      postIds: (posts || []).map((p) => p.id),
    };
    await this.assertPublishable(integration, guard.orgId, guard.postIds);

    setHeartbeatDetails(`${integration.providerIdentifier}: publish`);
    const published =
      allowPending && getIntegration.postPending
        ? await getIntegration.postPending(
            integration.internalId,
            integration.token,
            mappedPosts,
            integration
          )
        : await getIntegration.post(
            integration.internalId,
            integration.token,
            mappedPosts,
            integration
          );
    await this.alertIfRevokedAfterPublish(integration, guard, published);
    const postNow = (published || []).map((r: any) =>
      withPublishGuard(r, guard)
    );

    // The post is already published at this point: the streak is best-effort,
    // failing the activity here would retry it and publish again.
    setHeartbeatDetails(`${integration.providerIdentifier}: published, streak`);
    try {
      await this._temporalService.client
        .getRawClient()
        .workflow.start('streakWorkflowV2', {
          args: [{ organizationId: integration.organizationId }],
          workflowId: `streak_${integration.organizationId}`,
          taskQueue: 'main',
          // Keep a running streak, it checks for newer posts before it ends
          workflowIdConflictPolicy: 'USE_EXISTING',
          typedSearchAttributes: new TypedSearchAttributes([
            {
              key: organizationId,
              value: integration.organizationId,
            },
          ]),
        });
    } catch (err) {
      /**empty**/
    }

    return postNow;
  }

  @ActivityMethod()
  async checkPostStatus(integration: Integration, pendingData: any) {
    const getIntegration = this._integrationManager.getSocialIntegration(
      integration.providerIdentifier
    );

    const guard: GuardIds | undefined = pendingData?.[PUBLISH_GUARD_KEY];
    const result = await this.handleDisconnect(integration, () =>
      getIntegration.checkPostStatus(integration.token, pendingData, integration)
    );
    // providers may return a new pendingData object: keep the guard on it
    return guard ? withPublishGuard(result, guard) : result;
  }

  // For pending providers (Threads among them) finalizePost is the actual
  // publish, so it is guarded too. pendingData recorded before this guard
  // existed carries no post ids: fail closed rather than publish blind.
  @ActivityMethod()
  async finalizePost(integration: Integration, pendingData: any) {
    const getIntegration = this._integrationManager.getSocialIntegration(
      integration.providerIdentifier
    );
    const guard: GuardIds | undefined = pendingData?.[PUBLISH_GUARD_KEY];

    return withHeartbeat(() =>
      this.handleDisconnect(integration, async () => {
        // cheap pre-check; the provider repeats it under the row lock around
        // the request that makes the post public
        await this.assertPublishable(
          integration,
          guard?.orgId,
          guard?.postIds || []
        );
        const ids = guard as GuardIds;
        const result = await getIntegration.finalizePost(
          integration.token,
          pendingData,
          integration,
          this.publishGuardFor(integration, ids)
        );
        await this.alertIfRevokedAfterPublish(integration, ids, [result]);
        return withPublishGuard(result, ids);
      })
    );
  }

  @ActivityMethod()
  async inAppNotification(
    orgId: string,
    subject: string,
    message: string,
    sendEmail = false,
    digest = false,
    type: NotificationType = 'success'
  ) {
    await this._notificationService.inAppNotification(
      orgId,
      subject,
      message,
      sendEmail,
      digest,
      type
    );
  }

  @ActivityMethod()
  async globalPlugs(integration: Integration) {
    return this._postService.checkPlugs(
      integration.organizationId,
      integration.providerIdentifier,
      integration.id
    );
  }

  @ActivityMethod()
  async changeState(id: string, state: State, err?: any, body?: any) {
    // SMA-053: a workflow stopped by the publish guard must not overwrite the
    // owner's DRAFT with ERROR, and a failure after the root went live must
    // not mark that live root ERROR. The skip is a single conditional write.
    if (state === State.ERROR) {
      await this._postService.markErrorUnlessSettled(id, err, body);
      return;
    }
    await this._postService.changeState(id, state, err, body);
  }

  @ActivityMethod()
  async internalPlugs(integration: Integration, settings: any) {
    return this._postService.checkInternalPlug(
      integration,
      integration.organizationId,
      integration.id,
      settings
    );
  }

  @ActivityMethod()
  async sendWebhooks(postId: string, orgId: string, integrationId: string) {
    // Webhooks are best-effort and run after the post already published, so a
    // failure here must not fail the workflow.
    try {
      const webhooks = (await this._webhookService.getWebhooks(orgId)).filter(
        (f) => {
          return (
            f.integrations.length === 0 ||
            f.integrations.some((i) => i.integration.id === integrationId)
          );
        }
      );

      if (webhooks.length === 0) {
        return;
      }

      const post = await this._postService.getPostByForWebhookId(
        postId,
        integrationId
      );
      await Promise.all(
        webhooks.map(async (webhook) => {
          try {
            // webhook.url is validated at save time, but DNS can change
            // between then and now - pin resolution like every other
            // user-influenced outbound request.
            await fetch(webhook.url, {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
              },
              body: JSON.stringify(post),
              // @ts-ignore — undici option, not in lib.dom fetch types
              dispatcher: getSsrfSafeDispatcher(),
            });
          } catch (e) {
            /**empty**/
          }
        })
      );
    } catch (err) {
      /**empty**/
    }
  }
  @ActivityMethod()
  async processPlug(data: {
    plugId: string;
    postId: string;
    delay: number;
    totalRuns: number;
    currentRun: number;
  }) {
    this.assertNoPublishHold('plug');
    return this._integrationService.processPlugs(data);
  }

  @ActivityMethod()
  async processInternalPlug(data: {
    post: string;
    originalIntegration: string;
    integration: string;
    plugName: string;
    orgId: string;
    delay: number;
    information: any;
  }) {
    this.assertNoPublishHold('internal plug');
    await this._integrationService.processInternalPlug(data);
  }

  @ActivityMethod()
  async refreshToken(
    integration: Integration
  ): Promise<false | AuthTokenDetails> {
    const getIntegration = this._integrationManager.getSocialIntegration(
      integration.providerIdentifier
    );

    try {
      const refresh = await this._refreshIntegrationService.refresh(
        integration
      );
      if (!refresh) {
        return false;
      }

      if (getIntegration.refreshWait) {
        await timer(10000);
      }

      return refresh;
    } catch (err) {
      await this._refreshIntegrationService.setBetweenSteps(
        integration,
        (err as Error)?.message || ''
      );
      return false;
    }
  }

  @ActivityMethod()
  async refreshTokenWithCause(
    integration: Integration,
    cause: string
  ): Promise<false | AuthTokenDetails> {
    const getIntegration = this._integrationManager.getSocialIntegration(
      integration.providerIdentifier
    );

    try {
      const refresh = await this._refreshIntegrationService.refresh(
        integration,
        cause
      );
      if (!refresh) {
        return false;
      }

      if (getIntegration.refreshWait) {
        await timer(10000);
      }

      return refresh;
    } catch (err) {
      await this._refreshIntegrationService.setBetweenSteps(
        integration,
        cause || (err as Error)?.message || ''
      );
      return false;
    }
  }
}
