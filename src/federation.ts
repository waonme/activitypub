import { createFederation, exportJwk, generateCryptoKeyPair } from "@fedify/fedify";
import { Person, Application, Follow, Endpoints, Accept, Reject, Undo, Note, type Recipient, Activity, Create, Like, Delete, Announce, EmojiReact, Emoji, Image, Update, Mention, type Actor } from "@fedify/vocab";
import type { Context } from "@fedify/fedify";
import { getLogger } from "@logtape/logtape";
import { RedisKvStore, RedisMessageQueue } from "@fedify/redis";
import { Redis } from "ioredis";
import { db, apEntity, apKeys, apObjectReference, type ApEntity } from './db/index.ts';
import { importJwk } from "@fedify/fedify";
import { eq, and } from "drizzle-orm";
import { CDID, NotFoundError, type Document } from '@concrnt/client'

import concrntApi, { commit, importCommit } from "./concrnt.ts";
import { config } from "./config.ts";
import { SCHEMA_AP_NOTE, SCHEMA_REROUTE, SCHEMA_REFERENCE, SCHEMA_LIKE, SCHEMA_REACTION, SCHEMA_MENTION, SCHEMA_REPLY_ASSOCIATION, SCHEMA_DELETE, parseEmojiShortcode, renderMarkdownToHtml, buildNote, buildActivity } from "./convert.ts";
import { SCHEMA_AP_FOLLOWER, SCHEMA_AP_ACCEPT_STATE, followerKey, acceptStateKey, type ApFollowerValue } from "./schemas.ts";
import * as followStore from "./followStore.ts";
import * as settingsStore from "./settingsStore.ts";
import * as objectCache from "./objectCache.ts";

const logger = getLogger("activitypub");

// ブリッジ自身のアクター。authorized fetch環境向けに、特定ユーザーに紐づかない
// fetch(共有インボックスの署名検証・匿名resolve)の署名主体として使う。
// setup側でこのidの登録を拒否して予約する。
export const INSTANCE_ACTOR = "instance.actor";

// AP objectのURLから、ブリッジ管理下のconcrnt保存先キーを決定的に導出する
const inboxKey = (url: string) =>
    `cckv://${config.concrnt.ccid}/activitypub.concrnt.world/inbox/${CDID.newFromStringX(url).toString()}`;

// リモートアクターをフォローしているローカルエンティティのinboxタイムライン一覧
const getFollowerDistribution = async (actorUri: string): Promise<string[]> => {
    return followStore.getLocalFollowerCcids(actorUri)
        .map(ccid => `cckv://${ccid}/activitypub.concrnt.world/inbox`);
};

// リモートnoteを参照ドキュメント(ap/note.json)としてconcrntに保存し、保存先キーを返す。
// viaImport: createdAtがbackdate window(7日)より古くなりうる経路(照会・Announce内側note)用。
// import経路は配送を行わないため、distributesが空の呼び出しでのみ使えるとする
export const storeApNote = async (noteURL: string, actorURL: string, createdAt: Date, distributes: string[], opts?: { viaImport?: boolean }): Promise<string> => {
    const key = inboxKey(noteURL);
    const document: Document<any> = {
        kind: 'record',
        key,
        schema: SCHEMA_AP_NOTE,
        value: {
            "actorURL": actorURL,
            "noteURL": noteURL,
        },
        author: config.concrnt.ccid,
        createdAt,
        distributes,
    };
    if (opts?.viaImport) {
        await importCommit(document);
    } else {
        await commit(document);
    }
    return key;
};

// リモートアクターの表示情報をprofileOverrideとして抽出する
const buildProfileOverride = async (actor: Actor | null): Promise<{ username?: string, avatar?: string, link?: string } | undefined> => {
    if (actor == null) return undefined;
    const override: { username?: string, avatar?: string, link?: string } = {};

    const username = actor.name?.toString() ?? actor.preferredUsername?.toString();
    if (username) override.username = username;

    try {
        const icon = await actor.getIcon();
        const url = icon?.url;
        if (url instanceof URL) {
            override.avatar = url.href;
        } else if (url?.href != null) {
            override.avatar = url.href.href;
        }
    } catch {
        // アイコン取得失敗は装飾情報のため無視
    }

    if (actor.id != null) override.link = actor.id.href;

    return override;
};

// Like/EmojiReactから絵文字リアクション情報を抽出する。なければnull(プレーンなLike)。
const extractEmojiReaction = async (activity: Like | EmojiReact): Promise<{ shortcode: string, imageUrl: string | null } | null> => {
    let shortcode = parseEmojiShortcode(activity.content?.toString());
    let imageUrl: string | null = null;

    try {
        for await (const tag of activity.getTags()) {
            if (tag instanceof Emoji) {
                const name = parseEmojiShortcode(tag.name?.toString());
                if (name) shortcode = name;
                const icon = await tag.getIcon();
                const url = icon?.url;
                if (url instanceof URL) {
                    imageUrl = url.href;
                } else if (url?.href != null) {
                    imageUrl = url.href.href;
                }
                break;
            }
        }
    } catch {
        // タグ解決失敗時はcontentから得られた情報のみで判断する
    }

    if (!shortcode) return null;
    return { shortcode, imageUrl };
};

// Like / EmojiReact 共通の受信処理
const handleLikeActivity = async (ctx: { parseUri: (uri: URL | null) => any }, activity: Like | EmojiReact) => {
    const actorUri = activity.actorId?.toString();
    const activityId = activity.id?.toString();
    if (actorUri == null || activityId == null) {
        logger.warn(`Received Like/EmojiReact activity with missing actor or activity ID`);
        return;
    }

    const target = ctx.parseUri(activity.objectId);
    if (target == null || target.type !== "object") {
        logger.warn(`Received Like/EmojiReact activity with invalid object: ${activity.objectId}`);
        return;
    }

    const apid = target.values.identifier;
    const cckv = target.values.id;

    const entity = await db.select().from(apEntity).where(eq(apEntity.id, apid)).limit(1).then(res => res[0]);
    if (!entity) {
        logger.warn(`No entity found for identifier: ${apid}`);
        return;
    }

    const distributes: string[] = [
        `cckv://${entity.ccid}/concrnt.world/profiles/main/notify-timeline`
    ];

    const liker = await activity.getActor().catch(() => null);
    const profileOverride = await buildProfileOverride(liker);

    const reaction = await extractEmojiReaction(activity);

    let document: Document<any>;
    if (reaction != null) {
        document = {
            kind: 'association',
            author: config.concrnt.ccid,
            schema: SCHEMA_REACTION,
            associate: cckv,
            associationVariant: reaction.imageUrl ?? reaction.shortcode,
            value: {
                shortcode: reaction.shortcode,
                imageUrl: reaction.imageUrl ?? '',
                ...(profileOverride ? { profileOverride } : {}),
            },
            distributes,
            createdAt: new Date(),
        };
    } else {
        document = {
            kind: 'association',
            author: config.concrnt.ccid,
            schema: SCHEMA_LIKE,
            associate: cckv,
            value: profileOverride ? { profileOverride } : {},
            distributes,
            createdAt: new Date(),
        };
    }

    const signed = await commit(document);

    // Undo(Like)でassociationを削除できるよう、AP activity id → ccfs を記録する
    if (signed?.ccfs) {
        await db.insert(apObjectReference).values({
            apObjectId: activityId,
            ccUri: signed.ccfs,
            refType: 'inbound-like',
        }).onConflictDoNothing();
    }
};

// 消滅したリモートactorのfollowerレコードを全ローカルエンティティから削除する
const purgeFollower = async (actorURI: string, cause: string) => {
    for (const entry of followStore.getFollowersByActorURI(actorURI)) {
        logger.info(`Purging follower ${actorURI} of ${entry.ccid}: ${cause}`);
        await commit({
            kind: 'delete',
            schema: SCHEMA_DELETE,
            value: entry.key,
            author: config.concrnt.ccid,
            createdAt: new Date(),
        }).catch((error) => {
            // レコードが元々存在しない場合も先へ進む(冪等)
            logger.warn(`Failed to delete follower record ${entry.key}: ${error}`);
        });
        followStore.removeFollowerByKey(entry.key);
    }
};

const federation = createFederation({
    kv: new RedisKvStore(new Redis(config.redis.url)),
    queue: new RedisMessageQueue(() => new Redis(config.redis.url)),
    // dev専用フラグ。本番configでは設定しないこと(SSRF防御が無効になる)
    allowPrivateAddress: config.activitypub.allowPrivateAddress,
    // 配送失敗(リトライ毎)の観測用。LogTapeのproperties非表示問題を避けて本文に埋め込む
    onOutboxError: (error, activity) => {
        logger.warn(`Outbox delivery failure: activity=${activity?.id?.href} error=${error}`);
    },
});

// 410 Goneを返したinboxのフォロワーは消滅済みとみなして掃除する。
// 404は一時的な設定ミスの可能性があるため、circuit-breaker-ttl(7日不達)とともにログのみ。
federation.setOutboxPermanentFailureHandler(async (_ctx, { reason, inbox, statusCode, actorIds, activity }) => {
    if (reason === "http" && statusCode === 410) {
        for (const actorId of actorIds) {
            await purgeFollower(actorId.href, `inbox ${inbox.href} returned 410 Gone`);
        }
        return;
    }
    logger.warn(`Permanent delivery failure (${reason}, status=${statusCode}): activity=${activity.id?.href} inbox=${inbox.href}`);
});

federation.setNodeInfoDispatcher("/ap/nodeinfo/2.1", async (ctx) => {
    const users = await db.select().from(apEntity).where(eq(apEntity.enabled, true));
    return {
        software: {
            name: "concrnt-ap-bridge",
            version: "0.1.0",
            homepage: new URL("https://github.com/concrnt/activitypub"),
        },
        protocols: ["activitypub"],
        usage: {
            users: {
                total: users.length,
            },
            localPosts: 0,
            localComments: 0,
        }
    }
})

federation
    .setInboxListeners("/ap/acct/{identifier}/inbox", "/ap/inbox")
    .on(Follow, async (ctx, follow) => {

        const object = ctx.parseUri(follow.objectId);
        if (object == null || object.type !== "actor") {
            logger.warn(`Received Follow activity with invalid object: ${follow.objectId}`);
            return;
        }

        const follower = await follow.getActor();
        if (follower?.id == null || follower.inboxId == null) {
            logger.warn(`Received Follow activity with invalid actor: ${follow.actorId}`);
            return;
        }

        const subscriberId = follow.actorId?.toString();
        if (subscriberId == null) {
            logger.warn(`Received Follow activity with invalid actor ID: ${follow.actorId}`);
            return;
        }

        // フォロー対象のエンティティが存在し有効な場合のみ受け入れる
        const targetEntity = await db.select().from(apEntity)
            .where(eq(apEntity.id, object.identifier)).limit(1).then(res => res[0]);
        if (!targetEntity || !targetEntity.enabled) {
            logger.info(`Rejecting Follow for unknown or disabled entity: ${object.identifier}`);
            const reject = new Reject({
                actor: follow.objectId,
                to: follow.actorId,
                object: follow,
            });
            await ctx.sendActivity(object, follower, reject);
            return;
        }

        // フォロワーをサービスアカウントのレコードとして記録する。
        // 同一キーへの再commitはupdateになるため、再Follow時のinbox更新もこれで効く。
        const key = followerKey(config.concrnt.ccid, targetEntity.ccid, subscriberId);
        const value: ApFollowerValue = {
            ccid: targetEntity.ccid,
            actorURI: subscriberId,
            inbox: follower.inboxId.toString(),
        };
        const sharedInbox = follower.endpoints?.sharedInbox?.toString();
        if (sharedInbox) value.sharedInbox = sharedInbox;

        await commit({
            kind: 'record',
            key,
            schema: SCHEMA_AP_FOLLOWER,
            value,
            author: config.concrnt.ccid,
            createdAt: new Date(),
        });
        followStore.setFollower({ ccid: targetEntity.ccid, key, actorURI: subscriberId, inbox: value.inbox, sharedInbox });

        const accept = new Accept({
            actor: follow.objectId,
            to: follow.actorId,
            object: follow,
        });

        await ctx.sendActivity(object, follower, accept);
    })
    .on(Undo, async (ctx, undo) => {
        logger.debug(`Received Undo activity from ${undo.actorId}`);
        const object = await undo.getObject();
        if (object instanceof Follow) {
            if (undo.actorId == null || undo.objectId == null) return
            const parsed = ctx.parseUri(object.objectId);
            if (parsed == null || parsed.type !== "actor") return;

            const targetEntity = await db.select().from(apEntity)
                .where(eq(apEntity.id, parsed.identifier)).limit(1).then(res => res[0]);
            if (!targetEntity) {
                logger.warn(`Received Undo(Follow) for unknown entity: ${parsed.identifier}`);
                return;
            }

            const key = followerKey(config.concrnt.ccid, targetEntity.ccid, undo.actorId.toString());
            await commit({
                kind: 'delete',
                schema: SCHEMA_DELETE,
                value: key,
                author: config.concrnt.ccid,
                createdAt: new Date(),
            }).catch((error) => {
                // レコードが元々存在しない場合も先へ進む(冪等)
                logger.warn(`Failed to delete follower record ${key}: ${error}`);
            });
            followStore.removeFollowerByKey(key);

        } else if (object instanceof Announce) {
            if (object.id == null || undo.actorId == null) return;

            // Undo の送信者が Announce の作者本人であることを確認する
            // (他者のブースト削除を防ぐ)
            if (object.actorId != null && object.actorId.href !== undo.actorId.href) {
                logger.warn(`Undo(Announce) actor mismatch: ${undo.actorId} vs ${object.actorId}`);
                return;
            }

            // 受信AnnounceはannounceのURLから決定的に導出したキーで保存しているため、
            // 参照テーブルなしで削除対象を特定できる
            const document: Document<any> = {
                kind: 'delete',
                schema: SCHEMA_DELETE,
                value: inboxKey(object.id.href),
                author: config.concrnt.ccid,
                createdAt: new Date(),
            };
            await commit(document);
        } else if (object instanceof Like || object instanceof EmojiReact) {
            if (object.id == null) return;

            // 受信Likeとして記録した参照のみを対象にする
            // (送信済みLike等の他refTypeを誤って削除しない)
            const ref = await db.select().from(apObjectReference)
                .where(and(
                    eq(apObjectReference.apObjectId, object.id.href),
                    eq(apObjectReference.refType, 'inbound-like'),
                )).limit(1).then(res => res[0]);
            if (!ref) {
                logger.warn(`No inbound-like reference found for Undo(Like): ${object.id.href}`);
                return;
            }

            const document: Document<any> = {
                kind: 'delete',
                schema: SCHEMA_DELETE,
                value: ref.ccUri,
                author: config.concrnt.ccid,
                createdAt: new Date(),
            };
            await commit(document);

            await db.delete(apObjectReference).where(eq(apObjectReference.apObjectId, object.id.href));
        } else {
            logger.warn(`Received Undo activity with unsupported object: ${object}`);
        }
    })
    .on(Accept, async (ctx, accept) => {
        logger.debug(`Received Accept activity from ${accept.actorId}`);

        const follow = await accept.getObject({ crossOrigin: 'trust' });
        if (!(follow instanceof Follow)) return

        const followerId = follow.actorId
        if (followerId == null) return
        const parsed = ctx.parseUri(followerId)
        if (parsed == null || parsed.type !== "actor") return
        const followerIdentifier = parsed.identifier

        const followTarget = follow.objectId
        if (followTarget == null) return

        const entity = await db.select().from(apEntity)
            .where(eq(apEntity.id, followerIdentifier)).limit(1).then(res => res[0]);
        if (!entity) {
            logger.warn(`Received Accept(Follow) for unknown entity: ${followerIdentifier}`);
            return;
        }

        const actorURI = followTarget.toString();
        const key = acceptStateKey(config.concrnt.ccid, entity.ccid, actorURI);
        await commit({
            kind: 'record',
            key,
            schema: SCHEMA_AP_ACCEPT_STATE,
            value: { ccid: entity.ccid, actorURI, status: 'accepted' },
            author: config.concrnt.ccid,
            createdAt: new Date(),
        });
        followStore.setAcceptState(key, entity.ccid, actorURI, 'accepted');
    })
    .on(Create, async (ctx, create) => {
        const actorUri = create.actorId?.toString();
        if (actorUri == null) {
            logger.warn(`Received Create activity with missing actor ID`);
            return;
        }

        const object = await create.getObject();
        const objectUri = object?.id?.toString();
        if (object == null || objectUri == null) {
            logger.warn(`Received Create activity with missing or invalid object`);
            return;
        }

        // Mentionタグとto/ccからローカルユーザー宛てのメンションを検出する
        const addressed = [...object.toIds, ...object.ccIds, ...create.toIds, ...create.ccIds].map(u => u.href);
        const mentionCandidates = new Set<string>(addressed);
        try {
            for await (const tag of object.getTags()) {
                if (tag instanceof Mention && tag.href != null) mentionCandidates.add(tag.href.href);
            }
        } catch {
            // タグ解決失敗時はto/ccから得られた情報のみで判断する
        }
        const mentionedEntities: ApEntity[] = [];
        for (const href of mentionCandidates) {
            const parsed = ctx.parseUri(new URL(href));
            if (parsed?.type !== "actor") continue;
            const entity = await db.select().from(apEntity)
                .where(eq(apEntity.id, parsed.identifier)).limit(1).then(res => res[0]);
            if (!entity || !entity.enabled) continue;
            if (mentionedEntities.some(e => e.ccid === entity.ccid)) continue;
            mentionedEntities.push(entity);
        }

        // inReplyToがブリッジ管理下のconcrntメッセージ宛てならリプライとして扱う。
        // それ以外(リモートnote宛てのスレッド継続等)は通常のCreateとして続行する
        let replyTarget: { entity: ApEntity, messageUri: string } | null = null;
        if (object.replyTargetId != null) {
            const parsed = ctx.parseUri(object.replyTargetId);
            if (parsed?.type === "object") {
                const entity = await db.select().from(apEntity)
                    .where(eq(apEntity.id, parsed.values.identifier)).limit(1).then(res => res[0]);
                // URI中のowner(host)とentityのccidの整合を確認する(細工されたinReplyTo対策)
                const owner = URL.parse(parsed.values.id)?.host;
                if (entity?.enabled && owner === entity.ccid) {
                    replyTarget = { entity, messageUri: parsed.values.id };
                }
            }
        }

        const followerCcids = followStore.getLocalFollowerCcids(actorUri);
        if (followerCcids.length === 0 && mentionedEntities.length === 0 && replyTarget == null) {
            logger.info(`Actor ${actorUri} has no followers, local mentions or reply target. Skipping Create activity.`);
            return;
        }

        // 本文をキャッシュする。非publicノート(Misskeyのフォロワー限定等)は
        // リモートに再fetchできないため、生の宛先を保存して閲覧可否は
        // 読み出し時にisVisibleToで評価する
        const actor = await create.getActor().catch(() => null);
        await objectCache.putObject(objectUri, {
            json: await objectCache.buildCacheJson(object, create),
            actorUri,
            addressed,
            followersUri: actor?.followersId?.href,
            receivedAt: new Date().toISOString(),
        });

        // フォロワーのinboxに加え、リプライ先ユーザー自身のinboxにも配送する
        // (リプライ先がフォロワーでもある場合はSetで重複排除)
        const noteTimelines = new Set(followerCcids.map(ccid => `cckv://${ccid}/activitypub.concrnt.world/inbox`));
        if (replyTarget != null) {
            noteTimelines.add(`cckv://${replyTarget.entity.ccid}/activitypub.concrnt.world/inbox`);
        }

        const noteKey = await storeApNote(
            objectUri,
            actorUri,
            object.published ? new Date(object.published.toString()) : new Date(),
            [...noteTimelines],
        );

        // メンションされたユーザーへはnotify-timeline宛てのassociationで通知する。
        // Mastodon等のリプライはリプライ先のMentionタグを必ず含むため、
        // リプライ先本人はリプライ通知に一本化して2重通知を防ぐ
        const profileOverride = await buildProfileOverride(actor);
        for (const entity of mentionedEntities) {
            if (replyTarget != null && entity.ccid === replyTarget.entity.ccid) continue;
            await commit({
                kind: 'association',
                author: config.concrnt.ccid,
                schema: SCHEMA_MENTION,
                associate: noteKey,
                value: profileOverride ? { profileOverride } : {},
                distributes: [`cckv://${entity.ccid}/concrnt.world/profiles/main/notify-timeline`],
                createdAt: new Date(),
            });
        }

        if (replyTarget != null) {
            const signed = await commit({
                kind: 'association',
                author: config.concrnt.ccid,
                schema: SCHEMA_REPLY_ASSOCIATION,
                associate: replyTarget.messageUri,
                value: {
                    targetURI: noteKey,
                    ...(profileOverride ? { profileOverride } : {}),
                },
                distributes: [`cckv://${replyTarget.entity.ccid}/concrnt.world/profiles/main/notify-timeline`],
                createdAt: new Date(),
            });

            // Delete(Note)でassociationを削除できるよう、note object id → ccfs を記録する
            if (signed?.ccfs) {
                await db.insert(apObjectReference).values({
                    apObjectId: objectUri,
                    ccUri: signed.ccfs,
                    refType: 'inbound-reply',
                }).onConflictDoNothing();
            }
        }
    })
    .on(Announce, async (ctx, announce) => {
        const actorUri = announce.actorId?.toString();
        const announceUri = announce.id?.toString();
        if (actorUri == null || announceUri == null) {
            logger.warn(`Received Announce activity with missing actor or activity ID`);
            return;
        }

        const distribution = await getFollowerDistribution(actorUri);
        if (distribution.length === 0) {
            logger.info(`Actor ${actorUri} has no followers. Skipping Announce activity.`);
            return;
        }

        const object = await announce.getObject();
        const noteURL = object?.id?.toString();
        if (object == null || noteURL == null) {
            logger.warn(`Received Announce activity with unresolvable object: ${announce.objectId}`);
            return;
        }

        const noteActorURL = object.attributionId?.toString() ?? actorUri;

        // 内側のnoteは解決できればよいのでタイムラインへは配送しない。
        // ブースト元が古いとbackdate windowに掛かるためimport経路で実体化する
        const noteKey = await storeApNote(
            noteURL,
            noteActorURL,
            object.published ? new Date(object.published.toString()) : new Date(),
            [],
            { viaImport: true },
        );

        const booster = await announce.getActor().catch(() => null);
        const profileOverride = await buildProfileOverride(booster);

        const document: Document<any> = {
            kind: 'record',
            key: inboxKey(announceUri),
            schema: SCHEMA_REROUTE,
            value: {
                targetURI: noteKey,
                ...(profileOverride ? { profileOverride } : {}),
            },
            author: config.concrnt.ccid,
            createdAt: announce.published ? new Date(announce.published.toString()) : new Date(),
            distributes: distribution,
        };

        await commit(document);
    })
    .on(Reject, async (ctx, reject) => {
        // こちらから送ったFollowがリモートに拒否された場合。
        // ユーザー署名のfollowレコードはブリッジには削除できないため、
        // accept-stateにrejectedを永続化して配送・一覧から除外する。
        const follow = await reject.getObject({ crossOrigin: 'trust' });
        if (!(follow instanceof Follow)) return;

        const followerId = follow.actorId;
        if (followerId == null) return;
        const parsed = ctx.parseUri(followerId);
        if (parsed == null || parsed.type !== "actor") return;
        const followerIdentifier = parsed.identifier;

        const followTarget = follow.objectId;
        if (followTarget == null) return;

        const entity = await db.select().from(apEntity)
            .where(eq(apEntity.id, followerIdentifier)).limit(1).then(res => res[0]);
        if (!entity) {
            logger.warn(`Received Reject(Follow) for unknown entity: ${followerIdentifier}`);
            return;
        }

        const actorURI = followTarget.toString();
        const key = acceptStateKey(config.concrnt.ccid, entity.ccid, actorURI);
        await commit({
            kind: 'record',
            key,
            schema: SCHEMA_AP_ACCEPT_STATE,
            value: { ccid: entity.ccid, actorURI, status: 'rejected' },
            author: config.concrnt.ccid,
            createdAt: new Date(),
        });
        followStore.setAcceptState(key, entity.ccid, actorURI, 'rejected');
    })
    .on(Update, async (ctx, update) => {
        logger.debug(`Received Update activity from ${update.actorId}`);

        // キャッシュ済みオブジェクトの本文だけ追従する(未キャッシュ・actor更新はスルー)
        const object = await update.getObject();
        if (object?.id == null) return;

        // 更新者と対象オブジェクトが同一オリジンであることを確認する
        if (update.actorId == null || new URL(update.actorId.href).host !== object.id.host) {
            logger.warn(`Update actor/object origin mismatch: ${update.actorId} vs ${object.id}`);
            return;
        }

        const cached = await objectCache.getObject(object.id.href);
        if (cached == null) return;
        cached.json = await objectCache.buildCacheJson(object, update);
        await objectCache.putObject(object.id.href, cached);
    })
    .on(EmojiReact, async (ctx, react) => {
        await handleLikeActivity(ctx, react);
    })
    .on(Like, async (ctx, like) => {
        await handleLikeActivity(ctx, like);
    })
    .on(Delete, async (ctx, del) => {
        logger.debug(`Received Delete activity from ${del.actorId}`);

        const object = await del.getObject();
        if (object == null || object.id == null) {
            logger.warn(`Received Delete activity with missing or invalid object`);
            return;
        }

        // 削除者と対象オブジェクトが同一オリジンであることを確認する
        // (他サーバーのコンテンツ削除を防ぐ)
        if (del.actorId == null || new URL(del.actorId.href).host !== object.id.host) {
            logger.warn(`Delete actor/object origin mismatch: ${del.actorId} vs ${object.id}`);
            return;
        }

        // アカウント削除(objectがactor自身)はnoteの保存キーを持たないため対象なし
        if (object.id.href === del.actorId.href) {
            logger.debug(`Ignoring account deletion from ${del.actorId.href}`);
            return;
        }

        await objectCache.deleteObject(object.id.href);

        // リプライとして記録したassociationがあれば先に削除する
        // (note本体の削除が冪等スキップされるリトライ時にも取りこぼさないよう先行)
        const replyRef = await db.select().from(apObjectReference)
            .where(and(
                eq(apObjectReference.apObjectId, object.id.href),
                eq(apObjectReference.refType, 'inbound-reply'),
            )).limit(1).then(res => res[0]);
        if (replyRef != null) {
            try {
                await commit({
                    kind: 'delete',
                    schema: SCHEMA_DELETE,
                    value: replyRef.ccUri,
                    author: config.concrnt.ccid,
                    createdAt: new Date(),
                });
            } catch (error) {
                // 既に消えているassociationは冪等に成功扱いにする
                if (!(error instanceof NotFoundError || String(error).includes("not found"))) {
                    throw error;
                }
            }
            await db.delete(apObjectReference).where(eq(apObjectReference.apObjectId, object.id.href));
        }

        const document: Document<any> = {
            kind: 'delete',
            schema: SCHEMA_DELETE,
            value: inboxKey(object.id.href),
            author: config.concrnt.ccid,
            createdAt: new Date(),
        }

        try {
            await commit(document);
        } catch (error) {
            // 保存していないnoteのDeleteは冪等に成功扱いにする
            // (throwするとfedifyが無駄にリトライし続ける)
            // 現行コアはcommitハンドラーでErrNotFoundを404にマップせず
            // 500+"not found"本文で返すため、文字列判定も併用する
            if (error instanceof NotFoundError || String(error).includes("not found")) {
                logger.debug(`Delete for unstored object ${object.id.href}: ${error}`);
                return;
            }
            throw error;
        }
    })
    // 署名検証に失敗した配送の送信元と対象を記録する(戻り値なし=従来通り401で拒否)
    .onUnverifiedActivity(async (_ctx, activity, reason) => {
        const keyId = "keyId" in reason ? reason.keyId?.href : undefined;
        const fetchStatus =
            reason.type === "keyFetchError" && "status" in reason.result
                ? reason.result.status
                : undefined;

        // 消滅済みactorのDelete(actor)は鍵取得が410になり署名検証できない。
        // 401で拒否するとリモートが再配送し続けるため、actorのサーバー自身が
        // 鍵の消滅を主張している(keyIdがactorと同一オリジン)場合に限り202で受理し、
        // followerレコードを掃除する。
        if (
            fetchStatus === 410 &&
            activity instanceof Delete &&
            activity.actorId != null &&
            activity.objectId?.href === activity.actorId.href &&
            reason.type === "keyFetchError" &&
            reason.keyId.origin === activity.actorId.origin
        ) {
            await purgeFollower(activity.actorId.href, `actor deleted (key fetch returned 410)`);
            return new Response(null, { status: 202 });
        }

        logger.warn(
            `Rejected unverified inbox delivery: reason=${reason.type}` +
            ` key=${keyId} fetchStatus=${fetchStatus}` +
            ` actor=${activity.actorId?.href} activity=${activity.id?.href}` +
            ` object=${activity.objectId?.href}`,
        );
    })
    // 共有インボックス宛て配送の署名検証(鍵fetch・actor解決)をインスタンスアクターの
    // 鍵で署名する。未設定だと無署名fetchになり、authorized fetch実装(GoToSocial等)
    // からの配送が全て検証失敗する。個人インボックスはfedifyが受信者鍵で署名済み。
    .setSharedKeyDispatcher(() => ({ identifier: INSTANCE_ACTOR }))
;


// concrntプロフィール(p/main.json)を反映したPersonアクターを構築する。
// エンティティが存在しなければnull。
export const buildPerson = async (ctx: Context<unknown>, identifier: string): Promise<Person | null> => {
    const users = await db.select().from(apEntity).where(eq(apEntity.id, identifier)).limit(1);
    if (users.length === 0) return null;
    const entity = users[0];

    const keys = await ctx.getActorKeyPairs(identifier);

    // profile未作成entityのactor取得ごとの再フェッチを抑えるため、negative cacheを5分効かせる
    const profile = await concrntApi.getDocument<any>(`cckv://${entity.ccid}/concrnt.world/profiles/main`, undefined, { negativeTTL: 300_000 })
        .then(doc => doc?.value ?? null)
        .catch(() => null);

    return new Person({
        id: ctx.getActorUri(identifier),
        preferredUsername: identifier,
        name: profile?.username ?? identifier,
        summary: profile?.description ? renderMarkdownToHtml(profile.description) : null,
        icon: profile?.avatar ? new Image({ url: new URL(profile.avatar) }) : null,
        image: profile?.banner ? new Image({ url: new URL(profile.banner) }) : null,
        inbox: ctx.getInboxUri(identifier),
        outbox: ctx.getOutboxUri(identifier),
        endpoints: new Endpoints({
            sharedInbox: ctx.getInboxUri(),
        }),
        url: ctx.getActorUri(identifier),
        publicKey: keys[0]?.cryptographicKey,
        assertionMethods: keys.map((k) => k.multikey),
        followers: ctx.getFollowersUri(identifier),
    });
};

// id・inbox・publicKey等の必須プロパティはbuildPerson内で設定している(静的解析の誤検知)
// eslint-disable-next-line @fedify/lint/actor-id-required
federation.setActorDispatcher("/ap/acct/{identifier}", async (ctx, identifier) => {
    if (identifier === INSTANCE_ACTOR) {
        const keys = await ctx.getActorKeyPairs(identifier);
        return new Application({
            id: ctx.getActorUri(identifier),
            preferredUsername: identifier,
            name: "concrnt-ap-bridge",
            inbox: ctx.getInboxUri(identifier),
            endpoints: new Endpoints({
                sharedInbox: ctx.getInboxUri(),
            }),
            url: ctx.getActorUri(identifier),
            publicKey: keys[0]?.cryptographicKey,
            assertionMethods: keys.map((k) => k.multikey),
        });
    }
    return await buildPerson(ctx, identifier);
}).setKeyPairsDispatcher(async (ctx, identifier) => {


    const keys = await db.select().from(apKeys).where(eq(apKeys.ownerId, identifier));

    const pairs: CryptoKeyPair[] = []

    for (const keyType of ["RSASSA-PKCS1-v1_5", "Ed25519"] as const) {
        const key = keys.find(k => k.keyType === keyType);
        if (key == null) {
            logger.debug(
                `The user ${identifier} does not have a ${keyType} key; creating one...`,
            );
            const { privateKey, publicKey } = await generateCryptoKeyPair(keyType);
            await db.insert(apKeys).values({
                ownerId: identifier,
                keyType,
                private: JSON.stringify(await exportJwk(privateKey)),
                public: JSON.stringify(await exportJwk(publicKey)),
            });
            pairs.push({ privateKey, publicKey });
        } else {
            pairs.push({
                privateKey: await importJwk(JSON.parse(key.private), "private"),
                publicKey: await importJwk(JSON.parse(key.public), "public"),
            });
        }
    }

    return pairs;
});

// 過去投稿のページング付きoutbox。daemonのリアルタイム送出(handleOutboundCreate)と
// 同じ対象基準・同じactivity idで、listen対象タイムラインの履歴を列挙する。
// カーソルはISOタイムスタンプ「そのtimestampより厳密に古いitemから」、空文字=最新から。
// 注: daemonはdocument.distributesとlisten timelineの前方一致で判定するのに対し、
// こちらはlisten timelineの列挙なので、サフィックス付きURIがdistributesに載る
// ケースのみ差が出る(実運用のdistributesはtimeline URIそのもののため許容)。
const OUTBOX_PAGE_SIZE = 20;
// フィルタで1件も残らないページが続いた場合に内部で読み進める上限
const OUTBOX_MAX_SCAN_ROUNDS = 5;

federation.setOutboxDispatcher(
    "/ap/acct/{identifier}/outbox",
    async (ctx, identifier, cursor) => {
        const entity = await db.select().from(apEntity)
            .where(eq(apEntity.id, identifier)).limit(1).then(res => res[0]);
        if (!entity) return null;

        let untilMs: number | undefined;
        if (cursor) {
            untilMs = Date.parse(cursor);
            if (Number.isNaN(untilMs)) return null;
        }

        // 新規entityがdaemonの60秒周期ロードより先に読まれた場合に備える(ロード済みならno-op)
        await settingsStore.ensureEntitySettingsLoaded(entity.ccid).catch(() => {});
        const listenTimelines = settingsStore.getListenTimelines(entity.ccid);
        const timelines = listenTimelines.length > 0
            ? listenTimelines
            : [`cckv://${entity.ccid}/concrnt.world/profiles/main/home-timeline`];

        const activities: Activity[] = [];
        const seen = new Set<string>();
        let nextCursor: string | null = null;

        for (let round = 0; round < OUTBOX_MAX_SCAN_ROUNDS; round++) {
            const cutoffMs = untilMs;
            const raw = await concrntApi.getTimelineRanged(timelines, {
                until: cutoffMs != null ? new Date(cutoffMs) : undefined,
                limit: OUTBOX_PAGE_SIZE,
            });
            // untilは境界包含・チャンク単位の重複がありうるため厳密に切る
            const items = cutoffMs != null
                ? raw.filter(item => item.timestamp.getTime() < cutoffMs)
                : raw;

            for (const item of items) {
                if (!item.href || seen.has(item.href)) continue;
                seen.add(item.href);
                // コミュニティタイムライン等に混ざる他人のレコードはフェッチ前に排除する
                if (URL.parse(item.href)?.host !== entity.ccid) continue;

                const document = await concrntApi.getDocument<any>(item.href, undefined, { negativeTTL: 300_000 })
                    .catch(() => null);
                if (document == null) continue; // 削除済み・取得失敗
                // daemon側のfederate対象判定と同一基準
                if (document.author !== entity.ccid || document.kind !== 'record') continue;
                if (document.schema === SCHEMA_REFERENCE) continue;

                const activity = await buildActivity(ctx, { identifier, id: document.key ?? item.href }, document)
                    .catch(() => null);
                if (activity == null) continue; // Note化不能・Announce先解決不能
                activities.push(activity);
            }

            if (raw.length < OUTBOX_PAGE_SIZE) {
                // タイムライン終端
                nextCursor = null;
                break;
            }
            const oldestMs = items.length > 0 ? items[items.length - 1].timestamp.getTime() : NaN;
            if (Number.isNaN(oldestMs) || (cutoffMs != null && oldestMs >= cutoffMs)) {
                // カーソルが進まない(同時刻詰まり)場合はここで打ち切る
                nextCursor = null;
                break;
            }
            nextCursor = new Date(oldestMs).toISOString();
            if (activities.length > 0) break;
            untilMs = oldestMs; // フィルタで全滅したページは内部で読み進める
        }

        return { items: activities, nextCursor };
    },
).setFirstCursor(async (ctx, identifier) => {
    // firstCursor設定時、コレクション本体のGETではdispatcherが呼ばれないため、
    // ここでentity存在を確認する。不在時はnull → fedifyが非ページ経路で
    // dispatcher(cursor=null)を呼び、そちらのentityチェックで404になる。
    const exists = await db.select().from(apEntity)
        .where(eq(apEntity.id, identifier)).limit(1).then(res => res.length > 0);
    return exists ? "" : null;
});

federation.setFollowersDispatcher(
    "/ap/acct/{identifier}/followers",
    async (ctx, identifier) => {
        const entity = await db.select().from(apEntity)
            .where(eq(apEntity.id, identifier)).limit(1).then(res => res[0]);
        if (!entity) return null;

        const items: Recipient[] = followStore.getFollowers(entity.ccid).map(f => ({
            id: new URL(f.actorURI),
            inboxId: new URL(f.inbox),
            endpoints:
                f.sharedInbox
                ? { sharedInbox: new URL(f.sharedInbox) }
                : undefined,
        }));

        return { items }
    },
).setCounter(async (ctx, identifier) => {
    const entity = await db.select().from(apEntity)
        .where(eq(apEntity.id, identifier)).limit(1).then(res => res[0]);
    if (!entity) return 0;
    return followStore.getFollowers(entity.ccid).length;
});


federation.setObjectDispatcher(
    Note,
    "/ap/acct/{identifier}/posts/{+id}",
    async (ctx, values) => {

        const entity = await db.select().from(apEntity).where(eq(apEntity.id, values.identifier)).limit(1);
        if (entity.length === 0) {
            logger.warn(`No entity found for identifier: ${values.identifier}`);
            return null;
        }

        const uri = URL.parse(values.id)
        if (uri == null) {
            logger.warn(`Invalid URI for Note ID: ${values.id}`);
            return null;
        }
        const owner = uri.host

        if (owner !== entity[0].ccid) {
            logger.warn(`Owner mismatch for Note. Expected: ${entity[0].ccid}, Found: ${owner}`);
            return null;
        }

        const document = await concrntApi.getDocument<any>(values.id)

        return await buildNote(ctx, values, document);
    },
);

export default federation;
