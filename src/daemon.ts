import { db, apEntity, apObjectReference, type ApEntity } from './db/index.ts';
import { Redis } from "ioredis";
import { and, eq } from "drizzle-orm";
import fedi, { buildPerson } from "./federation.ts";
import { Announce, Delete, Emoji, Follow, Image, isActor, Like, Note, PUBLIC_COLLECTION, Tombstone, Undo, Update } from '@fedify/vocab';

import { getLogger } from "@logtape/logtape";
import { type Document } from "@concrnt/client";

import concrntApi, { commit } from "./concrnt.ts";
import { config } from "./config.ts";
import { buildActivity, SCHEMA_AP_NOTE, SCHEMA_REFERENCE, SCHEMA_LIKE, SCHEMA_REACTION, SCHEMA_DELETE } from "./convert.ts";
import { SCHEMA_AP_FOLLOW, SCHEMA_AP_FOLLOWER, SCHEMA_AP_ACCEPT_STATE, AP_NAMESPACE, acceptStateKey, settingsKey, type ApFollowerValue, type ApAcceptStateValue } from "./schemas.ts";
import * as followStore from "./followStore.ts";
import * as settingsStore from "./settingsStore.ts";

interface CoreSignedDocument {
    document: string;
    references?: Record<string, CoreSignedDocument>;
}

// concrntコアがpubsubへ流すイベント
interface CoreEvent {
    type: string;
    uri: string;
    source?: string;
    association?: string;
    documents?: Record<string, CoreSignedDocument>;
}

const logger = getLogger("activitypub");

let entities: ApEntity[] = [];

// 送信済みLike/リアクションのccfs集合。psubscribe("cc-event:*")で流れてくる大量の
// deleted イベントに対し、ブリッジ由来のものだけをDB照会するためのフィルタ。
const outboundLikeCcfs = new Set<string>();

const updateEntities = async () => {
    entities = await db.select().from(apEntity);
}

// 60秒周期: entityリロード後、新規entityのフォローと未ロードのサービスレコードを取り込む
const refreshEntities = async () => {
    await updateEntities();
    await followStore.ensureServiceRecordsLoaded().catch((error) => {
        logger.error(`Failed to load service follow records: ${error}`);
    });
    for (const entity of entities) {
        await followStore.ensureEntityFollowsLoaded(entity.ccid).catch((error) => {
            logger.error(`Failed to load follows for ${entity.ccid}: ${error}`);
        });
        await settingsStore.ensureEntitySettingsLoaded(entity.ccid).catch((error) => {
            logger.error(`Failed to load settings for ${entity.ccid}: ${error}`);
        });
    }
}

const loadOutboundLikes = async () => {
    const rows = await db.select().from(apObjectReference)
        .where(eq(apObjectReference.refType, 'outbound-like'));
    outboundLikeCcfs.clear();
    for (const row of rows) outboundLikeCcfs.add(row.ccUri);
}

// リモートタイムライン宛のイベントはローカルRedisに流れないため、タイムラインチャンネルの
// 監視では投稿を拾えない。代わりに、必ずローカルにpublishされる「レコード自身のURIチャンネル」の
// イベントを捕捉し、document.distributesと監視対象タイムライン(未設定ならhome-timeline)を
// 前方一致で突合する。1投稿=1イベントなので複数タイムライン同時配布でも重複送信しない。
const handleOwnRecordEvent = async (entity: ApEntity, channel: string, msg: CoreEvent) => {

    if (msg.type === "created") {

        // イベントに同梱された署名済みドキュメントを優先し、なければフェッチする
        const eventSD = msg.documents?.[channel];
        const document: any = eventSD
            ? JSON.parse(eventSD.document)
            : await concrntApi.getDocument<any>(channel).catch(() => null);
        if (document == null) return;

        if (document.author !== entity.ccid || document.kind !== 'record') return;

        // タイムラインへ配られる参照レコードは実体レコード側のイベントで処理する(二重federate防止)
        if (document.schema === SCHEMA_REFERENCE) return;

        const listenTimelines = settingsStore.getListenTimelines(entity.ccid);
        const prefixes = listenTimelines.length > 0
            ? listenTimelines
            : [`cckv://${entity.ccid}/concrnt.world/profiles/main/home-timeline`];

        const distributes: string[] = Array.isArray(document.distributes) ? document.distributes : [];
        if (!distributes.some(dest => prefixes.some(prefix => dest.startsWith(prefix)))) return;

        await handleOutboundCreate(entity, document.key ?? channel, document);

    } else if (msg.type === "deleted") {
        // deletedは配布先チャンネルにも流れるため、レコード自身のチャンネルのイベントのみ処理
        if (channel !== msg.uri) return;
        await handleOutboundDelete(entity, msg.uri);
    }
}

const handleOutboundCreate = async (entity: ApEntity, cckv: string, document: any) => {

    const baseURL = new URL(config.activitypub.baseUrl);
    const ctx = fedi.createContext(baseURL, undefined);

    // 手元のdocumentから直接アクティビティを構築する(自己HTTP経由の再取得を避ける)。
    const activity = await buildActivity(ctx, { identifier: entity.id, id: cckv }, document);
    if (activity == null) {
        logger.info(`Document does not resolve to an AP activity, skipping: ${cckv}`);
        return;
    }

    await ctx.sendActivity(
        { identifier: entity.id },
        "followers",
        activity,
    );

    if (activity instanceof Announce) {
        // unboost時にUndo(Announce)を送るための対応を記録
        await db.insert(apObjectReference).values({
            apObjectId: activity.id!.href,
            ccUri: cckv,
            refType: 'outbound-announce',
            meta: { object: activity.objectId!.href },
        }).onConflictDoNothing();

        return;
    }

    // deletedイベントはdistributesを運ばず監視設定と突合できないため、
    // 送信済みNoteを記録しておき、削除時はこの対応表で判定する
    await db.insert(apObjectReference).values({
        apObjectId: ctx.getObjectUri(Note, { identifier: entity.id, id: cckv }).href,
        ccUri: cckv,
        refType: 'outbound-note',
    }).onConflictDoNothing();

    // メンション・リプライ相手(ccに含まれるアクター)には直接配送する。
    // フォロワーの有無に関わらず届ける必要がある。
    const followersUri = ctx.getFollowersUri(entity.id).href;
    const documentLoader = await ctx.getDocumentLoader({ identifier: entity.id });
    const extraRecipients = (await Promise.all(
        activity.ccIds
            .filter(cc => cc.href !== PUBLIC_COLLECTION.href && cc.href !== followersUri)
            .map(cc => ctx.lookupObject(cc.href, { documentLoader }).catch(() => null))
    )).filter(isActor);
    if (extraRecipients.length > 0) {
        await ctx.sendActivity(
            { identifier: entity.id },
            extraRecipients,
            activity,
            // ローカル同士のメンションがAPブリッジ経由で二重通知されるのを防ぐ
            { excludeBaseUris: [baseURL] },
        );
    }
}

const handleOutboundDelete = async (entity: ApEntity, cckv: string) => {

    const ctx = fedi.createContext(new URL(config.activitypub.baseUrl), undefined);

    const refs = await db.select().from(apObjectReference)
        .where(eq(apObjectReference.ccUri, cckv));

    // 送信済みAnnounceの削除ならUndo(Announce)を送る
    const announceRef = refs.find(ref => ref.refType === 'outbound-announce');
    if (announceRef) {
        const announceId = new URL(announceRef.apObjectId);
        await ctx.sendActivity(
            { identifier: entity.id },
            "followers",
            new Undo({
                id: new URL("#undo", announceId),
                actor: ctx.getActorUri(entity.id),
                object: new Announce({
                    id: announceId,
                    actor: ctx.getActorUri(entity.id),
                    object: announceRef.meta?.object ? new URL(announceRef.meta.object) : null,
                }),
                tos: [PUBLIC_COLLECTION],
            }),
        );
        await db.delete(apObjectReference).where(eq(apObjectReference.apObjectId, announceRef.apObjectId));
        return;
    }

    // Note未送信のレコード削除(follows/settings等)でDelete(Tombstone)を誤配信しない。
    // outbound-note記録開始以前にfederate済みのNoteの救済として、投稿キーのパターンに
    // 一致する場合のみ記録なしでもDeleteを送る。
    const noteRef = refs.find(ref => ref.refType === 'outbound-note');
    const isPostKey = cckv.startsWith(`cckv://${entity.ccid}/concrnt.world/profiles/`) && cckv.includes('/posts/');
    if (!noteRef && !isPostKey) return;

    const noteArgs = { identifier: entity.id, id: cckv };
    const noteURL = ctx.getObjectUri(Note, noteArgs);

    await ctx.sendActivity(
        { identifier: entity.id },
        "followers",
        new Delete({
            id: new URL(`#delete-${Date.now()}`, noteURL),
            actor: ctx.getActorUri(entity.id),
            object: new Tombstone({ id: noteURL }),
            tos: [PUBLIC_COLLECTION],
        })
    );

    if (noteRef) {
        await db.delete(apObjectReference).where(eq(apObjectReference.apObjectId, noteRef.apObjectId));
    }
}

// concrntプロフィールが更新されたらUpdate(Person)をフォロワーへ配信する
const handleProfileUpdate = async (entity: ApEntity) => {

    const ctx = fedi.createContext(new URL(config.activitypub.baseUrl), undefined);

    const person = await buildPerson(ctx, entity.id);
    if (person == null) return;

    await ctx.sendActivity(
        { identifier: entity.id },
        "followers",
        new Update({
            id: new URL(`${config.activitypub.baseUrl}/ap/${config.activitypub.actorPathSegment}/${entity.id}#update-${Date.now()}`),
            actor: ctx.getActorUri(entity.id),
            object: person,
            tos: [PUBLIC_COLLECTION],
            ccs: [ctx.getFollowersUri(entity.id)],
        }),
    );
}

const handleAssociationEvent = async (msg: CoreEvent) => {

    const ccfs = msg.association;
    if (ccfs == null) return;

    // イベントに同梱されたassociationドキュメントを優先し、なければフェッチする
    const assocSD = msg.documents?.[ccfs];
    const association: any = assocSD
        ? JSON.parse(assocSD.document)
        : await concrntApi.getDocument<any>(ccfs).catch(() => null);
    if (association == null) {
        logger.error(`Failed to resolve association document: ${ccfs}`);
        return;
    }

    if (association.schema !== SCHEMA_LIKE && association.schema !== SCHEMA_REACTION) {
        return; // Like・リアクション以外のassociationは連合しない
    }

    const likerccid = association.author;

    const likerEntity = await db.select().from(apEntity).where(eq(apEntity.ccid, likerccid)).limit(1).then(res => res[0]);
    if (!likerEntity) {
        logger.error(`No entity found for author CCID: ${likerccid}`);
        return;
    }

    // Like対象は ap/note.json (リモート投稿の参照) でなければ連合しない。
    // 受信Announceも同じinbox名前空間にreroute記録を保存するため、スキーマで判別する。
    const target = await concrntApi.getDocument<any>(msg.uri, undefined, { negativeTTL: 300_000 }).catch(() => null);
    if (target == null || target.schema !== SCHEMA_AP_NOTE || !target.value?.actorURL || !target.value?.noteURL) {
        logger.info(`Association target is not a bridgeable AP note, skipping: ${msg.uri}`);
        return;
    }

    const actorURL = new URL(target.value.actorURL);
    const noteURL = new URL(target.value.noteURL);

    const ctx = fedi.createContext(new URL(config.activitypub.baseUrl), undefined);

    const documentLoader = await ctx.getDocumentLoader({ identifier: likerEntity.id });
    const actor = await ctx.lookupObject(actorURL.href, { documentLoader });
    if (!actor || !isActor(actor)) {
        logger.error(`Failed to fetch actor for association: ${actorURL.href}`);
        return;
    }

    const likerUri = ctx.getActorUri(likerEntity.id);
    const likeId = new URL(`${config.activitypub.baseUrl}/ap/likes/${encodeURIComponent(ccfs)}`);

    let like: Like;
    if (association.schema === SCHEMA_REACTION) {
        const shortcode: string | undefined = association.value?.shortcode;
        const imageUrl: string | undefined = association.value?.imageUrl;
        like = new Like({
            id: likeId,
            actor: likerUri,
            object: noteURL,
            content: shortcode ? `:${shortcode}:` : null,
            tags: (shortcode && imageUrl) ? [
                new Emoji({
                    id: new URL(imageUrl),
                    name: `:${shortcode}:`,
                    icon: new Image({ url: new URL(imageUrl) }),
                }),
            ] : [],
        });
    } else {
        like = new Like({
            id: likeId,
            actor: likerUri,
            object: noteURL,
            content: "⭐",
        });
    }

    await ctx.sendActivity(
        { identifier: likerEntity.id },
        actor,
        like,
    );

    // 削除時にUndo(Like)を送るための対応を記録
    await db.insert(apObjectReference).values({
        apObjectId: likeId.href,
        ccUri: ccfs,
        refType: 'outbound-like',
        meta: {
            likerId: likerEntity.id,
            actor: actorURL.href,
            object: noteURL.href,
        },
    }).onConflictDoNothing();
    outboundLikeCcfs.add(ccfs);
}

// ローカルユーザーがLike/リアクションを削除したらUndo(Like)を送る
const handleAssociationDeleted = async (msg: CoreEvent) => {

    // 全nodeのassociation削除が流れてくるため、ブリッジ由来のものだけDB照会する
    if (!outboundLikeCcfs.has(msg.uri)) return;

    const ref = await db.select().from(apObjectReference)
        .where(and(
            eq(apObjectReference.ccUri, msg.uri),
            eq(apObjectReference.refType, 'outbound-like'),
        )).limit(1).then(res => res[0]);
    if (!ref) {
        outboundLikeCcfs.delete(msg.uri);
        return;
    }

    const meta = ref.meta ?? {};
    const likerId = meta.likerId;
    const actorURL = meta.actor;
    const noteURL = meta.object;

    if (!likerId || !actorURL || !noteURL) {
        logger.error(`Incomplete metadata for outbound like reference: ${ref.apObjectId}`);
        await db.delete(apObjectReference).where(eq(apObjectReference.apObjectId, ref.apObjectId));
        outboundLikeCcfs.delete(msg.uri);
        return;
    }

    const ctx = fedi.createContext(new URL(config.activitypub.baseUrl), undefined);

    const documentLoader = await ctx.getDocumentLoader({ identifier: likerId });
    const actor = await ctx.lookupObject(actorURL, { documentLoader });
    if (!actor || !isActor(actor)) {
        logger.error(`Failed to fetch actor for undo like: ${actorURL}`);
        return;
    }

    const likeId = new URL(ref.apObjectId);

    await ctx.sendActivity(
        { identifier: likerId },
        actor,
        new Undo({
            id: new URL("#undo", likeId),
            actor: ctx.getActorUri(likerId),
            object: new Like({
                id: likeId,
                actor: ctx.getActorUri(likerId),
                object: new URL(noteURL),
            }),
        }),
    );

    await db.delete(apObjectReference).where(eq(apObjectReference.apObjectId, ref.apObjectId));
    outboundLikeCcfs.delete(msg.uri);
}

// Follow/Undo(Follow)を突合できるよう、followレコードのキーから決定的にアクティビティidを導出する
const followActivityId = (recordKey: string) =>
    new URL(`${config.activitypub.baseUrl}/ap/follows/${encodeURIComponent(recordKey)}`);

// pendingのフォローへFollowアクティビティを再送する(app.tsの内部API用)。
// v1移行はAcceptを取りこぼしたaccepted=false行をpendingのまま持ち込むため、
// リモートでは確立済みの関係が承認待ち表示で残ることがある。重複Followには
// Mastodon等が冪等にAcceptを返すので、再送は検証と修復を兼ねる。activity idは
// 初回送信と同じfollowActivityId(レコードキー由来)なのでAccept突合も変わらない。
export interface ResendFollowResult { ccid: string, actorURI: string, status: 'sent' | 'failed' | 'skipped', reason?: string }

export const resendPendingFollows = async (opts: { ccid?: string, actorURIs?: string[], dryRun?: boolean }): Promise<ResendFollowResult[]> => {
    if (opts.ccid !== undefined && opts.ccid.trim() === '') {
        throw new TypeError('ccid must be a non-empty string when provided');
    }
    const ctx = fedi.createContext(new URL(config.activitypub.baseUrl), undefined);
    // propertyが無い時だけ全pendingを対象にする。明示された空配列は「対象なし」。
    const wanted = opts.actorURIs === undefined ? null : new Set(opts.actorURIs);

    let entities = await db.select().from(apEntity).where(eq(apEntity.enabled, true));
    if (opts.ccid) entities = entities.filter((e) => e.ccid === opts.ccid);

    const results: ResendFollowResult[] = [];
    for (const entity of entities) {
        await followStore.ensureEntityFollowsLoaded(entity.ccid);
        for (const entry of followStore.getFollowing(entity.ccid)) {
            if (wanted && !wanted.has(entry.actorURI)) continue;
            if (entry.status !== 'pending') {
                // 明示指定された対象がpendingでない場合だけ、その旨を報告する
                if (wanted) results.push({ ccid: entity.ccid, actorURI: entry.actorURI, status: 'skipped', reason: `state is ${entry.status}` });
                continue;
            }
            if (opts.dryRun) {
                results.push({ ccid: entity.ccid, actorURI: entry.actorURI, status: 'skipped', reason: 'dry-run' });
                continue;
            }
            try {
                const documentLoader = await ctx.getDocumentLoader({ identifier: entity.id });
                const actor = await ctx.lookupObject(entry.actorURI, { documentLoader });
                if (actor == null || !isActor(actor) || actor.id == null) {
                    results.push({ ccid: entity.ccid, actorURI: entry.actorURI, status: 'failed', reason: 'actor does not resolve' });
                    continue;
                }
                await ctx.sendActivity(
                    { identifier: entity.id },
                    actor,
                    new Follow({
                        id: followActivityId(entry.key),
                        actor: ctx.getActorUri(entity.id),
                        object: new URL(entry.actorURI),
                        to: new URL(entry.actorURI),
                    }),
                    { excludeBaseUris: [new URL(config.activitypub.baseUrl)] },
                );
                logger.info(`Re-sent Follow to ${entry.actorURI} for ${entity.id}`);
                results.push({ ccid: entity.ccid, actorURI: entry.actorURI, status: 'sent' });
            } catch (error) {
                results.push({ ccid: entity.ccid, actorURI: entry.actorURI, status: 'failed', reason: String(error) });
            }
        }
    }
    return results;
}

const deleteServiceRecord = async (key: string) => {
    const document: Document<string> = {
        kind: 'delete',
        schema: SCHEMA_DELETE,
        value: key,
        author: config.concrnt.ccid,
        createdAt: new Date(),
    };
    await commit(document);
}

// ユーザー署名のfollowレコード(source of truth)のイベントを処理する。
// created → Followアクティビティ送信 / deleted → Undo(Follow)送信。
const handleFollowRecordEvent = async (entity: ApEntity, channel: string, msg: CoreEvent) => {

    const ctx = fedi.createContext(new URL(config.activitypub.baseUrl), undefined);

    if (msg.type === "created") {

        if (!entity.enabled) return;

        const eventSD = msg.documents?.[channel];
        const document: any = eventSD
            ? JSON.parse(eventSD.document)
            : await concrntApi.getDocument<any>(channel).catch(() => null);
        if (document == null) {
            logger.error(`Failed to resolve follow document: ${channel}`);
            return;
        }
        if (document.author !== entity.ccid || document.schema !== SCHEMA_AP_FOLLOW) return;

        const actorURI: string | undefined = document.value?.actorURI;
        if (!actorURI) {
            logger.warn(`Follow record without actorURI: ${channel}`);
            return;
        }

        // kv上書きの再createdで既に処理済みならスキップ。
        // rejected状態ならリトライとみなし、状態を消してFollowを再送する。
        if (followStore.getByKey(channel)) {
            if (followStore.getAcceptState(entity.ccid, actorURI) !== 'rejected') return;
            const stateKey = acceptStateKey(config.concrnt.ccid, entity.ccid, actorURI);
            await deleteServiceRecord(stateKey).catch((error) => {
                logger.warn(`Failed to delete accept-state record ${stateKey}: ${error}`);
            });
            followStore.removeAcceptStateByKey(stateKey);
        }

        // 送信前に登録し、送信に失敗したら取り消す(再createdで再試行可能にする)
        followStore.setFollowing({ ccid: entity.ccid, key: channel, actorURI });

        try {
            const documentLoader = await ctx.getDocumentLoader({ identifier: entity.id });
            const actor = await ctx.lookupObject(actorURI, { documentLoader });
            if (actor == null || !isActor(actor) || actor.id == null) {
                logger.warn(`Follow target does not resolve to an actor: ${actorURI}`);
                followStore.removeFollowingByKey(channel);
                return;
            }
            if (actor.id.href !== actorURI) {
                // クライアントは正規のアクターidを書く契約。ずれていても
                // Accept/Undoとの突合一貫性を優先してレコード値のURIを使い続ける。
                logger.warn(`Follow record actorURI is not canonical: ${actorURI} (canonical: ${actor.id.href})`);
            }

            await ctx.sendActivity(
                { identifier: entity.id },
                actor,
                new Follow({
                    id: followActivityId(channel),
                    actor: ctx.getActorUri(entity.id),
                    object: new URL(actorURI),
                    to: new URL(actorURI),
                }),
                { excludeBaseUris: [new URL(config.activitypub.baseUrl)] },
            );
            logger.info(`Sent Follow to ${actorURI} for ${entity.id}`);
        } catch (error) {
            followStore.removeFollowingByKey(channel);
            throw error;
        }

    } else if (msg.type === "deleted") {

        // deletedイベントは値を持たないため、メモリ上のキー逆引きで対象を特定する
        const ref = followStore.getByKey(msg.uri);
        if (!ref || ref.kind !== 'follow' || ref.ccid !== entity.ccid) return;
        const actorURI = ref.actorURI;

        const documentLoader = await ctx.getDocumentLoader({ identifier: entity.id });
        const actor = await ctx.lookupObject(actorURI, { documentLoader }).catch(() => null);
        if (actor != null && isActor(actor)) {
            await ctx.sendActivity(
                { identifier: entity.id },
                actor,
                new Undo({
                    id: new URL("#undo", followActivityId(msg.uri)),
                    actor: ctx.getActorUri(entity.id),
                    object: new Follow({
                        id: followActivityId(msg.uri),
                        actor: ctx.getActorUri(entity.id),
                        object: new URL(actorURI),
                        to: new URL(actorURI),
                    }),
                }),
                { excludeBaseUris: [new URL(config.activitypub.baseUrl)] },
            );
            logger.info(`Sent Undo(Follow) to ${actorURI} for ${entity.id}`);
        } else {
            logger.warn(`Failed to resolve actor for Undo(Follow), removing locally: ${actorURI}`);
        }

        const stateKey = acceptStateKey(config.concrnt.ccid, entity.ccid, actorURI);
        if (followStore.getByKey(stateKey)) {
            await deleteServiceRecord(stateKey).catch((error) => {
                logger.warn(`Failed to delete accept-state record ${stateKey}: ${error}`);
            });
            followStore.removeAcceptStateByKey(stateKey);
        }
        followStore.removeFollowingByKey(msg.uri);
    }
}

// サービスアカウント自身が書いたfollower/accept-stateレコードのイベントをストアへ反映する。
// 書き込み箇所(federation.ts)では commit と同時にストアも更新しているため、
// ここでの適用は自己エコーの冪等な再適用(+移行スクリプト等の別経路書き込みの取り込み)。
const handleServiceRecordEvent = async (channel: string, msg: CoreEvent) => {

    const followerPrefix = `cckv://${config.concrnt.ccid}/${AP_NAMESPACE}/followers/`;
    const acceptStatePrefix = `cckv://${config.concrnt.ccid}/${AP_NAMESPACE}/accept-states/`;

    if (msg.type === "created") {

        const eventSD = msg.documents?.[channel];
        const document: any = eventSD
            ? JSON.parse(eventSD.document)
            : await concrntApi.getDocument<any>(channel).catch(() => null);
        if (document == null || document.author !== config.concrnt.ccid) return;

        if (channel.startsWith(followerPrefix) && document.schema === SCHEMA_AP_FOLLOWER) {
            const value = document.value as ApFollowerValue;
            if (!value?.ccid || !value?.actorURI || !value?.inbox) return;
            followStore.setFollower({ ccid: value.ccid, key: channel, actorURI: value.actorURI, inbox: value.inbox, sharedInbox: value.sharedInbox });
        } else if (channel.startsWith(acceptStatePrefix) && document.schema === SCHEMA_AP_ACCEPT_STATE) {
            const value = document.value as ApAcceptStateValue;
            if (!value?.ccid || !value?.actorURI || !value?.status) return;
            followStore.setAcceptState(channel, value.ccid, value.actorURI, value.status);
        }

    } else if (msg.type === "deleted") {
        if (msg.uri.startsWith(followerPrefix)) {
            followStore.removeFollowerByKey(msg.uri);
        } else if (msg.uri.startsWith(acceptStatePrefix)) {
            followStore.removeAcceptStateByKey(msg.uri);
        }
    }
}

export const startEntityBroker = async () => {

    const redis = new Redis(config.redis.url);

    await updateEntities(); // Initial load of entities
    await loadOutboundLikes(); // 送信済みLikeのフィルタを初期化
    setInterval(refreshEntities, 60000); // Update entities every 60 seconds

    // concrntはrealtimeイベントをcc-event:プレフィックス付きchannelにpublishする
    // (channel = "cc-event:" + リソースURI。payload内のsourceは生URIのまま)
    const CC_EVENT_PREFIX = "cc-event:";

    redis.psubscribe(`${CC_EVENT_PREFIX}*`, (err, count) => {
        if (err) {
            logger.error(`Failed to subscribe to Redis channels: ${err}`);
            return;
        }
    });

    redis.on("pmessage", async (pattern, rawChannel, message) => {

        if (!rawChannel.startsWith(CC_EVENT_PREFIX)) {
            return;
        }
        const channel = rawChannel.slice(CC_EVENT_PREFIX.length);

        if (!channel.startsWith('ccfs://') && !channel.startsWith('cckv://')) {
            return; // Ignore irrelevant channels
        }

        try {
            const msg: CoreEvent = JSON.parse(message);

            for (const entity of entities) {
                if (!entity.enabled) continue;

                // 自分の空間のレコードイベント → distributesと監視対象タイムラインを突合して転送
                if (channel.startsWith(`cckv://${entity.ccid}/`)) {
                    await handleOwnRecordEvent(entity, channel, msg);
                }

                // 監視対象タイムライン設定の更新 → 即時反映
                if (channel === settingsKey(entity.ccid)) {
                    await settingsStore.applyEvent(entity.ccid, msg);
                }

                // プロフィール更新(kv上書きでもcreatedが発火する)
                const profileKey = `cckv://${entity.ccid}/concrnt.world/profiles/main`;
                if (channel === profileKey && msg.type === "created") {
                    await handleProfileUpdate(entity);
                }

                // ユーザーが書いたfollowレコード → Follow/Undo(Follow)送信
                const followPrefix = `cckv://${entity.ccid}/${AP_NAMESPACE}/follows/`;
                if (channel.startsWith(followPrefix)) {
                    await handleFollowRecordEvent(entity, channel, msg);
                }
            }

            // サービスアカウント自身のfollower/accept-stateレコードをストアへ反映
            const serviceNsPrefix = `cckv://${config.concrnt.ccid}/${AP_NAMESPACE}/`;
            if (channel.startsWith(serviceNsPrefix)) {
                await handleServiceRecordEvent(channel, msg);
            }

            const assocPrefix = `cckv://${config.concrnt.ccid}/activitypub.concrnt.world/inbox/`
            if (msg.type === "associated" && channel.startsWith(assocPrefix)) {
                await handleAssociationEvent(msg);
            }

            // association削除イベントはccfs URI自身のチャンネルに流れる
            if (msg.type === "deleted" && msg.uri?.startsWith("ccfs://")) {
                await handleAssociationDeleted(msg);
            }
        } catch (error) {
            logger.error(`Error processing Redis message: ${error}`);
        }

    });

    // 購読開始後に初期ロードする(ロード中に届いたイベントは冪等な適用で収束する)
    await followStore.initialize(entities.map(e => e.ccid)).catch((error) => {
        logger.error(`followStore initialization failed (will retry on refresh): ${error}`);
    });
    for (const entity of entities) {
        await settingsStore.ensureEntitySettingsLoaded(entity.ccid).catch((error) => {
            logger.error(`Failed to load settings for ${entity.ccid} (will retry on refresh): ${error}`);
        });
    }
}
