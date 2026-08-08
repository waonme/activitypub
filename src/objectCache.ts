import { Redis } from "ioredis";
import { PUBLIC_COLLECTION, type Object as ApObject } from "@fedify/vocab";
import { eq } from "drizzle-orm";
import { getLogger } from "@logtape/logtape";
import { config } from "./config.ts";
import { apInboundObject, db, type ApInboundObjectRow } from "./db/index.ts";
import { collectCachedAddresses } from "./inboundDelivery.ts";
import * as followStore from "./followStore.ts";

// inbox受信・resolve済みAPオブジェクトの本文キャッシュ。Redisを高速経路とし、
// 後から再fetchできないfollowers/directオブジェクトだけPostgresにも永続化する。
// これによりキャッシュ期限切れ・pod再作成・旧snapshot実装からの更新をまたいでも
// 正規の受信者は本文を表示できる。public/unlistedはリモート再取得可能なのでTTLのみ。

const OBJECT_PREFIX = "apcache:object:";
const ALIAS_PREFIX = "apcache:alias:";
const logger = getLogger("activitypub");

export interface CachedApObject {
    json: Record<string, unknown>;
    actorUri: string;
    // 生の宛先(object側+activity側のto/ccのunion)。閲覧可否は読み出し時に評価する
    addressed: string[];
    // 投稿者のfollowersコレクションURI(受信時にactorから取得できた場合)
    followersUri?: string;
    // 永続fallbackで配送時の受信者を保持する。Redis上では認可の補助情報であり、
    // 実際のfollowers判定はfollowStoreの現在状態も必須とする。
    recipientCcids?: string[];
    receivedAt: string;
}

// 閲覧可否の読み出し時評価(fedify docsのpost.isVisibleTo()相当)。
// 全てローカル情報で判定するためネットワーク往復はない。フォロー解除で失効し、
// 受信後の新規フォロワーにも見える(本家Misskeyの挙動と一致)。
export const isVisibleTo = (entry: CachedApObject, requester: { ccid: string; actorUri?: string } | null): boolean => {
    if (entry.addressed.includes(PUBLIC_COLLECTION.href)) return true;
    if (requester == null) return false;
    // DM/メンション: 本人のactor URIが宛先に含まれる
    if (requester.actorUri != null && entry.addressed.includes(requester.actorUri)) return true;
    // フォロワー限定: followersコレクションが宛先に含まれ、かつ現在フォロー中。
    // followersUri未取得時は<actorUri>/followersの慣行(Misskey/Mastodon/GTS)で代用。
    // フォロー中でも宛先条件を必須にすることでDMが開放されないようにする
    const followersUri = entry.followersUri ?? entry.actorUri + "/followers";
    return entry.addressed.includes(followersUri)
        && followStore.getLocalFollowerCcids(entry.actorUri).includes(requester.ccid);
};

const redis = new Redis(config.redis.url);

const persistentRowToEntry = (row: ApInboundObjectRow): CachedApObject => {
    const addressed = collectCachedAddresses(row.object);
    const followersUri = addressed.find((uri) => uri.endsWith("/followers"));
    return {
        json: row.object,
        actorUri: row.actorId,
        addressed,
        ...(followersUri ? { followersUri } : {}),
        recipientCcids: row.recipientCcids,
        receivedAt: row.updatedAt.toISOString(),
    };
};

const persistRestrictedObject = async (uri: string, entry: CachedApObject): Promise<void> => {
    if (entry.addressed.includes(PUBLIC_COLLECTION.href)) {
        // An Update may widen a formerly restricted object. Do not leave a
        // stale private snapshot that could reappear after the Redis TTL.
        await db.delete(apInboundObject).where(eq(apInboundObject.objectId, uri));
        return;
    }

    const existing = await db.select({
        actorId: apInboundObject.actorId,
        recipientCcids: apInboundObject.recipientCcids,
    }).from(apInboundObject).where(eq(apInboundObject.objectId, uri)).limit(1).then(rows => rows[0]);
    if (existing && existing.actorId !== entry.actorUri) {
        logger.warn(`Inbound object actor mismatch for ${uri}: ${entry.actorUri} vs ${existing.actorId}`);
        throw new Error(`Inbound object actor mismatch for ${uri}`);
    }

    const recipientCcids = [...new Set([
        ...(existing?.recipientCcids ?? []),
        ...(entry.recipientCcids ?? []),
    ])];
    const followersUri = entry.followersUri ?? entry.actorUri + "/followers";
    const visibility = entry.addressed.includes(followersUri) ? "followers" : "direct";

    await db.insert(apInboundObject).values({
        objectId: uri,
        actorId: entry.actorUri,
        object: entry.json,
        recipientCcids,
        visibility,
    }).onConflictDoUpdate({
        target: apInboundObject.objectId,
        set: {
            object: entry.json,
            recipientCcids,
            visibility,
            updatedAt: new Date(),
        },
    });
};

// fedifyのtoJsonLd()はvocab未知のプロパティを落とすため、受信時の生JSON-LDから
// _misskey_*(MFMソース等)を拾い直してマージする。埋め込みオブジェクトは自身の
// _cachedJsonLdを持たないため、アクティビティ側の生JSON-LDのobjectから拾う
export const buildCacheJson = async (object: ApObject, activity?: ApObject): Promise<Record<string, unknown>> => {
    const jsonLd = await object.toJsonLd() as Record<string, unknown>;
    let raw = (object as unknown as { _cachedJsonLd?: unknown })._cachedJsonLd;
    if (raw == null && activity != null) {
        const activityRaw = (activity as unknown as { _cachedJsonLd?: unknown })._cachedJsonLd;
        if (activityRaw != null && typeof activityRaw === "object") {
            const embedded = (activityRaw as Record<string, unknown>).object;
            if (embedded != null && typeof embedded === "object" && !Array.isArray(embedded)) {
                raw = embedded;
            }
        }
    }
    if (raw != null && typeof raw === "object") {
        for (const [key, value] of Object.entries(raw)) {
            if (key.startsWith("_misskey_")) jsonLd[key] = value;
        }
    }
    return jsonLd;
};

export const putObject = async (uri: string, entry: CachedApObject): Promise<void> => {
    await persistRestrictedObject(uri, entry);
    await redis.set(OBJECT_PREFIX + uri, JSON.stringify(entry), "EX", config.activitypub.objectCacheTTL);
};

// 正準id以外のURLでresolveされた場合の別名。実体は持たず正準idへのポインタに
// する(Deleteで正準idをpurgeするだけで実体が確実に消えるように)
export const putAlias = async (uri: string, canonicalUri: string): Promise<void> => {
    await redis.set(ALIAS_PREFIX + uri, canonicalUri, "EX", config.activitypub.objectCacheTTL);
};

export const getObject = async (uri: string): Promise<CachedApObject | null> => {
    let objectUri = uri;
    let raw = await redis.get(OBJECT_PREFIX + uri);
    if (raw == null) {
        const canonical = await redis.get(ALIAS_PREFIX + uri);
        if (canonical != null) {
            objectUri = canonical;
            raw = await redis.get(OBJECT_PREFIX + objectUri);
        }
    }
    if (raw != null) {
        const entry = JSON.parse(raw) as CachedApObject;
        if (Array.isArray(entry.addressed)) return entry;
    }

    // Legacy deployments kept these snapshots only in Postgres. Reading on
    // miss both preserves those deliveries and lazily warms the current cache.
    const row = await db.select().from(apInboundObject)
        .where(eq(apInboundObject.objectId, objectUri)).limit(1).then(rows => rows[0]);
    if (!row) return null;

    const entry = persistentRowToEntry(row);
    await redis.set(OBJECT_PREFIX + objectUri, JSON.stringify(entry), "EX", config.activitypub.objectCacheTTL);
    return entry;
};

export const deleteObject = async (uri: string): Promise<void> => {
    await Promise.all([
        redis.del(OBJECT_PREFIX + uri),
        db.delete(apInboundObject).where(eq(apInboundObject.objectId, uri)),
    ]);
};
