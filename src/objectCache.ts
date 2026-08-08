import { Redis } from "ioredis";
import { PUBLIC_COLLECTION, type Object as ApObject } from "@fedify/vocab";
import { config } from "./config.ts";
import * as followStore from "./followStore.ts";

// inbox受信・resolve済みAPオブジェクトの本文キャッシュ。DBには保存しない純粋な
// TTL付きキャッシュで、リモートへのfetch回数削減と即応答が目的。副次的に、
// リモートが再fetchを許さないオブジェクト(Misskeyのフォロワー限定ノート等)も
// TTLの間は表示できる。TTL切れ後に遡って取得不能になるのは仕様として許容する。

const OBJECT_PREFIX = "apcache:object:";
const ALIAS_PREFIX = "apcache:alias:";

export interface CachedApObject {
    json: Record<string, unknown>;
    actorUri: string;
    // 生の宛先(object側+activity側のto/ccのunion)。閲覧可否は読み出し時に評価する
    addressed: string[];
    // 投稿者のfollowersコレクションURI(受信時にactorから取得できた場合)
    followersUri?: string;
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
    await redis.set(OBJECT_PREFIX + uri, JSON.stringify(entry), "EX", config.activitypub.objectCacheTTL);
};

// 正準id以外のURLでresolveされた場合の別名。実体は持たず正準idへのポインタに
// する(Deleteで正準idをpurgeするだけで実体が確実に消えるように)
export const putAlias = async (uri: string, canonicalUri: string): Promise<void> => {
    await redis.set(ALIAS_PREFIX + uri, canonicalUri, "EX", config.activitypub.objectCacheTTL);
};

export const getObject = async (uri: string): Promise<CachedApObject | null> => {
    let raw = await redis.get(OBJECT_PREFIX + uri);
    if (raw == null) {
        const canonical = await redis.get(ALIAS_PREFIX + uri);
        if (canonical == null) return null;
        raw = await redis.get(OBJECT_PREFIX + canonical);
    }
    if (raw == null) return null;
    const entry = JSON.parse(raw) as CachedApObject;
    if (!Array.isArray(entry.addressed)) return null; // 旧形状エントリはミス扱い
    return entry;
};

export const deleteObject = async (uri: string): Promise<void> => {
    await redis.del(OBJECT_PREFIX + uri);
};
