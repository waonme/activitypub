import { Redis } from "ioredis";
import { PUBLIC_COLLECTION, type Object as ApObject } from "@fedify/vocab";
import { eq, sql } from "drizzle-orm";
import { drizzle, type NodePgDatabase } from "drizzle-orm/node-postgres";
import type { PoolClient, QueryConfig } from "pg";
import { randomUUID } from "node:crypto";
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
const GENERATION_PREFIX = "apcache:generation:";
const CACHE_FORMAT = 2;
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

const redis = new Redis(config.redis.url, { commandTimeout: 10_000, maxRetriesPerRequest: 1 });

// Fencing is also required when a Redis command times out but executes late:
// a previous cache fill must not overwrite a later revocation after its DB
// session lock has been released. Tokens contain no identity/content and expire.
const invalidateObject = async (uri: string): Promise<string> => {
    const generation = randomUUID();
    await redis.eval(`
        redis.call('SET', KEYS[2], ARGV[1], 'EX', ARGV[2])
        return redis.call('DEL', KEYS[1])
    `, 2, OBJECT_PREFIX + uri, GENERATION_PREFIX + uri, generation, Math.max(60, config.activitypub.objectCacheTTL));
    return generation;
};

const publishObject = async (uri: string, entry: CachedApObject, generation: string): Promise<void> => {
    const published = await redis.eval(`
        if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
        redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
        return 1
    `, 2, OBJECT_PREFIX + uri, GENERATION_PREFIX + uri, generation,
    JSON.stringify({ ...entry, cacheFormat: CACHE_FORMAT }), config.activitypub.objectCacheTTL);
    if (published !== 1) throw new Error('Inbound object cache publication superseded');
};

type CacheDatabase = NodePgDatabase;
type CacheTransaction = Parameters<Parameters<CacheDatabase['transaction']>[0]>[0];

// A transaction advisory lock would be released at COMMIT, before Redis is
// published. Pin a session instead: all cold fills and mutations for an object
// are serialized through the DB commit AND cache publication. Hot reads remain
// lock-free and are linearized at their Redis GET. No table/schema migration.
const withObjectLock = async <T>(uri: string, operation: (store: CacheDatabase) => Promise<T>): Promise<T> => {
    const client = await new Promise<PoolClient>((resolve, reject) => {
        let settled = false;
        const timeout = setTimeout(() => { settled = true; reject(new Error('Inbound object database connection timeout')); }, 10_000);
        void db.$client.connect().then(connection => {
            if (settled) { connection.release(true); return; }
            settled = true; clearTimeout(timeout); resolve(connection);
        }, error => { settled = true; clearTimeout(timeout); reject(error); });
    });
    const originalQuery = client.query;
    // The connection is exclusively checked out. Apply the driver deadline to
    // Drizzle's queries too, then restore the method before returning the client.
    client.query = ((request: string | QueryConfig, ...args: unknown[]) => {
        const timed = { ...(typeof request === 'string' ? { text: request } : request), query_timeout: 11_000 };
        return (originalQuery as (...values: unknown[]) => unknown).call(client, timed, ...args);
    }) as typeof client.query;
    const query = (text: string, values?: string[]) => {
        // node-postgres supports a per-query deadline; @types/pg currently
        // exposes it only on the connection config, not QueryConfig.
        const config: QueryConfig & { query_timeout: number } = { text, values, query_timeout: 11_000 };
        return client.query(config);
    };
    const lockKey = OBJECT_PREFIX + uri;
    let locked = false;
    let failed = false;
    try {
        await query('BEGIN');
        await query("SET LOCAL lock_timeout = '5s'");
        await query("SET LOCAL statement_timeout = '10s'");
        await query('SELECT pg_advisory_lock(hashtextextended($1, 0))', [lockKey]);
        locked = true;
        await query('COMMIT');
        return await operation(drizzle(client));
    } catch (error) {
        failed = true;
        throw error;
    } finally {
        let releaseError: unknown;
        try {
            // Also clears a transaction aborted during lock acquisition. Each
            // callback transaction rolls itself back before reaching here.
            if (failed) await query('ROLLBACK');
            if (locked) await query('SELECT pg_advisory_unlock(hashtextextended($1, 0))', [lockKey]);
        } catch (error) {
            releaseError = error;
        }
        // Never return a session holding an advisory lock to the pool, and
        // never mask the original operation failure with cleanup failure.
        client.query = originalQuery;
        client.release(failed || releaseError !== undefined);
        if (releaseError !== undefined && !failed) throw releaseError;
    }
};

const withStoreTransaction = <T>(store: CacheDatabase, operation: (tx: CacheTransaction) => Promise<T>): Promise<T> =>
    store.transaction(async tx => {
        await tx.execute(sql`SET LOCAL statement_timeout = '10s'`);
        await tx.execute(sql`SET LOCAL lock_timeout = '5s'`);
        return operation(tx);
    });

const isRecord = (value: unknown): value is Record<string, unknown> =>
    value != null && typeof value === 'object' && !Array.isArray(value);
const isStrings = (value: unknown): value is string[] =>
    Array.isArray(value) && value.every(item => typeof item === 'string');

const isCacheEntry = (value: unknown, uri: string): value is CachedApObject => {
    if (!isRecord(value) || !isRecord(value.json)) return false;
    const objectId = value.json.id ?? value.json['@id'];
    return objectId === uri
        && typeof value.actorUri === 'string' && value.actorUri.length > 0
        && isStrings(value.addressed)
        && (value.followersUri === undefined || typeof value.followersUri === 'string')
        && (value.recipientCcids === undefined || isStrings(value.recipientCcids))
        && typeof value.receivedAt === 'string' && Number.isFinite(Date.parse(value.receivedAt));
};

const readCacheEntry = (raw: string | null, uri: string): CachedApObject | null => {
    if (raw === null) return null;
    try {
        const value: unknown = JSON.parse(raw);
        if (!isCacheEntry(value, uri)) return null;
        const format = (value as CachedApObject & { cacheFormat?: unknown }).cacheFormat;
        if (format === CACHE_FORMAT) return value;
        if (Object.hasOwn(value, 'cacheFormat')) return null;
        // Old warm entries cannot distinguish actor-provided metadata from
        // the former legacy-row /followers suffix guess. Keep their original
        // JSON and direct/activity-only destinations, but trust only the exact
        // conventional author-owned followers URI. Opaque legacy provenance
        // cannot be reconstructed; a new verified delivery restores it.
        if (value.followersUri !== undefined && value.followersUri !== `${value.actorUri}/followers`) {
            return { ...value, followersUri: undefined };
        }
        return value;
    } catch {
        return null;
    }
};

const assertOwner = (uri: string, actorUri: string, owners: Array<string | undefined>): void => {
    if (owners.some(owner => owner !== undefined && owner !== actorUri)) {
        logger.warn('Inbound object actor mismatch; refusing cache mutation');
        throw new Error(`Inbound object actor mismatch for ${uri}`);
    }
};

// Keep transport/audience metadata outside the original (possibly signed)
// JSON-LD object. Older rows contain the object directly; only this versioned,
// single-key envelope is decoded as internal metadata, never returned to AP.
const SNAPSHOT_ENVELOPE = "__concrnt_ap_snapshot_v1";
interface SnapshotEnvelope {
    object: Record<string, unknown>;
    addressed: string[];
    followersUri?: string;
}

const readSnapshotEnvelope = (json: Record<string, unknown>): SnapshotEnvelope | null => {
    if (Object.keys(json).length !== 1) return null;
    const value = json[SNAPSHOT_ENVELOPE];
    if (value == null || typeof value !== "object" || Array.isArray(value)) return null;
    const envelope = value as Partial<SnapshotEnvelope>;
    if (envelope.object == null || typeof envelope.object !== "object" || Array.isArray(envelope.object)) return null;
    if (!Array.isArray(envelope.addressed) || !envelope.addressed.every(uri => typeof uri === "string")) return null;
    if (envelope.followersUri !== undefined && typeof envelope.followersUri !== "string") return null;
    return envelope as SnapshotEnvelope;
};

const persistentRowToEntry = (row: ApInboundObjectRow): CachedApObject | null => {
    if (!isRecord(row.object)) return null;
    const envelope = readSnapshotEnvelope(row.object);
    // A malformed internal envelope is not an external AP object.
    if (Object.hasOwn(row.object, SNAPSHOT_ENVELOPE) && envelope === null) return null;
    const json = envelope?.object ?? row.object;
    const addressed = envelope?.addressed ?? collectCachedAddresses(json);
    // A legacy row has no authenticated collection metadata. An arbitrary
    // recipient ending in /followers is not necessarily this author's list.
    // Only the standard author-owned URI may be inferred; opaque URIs require
    // metadata captured from the actor in a versioned snapshot.
    const followersUri = envelope?.followersUri
        ?? addressed.find((uri) => uri === `${row.actorId}/followers`);
    const entry = {
        json,
        actorUri: row.actorId,
        addressed,
        ...(followersUri ? { followersUri } : {}),
        recipientCcids: row.recipientCcids,
        receivedAt: row.updatedAt.toISOString(),
    };
    return isCacheEntry(entry, row.objectId) ? entry : null;
};

const persistRestrictedObject = async (store: CacheTransaction, uri: string, entry: CachedApObject, existing?: ApInboundObjectRow): Promise<void> => {
    if (entry.addressed.includes(PUBLIC_COLLECTION.href)) {
        // An Update may widen a formerly restricted object. Do not leave a
        // stale private snapshot that could reappear after the Redis TTL.
        await store.delete(apInboundObject).where(eq(apInboundObject.objectId, uri));
        return;
    }

    const recipientCcids = [...new Set([
        ...(existing?.recipientCcids ?? []),
        ...(entry.recipientCcids ?? []),
    ])];
    const followersUri = entry.followersUri ?? entry.actorUri + "/followers";
    const visibility = entry.addressed.includes(followersUri) ? "followers" : "direct";
    // Create.to/cc can be the sole audience; deriving it from Note.to/cc on a
    // cold read would deny the legitimate recipient after Redis expiry.
    const snapshot: Record<string, unknown> = {
        [SNAPSHOT_ENVELOPE]: {
            object: entry.json,
            addressed: entry.addressed,
            ...(entry.followersUri !== undefined ? { followersUri: entry.followersUri } : {}),
        } satisfies SnapshotEnvelope,
    };

    await store.insert(apInboundObject).values({
        objectId: uri,
        actorId: entry.actorUri,
        object: snapshot,
        recipientCcids,
        visibility,
    }).onConflictDoUpdate({
        target: apInboundObject.objectId,
        set: {
            object: snapshot,
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

export const putObject = async (uri: string, entry: CachedApObject, options: { requireExisting?: boolean } = {}): Promise<void> => {
    // Snapshot caller-owned state before the first await; callers cannot alter
    // the audience while a competing writer holds the lock.
    const incoming: CachedApObject = structuredClone(entry);
    const requireExisting = options.requireExisting === true;
    if (!isCacheEntry(incoming, uri)) throw new Error('Invalid inbound object cache entry');
    await withObjectLock(uri, async store => {
        let generation: string | undefined;
        await withStoreTransaction(store, async tx => {
            const existing = await tx.select().from(apInboundObject).where(eq(apInboundObject.objectId, uri)).limit(1).then(rows => rows[0]);
            const cached = readCacheEntry(await redis.get(OBJECT_PREFIX + uri), uri);
            // Update may have loaded a snapshot before a concurrent Delete.
            // Recheck existence under the mutation lock, before invalidation,
            // so a delayed replacement cannot recreate the deleted object.
            if (requireExisting && !existing && !cached) return;
            // Check BEFORE the public-delete path and before invalidation.
            assertOwner(uri, incoming.actorUri, [existing?.actorId, cached?.actorUri]);
            // A failed invalidation leaves the durable object unchanged. Once
            // invalidated, a failed SQL write cannot expose a newer audience.
            generation = await invalidateObject(uri);
            await persistRestrictedObject(tx, uri, incoming, existing);
        });
        // Commit first; failed publication remains an error for delivery retry.
        // Restricted content is already durable and the old cache is absent.
        if (generation !== undefined) await publishObject(uri, incoming, generation);
    });
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
    const cached = readCacheEntry(raw, objectUri);
    if (cached) return cached;

    // Legacy deployments kept these snapshots only in Postgres. Reading on
    // miss both preserves those deliveries and lazily warms the current cache.
    return withObjectLock(objectUri, async store => {
        const current = readCacheEntry(await redis.get(OBJECT_PREFIX + objectUri), objectUri);
        if (current) return current;
        const generation = await invalidateObject(objectUri);
        const row = await withStoreTransaction(store, tx => tx.select().from(apInboundObject)
            .where(eq(apInboundObject.objectId, objectUri)).limit(1).then(rows => rows[0]));
        const entry = row ? persistentRowToEntry(row) : null;
        if (!entry) {
            return null;
        }
        await publishObject(objectUri, entry, generation);
        return entry;
    });
};

export const deleteObject = async (uri: string, actorUri: string): Promise<boolean> =>
    withObjectLock(uri, store => withStoreTransaction(store, async tx => {
        const existing = await tx.select().from(apInboundObject).where(eq(apInboundObject.objectId, uri)).limit(1).then(rows => rows[0]);
        const cached = readCacheEntry(await redis.get(OBJECT_PREFIX + uri), uri);
        assertOwner(uri, actorUri, [existing?.actorId, cached?.actorUri]);
        if (!existing && !cached) {
            // A prior public transition may have committed its DB deletion
            // while its Redis publication is still delayed after a timeout.
            // Even an absent object must fence that in-flight publication.
            await invalidateObject(uri);
            return false;
        }
        await invalidateObject(uri);
        await tx.delete(apInboundObject).where(eq(apInboundObject.objectId, uri));
        return true;
    }));
