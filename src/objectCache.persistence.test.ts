import { afterAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { PUBLIC_COLLECTION } from '@fedify/vocab';
import { eq } from 'drizzle-orm';
import type { CachedApObject } from './objectCache.ts';

const state = vi.hoisted(() => ({
    redis: new Map<string, string>(), rows: new Map<string, any>(), followers: [] as string[],
    close: async () => {}, reset: async () => {},
    redisFailure: undefined as 'get' | 'set' | 'del' | undefined,
    beforeRedisSet: undefined as (() => Promise<void>) | undefined,
    databaseFailure: false,
    databaseWriteFailure: false,
    lockConnections: new Set<number>(),
    delayPublication: false,
    delayedPublication: undefined as (() => number) | undefined,
}));
vi.mock('ioredis', () => ({ Redis: class {
    async get(key: string) { if (state.redisFailure === 'get') throw new Error('synthetic Redis get outage'); return state.redis.get(key) ?? null; }
    async set(key: string, value: string) { if (state.redisFailure === 'set') throw new Error('synthetic Redis set outage'); await state.beforeRedisSet?.(); state.redis.set(key, value); return 'OK'; }
    async del(key: string) { if (state.redisFailure === 'del') throw new Error('synthetic Redis del outage'); return state.redis.delete(key) ? 1 : 0; }
    async eval(script: string, _count: number, key: string, generationKey: string, generation: string, value: string | number) {
        if (script.includes("'DEL'")) {
            if (state.redisFailure === 'del') throw new Error('synthetic Redis del outage');
            state.redis.set(generationKey, generation);
            return state.redis.delete(key) ? 1 : 0;
        }
        if (state.redisFailure === 'set') throw new Error('synthetic Redis set outage');
        const execute = () => {
            if (state.redis.get(generationKey) !== generation) return 0;
            state.redis.set(key, String(value)); return 1;
        };
        if (state.delayPublication) {
            state.delayPublication = false; state.delayedPublication = execute;
            throw new Error('synthetic Redis publication timed out before execution');
        }
        await state.beforeRedisSet?.();
        return execute();
    }
} }));
vi.mock('./config.ts', () => ({ config: { redis: { url: 'redis://synthetic.invalid' }, activitypub: { objectCacheTTL: 10 } } }));
vi.mock('./followStore.ts', () => ({ getLocalFollowerCcids: () => state.followers }));
vi.mock('drizzle-orm/node-postgres', async importOriginal => {
    const actual = await importOriginal<typeof import('drizzle-orm/node-postgres')>();
    return { ...actual, drizzle: (...args: any[]) => args[0]?.syntheticDb ?? (actual.drizzle as any)(...args) };
});
vi.mock('./db/index.ts', async () => {
    const schema = await import('./db/schema.ts');
    const url = process.env.AP_TEST_DATABASE_URL;
    if (url) {
        // Opt-in real PostgreSQL oracle. A unique schema in a dedicated test
        // DB is shared by separate pool connections: TEMP tables would hide
        // competing writers and cannot prove session-lock correctness.
        if (new URL(url).pathname !== '/ap_synthetic') throw new Error('AP_TEST_DATABASE_URL must use the dedicated ap_synthetic database');
        const { Pool } = await import('pg');
        const { randomUUID } = await import('node:crypto');
        const { drizzle } = await import('drizzle-orm/node-postgres');
        const namespace = `ap_storage_${randomUUID().replaceAll('-', '')}`;
        const pool = new Pool({ connectionString: url, max: 4, connectionTimeoutMillis: 5_000, query_timeout: 10_000,
            options: `-c search_path=${namespace}` });
        pool.on('acquire', client => { state.lockConnections.add((client as any).processID); });
        await pool.query(`CREATE SCHEMA ${namespace}`);
        await pool.query(`CREATE TABLE ap_inbound_objects (
            object_id text PRIMARY KEY, actor_id text NOT NULL, object jsonb NOT NULL,
            recipient_ccids text[] NOT NULL DEFAULT '{}', visibility text NOT NULL,
            c_date timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
        )`);
        const db = drizzle(pool);
        state.close = async () => {
            try { await pool.query(`DROP SCHEMA ${namespace} CASCADE`); } finally { await pool.end(); }
        };
        state.reset = async () => { await db.delete(schema.apInboundObject); };
        return { ...schema, db };
    }
    // Fast unit run: storage adapter double. The optional PostgreSQL run above
    // exercises identical assertions with real SQL/JSONB/upsert behavior.
    const key = (condition: any): string => condition.queryChunks.find((chunk: any) => typeof chunk?.value === 'string').value;
    const locks = new Map<string, Promise<void>>();
    let connectionId = 0;
    const db: any = {
        execute: async () => {},
        transaction: async (operation: (tx: any) => Promise<unknown>) => {
            const snapshot = structuredClone(state.rows);
            try { return await operation(db); } catch (error) { state.rows = snapshot; throw error; }
        },
        select: () => ({ from: () => ({ where: (condition: any) => ({ limit: async () => {
            const row = state.rows.get(key(condition)); return row ? [structuredClone(row)] : [];
        } }) }) }),
        insert: () => ({ values: (value: any) => ({ onConflictDoUpdate: async (update: any) => {
            if (state.databaseWriteFailure) throw new Error('synthetic PostgreSQL write outage');
            const existing = state.rows.get(value.objectId);
            state.rows.set(value.objectId, structuredClone(existing
                ? { ...existing, ...update.set }
                : { ...value, cDate: new Date(), updatedAt: new Date() }));
        } }) }),
        delete: () => ({ where: async (condition: any) => { state.rows.delete(key(condition)); } }),
        $client: { connect: async () => {
            if (state.databaseFailure) throw new Error('synthetic PostgreSQL outage');
            const id = ++connectionId;
            let unlock: (() => void) | undefined;
            return {
                syntheticDb: db,
                query: async ({ text, values }: { text: string, values?: string[] }) => {
                    if (text.includes('pg_advisory_unlock')) { unlock?.(); unlock = undefined; }
                    else if (text.includes('pg_advisory_lock')) {
                        const previous = locks.get(values![0]) ?? Promise.resolve();
                        const current = new Promise<void>(resolve => { unlock = resolve; });
                        locks.set(values![0], current);
                        state.lockConnections.add(id);
                        await previous;
                    }
                    return { rows: [] };
                },
                release: () => { unlock?.(); },
            };
        } },
    };
    state.reset = async () => { state.rows.clear(); };
    return { ...schema, db };
});

const cache = await import('./objectCache.ts');
const { db, apInboundObject } = await import('./db/index.ts');
const uri = 'https://remote.example/notes/synthetic-private';
const actorUri = 'https://remote.example/users/synthetic-sender';
const bob = { ccid: 'con1bob', actorUri: 'https://bridge.example/ap/users/synthetic-bob' };
const other = { ccid: 'con1other', actorUri: 'https://bridge.example/ap/users/synthetic-other' };
const entry = (addressed: string[]): CachedApObject => ({
    json: { id: uri, type: 'Note', attributedTo: actorUri, to: addressed, content: 'synthetic fixture only' },
    actorUri, addressed, followersUri: `${actorUri}/followers`, recipientCcids: [bob.ccid], receivedAt: '2026-09-01T00:00:00.000Z',
});
const persisted = async () => (await db.select().from(apInboundObject).where(eq(apInboundObject.objectId, uri)).limit(1))[0];
beforeEach(async () => {
    state.redis.clear(); state.followers = []; state.redisFailure = undefined;
    state.beforeRedisSet = undefined; state.databaseFailure = false; state.databaseWriteFailure = false;
    state.delayPublication = false; state.delayedPublication = undefined;
    state.lockConnections.clear(); await state.reset();
});
afterAll(async () => { await state.close(); });

describe(`private object persistence (${process.env.AP_TEST_DATABASE_URL ? 'real PostgreSQL' : 'adapter double'})`, () => {
    it('persists direct content and enforces exact actor recipients after Redis expiry', async () => {
        state.followers = [bob.ccid, other.ccid];
        await cache.putObject(uri, entry([bob.actorUri]));
        expect(await persisted()).toMatchObject({ visibility: 'direct', recipientCcids: [bob.ccid] });
        state.redis.clear();
        const restored = await cache.getObject(uri);
        expect(restored?.json.content).toBe('synthetic fixture only');
        expect(cache.isVisibleTo(restored!, bob)).toBe(true);
        expect(cache.isVisibleTo(restored!, other)).toBe(false);
        expect(cache.isVisibleTo(restored!, null)).toBe(false);
        expect(state.redis.has(`apcache:object:${uri}`)).toBe(true);
    });

    it('followers authorization is re-evaluated, not granted by persisted recipient IDs', async () => {
        await cache.putObject(uri, entry([`${actorUri}/followers`]));
        state.redis.clear();
        const restored = await cache.getObject(uri);
        expect(await persisted()).toMatchObject({ visibility: 'followers' });
        expect(cache.isVisibleTo(restored!, bob)).toBe(false);
        state.followers = [bob.ccid];
        expect(cache.isVisibleTo(restored!, bob)).toBe(true);
        state.followers = [];
        expect(cache.isVisibleTo(restored!, bob)).toBe(false);
    });

    it('unions recipients on repeated private delivery without allowing unrelated readers', async () => {
        await cache.putObject(uri, entry([bob.actorUri]));
        await cache.putObject(uri, { ...entry([bob.actorUri]), recipientCcids: ['con1historical'] });
        state.redis.clear();
        const restored = await cache.getObject(uri);
        expect(restored?.recipientCcids).toEqual([bob.ccid, 'con1historical']);
        expect(cache.isVisibleTo(restored!, { ccid: 'con1historical' })).toBe(false);
    });

    it('rejects a different actor overwriting the private snapshot', async () => {
        await cache.putObject(uri, entry([bob.actorUri]));
        await expect(cache.putObject(uri, { ...entry([bob.actorUri]), actorUri: 'https://other.example/actor' })).rejects.toThrow('actor mismatch');
        state.redis.clear();
        expect((await cache.getObject(uri))?.actorUri).toBe(actorUri);
    });

    it('removes stale private snapshots when a legitimate update becomes public', async () => {
        await cache.putObject(uri, entry([bob.actorUri]));
        await cache.putObject(uri, entry([PUBLIC_COLLECTION.href]));
        expect(await persisted()).toBeUndefined();
        state.redis.clear();
        expect(await cache.getObject(uri)).toBeNull();
    });

    it('deletes both cache and durable copies, including aliases pointing at the canonical ID', async () => {
        await cache.putObject(uri, entry([bob.actorUri]));
        await cache.putAlias(`${uri}/alias`, uri);
        await cache.deleteObject(uri, actorUri);
        expect(await cache.getObject(uri)).toBeNull();
        expect(await cache.getObject(`${uri}/alias`)).toBeNull();
        expect(await persisted()).toBeUndefined();
    });

    it('retains activity-only recipients through persistent fallback', async () => {
        const received = entry([bob.actorUri]);
        delete received.json.to; // The enclosing Create, not Note, addressed Bob.
        await cache.putObject(uri, received);
        expect(cache.isVisibleTo((await cache.getObject(uri))!, bob)).toBe(true);
        state.redis.clear();
        const restored = (await cache.getObject(uri))!;
        expect(cache.isVisibleTo(restored, bob)).toBe(true);
        expect(cache.isVisibleTo(restored, other)).toBe(false);
        expect(restored.json).toEqual(received.json); // No metadata in external AP JSON.
        expect(restored.json).not.toHaveProperty('__concrnt_ap_snapshot_v1');
        expect(received.json).not.toHaveProperty('to'); // No rewrite of the original object.
    });

    it('retains nonstandard followers collection URIs after cache expiry', async () => {
        const followersUri = 'https://remote.example/collections/synthetic-followers';
        await cache.putObject(uri, { ...entry([followersUri]), followersUri });
        state.redis.clear();
        const restored = (await cache.getObject(uri))!;
        expect(restored.followersUri).toBe(followersUri);
        state.followers = [bob.ccid];
        expect(cache.isVisibleTo(restored, bob)).toBe(true);
        expect(cache.isVisibleTo(restored, other)).toBe(false);
    });

    it('does not treat another actor collection as the legacy sender followers list', async () => {
        const legacy = entry(['https://remote.example/users/synthetic-other/followers']);
        await db.insert(apInboundObject).values({
            objectId: uri, actorId: actorUri, object: legacy.json,
            recipientCcids: [bob.ccid], visibility: 'followers',
        }).onConflictDoUpdate({ target: apInboundObject.objectId, set: { object: legacy.json } });
        state.followers = [bob.ccid];
        const restored = (await cache.getObject(uri))!;
        expect(restored.followersUri).toBeUndefined();
        expect(cache.isVisibleTo(restored, bob)).toBe(false);
        expect(cache.isVisibleTo(restored, null)).toBe(false);
    });

    it('reads pre-envelope PostgreSQL snapshots without migration', async () => {
        const legacy = entry([bob.actorUri]);
        await db.insert(apInboundObject).values({
            objectId: uri, actorId: actorUri, object: legacy.json,
            recipientCcids: [bob.ccid], visibility: 'direct',
        }).onConflictDoUpdate({ target: apInboundObject.objectId, set: { object: legacy.json } });
        const restored = (await cache.getObject(uri))!;
        expect(restored.json).toEqual(legacy.json);
        expect(cache.isVisibleTo(restored, bob)).toBe(true);
        expect(cache.isVisibleTo(restored, other)).toBe(false);
    });

    it.each([true, false])('legacy follower collection ownership must match the author, own collection=%s', async ownCollection => {
        const collection = ownCollection ? `${actorUri}/followers` : 'https://remote.example/users/synthetic-unrelated/followers';
        const legacy = entry([collection]);
        await db.insert(apInboundObject).values({
            objectId: uri, actorId: actorUri, object: legacy.json,
            recipientCcids: [bob.ccid], visibility: 'followers',
        }).onConflictDoUpdate({ target: apInboundObject.objectId, set: { object: legacy.json } });
        state.followers = [bob.ccid];
        const restored = (await cache.getObject(uri))!;
        expect(cache.isVisibleTo(restored, bob)).toBe(ownCollection);
        expect(cache.isVisibleTo(restored, other)).toBe(false);
        expect(cache.isVisibleTo(restored, null)).toBe(false);
        expect(restored.json).toEqual(legacy.json);
    });

    it('rejects a different actor making a private object public without changing either store', async () => {
        await cache.putObject(uri, entry([bob.actorUri]));
        const before = await persisted();
        const redisBefore = state.redis.get(`apcache:object:${uri}`);
        await expect(cache.putObject(uri, { ...entry([PUBLIC_COLLECTION.href]), actorUri: 'https://other.example/actor' }))
            .rejects.toThrow('actor mismatch');
        expect(await persisted()).toEqual(before);
        expect(state.redis.get(`apcache:object:${uri}`)).toEqual(redisBefore);
    });

    it('rejects non-owner deletion without clearing the private snapshot or hot cache', async () => {
        await cache.putObject(uri, entry([bob.actorUri]));
        const before = await persisted();
        const redisBefore = state.redis.get(`apcache:object:${uri}`);
        await expect(cache.deleteObject(uri, 'https://remote.example/users/synthetic-other')).rejects.toThrow('actor mismatch');
        expect(await persisted()).toEqual(before);
        expect(state.redis.get(`apcache:object:${uri}`)).toEqual(redisBefore);
    });

    it('permits exactly one owner for simultaneous competing first private writes', async () => {
        const first = entry([bob.actorUri]);
        const second = { ...entry([other.actorUri]), actorUri: 'https://remote.example/users/synthetic-other',
            json: { ...entry([other.actorUri]).json, attributedTo: 'https://remote.example/users/synthetic-other' } };
        const outcomes = await Promise.allSettled([cache.putObject(uri, first), cache.putObject(uri, second)]);
        expect(outcomes.filter(result => result.status === 'fulfilled')).toHaveLength(1);
        expect(outcomes.filter(result => result.status === 'rejected')).toHaveLength(1);
        // In the optional real-PG run these are distinct backend PIDs, proving
        // the contenders do not share a connection-local temporary table.
        expect(state.lockConnections.size).toBeGreaterThanOrEqual(2);
        const row = (await persisted())!;
        const hot = (await cache.getObject(uri))!;
        expect(hot.actorUri).toBe(row.actorId);
        const winner = row.actorId === first.actorUri ? first : second;
        expect(hot.json).toEqual(winner.json);
        state.redis.clear();
        expect((await cache.getObject(uri))?.json).toEqual(winner.json);
    });

    it.each(['{invalid', JSON.stringify({ addressed: [PUBLIC_COLLECTION.href], json: null }),
        JSON.stringify({ addressed: [PUBLIC_COLLECTION.href], json: {}, actorUri: 42 }),
        JSON.stringify({ json: { id: uri, type: 'Note' }, actorUri })])
    ('does not authorize malformed Redis values and recovers only from a valid durable snapshot: %s', async raw => {
        await cache.putObject(uri, entry([bob.actorUri]));
        state.redis.set(`apcache:object:${uri}`, raw);
        const restored = (await cache.getObject(uri))!;
        expect(restored.json).toEqual(entry([bob.actorUri]).json);
        expect(cache.isVisibleTo(restored, bob)).toBe(true);
        expect(cache.isVisibleTo(restored, other)).toBe(false);
        expect(cache.isVisibleTo(restored, null)).toBe(false);
    });

    it('fails closed on malformed Redis without a durable snapshot', async () => {
        state.redis.set(`apcache:object:${uri}`, JSON.stringify({ addressed: [PUBLIC_COLLECTION.href], json: null }));
        expect(await cache.getObject(uri)).toBeNull();
    });

    it('rejects a restricted write when Redis invalidation fails without changing durable content', async () => {
        await cache.putObject(uri, entry([bob.actorUri]));
        const before = await persisted();
        state.redisFailure = 'del';
        await expect(cache.putObject(uri, entry([other.actorUri]))).rejects.toThrow('Redis');
        state.redisFailure = undefined;
        expect(await persisted()).toEqual(before);
        expect((await cache.getObject(uri))?.addressed).toEqual([bob.actorUri]);
    });

    it('keeps committed restricted content durable but reports a failed cache publication for retry', async () => {
        state.redisFailure = 'set';
        await expect(cache.putObject(uri, entry([bob.actorUri]))).rejects.toThrow('Redis');
        expect(await persisted()).toBeDefined();
        expect(state.redis.has(`apcache:object:${uri}`)).toBe(false);
        state.redisFailure = undefined;
        expect((await cache.getObject(uri))?.json).toEqual(entry([bob.actorUri]).json);
    });

    it('replaces recipients, including explicit no audience, without historical recipient escalation', async () => {
        await cache.putObject(uri, entry([bob.actorUri, other.actorUri]));
        await cache.putObject(uri, entry([other.actorUri]));
        state.redis.clear();
        const replaced = (await cache.getObject(uri))!;
        expect(cache.isVisibleTo(replaced, bob)).toBe(false);
        expect(cache.isVisibleTo(replaced, other)).toBe(true);
        await cache.putObject(uri, entry([]));
        state.redis.clear();
        const revoked = (await cache.getObject(uri))!;
        expect(cache.isVisibleTo(revoked, bob)).toBe(false);
        expect(cache.isVisibleTo(revoked, other)).toBe(false);
        expect(cache.isVisibleTo(revoked, null)).toBe(false);
    });

    it.each(['delete', 'revoke'] as const)('serializes an in-flight cold fill before %s without resurrection', async operation => {
        await cache.putObject(uri, entry([bob.actorUri]));
        state.redis.clear();
        let resume!: () => void;
        let reached!: () => void;
        const paused = new Promise<void>(resolve => { reached = resolve; });
        state.beforeRedisSet = () => new Promise<void>(resolve => { resume = resolve; reached(); });
        const fill = cache.getObject(uri);
        await paused;
        let settled = false;
        const change = (operation === 'delete'
            ? cache.deleteObject(uri, actorUri)
            : cache.putObject(uri, entry([other.actorUri]))).then(() => { settled = true; });
        for (let turn = 0; turn < 10; turn++) await Promise.resolve();
        expect(settled).toBe(false);
        state.beforeRedisSet = undefined;
        resume();
        await Promise.all([fill, change]);
        if (operation === 'delete') {
            expect(await cache.getObject(uri)).toBeNull();
            expect(await persisted()).toBeUndefined();
        } else {
            expect(cache.isVisibleTo((await cache.getObject(uri))!, bob)).toBe(false);
            state.redis.clear();
            expect(cache.isVisibleTo((await cache.getObject(uri))!, bob)).toBe(false);
        }
    });

    it.each(['delete', 'revoke'] as const)('fences a timed-out fill executing after a newer %s', async operation => {
        await cache.putObject(uri, entry([bob.actorUri]));
        state.redis.clear();
        state.delayPublication = true;
        await expect(cache.getObject(uri)).rejects.toThrow('publication timed out');
        if (operation === 'delete') await cache.deleteObject(uri, actorUri);
        else await cache.putObject(uri, entry([other.actorUri]));
        expect(state.delayedPublication!()).toBe(0);
        if (operation === 'delete') expect(await cache.getObject(uri)).toBeNull();
        else expect(cache.isVisibleTo((await cache.getObject(uri))!, bob)).toBe(false);
    });

    it('reports Redis read outages without returning a permissive cached object', async () => {
        await cache.putObject(uri, entry([bob.actorUri]));
        state.redisFailure = 'get';
        await expect(cache.getObject(uri)).rejects.toThrow('Redis get outage');
    });

    it('rejects invalid incoming metadata before touching either store', async () => {
        await cache.putObject(uri, entry([bob.actorUri]));
        const before = await persisted();
        const redisBefore = new Map(state.redis);
        await expect(cache.putObject(uri, { ...entry([PUBLIC_COLLECTION.href]), json: { id: 'https://remote.example/different' } }))
            .rejects.toThrow('Invalid inbound');
        expect(await persisted()).toEqual(before);
        expect(state.redis).toEqual(redisBefore);
    });

    it('rejects malformed internal envelopes rather than exposing transport metadata as AP JSON', async () => {
        const malformed = { __concrnt_ap_snapshot_v1: { object: entry([bob.actorUri]).json, addressed: [42] } };
        await db.insert(apInboundObject).values({
            objectId: uri, actorId: actorUri,
            object: malformed,
            recipientCcids: [bob.ccid], visibility: 'direct',
        }).onConflictDoUpdate({ target: apInboundObject.objectId, set: { object: malformed } });
        expect(await cache.getObject(uri)).toBeNull();
    });

    it.each([
        [`${actorUri}/followers`, true],
        ['https://remote.example/users/synthetic-unrelated/followers', false],
        ['https://collections.example/opaque-followers-123', false],
    ] as const)('normalizes unversioned warm follower metadata conservatively: %s', async (followersUri, allowed) => {
        const legacy = { ...entry([followersUri]), followersUri };
        state.redis.set(`apcache:object:${uri}`, JSON.stringify(legacy));
        state.followers = [bob.ccid];
        const restored = (await cache.getObject(uri))!;
        expect(cache.isVisibleTo(restored, bob)).toBe(allowed);
        expect(cache.isVisibleTo(restored, other)).toBe(false);
        expect(cache.isVisibleTo(restored, null)).toBe(false);
        expect(restored.json).toEqual(legacy.json);
    });

    it.each([false, true])('keeps unversioned activity-only direct/public addresses without trusting opaque followers, public=%s', async isPublic => {
        const followersUri = 'https://collections.example/opaque-followers-123';
        const legacy = { ...entry([followersUri, bob.actorUri, ...(isPublic ? [PUBLIC_COLLECTION.href] : [])]), followersUri };
        delete legacy.json.to;
        state.redis.set(`apcache:object:${uri}`, JSON.stringify(legacy));
        state.followers = [other.ccid];
        const restored = (await cache.getObject(uri))!;
        expect(cache.isVisibleTo(restored, bob)).toBe(true);
        expect(cache.isVisibleTo(restored, other)).toBe(isPublic);
        expect(cache.isVisibleTo(restored, null)).toBe(isPublic);
        expect(restored.addressed).toEqual(legacy.addressed);
        expect(restored.json).toEqual(legacy.json);
    });

    it.each(['https://collections.example/opaque-followers-123', 'https://collections.example/shared/followers'])
    ('publishes format 2 to retain explicitly captured opaque collection metadata: %s', async followersUri => {
        const incoming = { ...entry([followersUri]), followersUri };
        await cache.putObject(uri, incoming);
        expect(JSON.parse(state.redis.get(`apcache:object:${uri}`)!).cacheFormat).toBe(2);
        state.followers = [bob.ccid];
        expect(cache.isVisibleTo((await cache.getObject(uri))!, bob)).toBe(true);
        state.redis.clear();
        const restored = (await cache.getObject(uri))!;
        expect(JSON.parse(state.redis.get(`apcache:object:${uri}`)!).cacheFormat).toBe(2);
        expect(cache.isVisibleTo(restored, bob)).toBe(true);
        state.followers = [];
        expect(cache.isVisibleTo(restored, bob)).toBe(false);
        expect(restored.json).toEqual(incoming.json);
        expect(restored.json).not.toHaveProperty('cacheFormat');
    });

    it.each([1, 3, '2', null])('rejects an explicitly unsupported/malformed warm cache format: %s', async cacheFormat => {
        state.redis.set(`apcache:object:${uri}`, JSON.stringify({ ...entry([PUBLIC_COLLECTION.href]), cacheFormat }));
        expect(await cache.getObject(uri)).toBeNull();
    });

    it('does not recreate an object deleted while an Update was building its replacement', async () => {
        await cache.putObject(uri, entry([bob.actorUri]));
        const capturedBeforeDelete = (await cache.getObject(uri))!;
        await cache.deleteObject(uri, actorUri);
        const redisAfterDelete = new Map(state.redis);
        await cache.putObject(uri, { ...capturedBeforeDelete,
            json: { ...capturedBeforeDelete.json, content: 'synthetic delayed update' } }, { requireExisting: true });
        expect(await persisted()).toBeUndefined();
        expect(state.redis).toEqual(redisAfterDelete);
        expect(await cache.getObject(uri)).toBeNull();
        // Ordinary Create remains permitted after the no-op Update.
        await cache.putObject(uri, entry([other.actorUri]));
        expect((await cache.getObject(uri))?.addressed).toEqual([other.actorUri]);
    });

    it('fences delayed public publication even when Delete finds no current row or cache', async () => {
        await cache.putObject(uri, entry([bob.actorUri]));
        state.delayPublication = true;
        await expect(cache.putObject(uri, entry([PUBLIC_COLLECTION.href]), { requireExisting: true })).rejects.toThrow();
        expect(await persisted()).toBeUndefined();
        expect(await cache.deleteObject(uri, actorUri)).toBe(false);
        expect(state.delayedPublication!()).toBe(0);
        expect(await cache.getObject(uri)).toBeNull();
    });

    it('allows a requireExisting owner update from durable storage after Redis expiry', async () => {
        await cache.putObject(uri, entry([bob.actorUri]));
        state.redis.clear();
        await cache.putObject(uri, entry([other.actorUri]), { requireExisting: true });
        state.redis.clear();
        const updated = (await cache.getObject(uri))!;
        expect(cache.isVisibleTo(updated, bob)).toBe(false);
        expect(cache.isVisibleTo(updated, other)).toBe(true);
    });
});

// These adapter faults are not claimed as a real-server outage test. The
// release harness injects actual network/service failure separately.
if (!process.env.AP_TEST_DATABASE_URL) describe('storage adapter fault injection', () => {
    it('leaves old durable content intact and no new cache when PostgreSQL rejects an update', async () => {
        await cache.putObject(uri, entry([bob.actorUri]));
        const before = await persisted();
        state.databaseWriteFailure = true;
        await expect(cache.putObject(uri, entry([other.actorUri]))).rejects.toThrow('PostgreSQL write outage');
        state.databaseWriteFailure = false;
        expect(await persisted()).toEqual(before);
        expect(state.redis.has(`apcache:object:${uri}`)).toBe(false);
        expect((await cache.getObject(uri))?.json).toEqual(entry([bob.actorUri]).json);
    });

    it('serves a valid hot entry without PostgreSQL but fails a cold read closed during DB outage', async () => {
        await cache.putObject(uri, entry([bob.actorUri]));
        state.databaseFailure = true;
        expect(cache.isVisibleTo((await cache.getObject(uri))!, bob)).toBe(true);
        state.redis.clear();
        await expect(cache.getObject(uri)).rejects.toThrow('PostgreSQL outage');
    });
});
