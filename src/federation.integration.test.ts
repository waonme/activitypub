import { beforeEach, describe, expect, it, vi } from 'vitest';
import { NotFoundError, PermissionError, ServerOfflineError } from '@concrnt/client';
import { Create, Delete, Note, Person, PUBLIC_COLLECTION, Tombstone, Update } from '@fedify/vocab';
import { Temporal } from '@js-temporal/polyfill';
import { SCHEMA_AP_NOTE, SCHEMA_REPLY, SCHEMA_REROUTE } from './schemas.ts';
import { isVisibleTo, type CachedApObject } from './objectCache.ts';

// Test the registered production callbacks and real converters. Only transports,
// storage, and Fedify's registration shell are replaced; no federation network IO.
const state = vi.hoisted(() => ({
    registrations: new Map<string, any[]>(), listeners: new Map<unknown, (...args: any[]) => any>(),
    config: { activitypub: { actorPathSegment: 'users', baseUrl: 'https://bridge.example', allowPrivateAddress: false },
        redis: { url: 'redis://synthetic.invalid' }, concrnt: { ccid: 'con1bridge', domain: 'bridge.example' } },
    getDocument: vi.fn(), query: vi.fn(), proxy: vi.fn(), commit: vi.fn(), putObject: vi.fn(), getObject: vi.fn(), deleteObject: vi.fn(),
    deleteReference: vi.fn(), replyReferences: [] as { apObjectId: string; ccUri: string; refType: string }[],
    entities: [{ id: 'synthetic-alice', ccid: 'con1alice', enabled: true }, { id: 'synthetic-bob', ccid: 'con1bob', enabled: true }],
    localFollowers: [] as string[], remoteFollowers: [] as { actorURI: string }[], timelines: [] as string[],
}));

vi.mock('@fedify/fedify', async importOriginal => ({
    ...await importOriginal<typeof import('@fedify/fedify')>(),
    createFederation: () => {
        const shell: any = new Proxy({}, { get: (_target, name: string) => (...args: any[]) => {
            if (name === 'on') state.listeners.set(args[0], args[1]);
            else state.registrations.set(name, args);
            return shell;
        } });
        return shell;
    },
}));
vi.mock('@fedify/redis', () => ({ RedisKvStore: class {}, RedisMessageQueue: class {} }));
vi.mock('ioredis', () => ({ Redis: class {} }));
vi.mock('./config.ts', () => ({ config: state.config }));
vi.mock('./metrics.ts', () => ({ meterProvider: undefined }));
vi.mock('./concrnt.ts', () => ({
    default: { getDocument: state.getDocument, requestConcrntApi: state.query },
    resolveAsProxy: state.proxy, commit: state.commit, importCommit: vi.fn(),
}));
vi.mock('./db/index.ts', async () => {
    const schema = await import('./db/schema.ts');
    const parameterValues = (condition: any): string[] => condition?.queryChunks?.flatMap((chunk: any) =>
        typeof chunk?.value === 'string' ? [chunk.value] : parameterValues(chunk)) ?? [];
    return {
        ...schema,
        db: {
            select: () => ({ from: (table: unknown) => ({ where: (condition: any) => {
                const values = parameterValues(condition);
                const rows = table === schema.apObjectReference
                    ? state.replyReferences.filter(ref => values.includes(ref.apObjectId) && values.includes(ref.refType))
                    : state.entities.filter(entity => values.includes(entity.id) || values.includes(entity.ccid));
                return Object.assign(Promise.resolve(rows), { limit: async (n: number) => rows.slice(0, n) });
            } }) }),
            delete: () => ({ where: state.deleteReference }),
        },
    };
});
vi.mock('./followStore.ts', () => ({
    getLocalFollowerCcids: () => state.localFollowers,
    getFollowers: () => state.remoteFollowers,
}));
vi.mock('./settingsStore.ts', () => ({
    MAX_LISTEN_TIMELINES: 32, ensureEntitySettingsLoaded: async () => {}, getListenTimelines: () => state.timelines,
}));
vi.mock('./objectCache.ts', async importOriginal => ({
    ...await importOriginal<typeof import('./objectCache.ts')>(),
    putObject: state.putObject,
    getObject: state.getObject,
    deleteObject: state.deleteObject,
}));

const timeline = 'cckv://con1alice/concrnt.world/profiles/main/home-timeline';
const instant = '2026-09-01T00:00:00.123456789Z';
const postKey = (i: number) => `cckv://con1alice/concrnt.world/profiles/main/posts/test${String(i).padStart(4, '0')}`;
const callback = (name: string) => state.registrations.get(name)!.at(-1) as (...args: any[]) => Promise<any>;
const context = () => {
    const actorPath = state.registrations.get('setActorDispatcher')![0] as string;
    const actorUri = (identifier: string) => new URL(actorPath.replace('{identifier}', identifier), 'https://bridge.example');
    return {
        getActorUri: actorUri,
        getActorKeyPairs: async () => [],
        getInboxUri: (identifier?: string) => identifier ? new URL(`${actorUri(identifier)}/inbox`) : new URL('https://bridge.example/ap/inbox'),
        getOutboxUri: (identifier: string) => new URL(`${actorUri(identifier)}/outbox`),
        getFollowersUri: (identifier: string) => new URL(`${actorUri(identifier)}/followers`),
        getObjectUri: (_cls: unknown, values: { identifier: string, id: string }) => new URL(`${actorUri(values.identifier)}/posts/${encodeURIComponent(values.id)}`),
        getDocumentLoader: async () => async () => { throw new Error('no remote IO in tests'); },
        lookupObject: async () => null,
        getSignedKeyOwner: async () => null as Person | null,
        parseUri: (uri: URL) => {
            const entity = state.entities.find(entity => actorUri(entity.id).href === uri.href);
            return entity ? { type: 'actor', identifier: entity.id } : null;
        },
    };
};

interface FixtureRecord { href: string; createdAt: string; referenceKey: string }
function installQueryFixture(count: number) {
    const alphabet = '0123456789abcdefghjkmnpqrstuvwyz';
    const records: FixtureRecord[] = Array.from({ length: count }, (_, i) => ({
        href: postKey(i), createdAt: instant,
        referenceKey: `${timeline}/x${alphabet[Math.floor(i / 30) % 30]}${alphabet[i % 30]}${'0'.repeat(22)}`,
    }));
    // Independent Core query oracle: filter by prefix and inclusive timestamp,
    // return at most limit items and the first unreturned item's timestamp.
    state.query.mockImplementation(async (_domain: string, endpoint: string, params: Record<string, string>) => {
        expect(endpoint).toBe('net.concrnt.core.query');
        const limit = Number(params.limit);
        expect(limit).toBeLessThanOrEqual(100);
        const rows = records.filter(record => record.referenceKey.startsWith(params.prefix)
            && !params.schema
            && (!params.until || Temporal.Instant.from(record.createdAt).epochNanoseconds <= Temporal.Instant.from(params.until).epochNanoseconds)
            && (!params.since || Temporal.Instant.from(record.createdAt).epochNanoseconds >= Temporal.Instant.from(params.since).epochNanoseconds));
        return {
            items: rows.slice(0, limit).map(record => ({
                cckv: record.referenceKey,
                document: JSON.stringify({ kind: 'record', author: 'con1alice', createdAt: record.createdAt,
                    value: { href: record.href, createdAt: record.createdAt, schema: 'https://schema.concrnt.net/markdown.json' } }),
            })),
            next: rows[limit]?.createdAt ?? null,
        };
    });
    state.getDocument.mockImplementation(async (href: string) => ({
        kind: 'record', key: href, author: 'con1alice', schema: 'https://schema.concrnt.net/markdown.json',
        createdAt: instant, value: { body: 'synthetic fixture' }, distributes: [timeline],
    }));
    return records;
}

async function installReferenceFixture(schema = SCHEMA_REROUTE, body?: string) {
    const records = installQueryFixture(3);
    const documents = new Map(await Promise.all(records.map(async record => [record.href, await state.getDocument(record.href)] as const)));
    const target = 'cckv://con1target/concrnt.world/profiles/main/posts/synthetic-target';
    const parent = documents.get(postKey(1))!;
    parent.schema = schema;
    parent.value = { targetURI: target, ...(body ? { body } : {}) };
    const targetRead = vi.fn().mockResolvedValue({
        schema: SCHEMA_AP_NOTE,
        value: { noteURL: 'https://remote.example/notes/synthetic-target', actorURL: 'https://remote.example/actors/synthetic-author' },
    });
    state.getDocument.mockImplementation(async (uri: string, _domain: unknown, options: unknown) => {
        if (uri === target) return targetRead(options);
        return documents.get(uri);
    });
    return { target, parent, targetRead };
}

beforeEach(async () => {
    vi.resetModules(); vi.clearAllMocks();
    state.registrations.clear(); state.listeners.clear();
    state.config.activitypub.actorPathSegment = 'users';
    state.localFollowers = []; state.remoteFollowers = []; state.timelines = [];
    state.getDocument.mockReset(); state.query.mockReset(); state.proxy.mockReset();
    state.commit.mockResolvedValue({}); state.putObject.mockResolvedValue(undefined);
    state.getObject.mockReset(); state.deleteObject.mockReset(); state.deleteObject.mockResolvedValue(true);
    state.replyReferences = []; state.deleteReference.mockResolvedValue(undefined);
    await import('./federation.ts');
});

describe('configured actor identity end-to-end', () => {
    it.each(['users', 'acct'])('uses /ap/%s for dispatchers, actor fields and activity references', async path => {
        state.config.activitypub.actorPathSegment = path;
        vi.resetModules();
        await import('./federation.ts');
        const prefix = `/ap/${path}/{identifier}`;
        expect(state.registrations.get('setActorDispatcher')![0]).toBe(prefix);
        expect(state.registrations.get('setInboxListeners')!.slice(0, 2)).toEqual([`${prefix}/inbox`, '/ap/inbox']);
        expect(state.registrations.get('setFollowersDispatcher')![0]).toBe(`${prefix}/followers`);
        expect(state.registrations.get('setObjectDispatcher')![1]).toBe(`${prefix}/posts/{+id}`);
        const ctx = context();
        state.getDocument.mockResolvedValue({ value: { username: 'Synthetic Alice' } });
        const actor = await callback('setActorDispatcher')(ctx, 'synthetic-alice');
        const actorURI = `https://bridge.example/ap/${path}/synthetic-alice`;
        expect(actor.id.href).toBe(actorURI);
        expect(actor.inboxId.href).toBe(`${actorURI}/inbox`);
        expect(actor.outboxId.href).toBe(`${actorURI}/outbox`);
        expect(actor.followersId.href).toBe(`${actorURI}/followers`);
        installQueryFixture(1);
        const page = await callback('setOutboxDispatcher')(ctx, 'synthetic-alice', '');
        expect(page.items[0].actorId.href).toBe(actorURI);
        expect(page.items[0].objectId.href).toBe(`${actorURI}/posts/${encodeURIComponent(postKey(0))}`);
    });
});

describe('inbound Create delivery', () => {
    it.each([true, false])('direct does not fan out to followers, recipient inbox present=%s', async hasInbox => {
        state.localFollowers = ['con1alice', 'con1unrelated'];
        state.getDocument.mockImplementation(async () => {
            if (!hasInbox) throw new NotFoundError('synthetic missing inbox', 'synthetic');
            return { kind: 'record' };
        });
        const remote = new Person({ id: new URL('https://remote.example/actor'), followers: new URL('https://remote.example/actor/followers') });
        const note = new Note({ id: new URL('https://remote.example/note/direct'), to: context().getActorUri('synthetic-bob'), content: 'synthetic direct' });
        const create = new Create({ id: new URL('https://remote.example/activity/direct'), actor: remote, object: note });
        await state.listeners.get(Create)!(context(), create);
        expect(state.putObject).toHaveBeenCalledWith(note.id!.href, expect.objectContaining({ recipientCcids: ['con1bob'] }));
        expect(state.commit.mock.calls.find(([doc]) => doc.kind === 'record')![0].distributes)
            .toEqual(hasInbox ? ['cckv://con1bob/activitypub.concrnt.world/inbox'] : []);
        expect(state.getDocument).toHaveBeenCalledExactlyOnceWith('cckv://con1bob/activitypub.concrnt.world/inbox');
    });
});

describe('inbound Update and Delete ownership and audience', () => {
    const owner = 'https://remote.example/actors/owner';
    const attacker = 'https://remote.example/actors/other';
    const uri = 'https://remote.example/notes/restricted';
    const bob = 'https://bridge.example/ap/users/synthetic-bob';
    const carol = 'https://bridge.example/ap/users/synthetic-carol';
    const cached = (): CachedApObject => ({
        json: { id: uri, type: 'Note', content: 'synthetic old content', attributedTo: owner, to: [bob, carol] },
        actorUri: owner, addressed: [bob, carol], followersUri: 'https://remote.example/collections/opaque',
        recipientCcids: ['con1bob', 'con1carol'], receivedAt: instant,
    });
    const updated = (values: ConstructorParameters<typeof Note>[0] = {}) => new Note({
        id: new URL(uri), attribution: new URL(owner), content: 'synthetic replacement', ...values,
    });
    const deliverUpdate = (note: Note | Person, actor = owner, activityAudience: { to?: URL; cc?: URL } = {}) =>
        state.listeners.get(Update)!(context(), new Update({
            id: new URL('https://remote.example/activities/update'), actor: new URL(actor), object: note, ...activityAudience,
        }));
    const deliverDelete = (actor = owner) => state.listeners.get(Delete)!(context(), new Delete({
        id: new URL('https://remote.example/activities/delete'), actor: new URL(actor), object: new Tombstone({ id: new URL(uri) }),
    }));
    const originalRecord = (key: string) => ({
        kind: 'record', key, schema: SCHEMA_AP_NOTE, author: 'con1bridge',
        value: { actorURL: owner, noteURL: uri }, createdAt: instant,
    });

    it('replaces the audience, revokes Bob and preserves the original serialized replacement', async () => {
        const old = cached();
        state.getObject.mockResolvedValue(old);
        const note = updated({ to: new URL(carol) });
        const json = await note.toJsonLd();
        await deliverUpdate(note);
        const written = state.putObject.mock.calls[0][1] as CachedApObject;
        expect(state.putObject.mock.calls[0][2]).toEqual({ requireExisting: true });
        expect(written.addressed).toEqual([carol]);
        expect(written.json).toEqual(json);
        expect(written.followersUri).toBe(old.followersUri);
        expect(isVisibleTo(written, { ccid: 'con1bob', actorUri: bob })).toBe(false);
        expect(isVisibleTo(written, { ccid: 'con1carol', actorUri: carol })).toBe(true);
        expect(isVisibleTo(written, null)).toBe(false);
        expect(old).toEqual(cached());
    });

    it('does not recreate a note when Delete completes during Update serialization', async () => {
        let stored: CachedApObject | null = cached();
        state.getObject.mockImplementation(async () => stored);
        state.deleteObject.mockImplementation(async () => { stored = null; return true; });
        state.putObject.mockImplementation(async (_uri, value, options) => {
            if (!options?.requireExisting || stored) stored = value;
        });
        const note = updated({ to: new URL(carol) });
        const json = await note.toJsonLd();
        let ready!: () => void, resume!: () => void;
        const entered = new Promise<void>(resolve => { ready = resolve; });
        const paused = new Promise<void>(resolve => { resume = resolve; });
        vi.spyOn(note, 'toJsonLd').mockImplementation(async () => { ready(); await paused; return json; });
        const pending = deliverUpdate(note);
        await entered;
        await deliverDelete();
        resume(); await pending;
        expect(stored).toBeNull();
        expect(state.putObject.mock.calls[0][2]).toEqual({ requireExisting: true });
    });

    it.each(['to', 'cc'] as const)('preserves activity-only %s without inserting it into the Note JSON', async field => {
        state.getObject.mockResolvedValue(cached());
        const note = updated();
        const json = await note.toJsonLd();
        await deliverUpdate(note, owner, { [field]: new URL(carol) });
        const written = state.putObject.mock.calls[0][1] as CachedApObject;
        expect(written.addressed).toEqual([carol]);
        expect(written.json).toEqual(json);
        expect(isVisibleTo(written, { ccid: 'con1bob', actorUri: bob })).toBe(false);
        expect(isVisibleTo(written, { ccid: 'con1carol', actorUri: carol })).toBe(true);
    });

    it.each([false, true])('revokes everyone for omitted or explicitly empty audience, explicit=%s', async explicit => {
        state.getObject.mockResolvedValue(cached());
        await deliverUpdate(updated(explicit ? { tos: [], ccs: [] } : {}));
        const written = state.putObject.mock.calls[0][1] as CachedApObject;
        expect(written.addressed).toEqual([]);
        expect(isVisibleTo(written, { ccid: 'con1bob', actorUri: bob })).toBe(false);
        expect(isVisibleTo(written, { ccid: 'con1carol', actorUri: carol })).toBe(false);
        expect(isVisibleTo(written, null)).toBe(false);
    });

    it('deduplicates only the new object and activity audiences, not historical addresses', async () => {
        state.getObject.mockResolvedValue(cached());
        await deliverUpdate(updated({ to: new URL(carol), cc: PUBLIC_COLLECTION }), owner, { to: new URL(carol) });
        expect(state.putObject.mock.calls[0][1].addressed).toEqual([carol, PUBLIC_COLLECTION.href]);
    });

    it.each([owner, attacker])('rejects a same-host non-owner Update even when claimed attribution is %s', async attribution => {
        state.getObject.mockResolvedValue(cached());
        await deliverUpdate(updated({ attribution: new URL(attribution), to: PUBLIC_COLLECTION }), attacker);
        expect(state.putObject).not.toHaveBeenCalled();
    });

    it.each([false, true])('rejects changed or additional attribution from the owner, multiple=%s', async multiple => {
        state.getObject.mockResolvedValue(cached());
        await deliverUpdate(updated(multiple ? { attribution: undefined, attributions: [new URL(owner), new URL(attacker)] } : { attribution: new URL(attacker) }));
        expect(state.putObject).not.toHaveBeenCalled();
    });

    it('allows absent attribution on a replacement from the independently known owner', async () => {
        state.getObject.mockResolvedValue(cached());
        await deliverUpdate(updated({ attribution: undefined, to: new URL(carol) }));
        expect(state.putObject).toHaveBeenCalledOnce();
    });

    it('ignores an actor object masquerading as a cached Note replacement', async () => {
        state.getObject.mockResolvedValue(cached());
        await deliverUpdate(new Person({ id: new URL(uri), name: 'synthetic wrong type' }));
        expect(state.putObject).not.toHaveBeenCalled();
    });

    it('does not create a new object through an uncached Update', async () => {
        state.getObject.mockResolvedValue(null);
        await deliverUpdate(updated({ to: new URL(carol) }));
        expect(state.putObject).not.toHaveBeenCalled();
    });

    it('retains an opaque follower collection without authorizing removed followers', async () => {
        state.getObject.mockResolvedValue(cached());
        state.localFollowers = ['con1carol'];
        await deliverUpdate(updated({ to: new URL(cached().followersUri!) }));
        const written = state.putObject.mock.calls[0][1] as CachedApObject;
        expect(isVisibleTo(written, { ccid: 'con1bob', actorUri: bob })).toBe(false);
        expect(isVisibleTo(written, { ccid: 'con1carol', actorUri: carol })).toBe(true);
        state.localFollowers = [];
        expect(isVisibleTo(written, { ccid: 'con1carol', actorUri: carol })).toBe(false);
    });

    it('does not mutate the loaded cache entry when persistence fails', async () => {
        const old = cached();
        state.getObject.mockResolvedValue(old);
        state.putObject.mockRejectedValueOnce(new Error('synthetic persistence outage'));
        await expect(deliverUpdate(updated({ to: new URL(carol) }))).rejects.toThrow('synthetic persistence outage');
        expect(old).toEqual(cached());
    });

    it('rejects a same-host non-owner Delete without side effects', async () => {
        state.getObject.mockResolvedValue(cached());
        await deliverDelete(attacker);
        expect(state.deleteObject).not.toHaveBeenCalled();
        expect(state.commit).not.toHaveBeenCalled();
        expect(state.deleteReference).not.toHaveBeenCalled();
        expect(state.proxy).not.toHaveBeenCalled();
    });

    it('deletes by URL without fetching an already removed remote object', async () => {
        state.getObject.mockResolvedValue(cached());
        const activity = new Delete({ actor: new URL(owner), object: new URL(uri) });
        const fetch = vi.spyOn(activity, 'getObject').mockRejectedValue(new Error('remote object is already gone'));
        await state.listeners.get(Delete)!(context(), activity);
        expect(fetch).not.toHaveBeenCalled();
        expect(state.deleteObject).toHaveBeenCalledExactlyOnceWith(uri, owner);
    });

    it('uses the original bridge-owned AP note as ownership evidence after cache expiration', async () => {
        state.getObject.mockResolvedValue(null);
        state.proxy.mockImplementation(async key => originalRecord(key));
        state.deleteObject.mockResolvedValue(false);
        await deliverDelete();
        expect(state.proxy).toHaveBeenCalledOnce();
        expect(state.proxy.mock.calls[0][0]).toMatch(/^cckv:\/\/con1bridge\/activitypub.concrnt.world\/inbox\//);
        expect(state.commit).toHaveBeenCalledWith(expect.objectContaining({ kind: 'delete', value: state.proxy.mock.calls[0][0] }));
        expect(state.deleteObject).toHaveBeenCalledExactlyOnceWith(uri, owner);
    });

    it.each(['actor', 'author', 'schema', 'key', 'noteURL', 'kind'])('rejects an uncached Delete if original evidence has the wrong %s', async field => {
        state.getObject.mockResolvedValue(null);
        state.proxy.mockImplementation(async key => {
            const record = originalRecord(key);
            if (field === 'actor') record.value.actorURL = attacker;
            else if (field === 'noteURL') record.value.noteURL = 'https://remote.example/notes/other';
            else Object.assign(record, { [field]: 'synthetic-invalid' });
            return record;
        });
        await deliverDelete();
        expect(state.deleteObject).not.toHaveBeenCalled();
        expect(state.commit).not.toHaveBeenCalled();
    });

    it('skips destructive unknown-owner work when the original record is missing', async () => {
        state.getObject.mockResolvedValue(null);
        state.proxy.mockRejectedValue(new NotFoundError('synthetic missing original', uri));
        await deliverDelete();
        expect(state.deleteObject).not.toHaveBeenCalled();
        expect(state.commit).not.toHaveBeenCalled();
        expect(state.deleteReference).not.toHaveBeenCalled();
    });

    it('retries rather than acknowledging a transient original-record lookup failure', async () => {
        state.getObject.mockResolvedValue(null);
        state.proxy.mockRejectedValue(new Error('synthetic temporary Core outage'));
        await expect(deliverDelete()).rejects.toThrow('synthetic temporary Core outage');
        expect(state.deleteObject).not.toHaveBeenCalled();
        expect(state.commit).not.toHaveBeenCalled();
    });

    it('retains cached ownership until association and note deletions have both succeeded', async () => {
        state.getObject.mockResolvedValue(cached());
        state.replyReferences = [{ apObjectId: uri, ccUri: 'ccfs://synthetic/reply', refType: 'inbound-reply' }];
        const order: string[] = [];
        state.commit.mockImplementation(async doc => { order.push(doc.value); return {}; });
        state.deleteReference.mockImplementation(async () => { order.push('reference'); });
        state.deleteObject.mockImplementation(async () => { order.push('cache'); return true; });
        await deliverDelete();
        expect(order[0]).toBe('ccfs://synthetic/reply');
        expect(order[1]).toBe('reference');
        expect(order[2]).toMatch(/^cckv:\/\/con1bridge\/activitypub.concrnt.world\/inbox\//);
        expect(order[3]).toBe('cache');
        expect(state.deleteObject).toHaveBeenCalledWith(uri, owner);
    });

    it('keeps cached ownership for retry when association cleanup fails', async () => {
        state.getObject.mockResolvedValue(cached());
        state.replyReferences = [{ apObjectId: uri, ccUri: 'ccfs://synthetic/reply', refType: 'inbound-reply' }];
        state.commit.mockRejectedValueOnce(new Error('synthetic association failure'));
        await expect(deliverDelete()).rejects.toThrow('synthetic association failure');
        expect(state.deleteObject).not.toHaveBeenCalled();
        expect(state.deleteReference).not.toHaveBeenCalled();
        await deliverDelete();
        expect(state.deleteObject).toHaveBeenCalledExactlyOnceWith(uri, owner);
    });

    it('finishes cache cleanup when the original note is already deleted', async () => {
        state.getObject.mockResolvedValue(cached());
        state.commit.mockImplementationOnce(async doc => { throw new NotFoundError('synthetic already deleted', doc.value); });
        await deliverDelete();
        expect(state.deleteObject).toHaveBeenCalledExactlyOnceWith(uri, owner);
    });

    it.each(['https://bridge.example/.well-known/concrnt', 'cckv://con1bridge'])('does not mistake missing commit prerequisite for target deletion: %s', async dependency => {
        state.getObject.mockResolvedValue(cached());
        state.commit.mockRejectedValueOnce(new NotFoundError('synthetic missing prerequisite', dependency));
        await expect(deliverDelete()).rejects.toThrow('synthetic missing prerequisite');
        expect(state.deleteObject).not.toHaveBeenCalled();
    });

    it.each(['DNS host not found', 'service not found (HTTP 502)',
        'fetch failed on transport: 500 {"error":"database host not found"}',
        'fetch failed on transport: 500 {"error":"not found"}',
    ])('preserves retry state for ambiguous missing text: %s', async message => {
        state.getObject.mockResolvedValue(cached());
        state.commit.mockRejectedValueOnce(new Error(message));
        await expect(deliverDelete()).rejects.toThrow(message);
        expect(state.deleteObject).not.toHaveBeenCalled();
    });

    it('does not discard association state for DNS failure', async () => {
        state.getObject.mockResolvedValue(cached());
        state.replyReferences = [{ apObjectId: uri, ccUri: 'ccfs://synthetic/reply', refType: 'inbound-reply' }];
        state.commit.mockRejectedValueOnce(new Error('DNS host not found'));
        await expect(deliverDelete()).rejects.toThrow('DNS host not found');
        expect(state.deleteReference).not.toHaveBeenCalled();
        expect(state.deleteObject).not.toHaveBeenCalled();
    });

    it.each(['', '\nrecord not found'])('accepts exact target-specific Core missing response: %j', async suffix => {
        state.getObject.mockResolvedValue(cached());
        state.commit.mockImplementationOnce(async doc => {
            throw new Error(`fetch failed on transport: 500 ${JSON.stringify({ error: `${doc.value} not found${suffix}` })}`);
        });
        await deliverDelete();
        expect(state.deleteObject).toHaveBeenCalledExactlyOnceWith(uri, owner);
    });
});

describe('outbox cursor and bounded history', () => {
    it.each([45, 121])('emits all %i equal-time posts over pages exactly once', async count => {
        installQueryFixture(count);
        const dispatcher = callback('setOutboxDispatcher');
        let cursor: string | null = '';
        const ids: string[] = [];
        for (let pageNumber = 0; cursor !== null && pageNumber < 20; pageNumber++) {
            const page = await dispatcher(context(), 'synthetic-alice', cursor);
            expect(page.items.length).toBeLessThanOrEqual(20);
            ids.push(...page.items.map((item: Create) => item.objectId!.href));
            cursor = page.nextCursor;
        }
        expect(cursor).toBeNull();
        expect(ids).toHaveLength(count);
        expect(new Set(ids).size).toBe(count);
        expect(new Set(ids)).toEqual(new Set(Array.from({ length: count }, (_, i) => context().getObjectUri(Note, { identifier: 'synthetic-alice', id: postKey(i) }).href)));
    });

    it('stable href cursor survives deletion of an earlier same-time row', async () => {
        const records = installQueryFixture(45);
        const dispatcher = callback('setOutboxDispatcher');
        const first = await dispatcher(context(), 'synthetic-alice', '');
        records.splice(0, 1);
        const second = await dispatcher(context(), 'synthetic-alice', first.nextCursor);
        expect(second.items[0].objectId.href).toBe(context().getObjectUri(Note, { identifier: 'synthetic-alice', id: postKey(20) }).href);
        expect(new Set([...first.items, ...second.items].map(item => item.objectId.href)).size).toBe(40);
    });

    it('transient document retrieval rejects the page and retries it without loss', async () => {
        installQueryFixture(3);
        state.getDocument.mockRejectedValueOnce(new Error('synthetic temporary transport error'));
        const dispatcher = callback('setOutboxDispatcher');
        await expect(dispatcher(context(), 'synthetic-alice', '')).rejects.toThrow('synthetic temporary transport error');
        const retry = await dispatcher(context(), 'synthetic-alice', '');
        expect(retry.items).toHaveLength(3);
        expect(retry.items[0].objectId.href).toBe(context().getObjectUri(Note, { identifier: 'synthetic-alice', id: postKey(0) }).href);
    });

    it('anonymous-inaccessible documents are not re-fetched as proxy or published', async () => {
        installQueryFixture(2);
        state.getDocument.mockRejectedValueOnce(new NotFoundError('synthetic private', postKey(0)));
        const page = await callback('setOutboxDispatcher')(context(), 'synthetic-alice', '');
        expect(page.items).toHaveLength(1);
        expect(page.items[0].objectId.href).toContain(encodeURIComponent(postKey(1)));
        expect(page.items[0].toIds.map((url: URL) => url.href)).toContain(PUBLIC_COLLECTION.href);
        expect(state.proxy).not.toHaveBeenCalled();
    });

    it('revalidates anonymous readability instead of publishing a stale formerly-public cache entry', async () => {
        installQueryFixture(1);
        const stalePublic = await state.getDocument(postKey(0));
        state.getDocument.mockImplementation(async (uri: string, _domain: unknown, options: { cache?: string }) => {
            if (options?.cache === 'no-cache') throw new NotFoundError('synthetic now private', uri);
            return stalePublic;
        });
        const page = await callback('setOutboxDispatcher')(context(), 'synthetic-alice', '');
        expect(page.items).toEqual([]);
        expect(state.getDocument).toHaveBeenCalledWith(postKey(0), undefined, expect.objectContaining({ cache: 'no-cache', auth: 'no-auth' }));
        expect(state.proxy).not.toHaveBeenCalled();
    });

    it('skips a parent document denied with typed 403 rather than retrying that page forever', async () => {
        installQueryFixture(2);
        state.getDocument.mockRejectedValueOnce(new PermissionError('synthetic explicit 403'));
        const page = await callback('setOutboxDispatcher')(context(), 'synthetic-alice', '');
        expect(page.items).toHaveLength(1);
        expect(page.items[0].objectId.href).toContain(encodeURIComponent(postKey(1)));
        expect(state.getDocument).toHaveBeenCalledWith(postKey(0), undefined, { cache: 'no-cache', auth: 'no-auth' });
        expect(state.proxy).not.toHaveBeenCalled();
    });

    it('limits configured source prefixes to 32', async () => {
        state.timelines = Array.from({ length: 40 }, (_, i) => `${timeline}/${i}`);
        state.query.mockResolvedValue({ items: [], next: null });
        expect((await callback('setOutboxDispatcher')(context(), 'synthetic-alice', '')).items).toEqual([]);
        expect(state.query).toHaveBeenCalledTimes(32);
    });

    it('rejects malformed cursors without fetching', async () => {
        expect(await callback('setOutboxDispatcher')(context(), 'synthetic-alice', 'tied:invalid')).toBeNull();
        expect(state.query).not.toHaveBeenCalled();
    });
});

describe('Outbox-only strict reference conversion', () => {
    it.each([
        new Error('synthetic HTTP 500'),
        new ServerOfflineError('synthetic-target.example'),
        new Error('synthetic unknown transport failure'),
    ])('rejects transient Announce target failures without consuming any part of the page: %s', async failure => {
        const { targetRead } = await installReferenceFixture();
        targetRead.mockRejectedValueOnce(failure);
        const dispatch = callback('setOutboxDispatcher');
        await expect(dispatch(context(), 'synthetic-alice', '')).rejects.toThrow(failure.message);
        const page = await dispatch(context(), 'synthetic-alice', '');
        const repeatedPage = await dispatch(context(), 'synthetic-alice', '');
        const expected = [
            `${context().getObjectUri(Note, { identifier: 'synthetic-alice', id: postKey(0) }).href}#activity`,
            `https://bridge.example/ap/announces/${encodeURIComponent(postKey(1))}`,
            `${context().getObjectUri(Note, { identifier: 'synthetic-alice', id: postKey(2) }).href}#activity`,
        ];
        expect(page.items.map((activity: Create) => activity.id!.href)).toEqual(expected);
        expect(repeatedPage.items.map((activity: Create) => activity.id!.href)).toEqual(expected);
        expect(new Set(expected).size).toBe(3);
        expect(page.nextCursor).toBeNull();
        expect(targetRead).toHaveBeenCalledWith({ cache: 'no-cache', auth: 'no-auth' });
        expect(state.proxy).not.toHaveBeenCalled();
    });

    it.each(['404', '403'])('skips an Announce with inaccessible target %s without blocking neighboring posts', async status => {
        const { target, targetRead } = await installReferenceFixture();
        targetRead.mockRejectedValue(status === '404' ? new NotFoundError('synthetic missing', target) : new PermissionError('synthetic denied'));
        const page = await callback('setOutboxDispatcher')(context(), 'synthetic-alice', '');
        expect(page.items.map((activity: Create) => activity.objectId!.href)).toEqual([0, 2].map(i => context().getObjectUri(Note, { identifier: 'synthetic-alice', id: postKey(i) }).href));
        expect(page.nextCursor).toBeNull();
        expect(targetRead).toHaveBeenCalledWith({ cache: 'no-cache', auth: 'no-auth' });
        expect(state.proxy).not.toHaveBeenCalled();
    });

    it('does not disclose a cached target URL when the target is no longer anonymously readable', async () => {
        const { target, targetRead } = await installReferenceFixture();
        targetRead.mockImplementation(async (options: { cache?: string, auth?: string }) => {
            if (options.cache === 'no-cache' && options.auth === 'no-auth') throw new NotFoundError('synthetic target now private', target);
            return { schema: SCHEMA_AP_NOTE, value: { noteURL: 'https://remote.example/notes/synthetic-now-private' } };
        });
        const page = await callback('setOutboxDispatcher')(context(), 'synthetic-alice', '');
        expect(page.items).toHaveLength(2);
        expect(page.items.every((activity: Create) => activity instanceof Create)).toBe(true);
        expect(state.proxy).not.toHaveBeenCalled();
    });

    it.each([SCHEMA_REROUTE, SCHEMA_REPLY])('propagates the same strict policy into linked Note conversion: %s', async schema => {
        const { targetRead } = await installReferenceFixture(schema, 'synthetic linked body');
        targetRead.mockRejectedValueOnce(new Error('synthetic linked-target failure'));
        const dispatch = callback('setOutboxDispatcher');
        await expect(dispatch(context(), 'synthetic-alice', '')).rejects.toThrow('synthetic linked-target failure');
        expect((await dispatch(context(), 'synthetic-alice', '')).items).toHaveLength(3);
        expect(targetRead).toHaveBeenCalledWith({ cache: 'no-cache', auth: 'no-auth' });
    });

    it('retains default best-effort behavior for existing daemon callers', async () => {
        const { parent, targetRead } = await installReferenceFixture();
        targetRead.mockRejectedValue(new Error('synthetic target temporarily unavailable'));
        const { buildActivity } = await import('./convert.ts');
        expect(await buildActivity(context() as any, { identifier: 'synthetic-alice', id: postKey(1) }, parent, 'public')).toBeNull();
        expect(targetRead).toHaveBeenCalledExactlyOnceWith({ negativeTTL: 300_000 });
    });

    it.each([SCHEMA_REROUTE, SCHEMA_REPLY])('keeps default linked Note conversion best-effort: %s', async schema => {
        const { parent, targetRead } = await installReferenceFixture(schema, 'synthetic linked body');
        targetRead.mockRejectedValue(new Error('synthetic optional target failure'));
        const { buildActivity } = await import('./convert.ts');
        const activity = await buildActivity(context() as any, { identifier: 'synthetic-alice', id: postKey(1) }, parent, 'public');
        expect(activity).toBeInstanceOf(Create);
        expect((await activity!.getObject())?.content?.toString()).toContain('synthetic linked body');
        expect(targetRead).toHaveBeenCalledExactlyOnceWith({ negativeTTL: 300_000 });
    });
});

describe('private outgoing object authorization', () => {
    it.each(['anonymous', 'unrelated', 'follower'])('returns a followers-only object only to a signed follower: %s', async requester => {
        installQueryFixture(1);
        const privateDocument = await state.getDocument(postKey(0));
        state.getDocument.mockRejectedValue(new PermissionError('synthetic private'));
        state.proxy.mockResolvedValue(privateDocument);
        state.remoteFollowers = [{ actorURI: 'https://remote.example/follower' }];
        const ctx = context();
        if (requester !== 'anonymous') ctx.getSignedKeyOwner = async () => new Person({ id: new URL(`https://remote.example/${requester}`) });
        const note = await callback('setObjectDispatcher')(ctx, { identifier: 'synthetic-alice', id: postKey(0) });
        if (requester !== 'follower') expect(note).toBeNull();
        else {
            expect(note.toIds.map((url: URL) => url.href)).toEqual(['https://bridge.example/ap/users/synthetic-alice/followers']);
            expect(note.toIds).not.toContainEqual(PUBLIC_COLLECTION);
        }
        if (requester === 'anonymous') expect(state.proxy).not.toHaveBeenCalled();
    });
});
