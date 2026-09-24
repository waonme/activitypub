import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const state = vi.hoisted(() => ({
    clients: [] as any[], events: [] as string[],
    connectionGate: undefined as Promise<void> | undefined,
    subscriptionGate: undefined as Promise<void> | undefined,
    connectionFailure: undefined as Error | undefined,
    subscriptionFailure: undefined as Error | undefined,
    initialLoadFailure: undefined as Error | undefined,
    atAcknowledgement: undefined as (() => void) | undefined,
    removeAcceptState: vi.fn(), initializeFollows: vi.fn(),
}));

vi.mock('ioredis', async () => {
    const { EventEmitter } = await import('node:events');
    return { Redis: class extends EventEmitter {
        connected = false;
        subscribed = false;
        disconnect = vi.fn(() => { this.connected = false; this.subscribed = false; state.events.push('disconnect'); });
        constructor(_url: string, readonly options: Record<string, unknown> = {}) {
            super(); state.clients.push(this);
            if (!options.lazyConnect) state.events.push('automatic-connect');
        }
        async connect() {
            state.events.push('connect');
            expect(this.listenerCount('error')).toBe(1);
            expect(this.listenerCount('pmessage')).toBe(1);
            if (state.connectionFailure) { this.emit('error', state.connectionFailure); throw state.connectionFailure; }
            await state.connectionGate;
            this.connected = true; state.events.push('ready'); this.emit('ready');
        }
        async psubscribe(pattern: string, callback?: (error: Error | null, count?: number) => void) {
            state.events.push(`subscribe:${pattern}`);
            if (state.subscriptionFailure) {
                // Model the old callback form without leaking an unrelated
                // rejected promise into Vitest's unhandled-error machinery.
                if (callback) { callback(state.subscriptionFailure); return; }
                throw state.subscriptionFailure;
            }
            if (state.subscriptionGate) await state.subscriptionGate;
            this.subscribed = true; state.events.push('ack');
            state.atAcknowledgement?.();
            callback?.(null, 1);
            return 1;
        }
        reconnect() {
            this.emit('close'); this.emit('reconnecting', 1);
            this.connected = true; this.emit('ready');
            if (!this.options.autoResubscribe) this.subscribed = false;
        }
        deliver() {
            if (this.subscribed) this.emit('pmessage', 'cc-event:*',
                'cc-event:cckv://con1service/activitypub.concrnt.world/accept-states/synthetic',
                JSON.stringify({ type: 'deleted', uri: 'cckv://con1service/activitypub.concrnt.world/accept-states/synthetic' }));
        }
    } };
});
vi.mock('./config.ts', () => ({ config: {
    redis: { url: 'redis://synthetic.invalid' }, concrnt: { ccid: 'con1service' },
    activitypub: { baseUrl: 'https://bridge.example', actorPathSegment: 'users' },
} }));
vi.mock('./db/index.ts', async () => {
    const schema = await import('./db/schema.ts');
    return { ...schema, db: { select: () => ({ from: (table: unknown) => {
        const result = table === schema.apEntity && state.initialLoadFailure
            ? Promise.reject(state.initialLoadFailure) : Promise.resolve([]);
        return Object.assign(result, { where: async () => [] });
    } }) } };
});
vi.mock('./federation.ts', () => ({ default: {}, buildPerson: vi.fn() }));
vi.mock('./concrnt.ts', () => ({ default: {}, commit: vi.fn() }));
vi.mock('./convert.ts', () => ({
    buildActivity: vi.fn(), resolveVisibility: vi.fn(), SCHEMA_AP_NOTE: 'note', SCHEMA_REFERENCE: 'reference',
    SCHEMA_LIKE: 'like', SCHEMA_REACTION: 'reaction', SCHEMA_DELETE: 'delete',
}));
vi.mock('./followStore.ts', () => ({
    initialize: state.initializeFollows, removeAcceptStateByKey: state.removeAcceptState,
    ensureServiceRecordsLoaded: async () => {}, ensureEntityFollowsLoaded: async () => {},
}));
vi.mock('./settingsStore.ts', () => ({ ensureEntitySettingsLoaded: async () => {} }));
vi.mock('./inboxStore.ts', () => ({ ensureEntityInboxLoaded: async () => {} }));

const settle = async () => { for (let turn = 0; turn < 20; turn++) await Promise.resolve(); };
beforeEach(() => {
    vi.resetModules(); vi.clearAllMocks(); vi.useFakeTimers();
    state.clients = []; state.events = [];
    state.connectionGate = undefined; state.subscriptionGate = undefined;
    state.connectionFailure = undefined; state.subscriptionFailure = undefined;
    state.initialLoadFailure = undefined; state.atAcknowledgement = undefined;
    state.initializeFollows.mockResolvedValue(undefined);
});
afterEach(() => { vi.clearAllTimers(); vi.useRealTimers(); });

describe('broker readiness boundary', () => {
    it('waits for ready before PSUBSCRIBE and for its ACK before resolving or starting refresh', async () => {
        let ready!: () => void; let acknowledge!: () => void;
        state.connectionGate = new Promise(resolve => { ready = resolve; });
        state.subscriptionGate = new Promise(resolve => { acknowledge = resolve; });
        const { startEntityBroker } = await import('./daemon.ts');
        let finished = false;
        const startup = startEntityBroker().then(() => { finished = true; });
        await settle();
        const client = state.clients[0];
        expect(client.options).toMatchObject({ lazyConnect: true, enableReadyCheck: true, autoResubscribe: true });
        expect(state.events).toEqual(['connect']);
        expect(vi.getTimerCount()).toBe(0);
        expect(finished).toBe(false);
        ready(); await settle();
        expect(state.events).toEqual(['connect', 'ready', 'subscribe:cc-event:*']);
        expect(state.initializeFollows).not.toHaveBeenCalled();
        expect(finished).toBe(false);
        expect(vi.getTimerCount()).toBe(0);
        acknowledge(); await startup;
        expect(state.events.at(-1)).toBe('ack');
        expect(state.initializeFollows).toHaveBeenCalledOnce();
        expect(vi.getTimerCount()).toBe(1);
    });

    it.each(['connection', 'subscription', 'initialLoad'] as const)('disconnects and propagates %s failure without starting refresh', async failure => {
        const error = new Error(`synthetic ${failure} failure`);
        state[`${failure}Failure`] = error;
        const { startEntityBroker } = await import('./daemon.ts');
        await expect(startEntityBroker()).rejects.toBe(error);
        expect(state.clients[0].disconnect).toHaveBeenCalledOnce();
        expect(vi.getTimerCount()).toBe(0);
        if (failure !== 'subscription') expect(state.events).not.toContain('subscribe:cc-event:*');
    });

    it('handles an event emitted immediately with subscription ACK', async () => {
        state.atAcknowledgement = () => state.clients[0].deliver();
        const { startEntityBroker } = await import('./daemon.ts');
        await startEntityBroker(); await settle();
        expect(state.removeAcceptState).toHaveBeenCalledExactlyOnceWith(
            'cckv://con1service/activitypub.concrnt.world/accept-states/synthetic');
    });

    it('preserves automatic resubscription without adding duplicate application subscriptions or listeners', async () => {
        const { startEntityBroker } = await import('./daemon.ts');
        await startEntityBroker();
        const client = state.clients[0];
        client.deliver(); client.reconnect(); client.deliver(); client.reconnect(); client.deliver();
        await settle();
        expect(state.removeAcceptState).toHaveBeenCalledTimes(3);
        expect(client.listenerCount('pmessage')).toBe(1);
        expect(client.listenerCount('error')).toBe(1);
        expect(state.events.filter(value => value.startsWith('subscribe:'))).toEqual(['subscribe:cc-event:*']);
        expect(vi.getTimerCount()).toBe(1);
    });
});
