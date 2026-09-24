import { beforeEach, describe, expect, it, vi } from 'vitest';

const state = vi.hoisted(() => ({ broker: vi.fn(), serve: vi.fn() }));
vi.mock('./logging.ts', () => ({}));
vi.mock('./daemon.ts', () => ({ startEntityBroker: state.broker }));
vi.mock('@hono/node-server', () => ({ serve: state.serve }));
vi.mock('x-forwarded-fetch', () => ({ behindProxy: (fetch: unknown) => fetch }));
vi.mock('./app.ts', () => ({ default: { fetch: async () => new Response('synthetic') } }));
vi.mock('./config.ts', () => ({ config: { server: { port: 18008 }, activitypub: { baseUrl: 'https://bridge.example' } } }));

beforeEach(() => { vi.resetModules(); vi.clearAllMocks(); });

describe('HTTP startup waits for the broker', () => {
    it('does not serve health or normal HTTP before successful broker startup', async () => {
        let ready!: () => void;
        state.broker.mockReturnValue(new Promise<void>(resolve => { ready = resolve; }));
        const startup = import('./index.ts');
        await vi.waitFor(() => expect(state.broker).toHaveBeenCalledOnce());
        expect(state.serve).not.toHaveBeenCalled();
        ready(); await startup;
        expect(state.serve).toHaveBeenCalledOnce();
    });

    it('propagates startup failure without opening HTTP', async () => {
        const error = new Error('synthetic subscription denied');
        state.broker.mockRejectedValue(error);
        await expect(import('./index.ts')).rejects.toBe(error);
        expect(state.serve).not.toHaveBeenCalled();
    });
});
