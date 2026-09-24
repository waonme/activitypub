import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Context, Next } from 'hono';

const fixture = vi.hoisted(() => ({
    config: { server: { adminToken: null as string | null } },
    resend: vi.fn(),
}));
vi.mock('./config.ts', () => ({ config: fixture.config }));
vi.mock('./federation.ts', () => ({ default: {}, INSTANCE_ACTOR: 'instance.actor', storeApNote: vi.fn() }));
vi.mock('@fedify/hono', () => ({ federation: () => (_c: Context, next: Next) => next() }));
vi.mock('./daemon.ts', () => ({ resendPendingFollows: fixture.resend }));
vi.mock('./db/index.ts', () => ({ db: {}, apEntity: {} }));
vi.mock('./followStore.ts', () => ({}));
vi.mock('./objectCache.ts', () => ({}));
vi.mock('./metrics.ts', () => ({ renderPrometheus: vi.fn() }));

const app = (await import('./app.ts')).default;
const request = (authorization?: string, body: unknown = {}) => app.request('/-/resend-follows', {
    method: 'POST', headers: { 'content-type': 'application/json', ...(authorization ? { authorization } : {}) },
    body: JSON.stringify(body),
});

describe('operator resend API authorization', () => {
    beforeEach(() => { fixture.config.server.adminToken = 'synthetic-test-token'; fixture.resend.mockReset(); });

    it('is disabled without configured token, even with Authorization', async () => {
        fixture.config.server.adminToken = null;
        expect((await request('Bearer synthetic-test-token')).status).toBe(404);
        expect(fixture.resend).not.toHaveBeenCalled();
    });

    it.each([undefined, 'Bearer wrong', 'synthetic-test-token', 'Basic synthetic-test-token'])('rejects %s before invoking resend', async auth => {
        expect((await request(auth)).status).toBe(401);
        expect(fixture.resend).not.toHaveBeenCalled();
    });

    it.each([null, [], { ccid: '' }, { actorURIs: 'https://remote.example/actor' }, { actorURIs: [''] }, { dryRun: 'true' }])('rejects invalid request %j', async body => {
        expect((await request('Bearer synthetic-test-token', body)).status).toBe(400);
        expect(fixture.resend).not.toHaveBeenCalled();
    });

    it('accepts only authorized valid requests without losing dryRun', async () => {
        const body = { ccid: 'con1synthetic', actorURIs: ['https://remote.example/actor'], dryRun: true };
        fixture.resend.mockResolvedValue([{ status: 'skipped' }]);
        const response = await request('Bearer synthetic-test-token', body);
        expect(response.status).toBe(200);
        expect(fixture.resend).toHaveBeenCalledExactlyOnceWith(body);
        expect(await response.json()).toMatchObject({ sent: 0, failed: 0, skipped: 1 });
    });
});
