import { beforeEach, describe, expect, it, vi } from 'vitest';

const fixture = vi.hoisted(() => ({ actorPathSegment: undefined as string | undefined }));
vi.mock('node:fs', () => ({
    statSync: () => ({ isDirectory: () => false }),
    readdirSync: () => [],
    readFileSync: () => JSON.stringify({
        server: { port: 8080 }, database: { url: 'postgresql://synthetic.test/test' },
        redis: { url: 'redis://synthetic.test' },
        concrnt: { domain: 'bridge.example', privateKey: 'synthetic-test-key-never-used' },
        activitypub: fixture.actorPathSegment === undefined ? {} : { actorPathSegment: fixture.actorPathSegment },
    }),
}));
vi.mock('@concrnt/client', () => ({ LoadKey: () => ({}), ComputeCCID: () => 'con1synthetic' }));

describe('actor path configuration', () => {
    beforeEach(() => { vi.resetModules(); fixture.actorPathSegment = undefined; });

    it('keeps the upstream acct default', async () => {
        expect((await import('./config.ts')).config.activitypub.actorPathSegment).toBe('acct');
    });

    it('preserves the configured users identity path', async () => {
        fixture.actorPathSegment = 'users';
        expect((await import('./config.ts')).config.activitypub.actorPathSegment).toBe('users');
    });

    it('rejects multi-segment actor paths', async () => {
        fixture.actorPathSegment = 'users/other';
        await expect(import('./config.ts')).rejects.toThrow('one URL path segment');
    });
});
