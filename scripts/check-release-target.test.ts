import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { parse } from 'yaml';
import { validateReleaseTarget } from './check-release-target.ts';

const root = new URL('../', import.meta.url);
const read = (path: string) => readFileSync(new URL(path, root), 'utf8');
const identity = { repository: 'waonme/activitypub', owner: 'waonme', registry: 'ghcr.io', image: 'waonme/activitypub' };
const valid = 'version: 2\nrelease:\n  github:\n    owner: waonme\n    name: activitypub\n';
const jobGuard = "${{ github.repository == 'waonme/activitypub' && github.repository_owner == 'waonme' }}";
const command = (kind: string) => `node --import tsx scripts/check-release-target.ts ${kind}`;

describe('fork release target validation', () => {
    it.each(['github', 'docker'] as const)('accepts the synthetic and actual %s configuration', kind => {
        expect(() => validateReleaseTarget(valid, identity, kind)).not.toThrow();
        expect(() => validateReleaseTarget(read('.goreleaser.yaml'), identity, kind)).not.toThrow();
    });

    it.each([
        { repository: 'concrnt/activitypub', owner: 'concrnt' },
        { repository: 'waonme/another-repository', owner: 'waonme' },
        { repository: 'waonme/activitypub', owner: 'concrnt' },
        { repository: 'WAONME/activitypub', owner: 'waonme' },
        { repository: '', owner: 'waonme' },
        { owner: 'waonme' }, { repository: 'waonme/activitypub' }, {},
    ])('rejects wrong or absent repository identity: %j', wrong => {
        expect(() => validateReleaseTarget(valid, wrong, 'github')).toThrow('repository identity');
    });

    it.each([
        valid.replace('owner: waonme', 'owner: concrnt'),
        valid.replace('name: activitypub', 'name: another-repository'),
        valid.replace('owner: waonme', 'owner: "{{ .Env.RELEASE_OWNER }}"'),
        'version: 2\n', 'release: null', 'release: [github]', 'release: {github: []}',
    ])('rejects the actual parsed target rather than assuming the workflow identity is enough: %s', source => {
        expect(() => validateReleaseTarget(source, identity, 'github')).toThrow('GoReleaser must target');
    });

    it.each([
        'release: [',
        `${valid}release: {github: {owner: concrnt, name: activitypub}}`,
        valid.replace('owner: waonme', 'owner: !unknown waonme'),
    ])('rejects malformed, duplicate-key or tagged YAML: %s', source => {
        expect(() => validateReleaseTarget(source, identity, 'github')).toThrow('Malformed');
    });

    it.each(['gitlab', 'gitea'])('rejects an additional %s release provider', provider => {
        expect(() => validateReleaseTarget(`${valid}  ${provider}: {owner: somebody, name: activitypub}\n`, identity, 'github'))
            .toThrow('Additional release providers');
    });

    it.each([
        { registry: 'docker.io' }, { registry: '' }, { image: 'concrnt/activitypub' }, { image: '' },
    ])('rejects mismatched Docker output: %j', wrong => {
        expect(() => validateReleaseTarget(valid, { ...identity, ...wrong }, 'docker')).toThrow('Docker publishing');
    });

    it('checks actual checked-in YAML through the executable CLI without publishing', () => {
        const result = spawnSync(process.execPath, ['--import', 'tsx', fileURLToPath(new URL('scripts/check-release-target.ts', root)), 'github'], {
            cwd: fileURLToPath(root), encoding: 'utf8',
            env: { ...process.env, GITHUB_REPOSITORY: identity.repository, GITHUB_REPOSITORY_OWNER: identity.owner },
        });
        expect(result.status, result.stderr).toBe(0);
        expect(result.stdout).toContain('Release target validated: waonme/activitypub');
    });

    it('exits nonzero with missing CI identity rather than substituting a default', () => {
        const environment = { ...process.env };
        delete environment.GITHUB_REPOSITORY;
        delete environment.GITHUB_REPOSITORY_OWNER;
        const result = spawnSync(process.execPath, ['--import', 'tsx', fileURLToPath(new URL('scripts/check-release-target.ts', root)), 'github'], {
            cwd: fileURLToPath(root), encoding: 'utf8', env: environment,
        });
        expect(result.status).toBe(1);
        expect(result.stderr).toContain('repository identity');
    });
});

describe('CLI symlink entrypoints validate the actual configuration', () => {
    it.each([
        ['checkout-root', 'wrong-config'], ['checkout-root', 'missing-identity'], ['checkout-root', 'valid'],
        ['script', 'wrong-config'], ['script', 'missing-identity'], ['script', 'valid'],
    ] as const)('%s symlink with %s', (linkKind, scenario) => {
        const fixture = mkdtempSync(join(tmpdir(), 'ap-release-guard-cli-'));
        try {
            const checkout = join(fixture, 'checkout');
            const scripts = join(checkout, 'scripts');
            mkdirSync(scripts, { recursive: true });
            const script = join(scripts, 'check-release-target.ts');
            copyFileSync(new URL('scripts/check-release-target.ts', root), script);
            symlinkSync(fileURLToPath(new URL('node_modules', root)), join(checkout, 'node_modules'), 'dir');
            writeFileSync(join(checkout, 'package.json'), '{"type":"module"}\n');
            writeFileSync(join(checkout, '.goreleaser.yaml'), scenario === 'wrong-config'
                ? valid.replace('owner: waonme', 'owner: concrnt') : valid);
            let entry: string;
            if (linkKind === 'checkout-root') {
                const linkedCheckout = join(fixture, 'linked-checkout');
                symlinkSync(checkout, linkedCheckout, 'dir');
                entry = join(linkedCheckout, 'scripts', 'check-release-target.ts');
            } else {
                entry = join(fixture, 'linked-guard.ts');
                symlinkSync(script, entry, 'file');
            }
            const environment = { ...process.env, GITHUB_REPOSITORY: identity.repository, GITHUB_REPOSITORY_OWNER: identity.owner };
            if (scenario === 'missing-identity') {
                delete (environment as NodeJS.ProcessEnv).GITHUB_REPOSITORY;
                delete (environment as NodeJS.ProcessEnv).GITHUB_REPOSITORY_OWNER;
            }
            const result = spawnSync(process.execPath, ['--import', 'tsx', entry, 'github'], {
                cwd: checkout, env: environment, encoding: 'utf8', timeout: 10_000,
            });
            expect(result.error).toBeUndefined();
            if (scenario === 'valid') {
                expect(result.status, result.stderr).toBe(0);
                expect(result.stdout).toContain('Release target validated: waonme/activitypub');
            } else {
                expect(result.status, result.stdout).toBe(1);
                expect(result.stdout).not.toContain('Release target validated');
                expect(result.stderr).toContain(scenario === 'wrong-config' ? 'GoReleaser must target' : 'repository identity');
            }
        } finally {
            // Only this test's mkdtemp-created synthetic fixture is removed.
            rmSync(fixture, { recursive: true, force: true });
        }
    });
});

describe('checked-in publishing workflow boundaries', () => {
    it('guards GoReleaser before packaging/publishing and again in its before-hook', () => {
        const workflow = parse(read('.github/workflows/release.yml'));
        const job = workflow.jobs.goreleaser;
        expect(job.if).toBe(jobGuard);
        const guard = job.steps.findIndex((step: any) => step.run === command('github'));
        const publish = job.steps.findIndex((step: any) => step.uses?.startsWith('goreleaser/goreleaser-action@'));
        expect(guard).toBeGreaterThanOrEqual(0);
        expect(publish).toBeGreaterThan(guard);
        expect(parse(read('.goreleaser.yaml')).before.hooks[0]).toBe(command('github'));
    });

    it.each(['build', 'manifest'])('guards Docker %s before registry login and fixes the image destination', name => {
        const workflow = parse(read('.github/workflows/docker.yaml'));
        const job = workflow.jobs[name];
        expect(job.if).toBe(jobGuard);
        const environment = { ...workflow.env, ...job.env };
        expect(environment.REGISTRY).toBe('ghcr.io');
        expect(environment.IMAGE_NAME).toBe('waonme/activitypub');
        const checkout = job.steps.findIndex((step: any) => step.uses?.startsWith('actions/checkout@'));
        const guard = job.steps.findIndex((step: any) => step.run === command('docker'));
        const login = job.steps.findIndex((step: any) => step.uses?.startsWith('docker/login-action@'));
        expect(checkout).toBeGreaterThanOrEqual(0);
        expect(guard).toBeGreaterThan(checkout);
        expect(login).toBeGreaterThan(guard);
        expect(job.steps.slice(0, guard).some((step: any) => step.run?.includes('pnpm install --frozen-lockfile --ignore-scripts'))).toBe(true);
    });

    it('validates branch and PR changes without any publishing action', () => {
        const workflow = parse(read('.github/workflows/check.yaml'));
        expect(workflow.on.push.branches).toEqual(['**']);
        expect(workflow.on).toHaveProperty('pull_request');
        expect(workflow.jobs.check.steps.some((step: any) => step.run === command('github'))).toBe(true);
        expect(read('.github/workflows/check.yaml')).not.toMatch(/packages: write|contents: write|goreleaser-action|docker\/login-action|push=true/);
    });
});
