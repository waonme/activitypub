import { readFileSync, realpathSync } from 'node:fs';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { parseDocument } from 'yaml';

export const FORK_REPOSITORY = 'waonme/activitypub';
export const FORK_OWNER = 'waonme';
export const FORK_REGISTRY = 'ghcr.io';

type ReleaseKind = 'github' | 'docker';
type Identity = {
    repository?: string;
    owner?: string;
    registry?: string;
    image?: string;
};

const record = (value: unknown): Record<string, unknown> | undefined =>
    value !== null && typeof value === 'object' && !Array.isArray(value)
        ? value as Record<string, unknown> : undefined;

/** Read-only check: does not invoke GoReleaser, Docker, GitHub or the network. */
export function validateReleaseTarget(source: string, identity: Identity, kind: ReleaseKind): void {
    if (kind !== 'github' && kind !== 'docker') throw new Error('Unknown release target kind');
    if (identity.repository !== FORK_REPOSITORY || identity.owner !== FORK_OWNER) {
        throw new Error('Publishing requires the exact waonme/activitypub repository identity');
    }

    let configuration: Record<string, unknown> | undefined;
    try {
        const document = parseDocument(source, { strict: true, uniqueKeys: true });
        if (document.errors.length || document.warnings.length) throw new Error('Invalid YAML');
        configuration = record(document.toJS({ maxAliasCount: 0 }));
    } catch {
        throw new Error('Malformed GoReleaser configuration');
    }
    const release = record(configuration?.release);
    const github = record(release?.github);
    if (github?.owner !== FORK_OWNER || github?.name !== 'activitypub') {
        throw new Error('GoReleaser must target exactly waonme/activitypub');
    }
    if (release && ('gitlab' in release || 'gitea' in release)) {
        throw new Error('Additional release providers are not allowed for this fork');
    }
    if (kind === 'docker' && (identity.registry !== FORK_REGISTRY || identity.image !== FORK_REPOSITORY)) {
        throw new Error('Docker publishing must target exactly ghcr.io/waonme/activitypub');
    }
}

function main(): void {
    const kind = process.argv[2];
    if (process.argv.length !== 3 || (kind !== 'github' && kind !== 'docker')) {
        throw new Error('Usage: node --import tsx scripts/check-release-target.ts github|docker');
    }
    // Anchor to the real checked-out guard, not a working directory or the
    // directory containing a symlink to this entrypoint.
    const moduleUrl = pathToFileURL(realpathSync(fileURLToPath(import.meta.url)));
    const source = readFileSync(new URL('../.goreleaser.yaml', moduleUrl), 'utf8');
    validateReleaseTarget(source, {
        repository: process.env.GITHUB_REPOSITORY,
        owner: process.env.GITHUB_REPOSITORY_OWNER,
        registry: process.env.REGISTRY,
        image: process.env.IMAGE_NAME,
    }, kind);
    console.log(`Release target validated: ${kind === 'docker' ? `${FORK_REGISTRY}/` : ''}${FORK_REPOSITORY}`);
}

try {
    // Node/tsx may canonicalize import.meta.url while leaving argv[1] as a
    // symlink (/tmp -> /private/tmp, a linked checkout, or a linked script).
    // Compare real paths so a direct CLI invocation can never silently skip
    // validation merely because these two spellings differ.
    if (process.argv[1] && realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url))) {
        main();
    }
} catch (error) {
    console.error(`Release guard: ${error instanceof Error ? error.message : 'validation failed'}`);
    process.exitCode = 1;
}
