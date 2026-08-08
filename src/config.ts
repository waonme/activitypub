import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { ComputeCCID, LoadKey } from "@concrnt/client";
import { parse } from "yaml";

export interface AppConfig {
  server: {
    port: number;
  };
  database: {
    url: string;
  };
  redis: {
    url: string;
  };
  concrnt: {
    ccid: string; // derived from privateKey
    domain: string;
    privateKey: string;
  };
  activitypub: {
    baseUrl: string;
    actorPathSegment: string;
    objectCacheTTL: number; // seconds
  };
}

type ConfigRecord = Record<string, unknown>;

const defaultConfigPath = fileURLToPath(new URL("../config.yaml", import.meta.url));
const configPath = process.env.CONFIG_PATH ?? defaultConfigPath;

const isRecord = (value: unknown): value is ConfigRecord =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const expectRecord = (value: unknown, path: string): ConfigRecord => {
  if (!isRecord(value)) {
    throw new Error(`Invalid config: "${path}" must be a mapping.`);
  }

  return value;
};

const expectString = (value: unknown, path: string): string => {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`Invalid config: "${path}" must be a non-empty string.`);
  }

  return value;
};

const expectNumber = (value: unknown, path: string): number => {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`Invalid config: "${path}" must be a finite number.`);
  }

  return value;
};

const expectHost = (value: unknown, path: string): string => {
  const host = expectString(value, path);

  if (host.includes("://")) {
    throw new Error(
      `Invalid config: "${path}" must be a host or FQDN without a URL scheme.`,
    );
  }

  if (/[/?#]/.test(host)) {
    throw new Error(
      `Invalid config: "${path}" must not include a path, query, or fragment.`,
    );
  }

  new URL(`https://${host}`);

  return host;
};

const expectPathSegment = (value: unknown, path: string): string => {
  const segment = expectString(value, path);
  if (!/^[A-Za-z0-9._~-]+$/.test(segment)) {
    throw new Error(`Invalid config: "${path}" must be one URL path segment.`);
  }
  return segment;
};

// concrnt本体のDeepMergeと同じ規則: マップは再帰、それ以外は後勝ちで置換、
// srcがnull/undefinedのときは上書きしない(Goのゼロ値スキップに相当)
const deepMerge = (dst: unknown, src: unknown): unknown => {
  if (src === null || src === undefined) {
    return dst;
  }

  if (isRecord(dst) && isRecord(src)) {
    const merged: ConfigRecord = { ...dst };
    for (const [key, value] of Object.entries(src)) {
      merged[key] = deepMerge(merged[key], value);
    }
    return merged;
  }

  return src;
};

const readConfig = (): AppConfig => {
  let parsed: unknown;

  try {
    if (statSync(configPath).isDirectory()) {
      parsed = {};
      for (const name of readdirSync(configPath).sort()) {
        const entryPath = join(configPath, name);
        try {
          if (statSync(entryPath).isDirectory()) {
            continue;
          }
          parsed = deepMerge(parsed, parse(readFileSync(entryPath, "utf8")));
        } catch (error) {
          const reason = error instanceof Error ? error.message : String(error);
          console.warn(`Skipping config file "${entryPath}": ${reason}`);
        }
      }
    } else {
      parsed = parse(readFileSync(configPath, "utf8")) as unknown;
    }
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Failed to read config at "${configPath}". Create config.yaml from config.example.yaml, or point CONFIG_PATH at a config directory. ${reason}`,
    );
  }

  const root = expectRecord(parsed, "config");
  const activitypub = root.activitypub === undefined ? {} : expectRecord(root.activitypub, "activitypub");
  const server = expectRecord(root.server, "server");
  const database = expectRecord(root.database, "database");
  const redis = expectRecord(root.redis, "redis");
  const concrnt = expectRecord(root.concrnt, "concrnt");
  const concrntDomain = expectHost(concrnt.domain, "concrnt.domain");
  const activitypubBaseUrl = new URL(`https://${concrntDomain}`).origin;
  const concrntPrivateKey = expectString(concrnt.privateKey, "concrnt.privateKey").trim();
  const keypair = LoadKey(concrntPrivateKey);
  if (!keypair) {
    throw new Error(
      'Invalid config: "concrnt.privateKey" is not a valid secp256k1 private key.',
    );
  }

  const config: AppConfig = {
    server: {
      port: expectNumber(server.port, "server.port"),
    },
    database: {
      url: expectString(database.url, "database.url"),
    },
    redis: {
      url: expectString(redis.url, "redis.url"),
    },
    concrnt: {
      ccid: ComputeCCID(keypair.publickey),
      domain: concrntDomain,
      privateKey: concrntPrivateKey,
    },
    activitypub: {
      baseUrl: activitypubBaseUrl,
      actorPathSegment: activitypub.actorPathSegment === undefined
        ? "acct"
        : expectPathSegment(activitypub.actorPathSegment, "activitypub.actorPathSegment"),
      objectCacheTTL: activitypub.objectCacheTTL === undefined
        ? 30 * 24 * 60 * 60
        : expectNumber(activitypub.objectCacheTTL, "activitypub.objectCacheTTL"),
    },
  };

  new URL(config.activitypub.baseUrl);

  return Object.freeze(config);
};

export const config = readConfig();
