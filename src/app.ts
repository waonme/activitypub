import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { Hono, type Context as HonoContext } from "hono";
import { cors } from "hono/cors";
import { federation } from "@fedify/hono";
import { getLogger } from "@logtape/logtape";
import fedi, { INSTANCE_ACTOR, storeApNote } from "./federation.ts";
import { Note, PUBLIC_COLLECTION } from "@fedify/vocab";
import { db, apEntity } from "./db/index.ts"
import { eq } from "drizzle-orm";
import { config } from "./config.ts";
import * as followStore from "./followStore.ts";
import * as objectCache from "./objectCache.ts";
import { resendPendingFollows } from "./daemon.ts";

const logger = getLogger("activitypub");

// ゲートウェイから伝搬される認証情報
interface AuthInfo {
    ccid: string
}

const receiveAuthInfo = (c: HonoContext): AuthInfo | null => {
    const authInfoStr = c.req.header("cc-requester")
    logger.debug(`Received request with auth info: ${authInfoStr}`);
    if (!authInfoStr) return null;
    try {
        const authInfo = JSON.parse(authInfoStr) as Partial<AuthInfo>;
        return typeof authInfo.ccid === "string" ? { ccid: authInfo.ccid } : null;
    } catch {
        logger.warn("Received malformed cc-requester header");
        return null;
    }
}


const app = new Hono();

interface ApServerInfo {
    serviceAccountId: string
}

const pkg = JSON.parse(
    readFileSync(fileURLToPath(new URL("../package.json", import.meta.url)), "utf8")
) as { name: string; version: string };

// 本体は path.Join(service.Path, endpoint) で広告値を組み立てるため、
// services 設定では path を指定せず paths: [/ap, ...] + preservePath で登録すること
// (Fediverse向けURLが /ap 固定なので、このサービスはマウント位置を変えられない)。
const ccEndpoints: Record<string, string> = {
    "net.concrnt.activitypub.info":      "/ap/api/info",
    "net.concrnt.activitypub.setup":     "/ap/api/setup",      // POST {id}
    "net.concrnt.activitypub.settings":  "/ap/api/settings",   // GET=取得 (listenTimelines等のユーザー設定はcckvレコード側)
    "net.concrnt.activitypub.stats":     "/ap/api/stats",
    "net.concrnt.activitypub.followers": "/ap/api/followers",
    "net.concrnt.activitypub.following": "/ap/api/following",
    "net.concrnt.activitypub.resolve":   "/ap/api/resolve?uri={uri}",
    "net.concrnt.activitypub.import":    "/ap/api/import",     // POST {uri}
};

// 以下2つは定期ポーリングされるため、fedifyミドルウェアより前に登録して
// fedify·federation·http のアクセスログ(毎リクエストINFO)に乗せない。

// concrnt本体が services 登録済みサービスへ直接ポーリングするサービス広告
app.get("/cc-info", (c) =>
    c.json({ name: pkg.name, version: pkg.version, endpoints: ccEndpoints })
);

// livenessProbe用ヘルスチェック
app.get("/health", (c) => c.json({ status: "ok" }));

// 運用者向け内部API(prometheus流の/-/プレフィックス): pendingのフォローを一括/選択で再送する。
// /-/配下はコアのproxy(services paths: [/ap])にも公開リバプロにも
// ルーティングされない前提で、ブリッジのポートへの直アクセス専用。
// body: { ccid?: string, actorURIs?: string[], dryRun?: boolean }
//   ccid省略=全enabledエンティティ / actorURIs指定=そのアクターのみ
app.post("/-/resend-follows", async (c) => {
    const body = await c.req.json().catch(() => ({})) as { ccid?: string, actorURIs?: string[], dryRun?: boolean };
    const results = await resendPendingFollows(body);
    const count = (s: string) => results.filter((r) => r.status === s).length;
    return c.json({ sent: count('sent'), failed: count('failed'), skipped: count('skipped'), results });
});

app.use(federation(fedi, () => undefined));
app.use(cors({
    origin: "*",
    allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowHeaders: ["Content-Type", "Authorization"],
}));

app.get("/ap", (c) => c.text("Hello, Fedify!"));

app.get("/ap/api/info", async (c) => {

    const serverInfo: ApServerInfo = {
        serviceAccountId: config.concrnt.ccid,
    };

    return c.json(serverInfo);
});


app.post("/ap/api/setup", async (c) => {

    const authInfo = receiveAuthInfo(c)
    if (!authInfo) {
        return c.json({ error: "Missing authentication information" }, 400);
    }

    const { id } = await c.req.json();
    if (!id) {
        return c.json({ error: "Missing 'id' in request body" }, 400);
    }
    if (id.toLowerCase() === INSTANCE_ACTOR) {
        return c.json({ error: `'${INSTANCE_ACTOR}' is a reserved id` }, 400);
    }

    const ccid = authInfo.ccid

    logger.info(`Setting up ActivityPub entity for ccid: ${ccid}`);

    await db.insert(apEntity).values({
        id: id.toLowerCase(),
        ccid: ccid,
        enabled: true,
    })

    return c.json({
        id: id,
        ccid: ccid,
        enabled: true,
    })

});

app.get("/ap/api/settings", async (c) => {

    const authInfo = receiveAuthInfo(c)
    if (!authInfo) {
        return c.json({ error: "Missing authentication information" }, 400);
    }

    const id = authInfo.ccid

    const entity = await db.select().from(apEntity).where(eq(apEntity.ccid, id)).limit(1).then(res => res[0]);
    if (!entity) {
        return c.json({ error: "No ActivityPub entity found for this user" }, 404);
    }

    return c.json({
        ccid: entity.ccid,
        id: entity.id,
        enabled: entity.enabled,
    });
});


app.get("/ap/api/stats", async (c) => {

    const authInfo = receiveAuthInfo(c)
    if (!authInfo) {
        return c.json({ error: "Missing authentication information" }, 400);
    }

    const id = authInfo.ccid

    const entity = await db.select().from(apEntity).where(eq(apEntity.ccid, id)).limit(1).then(res => res[0]);
    if (!entity) {
        return c.json({ error: "No ActivityPub entity found for this user" }, 404);
    }

    const follows = followStore.getFollowing(entity.ccid)
        .filter(f => f.status !== 'rejected')
        .map(f => f.actorURI);

    const followers = followStore.getFollowers(entity.ccid)
        .map(f => f.actorURI);

    return c.json({ follows, followers });
});

app.get("/ap/api/followers", async (c) => {

    const authInfo = receiveAuthInfo(c)
    if (!authInfo) {
        return c.json({ error: "Missing authentication information" }, 400);
    }

    const id = authInfo.ccid

    const entity = await db.select().from(apEntity).where(eq(apEntity.ccid, id)).limit(1).then(res => res[0]);
    if (!entity) {
        return c.json({ error: "No ActivityPub entity found for this user" }, 404);
    }

    const followers = followStore.getFollowers(entity.ccid)
        .map(f => f.actorURI);

    return c.json(followers);
});

app.get("/ap/api/following", async (c) => {

    const authInfo = receiveAuthInfo(c)
    if (!authInfo) {
        return c.json({ error: "Missing authentication information" }, 400);
    }

    const id = authInfo.ccid

    const entity = await db.select().from(apEntity).where(eq(apEntity.ccid, id)).limit(1).then(res => res[0]);
    if (!entity) {
        return c.json({ error: "No ActivityPub entity found for this user" }, 404);
    }

    // フォローはユーザー署名のcckvレコード(follows/)がsource of truth。
    // フォロー・アンフォロー操作はクライアントがレコードをcommit/deleteすることで行う。
    // レスポンス: { actorURI: string, status: 'accepted' | 'pending' }[]
    const following = followStore.getFollowing(entity.ccid)
        .filter(f => f.status !== 'rejected')
        .map(f => ({ actorURI: f.actorURI, status: f.status as 'accepted' | 'pending' }));

    return c.json(following);
});


app.get("/ap/api/resolve", async (c) => {
    const ctx = fedi.createContext(c.req.raw, undefined);
    let uri = c.req.query("uri")?.trim();
    if (typeof uri !== "string") {
        return c.json({ error: "Missing 'uri' query parameter" }, 400);
    }
    uri = decodeURIComponent(uri);
    uri = uri.replace(/^activity:\/\//, "https://");

    const authInfo = receiveAuthInfo(c);
    const entity = authInfo
        ? await db.select().from(apEntity).where(eq(apEntity.ccid, authInfo.ccid)).limit(1).then(res => res[0])
        : undefined;

    // キャッシュヒットかつ閲覧可(読み出し時評価)なら即返す。許可がない場合は
    // リモートfetchへフォールスルーして可否をリモートに委ねる
    const cached = await objectCache.getObject(uri);
    if (cached && objectCache.isVisibleTo(cached, authInfo
        ? { ccid: authInfo.ccid, actorUri: entity ? ctx.getActorUri(entity.id).href : undefined }
        : null)) {
        return c.json(cached.json);
    }

    // authorized fetch実装向けに、リクエストユーザー(AP未セットアップならインスタンス
    // アクター)の鍵で署名して解決する
    const documentLoader = await ctx.getDocumentLoader({ identifier: entity?.id ?? INSTANCE_ACTOR });

    return await ctx.lookupObject(uri, { crossOrigin: 'trust', documentLoader }).then(async (obj) => {
        if (obj) {
            const jsonLd = await obj.toJsonLd() as Record<string, unknown>;
            // fedifyのvocabは_misskey_content等の未知プロパティをJSON-LD変換で落とすため、生JSONから拾い直す
            try {
                const raw = await documentLoader(obj.id?.href ?? uri);
                const rawDoc = raw.document as Record<string, unknown>;
                for (const key of Object.keys(rawDoc)) {
                    if (key.startsWith("_misskey_")) jsonLd[key] = rawDoc[key];
                }
            } catch (e) {
                logger.debug(`failed to fetch raw document for ${uri}: ${e}`);
            }
            // publicオブジェクトはresolve結果もキャッシュしてリモートfetchを減らす
            // (非publicのresolve結果はリモートの認可が要求者個人に紐づくため保存しない)
            const addressed = [...obj.toIds, ...obj.ccIds].map(u => u.href);
            if (addressed.includes(PUBLIC_COLLECTION.href)) {
                const canonical = obj.id?.href ?? uri;
                await objectCache.putObject(canonical, {
                    json: jsonLd,
                    actorUri: obj.attributionId?.href ?? '',
                    addressed,
                    receivedAt: new Date().toISOString(),
                });
                if (canonical !== uri) await objectCache.putAlias(uri, canonical);
            }
            return c.json(jsonLd);
        } else {
            logger.info(`Object not found for URI: ${uri}`);
            return c.json({ error: "Object not found" }, 404);
        }
    })
});

// リモートnoteをオンデマンドでconcrntレコード(ap/note.json)として実体化する。
// プッシュ受信(Create/Announce)を経ていないnoteをクライアントが詳細ビューで扱えるようにするための入口。
app.post("/ap/api/import", async (c) => {
    const authInfo = receiveAuthInfo(c);
    if (!authInfo) {
        return c.json({ error: "Missing authentication information" }, 400);
    }

    let uri: unknown;
    try {
        uri = (await c.req.json()).uri;
    } catch {
        return c.json({ error: "Invalid JSON body" }, 400);
    }
    if (typeof uri !== "string" || uri.trim() === "") {
        return c.json({ error: "Missing 'uri' in request body" }, 400);
    }
    uri = decodeURIComponent(uri.trim()).replace(/^activity:\/\//, "https://");

    // AP未セットアップのユーザーでも実体化は許可する(署名主体だけ使い分ける)
    const entity = await db.select().from(apEntity)
        .where(eq(apEntity.ccid, authInfo.ccid)).limit(1).then(res => res[0]);

    const ctx = fedi.createContext(c.req.raw, undefined);
    const documentLoader = await ctx.getDocumentLoader({ identifier: entity?.id ?? INSTANCE_ACTOR });

    let obj;
    try {
        obj = await ctx.lookupObject(uri as string, { crossOrigin: 'trust', documentLoader });
    } catch (e) {
        logger.info(`import: failed to resolve ${uri}: ${e}`);
        return c.json({ error: "Failed to fetch remote object" }, 502);
    }
    if (!obj) {
        return c.json({ error: "Object not found" }, 404);
    }
    if (!(obj instanceof Note) || !obj.id) {
        return c.json({ error: "Object is not a Note" }, 422);
    }
    const actorURL = obj.attributionId?.href;
    if (!actorURL) {
        return c.json({ error: "Note has no attributedTo" }, 422);
    }

    // キーは正準ID(obj.id)から導出する。決定的キーへの再commitなので冪等。
    // 配送はしない(詳細ビューから参照できれば十分。Announce内側noteと同じ扱い)。
    // publishedがbackdate window(7日)より古い投稿を取り込めるようimport経路で実体化する
    let key;
    try {
        key = await storeApNote(
            obj.id.href,
            actorURL,
            obj.published ? new Date(obj.published.toString()) : new Date(),
            [],
            { viaImport: true },
        );
    } catch (e) {
        logger.warn(`import: failed to store ${obj.id.href}: ${e}`);
        return c.json({ error: `Failed to store note: ${e instanceof Error ? e.message : e}` }, 500);
    }

    logger.info(`import: stored ${obj.id.href} as ${key} (requested by ${authInfo.ccid})`);
    return c.json({ key });
});

export default app;
