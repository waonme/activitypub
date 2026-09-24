import { Note, Document as APDocument, Announce, Create, Emoji, Hashtag, Image, Mention, isActor, PUBLIC_COLLECTION } from "@fedify/vocab";
import type { Context } from "@fedify/fedify";
import { Temporal } from "@js-temporal/polyfill";
import { eq } from "drizzle-orm";
import { db, apEntity } from './db/index.ts';
import { NotFoundError, PermissionError } from "@concrnt/client";
import concrntApi, { resolveAsProxy } from "./concrnt.ts";
import { config } from "./config.ts";
import { buildNoteParts, type NoteParts } from './render.ts';
import {
    SCHEMA_AP_NOTE,
    SCHEMA_MEDIA,
    SCHEMA_PLAINTEXT,
    SCHEMA_REPLY,
    SCHEMA_REROUTE,
    isPlainReroute,
} from './schemas.ts';

export * from './schemas.ts';
export * from './render.ts';

export interface ConversionOptions {
    // Outbox pages can be retried at the same cursor. Resolve references with
    // current anonymous authorization and propagate temporary/unknown failures
    // rather than consuming a partially converted page. Existing push callers
    // intentionally retain best-effort reference resolution by omitting this.
    targetResolution?: 'strict-public';
}

const getConversionTarget = async (targetURI: string, options: ConversionOptions) => {
    const strict = options.targetResolution === 'strict-public';
    try {
        return await concrntApi.getDocument<any>(targetURI, undefined,
            strict ? { cache: 'no-cache', auth: 'no-auth' } : { negativeTTL: 300_000 });
    } catch (error) {
        // SDK 2.0.5 maps HTTP 404/403 to these two types. Offline, timeout,
        // network, 5xx and unknown errors must remain retryable in an Outbox.
        if (strict && !(error instanceof NotFoundError) && !(error instanceof PermissionError)) throw error;
        return null;
    }
};

// concrntメッセージURIをAP object URLへ解決する。
// ap/note.json (リモート投稿の参照) ならその元noteのURL、
// ローカルメッセージなら作者のAPエンティティ経由でこのブリッジが配信するNoteのURLを返す。
export const resolveApObjectUrl = async (ctx: Context<unknown>, targetURI: string, options: ConversionOptions = {}): Promise<string | null> => {
    const target = await getConversionTarget(targetURI, options);
    if (target == null) return null;

    if (target.schema === SCHEMA_AP_NOTE) {
        return target.value?.noteURL ?? null;
    }

    const owner = URL.parse(targetURI)?.host;
    if (!owner) return null;

    const authorEntity = await db.select().from(apEntity)
        .where(eq(apEntity.ccid, owner)).limit(1).then(res => res[0]);
    if (!authorEntity) return null;

    return ctx.getObjectUri(Note, { identifier: authorEntity.id, id: targetURI }).href;
}

const buildTags = (parts: NoteParts): (Hashtag | Emoji)[] => {
    const tags: (Hashtag | Emoji)[] = [];
    for (const tag of parts.hashtags) {
        tags.push(new Hashtag({
            name: `#${tag}`,
            href: new URL(`${config.activitypub.baseUrl}/tags/${encodeURIComponent(tag)}`),
        }));
    }
    for (const emoji of parts.emojis) {
        const imageUrl = URL.parse(emoji.imageUrl);
        if (!imageUrl) continue;
        tags.push(new Emoji({
            id: imageUrl,
            name: `:${emoji.shortcode}:`,
            icon: new Image({ url: imageUrl }),
        }));
    }
    return tags;
}

const buildAttachments = (
    parts: NoteParts,
    medias: { mediaURL: string, mediaType?: string, altText?: string, flag?: string }[],
): APDocument[] => {
    const attachments: APDocument[] = [];
    for (const media of medias) {
        const url = URL.parse(media.mediaURL);
        if (!url) continue;
        attachments.push(new APDocument({
            url,
            mediaType: media.mediaType,
            name: media.altText,
            sensitive: media.flag === 'sensitive',
        }));
    }
    for (const image of parts.inlineImages) {
        const url = URL.parse(image.url);
        if (!url) continue;
        attachments.push(new APDocument({ url, name: image.alt || null }));
    }
    return attachments;
}

// 本文中の @user@host をwebfingerで解決してMentionタグ+cc対象にする。
// authorized fetch実装向けに、投稿者(identifier)の鍵で署名して解決する。
const resolveMentions = async (ctx: Context<unknown>, parts: NoteParts, identifier: string): Promise<{ tags: Mention[], ccs: URL[] }> => {
    const tags: Mention[] = [];
    const ccs: URL[] = [];
    const documentLoader = await ctx.getDocumentLoader({ identifier });
    for (const handle of parts.mentions) {
        const actor = await ctx.lookupObject(handle, { documentLoader }).catch(() => null);
        if (actor && isActor(actor) && actor.id) {
            tags.push(new Mention({ href: actor.id, name: handle }));
            ccs.push(actor.id);
        }
    }
    return { tags, ccs };
}

// 送信側の公開範囲。匿名で読める → public、匿名では読めないがapProxy(サービス
// アカウント)には読める → フォロワー限定、どちらでもない → 連合しない(null)
export type Visibility = 'public' | 'followers';

// embeddedIsPublic: Redisイベント同梱ドキュメントのisPublic(サーバーの匿名可読判定)。
// falseはサーバー自身の確定判定なので匿名fetchで再確認しない。undefined(pull経路・
// 旧サーバー)は匿名fetchで判定する。policy判定なのでキャッシュ(正/負とも)は見ない。
// 拒否と不在はどちらも404(debug時は403)で区別できないが、いずれも「読めない」でよい
export const resolveVisibility = async (uri: string, embeddedIsPublic?: boolean): Promise<Visibility | null> => {
    if (embeddedIsPublic === true) return 'public';
    if (embeddedIsPublic === undefined) {
        try {
            await concrntApi.getDocument(uri, undefined, { cache: 'no-cache' });
            return 'public';
        } catch (e) {
            if (!(e instanceof NotFoundError) && !(e instanceof PermissionError)) throw e;
        }
    }
    try {
        await resolveAsProxy(uri);
        return 'followers';
    } catch (e) {
        if (e instanceof NotFoundError || e instanceof PermissionError) return null;
        throw e;
    }
};

// 公開: to=Public, cc=followers+宛先 / フォロワー限定: to=followers, cc=宛先のみ
export const audience = (visibility: Visibility, followersUri: URL, extra: URL[]): { tos: URL[], ccs: URL[] } =>
    visibility === 'public'
        ? { tos: [PUBLIC_COLLECTION], ccs: [followersUri, ...extra] }
        : { tos: [followersUri], ccs: extra };

// concrntメッセージドキュメントをAP Noteへ変換する。
// Noteとして表現できないドキュメント(テキストなしreroute等)はnull。
export const buildNote = async (
    ctx: Context<unknown>,
    values: { identifier: string, id: string },
    document: any,
    visibility: Visibility,
    options: ConversionOptions = {},
): Promise<Note | null> => {
    const noteId = ctx.getObjectUri(Note, values);
    const actorUri = ctx.getActorUri(values.identifier);
    const followersUri = ctx.getFollowersUri(values.identifier);
    // fedifyは型をTS標準lib(esnext.temporal)のTemporalで宣言するが実行時は
    // @js-temporal/polyfillを使う。polyfillの型とは構造非互換(sign: numberと-1|0|1等)
    // なので、コンストラクタ引数から期待型を導出してキャストする。
    type FedifyInstant = NonNullable<NonNullable<ConstructorParameters<typeof Note>[0]>["published"]>;
    const published = Temporal.Instant.from(new Date(document.createdAt).toISOString()) as unknown as FedifyInstant;

    if (document.schema === SCHEMA_REROUTE) {
        const body = document.value?.body?.trim();
        // テキストなしrerouteはAnnounceアクティビティとして連合するためNoteを持たない
        if (!body) return null;

        // 引用投稿: quoteUrl (FEP-044f) + 未対応サーバー向けに本文末尾へ参照リンク
        const quoteTarget = document.value?.targetURI
            ? await resolveApObjectUrl(ctx, document.value.targetURI, options)
            : null;

        const parts = buildNoteParts(body, document.value?.emojis);
        let content = parts.contentHtml;
        if (quoteTarget) {
            content += `<p>RE: <a href="${quoteTarget}">${quoteTarget}</a></p>`;
        }

        return new Note({
            id: noteId,
            attribution: actorUri,
            ...audience(visibility, followersUri, []),
            content,
            summary: parts.summary,
            sensitive: parts.sensitive,
            mediaType: "text/html",
            published,
            url: noteId,
            tags: buildTags(parts),
            attachments: buildAttachments(parts, []),
            quoteUrl: quoteTarget ? new URL(quoteTarget) : null,
        });
    }

    if (document.schema === SCHEMA_REPLY) {
        const parts = buildNoteParts(document.value?.body ?? '', document.value?.emojis);

        let replyTarget: URL | null = null;
        let replyToActorId: URL | null = null;

        const targetURI = document.value?.targetURI;
        if (targetURI) {
            const target = await getConversionTarget(targetURI, options);
            if (target?.schema === SCHEMA_AP_NOTE) {
                replyTarget = URL.parse(target.value?.noteURL);
                replyToActorId = URL.parse(target.value?.actorURL);
            } else if (target != null) {
                const apUrl = await resolveApObjectUrl(ctx, targetURI, options);
                if (apUrl) replyTarget = new URL(apUrl);
            }
        }

        const mentions = await resolveMentions(ctx, parts, values.identifier);
        const tags: (Hashtag | Emoji | Mention)[] = [...buildTags(parts), ...mentions.tags];
        const extra: URL[] = [...mentions.ccs];
        if (replyToActorId) {
            tags.push(new Mention({ href: replyToActorId, name: replyToActorId.href }));
            extra.push(replyToActorId);
        }

        return new Note({
            id: noteId,
            attribution: actorUri,
            ...audience(visibility, followersUri, extra),
            content: parts.contentHtml,
            summary: parts.summary,
            sensitive: parts.sensitive,
            mediaType: "text/html",
            published,
            url: noteId,
            replyTarget,
            tags,
            attachments: buildAttachments(parts, []),
        });
    }

    // markdown / gfm / mfm / plaintext / media / その他のテキスト系メッセージ
    const plaintext = document.schema === SCHEMA_PLAINTEXT;
    const parts = buildNoteParts(document.value?.body ?? '', document.value?.emojis, { plaintext });

    const medias = document.schema === SCHEMA_MEDIA ? (document.value?.medias ?? []) : [];
    const mentions = await resolveMentions(ctx, parts, values.identifier);

    return new Note({
        id: noteId,
        attribution: actorUri,
        ...audience(visibility, followersUri, mentions.ccs),
        content: parts.contentHtml,
        summary: parts.summary,
        sensitive: parts.sensitive,
        mediaType: "text/html",
        published,
        url: noteId,
        tags: [...buildTags(parts), ...mentions.tags],
        attachments: buildAttachments(parts, medias),
    });
}

// concrntメッセージドキュメントをAPアクティビティへ変換する。
// テキストなしreroute → Announce (boost)、それ以外 → Create(Note)。変換不能ならnull。
// idは決定的(reroute: /ap/announces/<cckv>、それ以外: <noteId>#activity)なので、
// daemonのリアルタイム送出とoutboxの列挙が同一アクティビティを生成する。
export const buildActivity = async (
    ctx: Context<unknown>,
    values: { identifier: string, id: string },
    document: any,
    visibility: Visibility,
    options: ConversionOptions = {},
): Promise<Announce | Create | null> => {
    if (isPlainReroute(document)) {
        const targetURI: string | undefined = document.value?.targetURI;
        if (!targetURI) return null;

        const objectRef = await resolveApObjectUrl(ctx, targetURI, options);
        if (!objectRef) return null;

        return new Announce({
            id: new URL(`${config.activitypub.baseUrl}/ap/announces/${encodeURIComponent(values.id)}`),
            actor: ctx.getActorUri(values.identifier),
            object: new URL(objectRef),
            ...audience(visibility, ctx.getFollowersUri(values.identifier), []),
        });
    }

    const note = await buildNote(ctx, values, document, visibility, options);
    if (note == null) return null;

    return new Create({
        id: new URL("#activity", note.id ?? undefined),
        object: note,
        actors: note.attributionIds,
        tos: note.toIds,
        ccs: note.ccIds,
    });
}
