import { Note, Document as APDocument, Emoji, Hashtag, Image, Mention, isActor, PUBLIC_COLLECTION } from "@fedify/vocab";
import type { Context } from "@fedify/fedify";
import { Temporal } from "@js-temporal/polyfill";
import { eq } from "drizzle-orm";
import { db, apEntity } from './db/index.ts';
import concrntApi from "./concrnt.ts";
import { config } from "./config.ts";
import { buildNoteParts, type NoteParts } from './render.ts';
import {
    SCHEMA_AP_NOTE,
    SCHEMA_MEDIA,
    SCHEMA_PLAINTEXT,
    SCHEMA_REPLY,
    SCHEMA_REROUTE,
} from './schemas.ts';

export * from './schemas.ts';
export * from './render.ts';

// concrntメッセージURIをAP object URLへ解決する。
// ap/note.json (リモート投稿の参照) ならその元noteのURL、
// ローカルメッセージなら作者のAPエンティティ経由でこのブリッジが配信するNoteのURLを返す。
export const resolveApObjectUrl = async (ctx: Context<unknown>, targetURI: string): Promise<string | null> => {
    // 存在しないtargetの繰り返しlookupを抑えるため、negative cacheを5分効かせる
    // (クラスデフォルトのnegativeCacheTTL=300はms比較のため実質無効)
    const target = await concrntApi.getDocument<any>(targetURI, undefined, { negativeTTL: 300_000 }).catch(() => null);
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

// concrntメッセージドキュメントをAP Noteへ変換する。
// Noteとして表現できないドキュメント(テキストなしreroute等)はnull。
export const buildNote = async (
    ctx: Context<unknown>,
    values: { identifier: string, id: string },
    document: any,
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
            ? await resolveApObjectUrl(ctx, document.value.targetURI)
            : null;

        const parts = buildNoteParts(body, document.value?.emojis);
        let content = parts.contentHtml;
        if (quoteTarget) {
            content += `<p>RE: <a href="${quoteTarget}">${quoteTarget}</a></p>`;
        }

        return new Note({
            id: noteId,
            attribution: actorUri,
            tos: [PUBLIC_COLLECTION],
            ccs: [followersUri],
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
            const target = await concrntApi.getDocument<any>(targetURI, undefined, { negativeTTL: 300_000 }).catch(() => null);
            if (target?.schema === SCHEMA_AP_NOTE) {
                replyTarget = URL.parse(target.value?.noteURL);
                replyToActorId = URL.parse(target.value?.actorURL);
            } else if (target != null) {
                const apUrl = await resolveApObjectUrl(ctx, targetURI);
                if (apUrl) replyTarget = new URL(apUrl);
            }
        }

        const mentions = await resolveMentions(ctx, parts, values.identifier);
        const tags: (Hashtag | Emoji | Mention)[] = [...buildTags(parts), ...mentions.tags];
        const ccs: URL[] = [followersUri, ...mentions.ccs];
        if (replyToActorId) {
            tags.push(new Mention({ href: replyToActorId, name: replyToActorId.href }));
            ccs.push(replyToActorId);
        }

        return new Note({
            id: noteId,
            attribution: actorUri,
            tos: [PUBLIC_COLLECTION],
            ccs,
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
        tos: [PUBLIC_COLLECTION],
        ccs: [followersUri, ...mentions.ccs],
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
