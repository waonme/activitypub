// concrntスキーマURLと、それに関する純粋なロジック。
// このモジュールは設定やDBに依存しない(ユニットテスト対象)。

import { CDID } from '@concrnt/client';

export const SCHEMA_MARKDOWN = "https://schema.concrnt.world/m/markdown.json";
export const SCHEMA_GFM = "https://schema.concrnt.world/m/gfm.json";
export const SCHEMA_MFM = "https://schema.concrnt.world/m/mfm.json";
export const SCHEMA_PLAINTEXT = "https://schema.concrnt.world/m/plaintext.json";
export const SCHEMA_MEDIA = "https://schema.concrnt.world/m/media.json";
export const SCHEMA_REPLY = "https://schema.concrnt.world/m/reply.json";
export const SCHEMA_REROUTE = "https://schema.concrnt.world/m/reroute.json";
export const SCHEMA_AP_NOTE = "https://schema.concrnt.world/ap/note.json";
export const SCHEMA_REFERENCE = "https://schema.concrnt.net/reference.json";
export const SCHEMA_DELETE = "https://schema.concrnt.net/delete.json";
export const SCHEMA_LIKE = "https://schema.concrnt.world/a/like.json";
export const SCHEMA_REACTION = "https://schema.concrnt.world/a/reaction.json";
export const SCHEMA_MENTION = "https://schema.concrnt.world/a/mention.json";
export const SCHEMA_REPLY_ASSOCIATION = "https://schema.concrnt.world/a/reply.json";
export const SCHEMA_AP_FOLLOW = "https://schema.concrnt.world/ap/follow.json";
export const SCHEMA_AP_FOLLOWER = "https://schema.concrnt.world/ap/follower.json";
export const SCHEMA_AP_ACCEPT_STATE = "https://schema.concrnt.world/ap/accept-state.json";
export const SCHEMA_AP_SETTINGS = "https://schema.concrnt.world/ap/settings.json";

export const AP_NAMESPACE = "activitypub.concrnt.world";

const hashOf = (s: string) => CDID.newFromStringX(s).toString();

// フォローの意思。ユーザー本人が署名して自分のcckv空間に書く(source of truth)。
// actorURIは解決済みの正規APアクターidであることがクライアント側の契約。
export const followKey = (userCcid: string, actorURI: string) =>
    `cckv://${userCcid}/${AP_NAMESPACE}/follows/${hashOf(actorURI)}`;

// AP側からのフォロワー。リモートアクターはconcrnt鍵を持たないため、
// ブリッジのサービスアカウントが自分のcckv空間に書く。
export const followerKey = (svcCcid: string, entityCcid: string, actorURI: string) =>
    `cckv://${svcCcid}/${AP_NAMESPACE}/followers/${hashOf(`${entityCcid}->${actorURI}`)}`;

// フォローのAccept/Reject状態。レコード無し=pending。
export const acceptStateKey = (svcCcid: string, entityCcid: string, actorURI: string) =>
    `cckv://${svcCcid}/${AP_NAMESPACE}/accept-states/${hashOf(`${entityCcid}->${actorURI}`)}`;

// ブリッジのユーザー設定。ユーザー本人が署名して自分のcckv空間に書く。
export const settingsKey = (userCcid: string) =>
    `cckv://${userCcid}/${AP_NAMESPACE}/settings`;

export type AcceptStatus = 'accepted' | 'rejected';

export interface ApFollowValue { actorURI: string }
export interface ApSettingsValue { listenTimelines: string[] }
export interface ApFollowerValue { ccid: string, actorURI: string, inbox: string, sharedInbox?: string }
export interface ApAcceptStateValue { ccid: string, actorURI: string, status: AcceptStatus }

// テキストなしreroute = boost (Announce)。テキストあり = 引用投稿として連合する。
export const isPlainReroute = (document: { schema: string, value?: { body?: string } }): boolean =>
    document.schema === SCHEMA_REROUTE && !document.value?.body?.trim();

// ":shortcode:" 形式の絵文字リアクションcontentからショートコードを取り出す。
// 該当しなければnull(プレーンなLike扱い)。
export const parseEmojiShortcode = (content?: string | null): string | null => {
    const trimmed = content?.trim();
    if (!trimmed) return null;
    const match = trimmed.match(/^:([^:\s]+):$/);
    return match ? match[1] : null;
}
