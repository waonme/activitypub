// ユーザーごとのブリッジ設定(listenTimelines)のメモリストア。
// source of truth は concrnt 上の cckv レコード:
//   - settings: cckv://<userCcid>/activitypub.concrnt.world/settings (ユーザー署名)
// キーが各ユーザーの空間に散っているため prefix 一括クエリはできず、
// entityごとに getDocument でロードし、以後は Redis イベント(daemon.ts)で更新する。
// レコード無し = 空配列(daemon側でhome-timelineへフォールバック)。

import { getLogger } from "@logtape/logtape";
import { NotFoundError } from "@concrnt/client";

import concrntApi from "./concrnt.ts";
import { SCHEMA_AP_SETTINGS, settingsKey, type ApSettingsValue } from "./schemas.ts";

const logger = getLogger("activitypub");

const listenTimelinesByCcid = new Map<string, string[]>();
const loadedCcids = new Set<string>();

const sanitize = (document: { author: string, schema: string, value?: ApSettingsValue }, ccid: string): string[] => {
    // 本人署名かつ正しいスキーマのレコードのみ採用する
    if (document.author !== ccid || document.schema !== SCHEMA_AP_SETTINGS) return [];
    const timelines = document.value?.listenTimelines;
    if (!Array.isArray(timelines)) return [];
    return timelines.filter((t): t is string => typeof t === 'string' && t.length > 0);
}

export const getListenTimelines = (ccid: string): string[] =>
    listenTimelinesByCcid.get(ccid) ?? [];

// entityのsettingsレコードを未ロードならロードする(新規entityの遅延ロード対応)
export const ensureEntitySettingsLoaded = async (ccid: string) => {
    if (loadedCcids.has(ccid)) return;
    loadedCcids.add(ccid);

    try {
        const document = await concrntApi.getDocument<ApSettingsValue>(settingsKey(ccid));
        const timelines = sanitize(document, ccid);
        listenTimelinesByCcid.set(ccid, timelines);
        logger.info(`settingsStore: loaded ${timelines.length} listen timelines for ${ccid}`);
    } catch (error) {
        if (error instanceof NotFoundError) {
            listenTimelinesByCcid.set(ccid, []);
            return;
        }
        // 失敗時は次の機会(updateEntitiesの60秒周期)に再試行できるようにする
        loadedCcids.delete(ccid);
        throw error;
    }
}

// settingsレコードのRedisイベントを反映する(即時反映)
export const applyEvent = async (ccid: string, msg: { type: string, documents?: Record<string, { document: string }> }) => {
    if (msg.type === "created") {
        const key = settingsKey(ccid);
        // イベントに同梱された署名済みドキュメントを優先し、なければフェッチする
        const eventSD = msg.documents?.[key];
        const document = eventSD
            ? JSON.parse(eventSD.document)
            : await concrntApi.getDocument<ApSettingsValue>(key).catch(() => null);
        if (document == null) return;
        const timelines = sanitize(document, ccid);
        listenTimelinesByCcid.set(ccid, timelines);
        loadedCcids.add(ccid);
        logger.info(`settingsStore: updated listen timelines for ${ccid}: ${JSON.stringify(timelines)}`);
    } else if (msg.type === "deleted") {
        listenTimelinesByCcid.set(ccid, []);
        loadedCcids.add(ccid);
        logger.info(`settingsStore: cleared listen timelines for ${ccid}`);
    }
}
