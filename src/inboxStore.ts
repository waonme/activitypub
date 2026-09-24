// ユーザーごとのinboxタイムライン(cckv://<userCcid>/activitypub.concrnt.world/inbox)の
// 有無のメモリストア。inboxはユーザー本人がアプリ側で作成するため、フォロー関係だけ
// 移行済みで未作成のユーザーが存在しうる。そこへ配送するとpolicy denyで
// dead-letterに落ちるだけなので、配送前にここで存在を確認して宛先から外す。
// settingsStoreと同じく entityごとに getDocument でロードし、以後は
// Redisイベント(daemon.ts)で更新する。

import { getLogger } from "@logtape/logtape";
import { NotFoundError } from "@concrnt/client";

import concrntApi from "./concrnt.ts";
import { inboxTimelineKey } from "./schemas.ts";

const logger = getLogger("activitypub");

const inboxExistsByCcid = new Map<string, boolean>();
const loadedCcids = new Set<string>();
const inFlightLoads = new Map<string, Promise<void>>();

export const hasInbox = (ccid: string): boolean =>
    inboxExistsByCcid.get(ccid) ?? false;

// entityのinboxレコードの有無を未ロードならロードする(新規entityの遅延ロード対応)
export const ensureEntityInboxLoaded = async (ccid: string) => {
    if (loadedCcids.has(ccid)) return;
    const current = inFlightLoads.get(ccid);
    if (current) return current;

    const load = Promise.resolve().then(async () => {
        try {
            await concrntApi.getDocument<unknown>(inboxTimelineKey(ccid));
            // ロード中に届いたcreated/deletedイベントの方が新しい。
            if (!loadedCcids.has(ccid)) inboxExistsByCcid.set(ccid, true);
        } catch (error) {
            if (loadedCcids.has(ccid)) return;
            if (!(error instanceof NotFoundError)) throw error;
            inboxExistsByCcid.set(ccid, false);
            logger.info(`inboxStore: ${ccid} has no inbox timeline yet; inbound notes will be skipped until it is created`);
        }
        loadedCcids.add(ccid);
    }).finally(() => {
        // 一時的失敗も次回に再試行できる。待機中の呼出しは同じ結果を受け取る。
        inFlightLoads.delete(ccid);
    });
    inFlightLoads.set(ccid, load);
    return load;
}

// 宛先候補のうちinboxを持つccidだけを返す(未ロードのものはその場でロードする)
export const filterCcidsWithInbox = async (ccids: string[]): Promise<string[]> => {
    const result: string[] = [];
    for (const ccid of ccids) {
        await ensureEntityInboxLoaded(ccid).catch((error) => {
            logger.error(`inboxStore: failed to load inbox state for ${ccid}: ${error}`);
        });
        if (hasInbox(ccid)) {
            result.push(ccid);
        } else {
            logger.debug(`inboxStore: skipping delivery to ${ccid}: no inbox timeline`);
        }
    }
    return result;
}

// inboxレコードのRedisイベントを反映する(即時反映)。
// inboxタイムラインのchannelには配送済みnoteの参照作成/削除(distributes経由)の
// イベントも同じchannelで流れてくるため、uriがinboxレコード自身のキーのものだけを
// レコードの作成/削除として扱う(子レコードのdeletedで配送を止めない)
export const applyEvent = (ccid: string, msg: { type: string, uri?: string }) => {
    if (msg.uri !== inboxTimelineKey(ccid)) return;
    if (msg.type === "created") {
        inboxExistsByCcid.set(ccid, true);
        loadedCcids.add(ccid);
        logger.info(`inboxStore: inbox timeline created for ${ccid}`);
    } else if (msg.type === "deleted") {
        inboxExistsByCcid.set(ccid, false);
        loadedCcids.add(ccid);
        logger.info(`inboxStore: inbox timeline deleted for ${ccid}`);
    }
}
