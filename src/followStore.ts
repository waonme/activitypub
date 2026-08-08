// フォロー・フォロワー関係のメモリストア。
// source of truth は concrnt 上の cckv レコード:
//   - follow:       cckv://<userCcid>/activitypub.concrnt.world/follows/<hash>       (ユーザー署名)
//   - follower:     cckv://<svcCcid>/activitypub.concrnt.world/followers/<hash>      (サービスAC)
//   - accept-state: cckv://<svcCcid>/activitypub.concrnt.world/accept-states/<hash>  (サービスAC)
// 起動時に query API で全件ロードし、以後は Redis イベント(daemon.ts)で更新する。
// レコード書き込み時は commit と同時に本ストアも同期更新し、自己エコーイベントは
// 冪等な set/delete として無害に収束させる。
//
// 読取制限ポリシー付きの follow レコードは query の読取権限フィルタで落ちるため
// 対象外(公開レコード前提)。

import { getLogger } from "@logtape/logtape";

import concrntApi from "./concrnt.ts";
import { config } from "./config.ts";
import { AP_NAMESPACE, SCHEMA_AP_FOLLOW, SCHEMA_AP_FOLLOWER, SCHEMA_AP_ACCEPT_STATE, type AcceptStatus, type ApFollowValue, type ApFollowerValue, type ApAcceptStateValue } from "./schemas.ts";

const logger = getLogger("activitypub");

export interface FollowingEntry { ccid: string, key: string, actorURI: string }
export interface FollowerEntry { ccid: string, key: string, actorURI: string, inbox: string, sharedInbox?: string }
export type FollowStatus = AcceptStatus | 'pending';

interface KeyRef { kind: 'follow' | 'follower' | 'accept-state', ccid: string, actorURI: string }

// ccid → actorURI → entry
const followingByCcid = new Map<string, Map<string, FollowingEntry>>();
const followersByCcid = new Map<string, Map<string, FollowerEntry>>();
// accept-state レコードが無ければ pending(読み出し時に合成するのでロード順に依存しない)
const acceptStates = new Map<string, Map<string, AcceptStatus>>();
// deleted イベントは値を持たないため、キーからの逆引きで解決する
const byKey = new Map<string, KeyRef>();
// getFollowerDistribution 用: actorURI → フォローしているローカルccid集合
const followingReverse = new Map<string, Set<string>>();
// follows プレフィックスをロード済みの entity ccid
const loadedFollowCcids = new Set<string>();

const getOrCreate = <V>(map: Map<string, Map<string, V>>, ccid: string): Map<string, V> => {
    let inner = map.get(ccid);
    if (!inner) {
        inner = new Map();
        map.set(ccid, inner);
    }
    return inner;
}

export const setFollowing = (entry: FollowingEntry) => {
    getOrCreate(followingByCcid, entry.ccid).set(entry.actorURI, entry);
    byKey.set(entry.key, { kind: 'follow', ccid: entry.ccid, actorURI: entry.actorURI });
    let ccids = followingReverse.get(entry.actorURI);
    if (!ccids) {
        ccids = new Set();
        followingReverse.set(entry.actorURI, ccids);
    }
    ccids.add(entry.ccid);
}

export const removeFollowingByKey = (key: string) => {
    const ref = byKey.get(key);
    if (!ref || ref.kind !== 'follow') return;
    byKey.delete(key);
    followingByCcid.get(ref.ccid)?.delete(ref.actorURI);
    const ccids = followingReverse.get(ref.actorURI);
    ccids?.delete(ref.ccid);
    if (ccids?.size === 0) followingReverse.delete(ref.actorURI);
}

export const setFollower = (entry: FollowerEntry) => {
    getOrCreate(followersByCcid, entry.ccid).set(entry.actorURI, entry);
    byKey.set(entry.key, { kind: 'follower', ccid: entry.ccid, actorURI: entry.actorURI });
}

export const removeFollowerByKey = (key: string) => {
    const ref = byKey.get(key);
    if (!ref || ref.kind !== 'follower') return;
    byKey.delete(key);
    followersByCcid.get(ref.ccid)?.delete(ref.actorURI);
}

export const setAcceptState = (key: string, ccid: string, actorURI: string, status: AcceptStatus) => {
    getOrCreate(acceptStates, ccid).set(actorURI, status);
    byKey.set(key, { kind: 'accept-state', ccid, actorURI });
}

export const removeAcceptStateByKey = (key: string) => {
    const ref = byKey.get(key);
    if (!ref || ref.kind !== 'accept-state') return;
    byKey.delete(key);
    acceptStates.get(ref.ccid)?.delete(ref.actorURI);
}

export const getByKey = (key: string): KeyRef | undefined => byKey.get(key);

export const getAcceptState = (ccid: string, actorURI: string): FollowStatus =>
    acceptStates.get(ccid)?.get(actorURI) ?? 'pending';

export const getFollowing = (ccid: string): Array<FollowingEntry & { status: FollowStatus }> =>
    [...(followingByCcid.get(ccid)?.values() ?? [])]
        .map(e => ({ ...e, status: getAcceptState(ccid, e.actorURI) }));

export const getFollowers = (ccid: string): FollowerEntry[] =>
    [...(followersByCcid.get(ccid)?.values() ?? [])];

// リモートactorが消滅した際の掃除用: 全ローカルエンティティ横断でそのactorのfollowerエントリを引く
export const getFollowersByActorURI = (actorURI: string): FollowerEntry[] =>
    [...followersByCcid.values()]
        .map(inner => inner.get(actorURI))
        .filter((e): e is FollowerEntry => e != null);

// リモートアクターをフォローしている(rejectedでない)ローカルccid一覧
export const getLocalFollowerCcids = (actorURI: string): string[] =>
    [...(followingReverse.get(actorURI) ?? [])]
        .filter(ccid => getAcceptState(ccid, actorURI) !== 'rejected');

interface LoadedRecord { key: string, schema: string, value: any, author: string, createdAt: string }

// nextカーソルが尽きるまでページングする。sinceは閉区間なので境界行が重複して
// 返るため、キーで重複排除する(同一キーの更新版は後勝ち。queryAllのccfs基準
// dedupeとは異なる挙動が必要なため自前実装)。itemsは読取権限フィルタ後の
// 内容なので、空ページでもnextがあれば継続する必要がある。
const queryAllByPrefix = async (prefix: string, schema: string): Promise<LoadedRecord[]> => {
    const results = new Map<string, LoadedRecord>();
    let since: string | undefined = undefined;

    for (;;) {
        const page = await concrntApi.query(
            { prefix, schema, limit: 100, order: 'asc', since },
            config.concrnt.domain,
        );

        for (const sd of page.items) {
            const doc = JSON.parse(sd.document);
            const key = sd.cckv;
            if (!key) continue;
            results.set(key, { key, schema: doc.schema, value: doc.value, author: doc.author, createdAt: doc.createdAt });
        }

        if (page.next === null) break;
        if (page.next === since) {
            // カーソルはcreatedAtのみなので、同一createdAtの行が1ページを超えて
            // 並ぶとnextが同じ値のまま前進できない。この分岐に入った時点で
            // 未取得の行が確実に残っている(nextはlimit+1行目のcreatedAt)。
            logger.warn(`queryAllByPrefix: pagination stalled at ${page.next} for ${prefix}; loaded only ${results.size} records, results are incomplete`);
            break;
        }
        since = page.next;
    }

    return [...results.values()];
}

let serviceRecordsLoaded = false;

// サービスアカウント空間の follower / accept-state レコードをロードする
const loadServiceRecords = async () => {
    const svcCcid = config.concrnt.ccid;

    const followers = await queryAllByPrefix(
        `cckv://${svcCcid}/${AP_NAMESPACE}/followers/`, SCHEMA_AP_FOLLOWER);
    for (const rec of followers) {
        const value = rec.value as ApFollowerValue;
        if (!value?.ccid || !value?.actorURI || !value?.inbox) {
            logger.warn(`Skipping malformed follower record: ${rec.key}`);
            continue;
        }
        setFollower({ ccid: value.ccid, key: rec.key, actorURI: value.actorURI, inbox: value.inbox, sharedInbox: value.sharedInbox });
    }

    const states = await queryAllByPrefix(
        `cckv://${svcCcid}/${AP_NAMESPACE}/accept-states/`, SCHEMA_AP_ACCEPT_STATE);
    for (const rec of states) {
        const value = rec.value as ApAcceptStateValue;
        if (!value?.ccid || !value?.actorURI || !value?.status) {
            logger.warn(`Skipping malformed accept-state record: ${rec.key}`);
            continue;
        }
        setAcceptState(rec.key, value.ccid, value.actorURI, value.status);
    }

    logger.info(`followStore: loaded ${followers.length} followers, ${states.length} accept-states`);
}

// 未ロード(または前回失敗)ならサービスレコードをロードする。60秒周期で再試行される。
export const ensureServiceRecordsLoaded = async () => {
    if (serviceRecordsLoaded) return;
    await loadServiceRecords();
    serviceRecordsLoaded = true;
}

// entityのfollowsプレフィックスを未ロードならロードする(新規entityの遅延ロード対応)
export const ensureEntityFollowsLoaded = async (ccid: string) => {
    if (loadedFollowCcids.has(ccid)) return;
    loadedFollowCcids.add(ccid);

    try {
        const follows = await queryAllByPrefix(
            `cckv://${ccid}/${AP_NAMESPACE}/follows/`, SCHEMA_AP_FOLLOW);
        let count = 0;
        for (const rec of follows) {
            const value = rec.value as ApFollowValue;
            // 本人署名かつ正しいスキーマのレコードのみ採用する
            if (rec.author !== ccid || rec.schema !== SCHEMA_AP_FOLLOW || !value?.actorURI) continue;
            setFollowing({ ccid, key: rec.key, actorURI: value.actorURI });
            count++;
        }
        logger.info(`followStore: loaded ${count} follows for ${ccid}`);
    } catch (error) {
        // 失敗時は次の機会(updateEntitiesの60秒周期)に再試行できるようにする
        loadedFollowCcids.delete(ccid);
        throw error;
    }
}

export const initialize = async (ccids: string[]) => {
    await ensureServiceRecordsLoaded();
    for (const ccid of ccids) {
        await ensureEntityFollowsLoaded(ccid).catch((error) => {
            logger.error(`followStore: failed to load follows for ${ccid}: ${error}`);
        });
    }
}
