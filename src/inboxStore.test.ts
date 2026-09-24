import { describe, it, expect, vi, beforeEach } from 'vitest';
import { NotFoundError } from '@concrnt/client';

const getDocument = vi.fn();
vi.mock('./concrnt.ts', () => ({ default: { getDocument: (...args: unknown[]) => getDocument(...args) } }));

const inboxStore = await import('./inboxStore.ts');
const { inboxTimelineKey } = await import('./schemas.ts');

describe('inboxStore', () => {
    beforeEach(() => {
        getDocument.mockReset();
    });

    it('inboxが存在するccidは配送先に残る', async () => {
        getDocument.mockResolvedValue({ kind: 'record' });
        expect(await inboxStore.filterCcidsWithInbox(['con1has'])).toEqual(['con1has']);
        expect(getDocument).toHaveBeenCalledWith(inboxTimelineKey('con1has'));
    });

    it('inboxが無い(404)ccidは配送先から外れ、結果はキャッシュされる', async () => {
        getDocument.mockRejectedValue(new NotFoundError('not found', inboxTimelineKey('con1none')));
        expect(await inboxStore.filterCcidsWithInbox(['con1none'])).toEqual([]);
        expect(await inboxStore.filterCcidsWithInbox(['con1none'])).toEqual([]);
        expect(getDocument).toHaveBeenCalledTimes(1);
    });

    it('404以外の失敗はキャッシュせず、次回に再試行する', async () => {
        getDocument.mockRejectedValueOnce(new Error('network'));
        expect(await inboxStore.filterCcidsWithInbox(['con1flaky'])).toEqual([]);
        getDocument.mockResolvedValueOnce({ kind: 'record' });
        expect(await inboxStore.filterCcidsWithInbox(['con1flaky'])).toEqual(['con1flaky']);
        expect(getDocument).toHaveBeenCalledTimes(2);
    });

    it('createdイベントで未作成→作成済みに切り替わり、deletedで戻る', async () => {
        getDocument.mockRejectedValue(new NotFoundError('not found', inboxTimelineKey('con1later')));
        expect(await inboxStore.filterCcidsWithInbox(['con1later'])).toEqual([]);

        inboxStore.applyEvent('con1later', { type: 'created', uri: inboxTimelineKey('con1later') });
        expect(await inboxStore.filterCcidsWithInbox(['con1later'])).toEqual(['con1later']);

        inboxStore.applyEvent('con1later', { type: 'deleted', uri: inboxTimelineKey('con1later') });
        expect(await inboxStore.filterCcidsWithInbox(['con1later'])).toEqual([]);
        // イベント反映後はフェッチし直さない
        expect(getDocument).toHaveBeenCalledTimes(1);
    });

    it('inbox配下の参照(配送済みnote)のdeletedイベントではinboxを削除扱いにしない', async () => {
        getDocument.mockResolvedValue({ kind: 'record' });
        expect(await inboxStore.filterCcidsWithInbox(['con1keep'])).toEqual(['con1keep']);

        // concrntは削除されたnoteのdistributes先(inboxタイムライン)のchannelにも
        // deletedを流す(uriは削除されたnote自身のキー)
        inboxStore.applyEvent('con1keep', { type: 'deleted', uri: 'cckv://con1svc/activitypub.concrnt.world/inbox/xnote' });
        expect(await inboxStore.filterCcidsWithInbox(['con1keep'])).toEqual(['con1keep']);

        // 参照作成のcreatedも未作成状態を作成済みに変えない
        getDocument.mockRejectedValue(new NotFoundError('not found', inboxTimelineKey('con1none2')));
        expect(await inboxStore.filterCcidsWithInbox(['con1none2'])).toEqual([]);
        inboxStore.applyEvent('con1none2', { type: 'created', uri: 'cckv://con1svc/activitypub.concrnt.world/inbox/xnote' });
        expect(await inboxStore.filterCcidsWithInbox(['con1none2'])).toEqual([]);
    });

    it('イベントが先に届いたccidはロード時にフェッチしない', async () => {
        inboxStore.applyEvent('con1early', { type: 'created', uri: inboxTimelineKey('con1early') });
        await inboxStore.ensureEntityInboxLoaded('con1early');
        expect(inboxStore.hasInbox('con1early')).toBe(true);
        expect(getDocument).not.toHaveBeenCalled();
    });

    it('同時 cold start は同じ取得を待ち、どちらの配送先も落とさない', async () => {
        let resolve!: (value: unknown) => void;
        getDocument.mockReturnValue(new Promise(done => { resolve = done; }));
        const first = inboxStore.filterCcidsWithInbox(['con1concurrent']);
        await Promise.resolve();
        let secondSettled = false;
        const second = inboxStore.filterCcidsWithInbox(['con1concurrent']).then(value => {
            secondSettled = true;
            return value;
        });
        // Give an incorrectly early return enough microtasks to settle.
        for (let turn = 0; turn < 10; turn++) await Promise.resolve();
        expect(secondSettled).toBe(false);
        expect(getDocument).toHaveBeenCalledTimes(1);
        resolve({ kind: 'record' });
        expect(await Promise.all([first, second])).toEqual([['con1concurrent'], ['con1concurrent']]);
    });

    it('同時初期化の一時障害は全待機者に伝搬し、次回は再試行する', async () => {
        let reject!: (reason: Error) => void;
        getDocument.mockReturnValueOnce(new Promise((_done, fail) => { reject = fail; }));
        const first = inboxStore.ensureEntityInboxLoaded('con1sharedfailure');
        const second = inboxStore.ensureEntityInboxLoaded('con1sharedfailure');
        const outcomes = Promise.allSettled([first, second]);
        await Promise.resolve();
        reject(new Error('synthetic connection failure'));
        expect((await outcomes).map(result => result.status)).toEqual(['rejected', 'rejected']);
        getDocument.mockResolvedValueOnce({ kind: 'record' });
        expect(await inboxStore.filterCcidsWithInbox(['con1sharedfailure'])).toEqual(['con1sharedfailure']);
        expect(getDocument).toHaveBeenCalledTimes(2);
    });

    it.each(['created', 'deleted'] as const)('ロード中の %s は古い取得結果に上書きされない', async type => {
        const ccid = `con1during${type}`;
        let resolve!: (value: unknown) => void;
        let reject!: (reason: Error) => void;
        getDocument.mockReturnValueOnce(new Promise((done, fail) => { resolve = done; reject = fail; }));
        const pending = inboxStore.ensureEntityInboxLoaded(ccid);
        await Promise.resolve();
        inboxStore.applyEvent(ccid, { type, uri: inboxTimelineKey(ccid) });
        if (type === 'created') reject(new NotFoundError('not found', inboxTimelineKey(ccid)));
        else resolve({ kind: 'record' });
        await pending;
        expect(inboxStore.hasInbox(ccid)).toBe(type === 'created');
    });
});
