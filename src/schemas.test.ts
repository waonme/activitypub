import { describe, it, expect } from 'vitest';
import { isPlainReroute, parseEmojiShortcode, SCHEMA_REROUTE, SCHEMA_AP_NOTE, followKey, followerKey, acceptStateKey, settingsKey } from './schemas.ts';

describe('isPlainReroute', () => {
    it('bodyのないrerouteはboost扱い', () => {
        expect(isPlainReroute({ schema: SCHEMA_REROUTE, value: { } })).toBe(true);
    });

    it('valueすらないrerouteもboost扱い', () => {
        expect(isPlainReroute({ schema: SCHEMA_REROUTE })).toBe(true);
    });

    it('空白のみのbodyはboost扱い', () => {
        expect(isPlainReroute({ schema: SCHEMA_REROUTE, value: { body: '   ' } })).toBe(true);
    });

    it('bodyのあるrerouteは引用投稿', () => {
        expect(isPlainReroute({ schema: SCHEMA_REROUTE, value: { body: 'これはコメント' } })).toBe(false);
    });

    it('reroute以外のスキーマは対象外', () => {
        expect(isPlainReroute({ schema: SCHEMA_AP_NOTE, value: {} })).toBe(false);
    });
});

describe('parseEmojiShortcode', () => {
    it(':shortcode:形式からショートコードを取り出す', () => {
        expect(parseEmojiShortcode(':blobcat:')).toBe('blobcat');
    });

    it('前後の空白を無視する', () => {
        expect(parseEmojiShortcode('  :party_parrot:  ')).toBe('party_parrot');
    });

    it('通常のcontentはnull', () => {
        expect(parseEmojiShortcode('⭐')).toBeNull();
        expect(parseEmojiShortcode('hello :world:')).toBeNull();
    });

    it('空・未定義はnull', () => {
        expect(parseEmojiShortcode('')).toBeNull();
        expect(parseEmojiShortcode(null)).toBeNull();
        expect(parseEmojiShortcode(undefined)).toBeNull();
    });

    it('コロンのみはnull', () => {
        expect(parseEmojiShortcode('::')).toBeNull();
    });
});

describe('フォロー関連キー導出', () => {
    const user = 'con1userxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';
    const svc = 'con1svcxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';
    const actor = 'https://mastodon.example/users/alice';

    it('決定的に導出される', () => {
        expect(followKey(user, actor)).toBe(followKey(user, actor));
        expect(followerKey(svc, user, actor)).toBe(followerKey(svc, user, actor));
        expect(acceptStateKey(svc, user, actor)).toBe(acceptStateKey(svc, user, actor));
    });

    it('所定のプレフィックスを持つ', () => {
        expect(followKey(user, actor)).toMatch(
            new RegExp(`^cckv://${user}/activitypub\\.concrnt\\.world/follows/[a-z0-9]+$`));
        expect(followerKey(svc, user, actor)).toMatch(
            new RegExp(`^cckv://${svc}/activitypub\\.concrnt\\.world/followers/[a-z0-9]+$`));
        expect(acceptStateKey(svc, user, actor)).toMatch(
            new RegExp(`^cckv://${svc}/activitypub\\.concrnt\\.world/accept-states/[a-z0-9]+$`));
    });

    it('アクター・entityが異なればキーも異なる', () => {
        expect(followKey(user, actor)).not.toBe(followKey(user, 'https://mastodon.example/users/bob'));
        expect(followerKey(svc, user, actor)).not.toBe(followerKey(svc, 'con1other', actor));
        // followerとaccept-stateは同じハッシュ入力でもプレフィックスで区別される
        expect(followerKey(svc, user, actor)).not.toBe(acceptStateKey(svc, user, actor));
    });

    it('settingsキーはユーザー空間の固定パス', () => {
        expect(settingsKey(user)).toBe(`cckv://${user}/activitypub.concrnt.world/settings`);
    });
});
