# concrnt-ap-bridge

concrnt v2 の ActivityPub ブリッジ。concrnt のユーザーを Fediverse (Mastodon / Misskey 等) へ連合し、逆にリモートのアクターを concrnt のタイムラインへ取り込みます。

## アーキテクチャ

```
Fediverse ⇄ [Fedify federation (src/federation.ts)] ⇄ concrnt core
                       ↑                                  ↓ Redis pub/sub
              [REST API (src/app.ts)]        [daemon (src/daemon.ts)]
```

- **src/federation.ts** — Fedify の inbox リスナー(Follow / Undo / Accept / Reject / Create / Announce / Like / EmojiReact / Update / Delete)、アクター・鍵・followers・outbox・Note の各ディスパッチャ。
- **src/daemon.ts** — Redis pub/sub で concrnt のイベントを購読し、投稿→Create、boost→Announce、Like/リアクション→Like、削除→Delete/Undo、プロフィール更新→Update(Person) を配信。
- **src/app.ts** — concrnt クライアント向けの REST API (`/ap/api/*`): setup / settings / stats / followers / following / follow / unfollow / resolve。`/cc-info` で各エンドポイントを `net.concrnt.activitypub.*` シグネチャとして concrnt 本体へ広告。
- **src/convert.ts, src/render.ts, src/schemas.ts** — concrnt ドキュメント ⇄ AP オブジェクトの変換ロジック。render/schemas は純粋関数でユニットテスト対象。
- **src/db/** — Drizzle ORM (Postgres)。

## スキーママッピング

| concrnt | ActivityPub | 方向 |
|---|---|---|
| `m/markdown.json` ほかテキスト系 | `Create{Note}` | 送信 |
| `m/media.json` | `Create{Note}` + attachments | 送信 |
| `m/reply.json` | `Create{Note}` + `inReplyTo` | 送信 |
| `m/reroute.json` (bodyなし) | `Announce` | 双方向 |
| `m/reroute.json` (bodyあり) | `Create{Note}` + `quoteUrl` | 送信 |
| `a/like.json` | `Like` | 双方向 |
| `a/reaction.json` | `Like` + `Emoji` tag / `EmojiReact` | 双方向 |
| `a/mention.json` | `Create{Note}` + `Mention` tag | 受信 |
| `a/reply.json` | `Create{Note}` + `inReplyTo` (ローカル投稿宛て) | 受信 |
| `ap/note.json` | リモート Note への軽量参照 | 受信 |
| `delete.json` | `Delete` / `Undo` | 双方向 |
| `p/main.json` | `Person` (name/summary/icon) + `Update` | 送信 |

受信したリモート投稿は本文を複製せず、`ap/note.json` (`{actorURL, noteURL}`) としてフォロワーの inbox タイムラインへ配送します(表示時にクライアントが解決)。受信 boost は同様にサービスアカウント名義の `m/reroute.json` + `profileOverride` で表現します。

AP オブジェクト ⇄ concrnt URI の対応は、note/announce は URL ハッシュによる決定的キー、like/reaction と送信済み activity は `ap_object_references` テーブルで管理します。

## セットアップ

```sh
cp config.example.yaml config.yaml   # 編集する
pnpm install
pnpm migrate
pnpm dev        # 開発 (tsx watch)
pnpm prod       # 本番
```

必要なもの: Postgres、Redis(concrnt コアと同じインスタンス)、concrnt コア。

設定は環境変数 `CONFIG_PATH` で場所を指定できます(既定はリポジトリルートの `config.yaml`)。concrnt 本体と同様にディレクトリを指定することもでき、その場合は中のファイルをファイル名昇順で読み、後のファイルが前のファイルを深いマージで上書きします(秘匿値だけ `secret.yaml` に分ける、といった運用向け)。

既存インストールのActor URLを維持する必要がある場合は、`activitypub.actorPathSegment` に従来のパスセグメントを設定します。未指定時は `acct` です。

concrnt 本体のゲートウェイにサービスとして登録します (本体 config.yaml の `services:`):

```yaml
services:
  - name: net.concrnt.activitypub
    host: activitypub
    port: 8008
    paths:
      - /ap
      - /.well-known/webfinger
      - /.well-known/nodeinfo
      - /.well-known/host-meta
    preservePath: true
```

登録すると本体が `http://<host>:<port>/cc-info` を直接ポーリングし、広告された `net.concrnt.activitypub.*` エンドポイントを `/.well-known/concrnt` の `endpoints` に統合します。クライアント(world-app 等)はこのシグネチャ経由でエンドポイントを解決します。

注意: `path` は指定せず `paths` で登録してください。本体は広告されたエンドポイントに `path` を前置するため、`path: /ap` を指定すると `/ap/ap/api/...` に壊れます (このサービスは Fediverse 向けURLが `/ap` 固定のためマウント位置を変えられず、`/cc-info` は絶対パスで広告しています)。

## 開発

```sh
pnpm typecheck  # tsc --noEmit
pnpm test       # vitest
pnpm lint       # eslint
```
