import { Api, InMemoryAuthProvider, InMemoryKVS, type Document, type SignedDocument } from '@concrnt/client'
import { config } from "./config.ts";

const authProvider = new InMemoryAuthProvider(config.concrnt.privateKey);
const kvs = new InMemoryKVS();

const api = new Api(config.concrnt.domain, authProvider, kvs)

// サービスアカウントはマスター鍵のみ(subkeyなし)なので useMasterkey を明示する
export const commit = <T>(document: Document<T>): Promise<SignedDocument> =>
    api.commit(document, undefined, { useMasterkey: true });

// backdate window(7日)より古いdocumentの実体化用。importエンドポイントは
// HTTP認証主体がdocumentのauthor/Key ownerと一致する場合のみbackdate免除するため、
// マスター鍵JWTで認証して送る。LocalOnlyExecuteのため配送(distributes)は無視される。
// サーバーは200でも本文に失敗行を返すので空配列チェックが成否判定になる
export const importCommit = async <T>(document: Document<T>): Promise<void> => {
    const docString = JSON.stringify(document);
    const signature = await authProvider.signMaster(docString);
    const line = JSON.stringify({
        document: docString,
        proof: { type: 'concrnt-ecrecover-direct', signature },
    });
    const results = await api.importRepository(line, undefined, { useMasterkey: true });
    if (results.length > 0) {
        throw new Error(`import failed: ${results[0].error}`);
    }
};

export default api;
