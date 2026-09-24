import { describe, expect, it } from "vitest";
import { PUBLIC_COLLECTION } from "@fedify/vocab";
import { collectCachedAddresses, selectCreateRecipientCcids } from "./inboundDelivery.ts";

const followersUri = "https://remote.example/users/alice/followers";

describe("selectCreateRecipientCcids", () => {
    it("fans public posts out to followers", () => {
        expect(selectCreateRecipientCcids(
            [PUBLIC_COLLECTION.href], followersUri, ["con1a", "con1b"], [],
        )).toEqual(["con1a", "con1b"]);
    });

    it("fans followers-only posts out to followers", () => {
        expect(selectCreateRecipientCcids(
            [followersUri], followersUri, ["con1a"], [],
        )).toEqual(["con1a"]);
    });

    it("does not leak direct posts to unrelated followers", () => {
        expect(selectCreateRecipientCcids(
            ["https://local.example/ap/users/bob"], followersUri, ["con1a", "con1b"], ["con1bob"],
        )).toEqual(["con1bob"]);
    });

    it("deduplicates followers who are also explicitly addressed", () => {
        expect(selectCreateRecipientCcids(
            [PUBLIC_COLLECTION.href], followersUri, ["con1a"], ["con1a"],
        )).toEqual(["con1a"]);
    });
});

describe("collectCachedAddresses", () => {
    it("normalizes scalar and array audiences without duplicates", () => {
        expect(collectCachedAddresses({
            to: followersUri,
            cc: [followersUri, PUBLIC_COLLECTION.href, 123],
        })).toEqual([followersUri, PUBLIC_COLLECTION.href]);
    });
});
