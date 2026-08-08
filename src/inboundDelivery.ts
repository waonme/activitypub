import { PUBLIC_COLLECTION } from "@fedify/vocab";

const stringValues = (value: unknown): string[] => {
    const values = Array.isArray(value) ? value : value == null ? [] : [value];
    return values.filter((item): item is string => typeof item === "string");
};

export const collectCachedAddresses = (json: Record<string, unknown>): string[] =>
    [...new Set([...stringValues(json.to), ...stringValues(json.cc)])];

export const selectCreateRecipientCcids = (
    addressed: readonly string[],
    followersUri: string,
    followerCcids: readonly string[],
    explicitlyAddressedCcids: readonly string[],
): string[] => {
    const recipients = new Set(explicitlyAddressedCcids);
    if (addressed.includes(PUBLIC_COLLECTION.href) || addressed.includes(followersUri)) {
        for (const ccid of followerCcids) recipients.add(ccid);
    }
    return [...recipients];
};
