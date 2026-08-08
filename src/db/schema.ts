import { boolean, pgTable, text, date, primaryKey, jsonb, index } from 'drizzle-orm/pg-core'

export const apEntity = pgTable("ap_entities", {
    id: text("id").notNull().primaryKey(),
    ccid: text("ccid").notNull().unique(),
    enabled: boolean("enabled").notNull().default(true),
    cDate: date("c_date").notNull().defaultNow(),
});

export type ApEntity = typeof apEntity.$inferSelect;

export const apKeys = pgTable(
    "ap_keys", 
    {
        ownerId: text("owner_id").notNull(),
        keyType: text("key_type").notNull(),
        private: text("private").notNull(),
        public: text("public").notNull(),
        cDate: date("c_date").notNull().defaultNow(),
    },
    (table) => [
        primaryKey({
            columns: [table.ownerId, table.keyType]
        })
    ]
);

export type ApKey = typeof apKeys.$inferSelect;

// AP activity/object id と concrnt URI の相互参照。
// 決定的キーで解決できない対応（like/reaction の ccfs://、送信済みAnnounce等）の
// Undo/Delete 解決に使う。
export const apObjectReference = pgTable(
    "ap_object_references",
    {
        apObjectId: text("ap_object_id").notNull().primaryKey(),
        ccUri: text("cc_uri").notNull(),
        refType: text("ref_type").notNull(),
        meta: jsonb("meta").$type<Record<string, string>>(),
        cDate: date("c_date").notNull().defaultNow(),
    },
    (table) => [
        index("ap_object_references_cc_uri_idx").on(table.ccUri)
    ]
);

export type ApObjectReferenceRow = typeof apObjectReference.$inferSelect;

