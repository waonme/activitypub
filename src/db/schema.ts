import { boolean, pgTable, text, date, primaryKey, jsonb, index, timestamp } from 'drizzle-orm/pg-core'

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

// Inbox delivery is the only reliable copy of followers-only/direct objects:
// many remote servers intentionally reject later anonymous dereferencing.
// Redis remains the fast path, while this table is the durable fallback used
// after cache expiry, process restarts, and upgrades from the former snapshot
// implementation.
export const apInboundObject = pgTable(
    "ap_inbound_objects",
    {
        objectId: text("object_id").notNull().primaryKey(),
        actorId: text("actor_id").notNull(),
        object: jsonb("object").$type<Record<string, unknown>>().notNull(),
        recipientCcids: text("recipient_ccids").array().notNull().default([]),
        // Legacy rows also contain public/unlisted snapshots. New writes keep
        // only restricted objects, but the type must describe the live table.
        visibility: text("visibility").$type<"public" | "unlisted" | "followers" | "direct">().notNull(),
        cDate: timestamp("c_date", { withTimezone: true }).notNull().defaultNow(),
        updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
    },
    (table) => [
        index("ap_inbound_objects_actor_id_idx").on(table.actorId),
    ],
);

export type ApInboundObjectRow = typeof apInboundObject.$inferSelect;
