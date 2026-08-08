CREATE TABLE IF NOT EXISTS "ap_inbound_objects" (
	"object_id" text PRIMARY KEY NOT NULL,
	"actor_id" text NOT NULL,
	"object" jsonb NOT NULL,
	"recipient_ccids" text[] DEFAULT '{}' NOT NULL,
	"visibility" text NOT NULL,
	"c_date" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "ap_inbound_objects_actor_id_idx" ON "ap_inbound_objects" USING btree ("actor_id");
