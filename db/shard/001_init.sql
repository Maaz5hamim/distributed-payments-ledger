CREATE TABLE IF NOT EXISTS schema_version(version integer PRIMARY KEY);
INSERT INTO schema_version VALUES(1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS accounts (
 id uuid PRIMARY KEY, kind text NOT NULL CHECK(kind IN ('customer','funding','clearing')),
 balance bigint NOT NULL DEFAULT 0, version bigint NOT NULL DEFAULT 0,
 created_at timestamptz NOT NULL DEFAULT now(), CHECK(kind<>'customer' OR balance>=0)
);
INSERT INTO accounts(id,kind) VALUES
 ('00000000-0000-0000-0000-000000000001','funding'),
 ('00000000-0000-0000-0000-000000000002','clearing') ON CONFLICT DO NOTHING;
-- A durable step row is also a cancellation fence. Every apply/cancel first
-- takes the same advisory lock, including attempts with an uncertain outcome.
CREATE TABLE IF NOT EXISTS steps (
 transfer_id uuid NOT NULL, step text NOT NULL, status text NOT NULL CHECK(status IN ('APPLIED','CANCELLED')),
 created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(transfer_id,step)
);
CREATE TABLE IF NOT EXISTS entries (
 id bigserial PRIMARY KEY, transfer_id uuid NOT NULL, step text NOT NULL,
 account_id uuid NOT NULL REFERENCES accounts(id), amount bigint NOT NULL CHECK(amount<>0),
 created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(transfer_id,step,account_id),
 FOREIGN KEY(transfer_id,step) REFERENCES steps(transfer_id,step)
);
CREATE INDEX IF NOT EXISTS entries_history ON entries(account_id,id);
CREATE TABLE IF NOT EXISTS outbox (
 id bigserial PRIMARY KEY, event_id uuid NOT NULL UNIQUE, transfer_id uuid NOT NULL,
 topic text NOT NULL DEFAULT 'transfer-events', payload jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), sent_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(id) WHERE sent_at IS NULL;
