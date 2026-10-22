CREATE TABLE IF NOT EXISTS schema_version(version integer PRIMARY KEY);
INSERT INTO schema_version VALUES(1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS jobs (
 id uuid PRIMARY KEY, kind text NOT NULL CHECK(kind IN ('account','transfer')),
 from_account uuid, to_account uuid NOT NULL, amount bigint NOT NULL CHECK(amount>=0),
 from_shard integer NOT NULL, to_shard integer NOT NULL,
 state text NOT NULL DEFAULT 'PENDING' CHECK(state IN ('PENDING','DEBITED','CANCELLING','COMPENSATING','COMPLETING','REVERSING','COMPLETED','REVERSED','FAILED')),
 attempts integer NOT NULL DEFAULT 0, total_attempts bigint NOT NULL DEFAULT 0,
 next_attempt timestamptz NOT NULL DEFAULT now(), error text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 response jsonb, status_code integer
);
CREATE INDEX IF NOT EXISTS jobs_pending ON jobs(next_attempt,created_at) WHERE state NOT IN ('COMPLETED','FAILED','REVERSED');
CREATE TABLE IF NOT EXISTS idempotency_keys (
 key text PRIMARY KEY, request_hash text NOT NULL, job_id uuid NOT NULL UNIQUE REFERENCES jobs(id), created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS consumed_events (
 event_id uuid PRIMARY KEY, payload jsonb NOT NULL, received_at timestamptz NOT NULL DEFAULT now()
);
