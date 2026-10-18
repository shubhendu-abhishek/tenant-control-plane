CREATE TABLE IF NOT EXISTS tenants (
    id          TEXT PRIMARY KEY,
    slug        TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL CHECK (length(trim(name)) > 0),
    status      TEXT NOT NULL CHECK (status IN ('provisioning','active','updating','destroying','destroyed','failed')),
    version     BIGINT NOT NULL CHECK (version >= 1),
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS tenants_created_at_idx ON tenants (created_at DESC);
CREATE INDEX IF NOT EXISTS tenants_status_idx ON tenants (status);

CREATE TABLE IF NOT EXISTS tasks (
    id          TEXT PRIMARY KEY,
    type        TEXT NOT NULL CHECK (type IN ('deploy','update','destroy')),
    tenant_id   TEXT NOT NULL REFERENCES tenants(id),
    status      TEXT NOT NULL CHECK (status IN ('accepted','in_progress','done','failed')),
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS tasks_tenant_created_idx ON tasks (tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS tasks_status_idx ON tasks (status);

-- The task row and this outbox row are committed in one database transaction.
-- The dispatcher publishes only committed outbox entries.
CREATE TABLE IF NOT EXISTS outbox (
    id                BIGSERIAL PRIMARY KEY,
    event_id          TEXT NOT NULL UNIQUE REFERENCES tasks(id),
    topic             TEXT NOT NULL,
    payload           JSONB NOT NULL,
    attempts          INTEGER NOT NULL DEFAULT 0,
    next_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at      TIMESTAMPTZ,
    last_error        TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox (next_attempt_at, id) WHERE published_at IS NULL;

-- This ledger makes progress updates idempotent across broker redeliveries.
CREATE TABLE IF NOT EXISTS processed_updates (
    update_id    TEXT PRIMARY KEY,
    task_id      TEXT NOT NULL REFERENCES tasks(id),
    status       TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
