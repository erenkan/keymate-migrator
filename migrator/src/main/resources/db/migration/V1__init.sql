CREATE TABLE IF NOT EXISTS orders
(
    id          BIGSERIAL PRIMARY KEY,
    customer_id BIGINT         NOT NULL,
    amount      NUMERIC(18, 2) NOT NULL CHECK (amount >= 0),
    currency    VARCHAR(3)     NOT NULL DEFAULT 'TRY',
    status      TEXT           NOT NULL DEFAULT 'NEW',       -- NEW|PAID|CANCELLED|REFUNDED...
    data        JSONB          NOT NULL DEFAULT '{}'::jsonb, -- raw/additional data
    created_at  TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders (updated_at);

CREATE TABLE IF NOT EXISTS invoices
(
    id         BIGSERIAL PRIMARY KEY,
    order_id   BIGINT         NOT NULL,
    invoice_no TEXT           NOT NULL,
    total      NUMERIC(18, 2) NOT NULL CHECK (total >= 0),
    currency   VARCHAR(3)     NOT NULL DEFAULT 'TRY',
    status     TEXT           NOT NULL DEFAULT 'DRAFT', -- DRAFT|ISSUED|PAID|VOID...
    issued_at  TIMESTAMPTZ,
    due_at     TIMESTAMPTZ,
    data       JSONB          NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT fk_invoices_order
        FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_invoices_invoice_no ON invoices (invoice_no);
CREATE INDEX IF NOT EXISTS idx_invoices_order_id ON invoices (order_id);
CREATE INDEX IF NOT EXISTS idx_invoices_status ON invoices (status);
CREATE INDEX IF NOT EXISTS idx_invoices_issued_at ON invoices (issued_at);
CREATE INDEX IF NOT EXISTS idx_invoices_updated_at ON invoices (updated_at);

-- Work queue (multi-source)
CREATE TABLE IF NOT EXISTS work_queue
(
    id            BIGSERIAL PRIMARY KEY,
    source_table  TEXT        NOT NULL,
    source_pk     BIGINT      NOT NULL,
    payload       JSONB       NOT NULL,
    enqueued_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt_count INT         NOT NULL DEFAULT 0,
    last_error    TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_queue_source ON work_queue (source_table, source_pk);
CREATE INDEX IF NOT EXISTS idx_queue_retry ON work_queue (next_retry_at, source_table);


-- Idempotency / audit
CREATE TABLE IF NOT EXISTS processed_log
(
    source_table  TEXT        NOT NULL,
    source_pk     BIGINT      NOT NULL,
    attempt_count INT         NOT NULL DEFAULT 0,
    status        TEXT        NOT NULL, -- 'OK' | 'FAILED'
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    error         TEXT,
    PRIMARY KEY (source_table, source_pk)
);