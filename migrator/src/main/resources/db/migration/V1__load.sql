-- For this session only
SET synchronous_commit = off;
SET maintenance_work_mem = '1GB';
SET work_mem = '256MB';
SET jit = off;

ALTER TABLE invoices SET UNLOGGED;
ALTER TABLE orders SET UNLOGGED;

DROP INDEX IF EXISTS idx_orders_customer;
DROP INDEX IF EXISTS idx_orders_status;
DROP INDEX IF EXISTS idx_orders_updated_at;

DROP INDEX IF EXISTS uq_invoices_invoice_no;
DROP INDEX IF EXISTS idx_invoices_order_id;
DROP INDEX IF EXISTS idx_invoices_status;
DROP INDEX IF EXISTS idx_invoices_issued_at;
DROP INDEX IF EXISTS idx_invoices_updated_at;

DO
    $$
    DECLARE
        batch_size int := 100000;
        batches    int := 10;        -- 10 * 100k = 1,000,000
        i          int;
    BEGIN
        FOR i IN 1..batches LOOP
                INSERT INTO orders (customer_id, amount, currency, status, data, created_at, updated_at)
                SELECT
                    (1 + floor(random()*50000))::bigint AS customer_id,
                    -- Amount between 10.00 .. 10000.00
                    round( (10 + random()*9990)::numeric, 2 )         AS amount,
                    -- TRY (0.6), USD (0.25), EUR (0.15)
                    (ARRAY['TRY','TRY','TRY','TRY','TRY','TRY','USD','USD','EUR','EUR'])[1 + floor(random()*10)::int] AS currency,
                    -- NEW, PAID, CANCELLED, REFUNDED  (approximate distribution)
                    (ARRAY['NEW','NEW','NEW','PAID','PAID','CANCELLED','REFUNDED'])[1 + floor(random()*7)::int]       AS status,
                    -- little JSONB payload
                    jsonb_build_object(
                            'channel', (ARRAY['web','mobile','store','partner'])[1 + floor(random()*4)::int],
                            'note',    substr(md5(random()::text), 1, 12),
                            'tags',    jsonb_build_array(substr(md5(random()::text), 1, 6), substr(md5(random()::text), 1, 6))
                    ) AS data,
                    -- random timestamp within the last 365 days
                    now() - ((floor(random()*365))::text || ' days')::interval
                        - ((floor(random()*86400))::text || ' seconds')::interval AS created_at,
                    now() - ((floor(random()*365))::text || ' days')::interval
                        - ((floor(random()*86400))::text || ' seconds')::interval AS updated_at
                FROM generate_series(1, batch_size);

                COMMIT;  -- commit after each batch (less table bloat)
            END LOOP;
    END
    $$;

WITH picked AS (
    SELECT o.id AS order_id,
           o.amount,
           o.currency,
           o.created_at,
           row_number() OVER () AS rn
    FROM orders o
    WHERE random() < 0.7
)
INSERT INTO invoices (order_id, invoice_no, total, currency, status, issued_at, due_at, data, created_at, updated_at)
SELECT
    p.order_id,
    'INV-' || to_char(now(), 'YYYY') || '-' || lpad(p.rn::text, 7, '0') AS invoice_no,
    round( (p.amount * (0.975 + random()*0.05))::numeric, 2 ) AS total,
    p.currency,
    (ARRAY['DRAFT','ISSUED','ISSUED','PAID','PAID','VOID'])[1 + floor(random()*6)::int] AS status,
    p.created_at + ((floor(random()*30))::text || ' days')::interval AS issued_at,
    (p.created_at + ((floor(random()*30))::text || ' days')::interval)
        + ((7 + floor(random()*23))::text || ' days')::interval       AS due_at,
    jsonb_build_object(
            'seller', (ARRAY['acme','contoso','globex','initech'])[1 + floor(random()*4)::int],
            'ref',    substr(md5(random()::text), 1, 10)
    ) AS data,
    now() - ((floor(random()*120))::text || ' days')::interval AS created_at,
    now() - ((floor(random()*120))::text || ' days')::interval AS updated_at
FROM picked p;

ALTER TABLE orders SET LOGGED;
ALTER TABLE invoices SET LOGGED;

CREATE INDEX IF NOT EXISTS idx_orders_customer    ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status      ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at  ON orders(updated_at);

CREATE UNIQUE INDEX IF NOT EXISTS uq_invoices_invoice_no ON invoices(invoice_no);
CREATE INDEX IF NOT EXISTS idx_invoices_order_id         ON invoices(order_id);
CREATE INDEX IF NOT EXISTS idx_invoices_status           ON invoices(status);
CREATE INDEX IF NOT EXISTS idx_invoices_issued_at        ON invoices(issued_at);
CREATE INDEX IF NOT EXISTS idx_invoices_updated_at       ON invoices(updated_at);
