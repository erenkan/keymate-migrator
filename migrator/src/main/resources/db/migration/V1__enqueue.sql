-- orders table example
INSERT INTO work_queue (source_table, source_pk, payload)
SELECT 'orders', o.id, jsonb_build_object('id', o.id, 'data', o.data)
FROM orders o
WHERE NOT EXISTS (SELECT 1
                  FROM processed_log p
                  WHERE p.source_table = 'orders'
                    AND p.source_pk = o.id
                    AND p.status = 'OK')
ON CONFLICT (source_table, source_pk) DO NOTHING;

-- invoices table example
INSERT INTO work_queue (source_table, source_pk, payload)
SELECT 'invoices', i.id, to_jsonb(i) -- esnek
FROM invoices i
WHERE NOT EXISTS (SELECT 1
                  FROM processed_log p
                  WHERE p.source_table = 'invoices'
                    AND p.source_pk = i.id
                    AND p.status = 'OK')
ON CONFLICT (source_table, source_pk) DO NOTHING;
