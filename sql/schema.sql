-- =============================================================
--  schema.sql  –  Run this once against your PostgreSQL database
-- =============================================================

-- 1. Create the orders table
CREATE TABLE IF NOT EXISTS orders (
    id              SERIAL PRIMARY KEY,
    customer_name   VARCHAR(100)    NOT NULL,
    product_name    VARCHAR(100)    NOT NULL,
    status          VARCHAR(20)     NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'shipped', 'delivered')),
    updated_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- 2. Trigger function: fires a NOTIFY on the 'order_changes' channel
--    Payload is a JSON object containing the operation type and the
--    affected row, so the Spring listener can forward it to clients
--    without an extra round-trip to the database.
CREATE OR REPLACE FUNCTION notify_order_change()
RETURNS TRIGGER AS $$
DECLARE
    payload JSON;
    record_row orders%ROWTYPE;
BEGIN
    -- For DELETE use OLD row; for INSERT/UPDATE use NEW row
    IF (TG_OP = 'DELETE') THEN
        record_row := OLD;
    ELSE
        record_row := NEW;
    END IF;

    payload := json_build_object(
        'operation',     TG_OP,
        'id',            record_row.id,
        'customerName',  record_row.customer_name,
        'productName',   record_row.product_name,
        'status',        record_row.status,
        'updatedAt',     record_row.updated_at
    );

    -- Broadcast on the well-known channel name
    PERFORM pg_notify('order_changes', payload::TEXT);

    RETURN record_row;
END;
$$ LANGUAGE plpgsql;

-- 3. Attach the trigger to the orders table (INSERT + UPDATE + DELETE)
DROP TRIGGER IF EXISTS order_change_trigger ON orders;

CREATE TRIGGER order_change_trigger
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW EXECUTE FUNCTION notify_order_change();

-- 4. Seed some sample data so the UI has something to display on first load
INSERT INTO orders (customer_name, product_name, status) VALUES
    ('Alice Johnson',  'Laptop Pro 15',  'pending'),
    ('Bob Smith',      'Wireless Mouse', 'shipped'),
    ('Carol White',    'USB-C Hub',      'delivered');
