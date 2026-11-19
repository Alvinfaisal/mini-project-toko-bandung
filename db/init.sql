-- Dijalankan otomatis oleh container Postgres saat pertama kali dibuat (volume masih kosong).
CREATE TABLE orders (
    order_id    TEXT PRIMARY KEY,
    customer_id TEXT,
    product_id  TEXT NOT NULL,
    qty         INTEGER NOT NULL,
    unit_price  INTEGER NOT NULL,
    status      TEXT NOT NULL,
    order_ts    TIMESTAMP NOT NULL,
    updated_at  TIMESTAMP NOT NULL
);

COPY orders FROM '/docker-entrypoint-initdb.d/orders_seed.csv' WITH (FORMAT csv, HEADER true, NULL '');

CREATE INDEX idx_orders_updated_at ON orders (updated_at);
