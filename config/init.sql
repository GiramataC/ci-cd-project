-- Create metabase database (Metabase needs its own DB for its internal state)
CREATE DATABASE metabase;

-- Connect to analytics DB and create the sales table up front
\connect analytics;

CREATE TABLE IF NOT EXISTS sales (
    id          INT,
    product     TEXT,
    quantity    INT,
    price       FLOAT,
    timestamp   TIMESTAMP,
    total       FLOAT,
    profit      FLOAT,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sales_product   ON sales (product);
CREATE INDEX IF NOT EXISTS idx_sales_timestamp ON sales (timestamp);