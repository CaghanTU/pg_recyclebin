-- =============================================================================
-- pg_recyclebin — backup restore test matrix (schema only)
-- Run against your test database after CREATE EXTENSION pg_recyclebin;
-- =============================================================================
-- Objects:
--   fb_matrix_big          — hedef tablo (10GB civarı veri buraya)
--   fb_matrix_orders       — FK parent
--   fb_matrix_order_lines  — FK child
--   fb_matrix_part         — partitioned (range by month key)
--   fb_matrix_parent       — inheritance parent
--   fb_matrix_child        — inheritance child (extra column)
-- =============================================================================

DROP TABLE IF EXISTS fb_matrix_order_lines CASCADE;
DROP TABLE IF EXISTS fb_matrix_orders CASCADE;
DROP TABLE IF EXISTS fb_matrix_big CASCADE;
DROP TABLE IF EXISTS fb_matrix_part CASCADE;
DROP TABLE IF EXISTS fb_matrix_child CASCADE;
DROP TABLE IF EXISTS fb_matrix_parent CASCADE;

-- Büyük veri tablosu (payload ~800 byte/row → ~13M row ≈ 10GB)
CREATE TABLE fb_matrix_big (
    id       bigserial PRIMARY KEY,
    payload  text NOT NULL
);

-- FK zinciri
CREATE TABLE fb_matrix_orders (
    id          bigserial PRIMARY KEY,
    customer_id int NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE fb_matrix_order_lines (
    id       bigserial PRIMARY KEY,
    order_id bigint NOT NULL REFERENCES fb_matrix_orders (id) ON DELETE CASCADE,
    sku      text NOT NULL,
    qty      int NOT NULL DEFAULT 1
);

CREATE INDEX idx_fb_matrix_order_lines_order ON fb_matrix_order_lines (order_id);

-- Partitioned table (PostgreSQL native partitioning)
CREATE TABLE fb_matrix_part (
    id    bigserial,
    pkey  int NOT NULL,
    data  text NOT NULL,
    PRIMARY KEY (id, pkey)
) PARTITION BY RANGE (pkey);

CREATE TABLE fb_matrix_part_p1 PARTITION OF fb_matrix_part
    FOR VALUES FROM (1) TO (10001);
CREATE TABLE fb_matrix_part_p2 PARTITION OF fb_matrix_part
    FOR VALUES FROM (10001) TO (20001);

-- Table inheritance (legacy model; still seen in production)
CREATE TABLE fb_matrix_parent (
    id   int PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE fb_matrix_child (
    extra numeric(10,2)
) INHERITS (fb_matrix_parent);

COMMENT ON TABLE fb_matrix_big IS 'pg_recyclebin matrix: large table for ~10GB load test';
COMMENT ON TABLE fb_matrix_orders IS 'pg_recyclebin matrix: FK parent';
COMMENT ON TABLE fb_matrix_order_lines IS 'pg_recyclebin matrix: FK child';
