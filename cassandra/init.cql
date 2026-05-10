CREATE KEYSPACE IF NOT EXISTS warehouse
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE warehouse;

-- Таблица остатков по товару и зоне (обычные int)
CREATE TABLE IF NOT EXISTS inventory_by_product_zone (
    product_id text,
    zone_id text,
    available int,
    reserved int,
    last_updated timestamp,
    supplier_id text,
    PRIMARY KEY ((product_id, zone_id))
);

-- Агрегированная таблица по товару (обычные int)
CREATE TABLE IF NOT EXISTS inventory_by_product (
    product_id text PRIMARY KEY,
    total_available int,
    total_reserved int
);

-- Таблица по зоне (обычные int)
CREATE TABLE IF NOT EXISTS inventory_by_zone (
    zone_id text,
    product_id text,
    available int,
    PRIMARY KEY (zone_id, product_id)
);

-- Таблица последних timestamp для out-of-order
CREATE TABLE IF NOT EXISTS product_last_ts (
    product_id text PRIMARY KEY,
    last_timestamp timestamp
);

-- Таблица идемпотентности
CREATE TABLE IF NOT EXISTS processed_events (
    event_id text PRIMARY KEY,
    processed_at timestamp
) WITH default_time_to_live = 86400;

-- Таблица заказов
CREATE TABLE IF NOT EXISTS orders (
    order_id text PRIMARY KEY,
    status text,
    items text,
    created_at timestamp,
    completed_at timestamp
);

-- Метаданные для инвентаря (supplier_id, last_updated)
CREATE TABLE IF NOT EXISTS inventory_metadata (
    product_id text,
    zone_id text,
    supplier_id text,
    last_updated timestamp,
    PRIMARY KEY ((product_id, zone_id))
);