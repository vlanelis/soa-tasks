-- Kafka Engine table
CREATE TABLE IF NOT EXISTS movie_events_queue
(
    event_id String,
    user_id String,
    movie_id String,
    event_type String,
    timestamp DateTime64(3),
    device_type String,
    session_id String,
    progress_seconds Nullable(Int32)
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka1:9092,kafka2:9092',
         kafka_topic_list = 'movie-events',
         kafka_group_name = 'clickhouse_consumer',
         kafka_format = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://schema-registry:8081';

-- MergeTree table for storage
CREATE TABLE IF NOT EXISTS movie_events
(
    event_id String,
    user_id String,
    movie_id String,
    event_type String,
    timestamp DateTime64(3),
    device_type String,
    session_id String,
    progress_seconds Nullable(Int32),
    _partition_date Date DEFAULT toDate(timestamp)
) ENGINE = MergeTree
PARTITION BY _partition_date
ORDER BY (user_id, timestamp);

-- Materialized View to move data
CREATE MATERIALIZED VIEW IF NOT EXISTS movie_events_mv TO movie_events
AS SELECT
    event_id, user_id, movie_id, event_type, timestamp, device_type, session_id, progress_seconds
FROM movie_events_queue;