import os

class Config:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    CASSANDRA_CONTACT_POINTS = os.getenv("CASSANDRA_CONTACT_POINTS", "cassandra-1,cassandra-2,cassandra-3").split(",")
    CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "warehouse")
    DLQ_TOPIC = os.getenv("DLQ_TOPIC", "warehouse-events-dlq")
    CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "warehouse-state-consumer")
    METRICS_PORT = int(os.getenv("METRICS_PORT", 8080))

config = Config()