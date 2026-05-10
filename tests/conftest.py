import uuid

import pytest
import requests
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
import os
import docker
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def producer_url():
    return os.getenv("PRODUCER_URL", "http://localhost:8000")

@pytest.fixture(scope="session")
def consumer_url():
    return os.getenv("CONSUMER_URL", "http://consumer:8080")

profile = ExecutionProfile(
    load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc='datacenter1')),
    consistency_level=ConsistencyLevel.QUORUM,
    request_timeout=10
)

@pytest.fixture(scope="session")
def cassandra_session():
    contact_points = os.getenv("CASSANDRA_CONTACT_POINTS", "localhost").split(",")
    keyspace = os.getenv("CASSANDRA_KEYSPACE", "warehouse")
    cluster = Cluster(contact_points, protocol_version=5, execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect(keyspace)
    yield session
    cluster.shutdown()

@pytest.fixture(scope="session")
def kafka_bootstrap():
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

@pytest.fixture(scope="session")
def schema_registry_url():
    return os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

@pytest.fixture
def dlq_consumer(kafka_bootstrap):
    """Создаёт уникального consumer для каждого теста, читает только новые сообщения."""
    group_id = f"test-dlq-{uuid.uuid4()}"
    c = Consumer({
        'bootstrap.servers': kafka_bootstrap,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # но так как group новый, начнёт с самого начала
        'enable.auto.commit': False,
    })
    c.subscribe(['warehouse-events-dlq'])
    yield c
    c.close()

@pytest.fixture(scope="session")
def kafka_producer(kafka_bootstrap):
    p = Producer({'bootstrap.servers': kafka_bootstrap})
    yield p
    p.flush()

@pytest.fixture(scope="session")
def sr_client(schema_registry_url):
    return SchemaRegistryClient({'url': schema_registry_url})

@pytest.fixture(scope="session")
def docker_client():
    return docker.from_env()

@pytest.fixture
def send_event(producer_url):
    def _send(event_type, data, event_id=None, timestamp=None):
        payload = {"event_type": event_type, "data": data}
        if event_id:
            payload["event_id"] = event_id
        if timestamp:
            payload["timestamp"] = timestamp.isoformat()
        resp = requests.post(f"{producer_url}/events", json=payload)
        assert resp.status_code == 200, f"Failed to send event: {resp.text}"
        return resp.json()
    return _send

@pytest.fixture(autouse=True)
def clear_cassandra(cassandra_session):
    tables = [
        "inventory_by_product_zone",
        "inventory_by_product",
        "inventory_by_zone",
        "product_last_ts",
        "processed_events",
        "orders",
        "inventory_metadata"
    ]
    for table in tables:
        cassandra_session.execute(f"TRUNCATE {table}")
    yield