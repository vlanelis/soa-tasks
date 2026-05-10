from fastapi import APIRouter, Response
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from confluent_kafka import Consumer
from config import config
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/health")
async def health():
    # Проверка Cassandra
    try:
        cluster = Cluster(
            config.CASSANDRA_CONTACT_POINTS,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
            protocol_version=5,
            connect_timeout=5
        )
        session = cluster.connect()
        # Проверяем, что keyspace существует и доступен
        session.execute(f"SELECT * FROM {config.CASSANDRA_KEYSPACE}.inventory_by_product_zone LIMIT 1")
        cluster.shutdown()
    except Exception as e:
        logger.error(f"Cassandra health check failed: {e}")
        return Response("Cassandra unavailable", status_code=503)

    # Проверка Kafka
    try:
        consumer = Consumer({
            'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'health-check',
            'session.timeout.ms': 3000
        })
        consumer.list_topics(timeout=5)
        consumer.close()
    except Exception as e:
        logger.error(f"Kafka health check failed: {e}")
        return Response("Kafka unavailable", status_code=503)

    return Response("OK", status_code=200)