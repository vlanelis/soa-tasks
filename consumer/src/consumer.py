import threading
import logging
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from processor import EventProcessor
from dlq_producer import DLQProducer
from metrics import events_processed, cassandra_write_errors, consumer_lag
from config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaConsumerService:
    def __init__(self):
        self.topic = "warehouse-events"
        self.conf = {
            'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': config.CONSUMER_GROUP,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
        self.sr_client = SchemaRegistryClient({'url': config.SCHEMA_REGISTRY_URL})
        self.deserializer = AvroDeserializer(self.sr_client)
        self.consumer = Consumer(self.conf)
        self.processor = EventProcessor()
        self.dlq_producer = DLQProducer()
        self.lag_updater_started = False

    def _wait_for_topic(self, topic, max_retries=30, delay=2):
        admin = AdminClient({'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS})
        for attempt in range(max_retries):
            metadata = admin.list_topics(timeout=5)
            if topic in metadata.topics:
                logger.info(f"Topic {topic} exists")
                return True
            logger.info(f"Waiting for topic {topic}... attempt {attempt+1}/{max_retries}")
            time.sleep(delay)
        raise RuntimeError(f"Topic {topic} not found after {max_retries} attempts")

    def run(self):
        self._wait_for_topic('warehouse-events')
        self._wait_for_topic(config.DLQ_TOPIC)

        self.consumer.subscribe(['warehouse-events'])
        logger.info("Consumer started")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())
                try:
                    ctx = SerializationContext(msg.topic(), MessageField.VALUE)
                    event = self.deserializer(msg.value(), ctx)
                except Exception as e:
                    logger.error(f"Deserialization error: {e}")
                    self._send_to_dlq(msg, str(e))
                    self.consumer.commit(msg)
                    continue

                try:
                    success = self.processor.process(event, msg)
                    if success:
                        events_processed.labels(event_type=event['event_type']).inc()
                        self.consumer.commit(msg)
                        logger.info(f"Processed {event['event_id']} offset {msg.offset()}")
                        if not self.lag_updater_started:
                            self.lag_updater_started = True
                            threading.Thread(target=self._lag_updater_loop, daemon=True).start()
                    else:
                        self.consumer.commit(msg)
                except Exception as e:
                    logger.exception(f"Processing error: {e}")
                    self._send_to_dlq(msg, str(e), event)
                    self.consumer.commit(msg)
        finally:
            self.consumer.close()

    def _lag_updater_loop(self):
        while True:
            try:
                self._update_lag_metrics()
            except Exception:
                pass
            time.sleep(15)

    def _send_to_dlq(self, msg, error_reason, event=None):
        dlq_message = {
            "original_event": event if event else msg.value().decode('utf-8'),
            "error_reason": error_reason,
            "failed_at": datetime.utcnow().isoformat(),
            "kafka_metadata": {
                "partition": msg.partition(),
                "offset": msg.offset()
            }
        }
        self.dlq_producer.send(dlq_message)
        cassandra_write_errors.inc()

    def _update_lag_metrics(self):
        admin = AdminClient({'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS})
        try:
            metadata = admin.list_topics(timeout=10)
            topic_metadata = metadata.topics.get(self.topic)
            if not topic_metadata:
                return
            partitions = [TopicPartition(self.topic, p) for p in topic_metadata.partitions.keys()]
            committed = self.consumer.committed(partitions, timeout=10)
            watermarks = {}
            for tp in partitions:
                low, high = self.consumer.get_watermark_offsets(tp, timeout=10)
                watermarks[tp.partition] = high
            for comm in committed:
                if comm and comm.offset >= 0:
                    lag = watermarks.get(comm.partition, 0) - comm.offset
                    consumer_lag.labels(partition=str(comm.partition)).set(max(lag, 0))
        except Exception as e:
            logger.warning(f"Failed to update lag metrics (group may be initializing): {e}")
