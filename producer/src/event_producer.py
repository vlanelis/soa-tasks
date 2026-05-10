import os
import json
import uuid
from datetime import datetime
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from config import settings

class EventProducer:
    def __init__(self):
        self.sr_client = SchemaRegistryClient({'url': settings.SCHEMA_REGISTRY_URL})
        self.producer = None
        self.topic = "warehouse-events"
        self._serializers = {}

    def start(self):
        self.producer = SerializingProducer({
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'key.serializer': StringSerializer('utf_8')
        })

    def _load_schema(self, event_type: str):
        file_name = event_type.lower() + '.avsc'
        schema_path = os.path.join(os.path.dirname(__file__), 'schemas', file_name)
        with open(schema_path, 'r') as f:
            schema_dict = json.load(f)
        return json.dumps(schema_dict)

    def _get_serializer(self, event_type: str):
        # Формируем уникальный subject для каждого типа события
        subject = f"{event_type}-value"
        if subject in self._serializers:
            return self._serializers[subject]

        schema_str = self._load_schema(event_type)
        # Стратегия именования: всегда возвращать заранее заданный subject
        serializer = AvroSerializer(
            self.sr_client,
            schema_str,
            conf={'subject.name.strategy': lambda ctx, schema_name: subject}
        )
        self._serializers[subject] = serializer
        return serializer

    async def send(self, event_type: str, data: dict, custom_event_id: str = None, custom_timestamp: datetime = None):
        event_id = custom_event_id or str(uuid.uuid4())
        timestamp = custom_timestamp or datetime.utcnow()
        event = {
            "event_id": event_id,
            "event_type": event_type,
            "timestamp": timestamp.isoformat(),
            "data": data
        }
        serializer = self._get_serializer(event_type)
        ctx = SerializationContext(self.topic, MessageField.VALUE)
        value = serializer(event, ctx)
        self.producer.produce(
            topic=self.topic,
            key=event_id,
            value=value,
            on_delivery=self._delivery_report
        )
        self.producer.flush()

    @staticmethod
    def _delivery_report(err, msg):
        if err:
            print(f"Delivery failed: {err}")
        else:
            print(f"Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def close(self):
        if self.producer:
            self.producer.flush()