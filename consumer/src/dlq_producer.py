from confluent_kafka import Producer
from config import config
import json

class DLQProducer:
    def __init__(self):
        self.producer = Producer({'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS})

    def send(self, message):
        self.producer.produce(config.DLQ_TOPIC, json.dumps(message).encode('utf-8'))
        self.producer.flush()
