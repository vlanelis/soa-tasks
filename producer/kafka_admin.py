import os
from confluent_kafka.admin import AdminClient, NewTopic

def create_topic():
    conf = {'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS')}
    admin = AdminClient(conf)
    topic = NewTopic(
        'movie-events',
        num_partitions=3,
        replication_factor=2,
        config={'min.insync.replicas': '1'}
    )
    fs = admin.create_topics([topic])
    for topic_name, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic_name} created")
        except Exception as e:
            print(f"Topic {topic_name} already exists or error: {e}")
