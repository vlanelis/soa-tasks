import uvicorn
from fastapi import FastAPI, HTTPException
from event_producer import EventProducer
from config import settings
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
from datetime import datetime
from contextlib import asynccontextmanager
from confluent_kafka.admin import AdminClient, NewTopic

def create_topics():
    admin_client = AdminClient({'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS})
    topics = ['warehouse-events', 'warehouse-events-dlq']
    existing = admin_client.list_topics().topics
    new_topics = []
    for topic in topics:
        if topic not in existing:
            new_topics.append(NewTopic(topic, num_partitions=3, replication_factor=1))
    if new_topics:
        fs = admin_client.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Topic {topic} created")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")
    else:
        print("Topics already exist")

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Creating Kafka topics...")
    create_topics()
    print("Starting producer...")
    app.state.producer = EventProducer()
    app.state.producer.start()
    yield
    print("Stopping producer...")
    app.state.producer.close()

app = FastAPI(title="WMS Producer", lifespan=lifespan)

class EventRequest(BaseModel):
    event_type: str
    data: Dict[str, Any]
    event_id: Optional[str] = Field(default=None)
    timestamp: Optional[datetime] = Field(default=None)

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/events")
async def send_event(event: EventRequest):
    await app.state.producer.send(
        event.event_type,
        event.data,
        custom_event_id=event.event_id,
        custom_timestamp=event.timestamp
    )
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=settings.PRODUCER_PORT)