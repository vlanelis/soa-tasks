import os
import uuid
import logging
import random
import threading
import asyncio
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, validator
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from tenacity import retry, stop_after_attempt, wait_exponential
import dotenv

from kafka_admin import create_topic

dotenv.load_dotenv()
logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'))
logger = logging.getLogger(__name__)

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL')
TOPIC = 'movie-events'
ENABLE_GENERATOR = os.getenv('ENABLE_EVENT_GENERATOR', 'false').lower() == 'true'

# --- Avro schema loading ---
with open('schemas/movie_event.avsc', 'r') as f:
    AVRO_SCHEMA_STR = f.read()

schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
avro_serializer = AvroSerializer(schema_registry_client, AVRO_SCHEMA_STR)

producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'acks': 'all',
    'retries': 3,
}
producer = Producer(producer_conf)

# --- Pydantic models ---
class EventType(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"

class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"

class EventCreate(BaseModel):
    event_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    movie_id: str
    event_type: EventType
    timestamp: Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))
    device_type: DeviceType
    session_id: str
    progress_seconds: Optional[int] = None

    @validator('progress_seconds')
    def validate_progress(cls, v, values):
        if values.get('event_type') in ('VIEW_STARTED', 'VIEW_FINISHED', 'VIEW_PAUSED', 'VIEW_RESUMED') and v is None:
            raise ValueError('progress_seconds required for view events')
        return v

app = FastAPI()

def delivery_report(err, msg):
    if err:
        logger.error(f'Delivery failed: {err}')
    else:
        logger.info(f'Event delivered: {msg.key().decode()}')

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, max=2))
def publish_to_kafka(key, value):
    producer.produce(TOPIC, key=key, value=value, on_delivery=delivery_report)
    producer.flush()

def send_event_internal(event: EventCreate):
    event_dict = event.dict()
    event_dict['timestamp'] = int(event_dict['timestamp'].timestamp() * 1000)
    serialized_value = avro_serializer(event_dict, SerializationContext(TOPIC, MessageField.VALUE))
    key = event.user_id.encode('utf-8')
    publish_to_kafka(key, serialized_value)
    logger.info(
        f"Event published: event_id={event.event_id}, event_type={event.event_type}, timestamp={event.timestamp}, user_id={event.user_id}")
    return event.event_id

@app.post("/event")
async def send_event(event: EventCreate):
    try:
        event_id = send_event_internal(event)
        return {"event_id": event_id}
    except Exception as e:
        logger.exception("Failed to publish event")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health():
    return {"status": "ok"}

USERS = [f"user_{i}" for i in range(1, 201)]          # 200 пользователей
MOVIES = [f"movie_{i}" for i in range(1, 51)]          # 50 фильмов
DEVICES = ["MOBILE", "DESKTOP", "TV", "TABLET"]
SESSION_DURATION_SEC = random.uniform(1800, 7200)      # длительность фильма от 30 до 120 минут

# Хранилище активных сессий просмотра
# key: (user_id, movie_id, session_id) -> {start_time, progress, last_event_time}
active_sessions = {}

def random_date_in_range(start_date: datetime, end_date: datetime) -> datetime:
    """Случайная дата между start_date и end_date"""
    delta = end_date - start_date
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start_date + timedelta(seconds=random_seconds)

def generate_history_for_day(target_date: datetime, events_target: int = 500):
    """
    Генерирует события за один конкретный день (target_date).
    Создаёт реалистичные сессии просмотра и дополнительные события (лайки, поиски).
    """
    events = []
    # Для каждого пользователя может быть 0..3 сессии в этот день
    users_for_day = random.sample(USERS, min(len(USERS), random.randint(50, 150)))
    for user_id in users_for_day:
        num_sessions = random.randint(0, 3)
        for _ in range(num_sessions):
            movie_id = random.choice(MOVIES)
            session_id = str(uuid.uuid4())
            device = random.choice(DEVICES)
            # Время начала сессии (случайный момент в течение дня)
            session_start = random_date_in_range(
                target_date.replace(hour=0, minute=0, second=0),
                target_date.replace(hour=23, minute=59, second=59)
            )
            # VIEW_STARTED
            events.append(EventCreate(
                event_id=str(uuid.uuid4()),
                user_id=user_id,
                movie_id=movie_id,
                event_type=EventType.VIEW_STARTED,
                timestamp=session_start,
                device_type=DeviceType(device),
                session_id=session_id,
                progress_seconds=0
            ))
            # Прогресс растёт
            progress = 0
            last_time = session_start
            # Несколько пауз и возобновлений (опционально)
            num_pauses = random.randint(0, 2)
            for i in range(num_pauses):
                pause_duration = random.randint(30, 300)
                progress += random.randint(60, 900)
                if progress > SESSION_DURATION_SEC:
                    progress = int(SESSION_DURATION_SEC)
                pause_time = last_time + timedelta(seconds=random.randint(10, 120))
                if pause_time > target_date + timedelta(days=1):
                    break
                # VIEW_PAUSED
                events.append(EventCreate(
                    event_id=str(uuid.uuid4()),
                    user_id=user_id,
                    movie_id=movie_id,
                    event_type=EventType.VIEW_PAUSED,
                    timestamp=pause_time,
                    device_type=DeviceType(device),
                    session_id=session_id,
                    progress_seconds=progress
                ))
                # Пропускаем время паузы
                resume_time = pause_time + timedelta(seconds=pause_duration)
                if resume_time > target_date + timedelta(days=1):
                    break
                progress += random.randint(0, 300)
                if progress > SESSION_DURATION_SEC:
                    progress = int(SESSION_DURATION_SEC)
                # VIEW_RESUMED
                events.append(EventCreate(
                    event_id=str(uuid.uuid4()),
                    user_id=user_id,
                    movie_id=movie_id,
                    event_type=EventType.VIEW_RESUMED,
                    timestamp=resume_time,
                    device_type=DeviceType(device),
                    session_id=session_id,
                    progress_seconds=progress
                ))
                last_time = resume_time
            # VIEW_FINISHED
            finish_time = last_time + timedelta(seconds=random.randint(30, 600))
            if finish_time <= target_date + timedelta(days=1):
                events.append(EventCreate(
                    event_id=str(uuid.uuid4()),
                    user_id=user_id,
                    movie_id=movie_id,
                    event_type=EventType.VIEW_FINISHED,
                    timestamp=finish_time,
                    device_type=DeviceType(device),
                    session_id=session_id,
                    progress_seconds=int(SESSION_DURATION_SEC)
                ))
        # Дополнительные лайки и поиски (не связанные с сессиями)
        num_likes = random.randint(0, 2)
        for _ in range(num_likes):
            like_time = random_date_in_range(
                target_date.replace(hour=0, minute=0, second=0),
                target_date.replace(hour=23, minute=59, second=59)
            )
            events.append(EventCreate(
                event_id=str(uuid.uuid4()),
                user_id=user_id,
                movie_id=random.choice(MOVIES),
                event_type=EventType.LIKED,
                timestamp=like_time,
                device_type=DeviceType(random.choice(DEVICES)),
                session_id=str(uuid.uuid4()),
                progress_seconds=None
            ))
        num_searches = random.randint(0, 1)
        for _ in range(num_searches):
            search_time = random_date_in_range(
                target_date.replace(hour=0, minute=0, second=0),
                target_date.replace(hour=23, minute=59, second=59)
            )
            events.append(EventCreate(
                event_id=str(uuid.uuid4()),
                user_id=user_id,
                movie_id="",                         # поиск не привязан к фильму
                event_type=EventType.SEARCHED,
                timestamp=search_time,
                device_type=DeviceType(random.choice(DEVICES)),
                session_id=str(uuid.uuid4()),
                progress_seconds=None
            ))
    return events

async def generate_historical_events(days_back: int = 7, events_per_day: int = 500):
    """Генерирует события за последние `days_back` дней (включая сегодня)"""
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    for day_offset in range(days_back, -1, -1):
        date = today - timedelta(days=day_offset)
        logger.info(f"Generating historical events for {date.date()}")
        events = generate_history_for_day(date, events_per_day)
        for event in events:
            try:
                send_event_internal(event)
            except Exception as e:
                logger.error(f"Failed to send historical event: {e}")
            # Небольшая задержка, чтобы не перегружать Kafka
            await asyncio.sleep(0.001)
        logger.info(f"Generated {len(events)} events for {date.date()}")

async def generate_real_time_events(delay: float = 0.5):
    """Генерирует события в реальном времени (текущий момент) с правильными последовательностями"""
    while True:
        user_id = random.choice(USERS)
        movie_id = random.choice(MOVIES)
        device = random.choice(DEVICES)
        session_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        # С вероятностью 70% начинаем новый просмотр, иначе – случайное действие (лайк/поиск)
        if random.random() < 0.7:
            # ---- Серия просмотра ----
            # VIEW_STARTED
            start_event = EventCreate(
                event_id=str(uuid.uuid4()),
                user_id=user_id,
                movie_id=movie_id,
                event_type=EventType.VIEW_STARTED,
                timestamp=now,
                device_type=DeviceType(device),
                session_id=session_id,
                progress_seconds=0
            )
            send_event_internal(start_event)
            await asyncio.sleep(random.uniform(5, 30))
            # Прогресс растёт
            progress = random.randint(60, 1800)
            # VIEW_PAUSED (опционально)
            if random.random() < 0.3:
                pause_time = datetime.now(timezone.utc)
                pause_event = EventCreate(
                    event_id=str(uuid.uuid4()),
                    user_id=user_id,
                    movie_id=movie_id,
                    event_type=EventType.VIEW_PAUSED,
                    timestamp=pause_time,
                    device_type=DeviceType(device),
                    session_id=session_id,
                    progress_seconds=progress
                )
                send_event_internal(pause_event)
                await asyncio.sleep(random.uniform(10, 60))
                resume_time = datetime.now(timezone.utc)
                progress += random.randint(30, 600)
                resume_event = EventCreate(
                    event_id=str(uuid.uuid4()),
                    user_id=user_id,
                    movie_id=movie_id,
                    event_type=EventType.VIEW_RESUMED,
                    timestamp=resume_time,
                    device_type=DeviceType(device),
                    session_id=session_id,
                    progress_seconds=progress
                )
                send_event_internal(resume_event)
                await asyncio.sleep(random.uniform(5, 15))
            # VIEW_FINISHED
            finish_event = EventCreate(
                event_id=str(uuid.uuid4()),
                user_id=user_id,
                movie_id=movie_id,
                event_type=EventType.VIEW_FINISHED,
                timestamp=datetime.now(timezone.utc),
                device_type=DeviceType(device),
                session_id=session_id,
                progress_seconds=random.randint(int(SESSION_DURATION_SEC * 0.9), int(SESSION_DURATION_SEC))
            )
            send_event_internal(finish_event)
        else:
            # ---- Лайк или поиск ----
            event_type = random.choice([EventType.LIKED, EventType.SEARCHED])
            event = EventCreate(
                event_id=str(uuid.uuid4()),
                user_id=user_id,
                movie_id=movie_id if event_type == EventType.LIKED else "",
                event_type=event_type,
                timestamp=now,
                device_type=DeviceType(device),
                session_id=session_id,
                progress_seconds=None
            )
            send_event_internal(event)
        await asyncio.sleep(delay)

async def run_generator():
    logger.info("Starting historical event generation (last 7 days)...")
    await generate_historical_events(days_back=7, events_per_day=500)   # ~3500 событий
    logger.info("Historical generation complete. Switching to real-time mode.")
    await generate_real_time_events(delay=0.5)

def start_generator():
    asyncio.run(run_generator())


try:
    create_topic()
    logger.info("Kafka topic 'movie-events' ensured (3 partitions, replication factor 2)")
except Exception as e:
    logger.error(f"Failed to create/verify topic: {e}")


if ENABLE_GENERATOR:
    logger.info("Starting event generator thread")
    generator_thread = threading.Thread(target=start_generator, daemon=True)
    generator_thread.start()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)