import os
import pytest
import requests
import time
import uuid
from datetime import datetime, timezone
from clickhouse_driver import Client

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://localhost:8000")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")

def test_event_pipeline():
    # Generate unique event
    event_id = str(uuid.uuid4())
    event = {
        "event_id": event_id,
        "user_id": "test_user",
        "movie_id": "test_movie",
        "event_type": "VIEW_STARTED",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device_type": "DESKTOP",
        "session_id": str(uuid.uuid4()),
        "progress_seconds": 0
    }
    # Send via producer
    resp = requests.post(f"{PRODUCER_URL}/event", json=event)
    assert resp.status_code == 200
    assert resp.json()["event_id"] == event_id

    # Wait for ingestion into ClickHouse
    ch = Client(host=CLICKHOUSE_HOST, port=9000, password='clickhouse')
    for _ in range(15):
        result = ch.execute("SELECT count() FROM movie_events WHERE event_id = %(id)s", {'id': event_id})
        if result[0][0] == 1:
            break
        time.sleep(2)
    else:
        pytest.fail("Event not found in ClickHouse after 30 seconds")

    # Verify fields
    row = ch.execute("SELECT * FROM movie_events WHERE event_id = %(id)s", {'id': event_id})[0]
    assert row[1] == "test_user"
    assert row[2] == "test_movie"
    assert row[3] == "VIEW_STARTED"
