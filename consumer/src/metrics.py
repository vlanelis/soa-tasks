from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi import FastAPI, Response
from health import router as health_router

metrics_app = FastAPI()
metrics_app.include_router(health_router)

events_processed = Counter('events_processed_total', 'Total processed events', ['event_type'])
event_processing_duration = Histogram('event_processing_duration_seconds', 'Processing time')
cassandra_write_errors = Counter('cassandra_write_errors_total', 'Cassandra write errors')
consumer_lag = Gauge('consumer_lag', 'Consumer lag per partition', ['partition'])

@metrics_app.get("/metrics")
async def get_metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
