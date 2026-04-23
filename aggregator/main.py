import math
import logging
import os
from datetime import datetime, timedelta, date
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
import dotenv

dotenv.load_dotenv()
logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'))
logger = logging.getLogger(__name__)

from config import *
from metrics import ClickHouseMetrics
from postgres_writer import PostgresWriter
from s3_exporter import S3Exporter

ch_client = ClickHouseMetrics(CLICKHOUSE_HOST, CLICKHOUSE_PORT)
pg_writer = PostgresWriter(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD)
s3_exporter = S3Exporter(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET)


def aggregate_for_date(target_date: date):
    logger.info(f"Starting aggregation for {target_date}")
    start = datetime.now()
    try:
        dau = ch_client.get_dau(target_date)
        avg_watch = ch_client.get_avg_watch_time(target_date)
        conversion = ch_client.get_conversion_rate(target_date)
        if math.isnan(conversion):
            conversion = 0.0
        top_movies = ch_client.get_top_movies(target_date)
        new_users, d1_ret, d7_ret = ch_client.get_retention(target_date)

        metrics = {
            'dau': dau,
            'avg_watch_time': avg_watch,
            'conversion': conversion,
            'retention_d1': d1_ret / new_users if new_users else 0,
            'retention_d7': d7_ret / new_users if new_users else 0,
        }
        # Save to PostgreSQL
        pg_writer.upsert_metric(target_date, 'DAU', metrics['dau'])
        pg_writer.upsert_metric(target_date, 'avg_watch_time', metrics['avg_watch_time'])
        pg_writer.upsert_metric(target_date, 'conversion_rate', metrics['conversion'])
        pg_writer.upsert_metric(target_date, 'retention_day1', metrics['retention_d1'])
        pg_writer.upsert_metric(target_date, 'retention_day7', metrics['retention_d7'])
        pg_writer.upsert_top_movies(target_date, top_movies)

        # Export to S3
        s3_exporter.export_metrics(target_date, metrics, top_movies)

        duration = (datetime.now() - start).total_seconds()
        logger.info(f"Aggregation for {target_date} completed in {duration:.2f}s. Metrics: {metrics}")
    except Exception as e:
        logger.exception(f"Aggregation failed for {target_date}: {e}")


def scheduled_aggregation():
    # Run for yesterday by default
    yesterday = date.today() - timedelta(days=1)
    aggregate_for_date(yesterday)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start scheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(scheduled_aggregation, 'interval', minutes=AGGREGATION_INTERVAL_MINUTES)
    scheduler.start()
    logger.info(f"Scheduler started, interval {AGGREGATION_INTERVAL_MINUTES} minutes")
    yield
    scheduler.shutdown()


app = FastAPI(lifespan=lifespan)


@app.post("/aggregate")
async def manual_aggregate(date_str: str):
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format, use YYYY-MM-DD")
    aggregate_for_date(target_date)
    return {"status": "ok", "date": date_str}


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)