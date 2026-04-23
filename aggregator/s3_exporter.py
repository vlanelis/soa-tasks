import boto3
from botocore.client import Config
import pandas as pd
from io import BytesIO
from datetime import date
import logging

logger = logging.getLogger(__name__)


class S3Exporter:
    def __init__(self, endpoint, access_key, secret_key, bucket):
        self.client = boto3.client(
            's3',
            endpoint_url=f'http://{endpoint}',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4')
        )
        self.bucket = bucket

    def export_metrics(self, target_date: date, metrics: dict, top_movies: list):
        # top_movies: list of (movie_id, views)
        df = pd.DataFrame([{
            'date': target_date,
            'dau': metrics['dau'],
            'avg_watch_time_sec': metrics['avg_watch_time'],
            'conversion_rate': metrics['conversion'],
            'retention_day1': metrics['retention_d1'],
            'retention_day7': metrics['retention_d7'],
            'computed_at': pd.Timestamp.now()
        }])
        # Добавляем top movies как список словарей
        movies_with_rank = [{'rank': i + 1, 'movie_id': movie_id, 'views': views}
                            for i, (movie_id, views) in enumerate(top_movies)]
        df['top_movies'] = [movies_with_rank]

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        key = f"daily/{target_date}/aggregates.parquet"
        self.client.put_object(Bucket=self.bucket, Key=key, Body=buffer)
        logger.info(f"Exported metrics to s3://{self.bucket}/{key}")