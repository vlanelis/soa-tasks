import psycopg2
from psycopg2.extras import execute_values
from datetime import date
import logging

logger = logging.getLogger(__name__)

class PostgresWriter:
    def __init__(self, host, port, db, user, password):
        self.conn = psycopg2.connect(
            host=host, port=port, dbname=db, user=user, password=password
        )
        self.conn.autocommit = False

    def upsert_metric(self, metric_date: date, metric_name: str, value: float):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO daily_metrics (metric_date, metric_name, metric_value)
                VALUES (%s, %s, %s)
                ON CONFLICT (metric_date, metric_name)
                DO UPDATE SET metric_value = EXCLUDED.metric_value, computed_at = CURRENT_TIMESTAMP
            """, (metric_date, metric_name, value))
        self.conn.commit()

    def upsert_top_movies(self, metric_date: date, movies: list):
        # movies: list of (rank, movie_id, views)
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM top_movies WHERE metric_date = %s", (metric_date,))
            execute_values(cur, """
                INSERT INTO top_movies (metric_date, rank, movie_id, views)
                VALUES %s
            """, [(metric_date, rank, movie_id, views) for rank, (movie_id, views) in enumerate(movies, 1)])
        self.conn.commit()

    def close(self):
        self.conn.close()
