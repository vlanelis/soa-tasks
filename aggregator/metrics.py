from clickhouse_driver import Client
from datetime import date
import logging
import math

logger = logging.getLogger(__name__)

class ClickHouseMetrics:
    def __init__(self, host, port):
        self.client = Client(host=host, port=port, password='clickhouse')

    def get_dau(self, target_date: date) -> int:
        query = "SELECT uniq(user_id) FROM movie_events WHERE toDate(timestamp) = %(date)s"
        result = self.client.execute(query, {'date': target_date})
        return result[0][0] if result else 0

    def get_avg_watch_time(self, target_date: date) -> float:
        query = "SELECT avg(progress_seconds) FROM movie_events WHERE event_type = 'VIEW_FINISHED' AND toDate(timestamp) = %(date)s"
        result = self.client.execute(query, {'date': target_date})
        return result[0][0] or 0.0

    def get_top_movies(self, target_date: date, limit=10):
        query = """
            SELECT movie_id, count() AS views
            FROM movie_events
            WHERE event_type = 'VIEW_FINISHED' AND toDate(timestamp) = %(date)s
            GROUP BY movie_id ORDER BY views DESC LIMIT %(limit)s
        """
        return self.client.execute(query, {'date': target_date, 'limit': limit})

    def get_conversion_rate(self, target_date: date) -> float:
        query = """
            SELECT countIf(event_type = 'VIEW_FINISHED') / countIf(event_type = 'VIEW_STARTED')
            FROM movie_events
            WHERE toDate(timestamp) = %(date)s
        """
        result = self.client.execute(query, {'date': target_date})
        val = result[0][0]
        if val is None or math.isnan(val):
            return 0.0
        return val

    def get_retention(self, cohort_date: date):
        # Returns (new_users, d1_retained, d7_retained)
        query = """
            WITH first_activity AS (
                SELECT user_id, min(toDate(timestamp)) AS cohort_day
                FROM movie_events
                GROUP BY user_id
            ),
            user_activity AS (
                SELECT user_id, toDate(timestamp) AS activity_day
                FROM movie_events
                GROUP BY user_id, activity_day
            )
            SELECT
                count(DISTINCT if(activity_day = cohort_day, user_id, NULL)) AS new_users,
                count(DISTINCT if(activity_day = cohort_day + INTERVAL 1 DAY, user_id, NULL)) AS d1_retained,
                count(DISTINCT if(activity_day = cohort_day + INTERVAL 7 DAY, user_id, NULL)) AS d7_retained
            FROM first_activity
            LEFT JOIN user_activity USING (user_id)
            WHERE cohort_day = %(date)s
            GROUP BY cohort_day
        """
        result = self.client.execute(query, {'date': cohort_date})
        if result:
            return result[0][0], result[0][1], result[0][2]
        return 0, 0, 0
