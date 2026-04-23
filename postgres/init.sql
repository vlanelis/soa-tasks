CREATE TABLE IF NOT EXISTS daily_metrics (
    metric_date DATE NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    metric_value FLOAT NOT NULL,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (metric_date, metric_name)
);

CREATE TABLE IF NOT EXISTS top_movies (
    metric_date DATE NOT NULL,
    rank INTEGER NOT NULL,
    movie_id VARCHAR(100) NOT NULL,
    views INTEGER NOT NULL,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (metric_date, rank)
);