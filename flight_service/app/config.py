import os

DB_URL = os.getenv("DB_URL", "postgresql://postgres:postgres@flight-db:5432/flight")
SERVICE_API_KEY = os.getenv("SERVICE_API_KEY", "secret-service-key")

REDIS_SENTINEL_HOST = os.getenv("REDIS_SENTINEL_HOST", "redis-sentinel")
REDIS_SENTINEL_PORT = int(os.getenv("REDIS_SENTINEL_PORT", 26379))
REDIS_MASTER_NAME = os.getenv("REDIS_MASTER_NAME", "mymaster")

GRPC_PORT = 50051
