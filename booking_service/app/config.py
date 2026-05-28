import os

DB_URL = os.getenv("DB_URL", "postgresql://postgres:postgres@booking-db:5432/booking")
FLIGHT_GRPC_TARGET = os.getenv("FLIGHT_GRPC_TARGET", "flight-service:50051")
SERVICE_API_KEY = os.getenv("SERVICE_API_KEY", "secret-service-key")

BREAKER_FAIL_MAX = os.getenv("BREAKER_FAIL_MAX", 5)
BREAKER_RESET_TIMEOUT = os.getenv("BREAKER_RESET_TIMEOUT", 60)
