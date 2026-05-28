FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
RUN GRPC_HEALTH_PROBE_VERSION=v0.4.24 && \
    curl -sSL -o /bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

COPY flight_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY proto /app/proto
COPY flight_service /app/flight_service

WORKDIR /app/flight_service

RUN python -m grpc_tools.protoc -I../proto --python_out=. --grpc_python_out=. ../proto/flight.proto

CMD ["bash", "-lc", "alembic upgrade head && python -u -m app.server"]
