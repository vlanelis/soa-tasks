FROM python:3.13-slim

WORKDIR /app

COPY booking_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY proto /app/proto
COPY booking_service /app/booking_service

WORKDIR /app/booking_service

RUN python -m grpc_tools.protoc -I../proto --python_out=. --grpc_python_out=. ../proto/flight.proto

CMD ["bash", "-lc", "alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000 --log-level info"]
