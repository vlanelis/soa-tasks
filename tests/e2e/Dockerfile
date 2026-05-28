FROM python:3.13-slim
WORKDIR /e2e

COPY tests/e2e/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto /proto
RUN python -m grpc_tools.protoc -I/proto --python_out=. --grpc_python_out=. /proto/flight.proto

COPY tests/e2e/ .

CMD ["python", "-m", "pytest", "-v"]