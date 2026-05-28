FROM python:3.13-slim
WORKDIR /tests

COPY tests/integration/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto /proto
RUN python -m grpc_tools.protoc -I/proto --python_out=. --grpc_python_out=. /proto/flight.proto

COPY tests/integration/ .

CMD ["python", "-m", "pytest", "-v"]