FROM openapitools/openapi-generator-cli:latest AS generator

WORKDIR /generator

COPY templates /templates
COPY openapi/openapi.yaml .

RUN openapi-generator-cli generate \
    -i openapi.yaml \
    -g python-fastapi \
    -o /generated \
    -t /templates \
    --additional-properties=packageName=marketplace \
    --global-property=models,apis


FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --from=generator /generated/src/marketplace/ /app/marketplace/
COPY . .

ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["uvicorn", "marketplace.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]