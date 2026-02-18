FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

COPY main.py .
EXPOSE 8000
CMD ["python", "main.py"]
