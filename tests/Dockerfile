FROM python:3.13-slim
WORKDIR /tests
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["pytest", "-v", "--tb=short"]