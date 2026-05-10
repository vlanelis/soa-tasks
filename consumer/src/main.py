import threading
import uvicorn
from fastapi import FastAPI
from consumer import KafkaConsumerService
from health import router as health_router
from metrics import metrics_app
from config import Config

app = FastAPI()
app.include_router(health_router)

def start_consumer():
    consumer = KafkaConsumerService()
    consumer.run()

if __name__ == "__main__":
    t = threading.Thread(target=start_consumer, daemon=True)
    t.start()
    uvicorn.run(metrics_app, host="0.0.0.0", port=Config.METRICS_PORT)
