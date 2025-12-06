from fastapi import FastAPI
from contextlib import asynccontextmanager
import structlog

from src.config import Config
from src.infrastructure.database import Database
from src.infrastructure.repository import BiometricRepository
from src.infrastructure.kafka_client import KafkaProducer
from src.api.routes import router
from src.api.state import app_state

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = Config()
    
    db = Database(config)
    await db.create_database_if_not_exists()
    await db.create_tables()
    
    repository = BiometricRepository(db)
    kafka_producer = KafkaProducer(config)
    kafka_producer.start()
    
    app_state["config"] = config
    app_state["db"] = db
    app_state["repository"] = repository
    app_state["kafka_producer"] = kafka_producer
    
    logger.info("Biometric Service started")
    yield
    
    kafka_producer.stop()
    await db.close()
    logger.info("Biometric Service stopped")


app = FastAPI(
    title="Biometric Service",
    version="1.0.0",
    lifespan=lifespan
)

app.include_router(router)


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "biometric-service"}

