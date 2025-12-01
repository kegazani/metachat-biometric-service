from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False)
    
    service_name: str = "biometric-service"
    log_level: str = "INFO"
    http_port: int = 8003
    http_host: str = "0.0.0.0"
    
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/metachat_biometric"
    kafka_brokers: List[str] = ["localhost:9092"]
    biometric_data_received_topic: str = "metachat.biometric.data.received"

