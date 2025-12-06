import os
import yaml
from pathlib import Path
from typing import List
from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def load_yaml_config(config_path: str = "config.yaml") -> dict:
    config_file = Path(config_path)
    if not config_file.exists():
        config_file = Path(__file__).parent.parent / config_path
    
    if config_file.exists():
        with open(config_file, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    return {}


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        protected_namespaces=(),
        extra="allow"
    )
    
    def __init__(self, **kwargs):
        yaml_config = load_yaml_config()
        
        if yaml_config:
            service_config = yaml_config.get("service", {})
            server_config = yaml_config.get("server", {})
            database_config = yaml_config.get("database", {})
            kafka_config = yaml_config.get("kafka", {})
            
            kwargs.setdefault("service_name", service_config.get("name", "biometric-service"))
            kwargs.setdefault("log_level", service_config.get("log_level", "INFO"))
            kwargs.setdefault("http_port", server_config.get("http_port", 8003))
            kwargs.setdefault("http_host", server_config.get("http_host", "0.0.0.0"))
            kwargs.setdefault("database_url", database_config.get("url", "postgresql+asyncpg://postgres:postgres@localhost:5432/metachat_biometric"))
            kwargs.setdefault("database_pool_size", database_config.get("pool_size", 10))
            kwargs.setdefault("database_max_overflow", database_config.get("max_overflow", 20))
            kwargs.setdefault("kafka_brokers", kafka_config.get("brokers", ["localhost:9092"]))
            
            topics = kafka_config.get("topics", {})
            kwargs.setdefault("biometric_data_received_topic", topics.get("biometric_data_received", "metachat.biometric.data.received"))
        
        super().__init__(**kwargs)
    
    service_name: str = "biometric-service"
    log_level: str = "INFO"
    http_port: int = 8003
    http_host: str = "0.0.0.0"
    
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/metachat_biometric"
    database_pool_size: int = 10
    database_max_overflow: int = 20
    
    kafka_brokers: List[str] = ["localhost:9092"]
    biometric_data_received_topic: str = "metachat.biometric.data.received"
    
    @model_validator(mode='after')
    def fix_localhost_addresses(self):
        if "localhost" in self.database_url:
            object.__setattr__(self, 'database_url', self.database_url.replace("localhost", "127.0.0.1"))
        
        if self.kafka_brokers:
            new_brokers = [broker.replace("localhost", "127.0.0.1") if "localhost" in broker else broker for broker in self.kafka_brokers]
            object.__setattr__(self, 'kafka_brokers', new_brokers)
        
        return self
    
    all_kafka_topics: dict = {
        "user_service": ["metachat-user-events"],
        "diary_service": [
            "diary-events",
            "session-events",
            "metachat.diary.entry.created",
            "metachat.diary.entry.updated",
            "metachat.diary.entry.deleted"
        ],
        "mood_analysis_service": [
            "metachat.diary.entry.created",
            "metachat.diary.entry.updated",
            "metachat.mood.analyzed",
            "metachat.mood.analysis.failed"
        ],
        "analytics_service": [
            "metachat.mood.analyzed",
            "metachat.diary.entry.created",
            "metachat.diary.entry.deleted",
            "metachat.archetype.updated"
        ],
        "archetype_service": [
            "metachat.mood.analyzed",
            "metachat.diary.entry.created",
            "metachat.archetype.assigned",
            "metachat.archetype.updated",
            "metachat.archetype.calculation.triggered"
        ],
        "biometric_service": ["metachat.biometric.data.received"],
        "correlation_service": [
            "metachat.mood.analyzed",
            "metachat.biometric.data.received",
            "metachat.correlation.discovered"
        ]
    }

