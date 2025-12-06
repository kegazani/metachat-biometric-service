import json
import uuid
from datetime import datetime
from typing import Dict, Any, Optional
from confluent_kafka import Producer, KafkaException
import structlog

from src.config import Config

logger = structlog.get_logger()


class KafkaProducer:
    def __init__(self, config: Config):
        self.config = config
        
        self.producer_config = {
            'bootstrap.servers': ','.join(config.kafka_brokers),
            'client.id': f'{config.service_name}-producer',
            'acks': 'all',
            'retries': 3,
            'max.in.flight.requests.per.connection': 5,
            'compression.type': 'snappy',
            'linger.ms': 10,
            'batch.num.messages': 1000,
            'queue.buffering.max.messages': 10000,
            'queue.buffering.max.kbytes': 10240,
            'enable.idempotence': True
        }
        
        self.producer = None
        logger.info("KafkaProducer initialized")
    
    def start(self):
        if self.producer:
            logger.warning("Kafka producer is already started")
            return
        
        try:
            self.producer = Producer(self.producer_config)
            logger.info("Kafka producer started")
        except KafkaException as e:
            logger.error("Failed to start Kafka producer", error=str(e))
            raise
    
    def stop(self):
        if not self.producer:
            return
        
        try:
            remaining = self.producer.flush(timeout=10)
            if remaining > 0:
                logger.warning("Messages not delivered", count=remaining)
            logger.info("Kafka producer stopped")
        except Exception as e:
            logger.error("Error stopping Kafka producer", error=str(e))
    
    def publish_biometric_data_received(
        self,
        user_id: str,
        heart_rate: Optional[float] = None,
        sleep_data: Optional[Dict[str, Any]] = None,
        activity: Optional[Dict[str, Any]] = None,
        device_id: Optional[str] = None,
        timestamp: Optional[datetime] = None,
        correlation_id: Optional[str] = None
    ):
        if not self.producer:
            logger.error("Kafka producer is not started")
            return
        
        try:
            event_timestamp = timestamp or datetime.utcnow()
            event = {
                "event_id": str(uuid.uuid4()),
                "event_type": "BiometricDataReceived",
                "timestamp": event_timestamp.isoformat() + "Z",
                "correlation_id": correlation_id or str(uuid.uuid4()),
                "payload": {
                    "user_id": user_id,
                    "heart_rate": heart_rate,
                    "sleep_data": sleep_data,
                    "activity": activity,
                    "device_id": device_id,
                    "timestamp": event_timestamp.isoformat() + "Z"
                }
            }
            
            value = json.dumps(event).encode('utf-8')
            key = user_id.encode('utf-8')
            
            self.producer.produce(
                topic=self.config.biometric_data_received_topic,
                key=key,
                value=value,
                callback=self._delivery_callback
            )
            
            self.producer.poll(0)
            
            logger.debug("Published BiometricDataReceived event", user_id=user_id, device_id=device_id)
            
        except Exception as e:
            logger.error("Error publishing BiometricDataReceived event", error=str(e), exc_info=True)
    
    def _delivery_callback(self, err, msg):
        if err is not None:
            logger.error("Message delivery failed", error=str(err))
        else:
            logger.debug(
                "Message delivered",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset()
            )

