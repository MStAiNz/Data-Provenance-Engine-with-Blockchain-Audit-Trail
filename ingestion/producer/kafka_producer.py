import json
from confluent_kafka import Producer, KafkaError, KafkaException
from typing import Any, Dict, Optional
from confluent_kafka.admin import AdminClient, NewTopic
from config.kafka_config import KafkaConfig
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaProducer:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or KafkaConfig.get_producer_config()
        self.producer = Producer(self.config)
        self.message_counter = 0
        self.error_counter = 0
    def delivery_report(self, err: Optional[KafkaError], msg: Any) -> None:
        if err is not None:
            self.error_counter += 1
            logger.error(f"Message delivery failed: {err}")
        else:
            self.message_counter += 1
            if self.message_counter % 1000 == 0:
                logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]" f"at offset {msg.offset()}. Total messages: {self.message_counter}, Errors: {self.error_counter}")
    def send_message(self, topic: str, message: Dict[str, Any], key: Optional[str] = None) -> bool:
        try:
            serialized_message = json.dumps(message).encode('utf-8')
            key = key.encode('utf-8') if key else None
            self.producer.produce(topic, value=serialized_message, key=key, callback=self.delivery_report)
            self.producer.poll(0)
            return True
        except BufferError:
            logger.error("Local producer queue is full, ({len(self.producer)} messages)")
            return False
        except KafkaException as e:
            logger.error(f"Failed to send message: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False
    def flush(self, timeout: float = 10.0):
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            logger.warning(f'{remaining} messages were not delivered')
    
    def close(self):
        logger.info('Closing producer...')
        self.flush()
        logger.info(f'Producer closed. Total messages sent: {self.message_count}, '
                   f'Errors: {self.error_count}')

    @staticmethod
    def create_topics():
        admin_client = AdminClient({
            'bootstrap.servers': KafkaConfig.BOOTSTRAP_SERVERS
        })
        
        topics_to_create = []
        for topic_name, config in KafkaConfig.TOPIC_CONFIGS.items():
            topic = NewTopic(
                topic=topic_name,
                num_partitions=config['num_partitions'],
                replication_factor=config['replication_factor'],
                config=config['config']
            )
            topics_to_create.append(topic)
        
        fs = admin_client.create_topics(topics_to_create)
        
        for topic, f in fs.items():
            try:
                f.result()
                logger.info(f'Topic {topic} created successfully')
            except KafkaException as e:
                if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    logger.info(f'Topic {topic} already exists')
                else:
                    logger.error(f'Failed to create topic {topic}: {e}')