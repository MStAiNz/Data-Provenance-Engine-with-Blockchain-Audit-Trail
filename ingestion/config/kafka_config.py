import os
from typing import Dict, Any


class KafkaConfig:
    # Kafka Configuration
    BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    #Topics
    TOPICS = {
        'RAW_DATA': os.getenv("KAFKA_TOPIC_RAW_DATA", "raw_data_topic"),
        'PROCESSED_DATA': os.getenv("KAFKA_TOPIC_PROCESSED_DATA", "processed_data_topic"),
        'PROVENANCE_EVENTS': os.getenv("KAFKA_TOPIC_PROVENANCE_EVENTS", "provenance_events"),
    }

    #Topic Configurations
    TOPIC_CONFIGS = {
        'raw-data-stream': {
            'partitions': 3,
            'replication_factor': 1,
            'config': {
                'retention.ms': '604800000',  # 7 days
                'compression.type': 'snappy'
            }
        },

        'processed-data-stream': {
            'partitions': 3,
            'replication_factor': 1,
            'config': {
                'retention.ms': '604800000',  # 7 days
                'compression.type': 'snappy'
            }
        },

        'provenance-events': {
            'partitions': 2,
            'replication_factor': 1,
            'config': {
                'retention.ms': '2592000000',  # 30 days
                'cleanup.policy': 'gzip'
            }
        }

    }

    #Producer Configuration
    PRODUCER_CONFIG = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'data_provenance_producer',
        'acks': 'all',
        'retries': 3,
        'linger.ms': 10,
        'batch.size': 32768,
        'max.in.flight.requests.per.connection': 1,
        'compression.type': 'snappy'
    }

    #Consumer Configuration
    CONSUMER_CONFIG = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'data_provenance_consumer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'session.timeout.ms': 30000
    }

    #Storage Configuration
    S3_BUCKET = os.getenv("S3_BUCKET", "data-provenance-raw")
    LOCAL_STORAGE_PATH = os.getenv("LOCAL_STORAGE_PATH", "/data/local_raw/")
    USE_S3 = os.getenv('USE_S3', 'false').lower() == 'true'
    
    # AWS Configuration
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', '')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')
    AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')

    @classmethod
    def get_producer_config(cls) -> Dict[str, Any]:
        return cls.PRODUCER_CONFIG.copy()
    
    @classmethod
    def get_consumer_config(cls) -> Dict[str, Any]:
        return cls.CONSUMER_CONFIG.copy()
    