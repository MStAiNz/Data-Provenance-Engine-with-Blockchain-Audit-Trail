import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import logging

try:
    import boto3
    from botocore.exceptions import ClientError
    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False

from config.kafka_config import KafkaConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StorageManager:
    def __init__(self):
        self.use_s3 = KafkaConfig.USE_S3 and S3_AVAILABLE
        self.local_path = Path(KafkaConfig.LOCAL_STORAGE_PATH)
        
        if self.use_s3:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=KafkaConfig.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=KafkaConfig.AWS_SECRET_ACCESS_KEY,
                region_name=KafkaConfig.AWS_REGION
            )
            self.bucket_name = KafkaConfig.S3_BUCKET
            logger.info(f'Using S3 storage: {self.bucket_name}')
        else:
            self.local_path.mkdir(parents=True, exist_ok=True)
            logger.info(f'Using local storage: {self.local_path}')
    
    def generate_storage_path(self, source: str, message_id: str) -> str:
        now = datetime.utcnow()
        path = f"{source}/{now.year}/{now.month:02d}/{now.day:02d}/{now.hour:02d}/{message_id}.json"
        return path
    
    def save_to_local(self, data: Dict[str, Any], path: str) -> bool:
        try:
            full_path = self.local_path / path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(full_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.debug(f'Saved to local: {full_path}')
            return True
            
        except Exception as e:
            logger.error(f'Failed to save to local storage: {e}')
            return False
    
    def save_to_s3(self, data: Dict[str, Any], path: str) -> bool:
        try:
            json_data = json.dumps(data)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=path,
                Body=json_data,
                ContentType='application/json'
            )
            
            logger.debug(f'Saved to S3: s3://{self.bucket_name}/{path}')
            return True
            
        except ClientError as e:
            logger.error(f'Failed to save to S3: {e}')
            return False
    
    def save(self, data: Dict[str, Any], source: str, message_id: str) -> bool:
        path = self.generate_storage_path(source, message_id)
        if self.use_s3:
            return self.save_to_s3(data, path)
        else:
            return self.save_to_local(data, path)
    
    def batch_save(self, messages: list, source: str) -> int:
        success_count = 0
        for message in messages:
            message_id = message.get('message_id', 'unknown')
            if self.save(message, source, message_id):
                success_count += 1
        
        return success_count