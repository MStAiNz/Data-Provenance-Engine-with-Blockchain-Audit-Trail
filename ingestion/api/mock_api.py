from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid
import logging
from producer.kafka_producer import KafkaDataProducer
from producer.base_producer import BaseProducer
from config.kafka_config import KafkaConfig
from utils.storage import StorageManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Data Provenance Ingestion API", version="1.0.0")

kafka_producer = KafkaDataProducer()
storage_manager = StorageManager()

class DataPoint(BaseModel):
    data_type: str = Field(..., description="Type of data")
    payload: Dict[str, Any] = Field(..., description="Data payload")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Additional metadata")

class CSVUpload(BaseModel):
    filename: str
    rows: List[Dict[str, Any]]
    metadata: Optional[Dict[str, Any]] = None

class BatchUpload(BaseModel):
    source: str
    records: List[Dict[str, Any]]

def process_and_send(data: Dict[str, Any], source: str, topic: str):
    try:
        producer = BaseProducer(source)
        message = producer.create_message_envelope(data)
        message_id = message['message_id']
 
        kafka_producer.send_message(topic, message, key=message_id)
        
        storage_manager.save(message, source, message_id)
        provenance_event = producer.create_provenance_event(
            message_id, 
            'ingested',
            {'topic': topic, 'source': source}
        )
        kafka_producer.send_message(
            KafkaConfig.TOPICS['PROVENANCE_EVENTS'],
            provenance_event
        )
        
        logger.info(f"Processed message {message_id} from {source}")
        
    except Exception as e:
        logger.error(f"Failed to process message: {e}")

@app.on_event("startup")
async def startup_event():
    logger.info("Starting Data Provenance Ingestion API")
    KafkaDataProducer.create_topics()

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down API")
    kafka_producer.close()

@app.get("/")
async def root():
    return {
        "service": "Data Provenance Ingestion API",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "kafka": "connected",
        "storage": "available"
    }

@app.post("/ingest/data")
async def ingest_data(data_point: DataPoint, background_tasks: BackgroundTasks):
    try:
        data = {
            "data_type": data_point.data_type,
            "payload": data_point.payload,
            "metadata": data_point.metadata or {}
        }
        
        background_tasks.add_task(
            process_and_send,
            data,
            "api-endpoint",
            KafkaConfig.TOPICS['RAW_DATA']
        )
        
        return {
            "status": "accepted",
            "message": "Data queued for ingestion",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Ingestion error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest/csv")
async def ingest_csv(csv_data: CSVUpload, background_tasks: BackgroundTasks):
    try:
        data = {
            "filename": csv_data.filename,
            "row_count": len(csv_data.rows),
            "rows": csv_data.rows,
            "metadata": csv_data.metadata or {}
        }
        
        background_tasks.add_task(
            process_and_send,
            data,
            "csv-upload",
            KafkaConfig.TOPICS['RAW_DATA']
        )
        
        return {
            "status": "accepted",
            "filename": csv_data.filename,
            "rows_received": len(csv_data.rows),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"CSV ingestion error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest/batch")
async def ingest_batch(batch: BatchUpload, background_tasks: BackgroundTasks):
    try:
        for record in batch.records:
            background_tasks.add_task(
                process_and_send,
                record,
                batch.source,
                KafkaConfig.TOPICS['RAW_DATA']
            )
        
        return {
            "status": "accepted",
            "source": batch.source,
            "records_queued": len(batch.records),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Batch ingestion error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_stats():
    return {
        "messages_sent": kafka_producer.message_count,
        "errors": kafka_producer.error_count,
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)