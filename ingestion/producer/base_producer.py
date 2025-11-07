import uuid
from datetime import datetime
from typing import Any, Dict, Optional
from abc import ABC, abstractmethod

class BaseProducer(ABC):
    def __init__(self, source_name: str, schema_version: str = "1.0"):
        self.source_name = source_name
        self.schema_version = schema_version
    
    def create_message_envelope(
        self, data: Dict[str, Any],
        message_type: str = "data") -> Dict[str, Any]:

        message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat()

        envelope = {
            "message_id": message_id,
            "source": self.source_name,
            "schema_version": self.schema_version,
            "message_type": message_type,
            "timestamp": timestamp,
            "data": data
        }
        
        return envelope
    
    def create_provenance_event(
        self, message_id: str, event_type: str,
        details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:

        provenance_event = {
            "provenance_id": str(uuid.uuid4()),
            "message_id": message_id,
            "event_type": event_type,
            "timestamp": datetime.utcnow().isoformat(),
            "source": self.source_name,
            "details": details or {}
        }
        
        return provenance_event
    
    @abstractmethod
    def generate_data(self, data: Dict[str, Any]):
        pass