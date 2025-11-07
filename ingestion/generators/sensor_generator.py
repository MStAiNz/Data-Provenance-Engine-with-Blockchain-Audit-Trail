import uuid
from datetime import datetime
from typing import Dict, Any, List
import random
import math

class SensorDataGenerator:
    #Generates synthetic sensor data for ingestion
    
    def __init__(self, seed: int = 42):
        random.seed(seed)
        self.sensor_ids = [str(uuid.uuid4()) for _ in range(100)]
        self.sensor_types = ['temperature', 'humidity', 'pressure', 'light', 'motion']
        self.locations = ['warehouse_1', 'warehouse_2', 'outdoor_1', 'outdoor_2', 'indoor_1']   
    
    def generate_sensor_reading(self) -> Dict[str, Any]:
        #Generates a single sensor reading
        sensor_id = random.choice(self.sensor_ids)
        sensor_type = random.choice(self.sensor_types)
        location = random.choice(self.locations)
        timestamp = datetime.utcnow()

        if sensor_type == 'temperature':
            value = round(random.uniform(-20.0, 50.0), 2)  
        elif sensor_type == 'humidity':
            value = round(random.uniform(0.0, 100.0), 2) 
        elif sensor_type == 'pressure':
            value = round(random.uniform(950.0, 1050.0), 2) 
        elif sensor_type == 'light':
            value = round(random.uniform(0.0, 10000.0), 2) 
        elif sensor_type == 'motion':
            value = random.choice([0, 1])                  

        reading = {
            "sensor_id": sensor_id,
            "sensor_type": sensor_type,
            "location": location,
            "timestamp": timestamp.isoformat(),
            "value": value,
            "unit": self.get_unit(sensor_type)
        }
        return reading
    
    def generate_batch(self, n: int = 10) -> List[Dict[str, Any]]:
        #Generates a batch of sensor readings
        return [self.generate_sensor_reading() for _ in range(n)]