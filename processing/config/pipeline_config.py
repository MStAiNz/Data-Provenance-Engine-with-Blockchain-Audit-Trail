import os
from typing import Dict, Any

class PipelineConfig:
    RAW_DATA_TOPIC = os.getenv('RAW_DATA_TOPIC', 'raw_data_stream')
    PROCESSED_DATA_TOPIC = os.getenv('PROCESSED_DATA_TOPIC', 'processed_data_stream')  
    PROVENANCE_DATA_TOPIC = os.getenv('PROVENANCE_DATA_TOPIC', 'provenance_data_stream')
    ERROR_DATA_TOPIC = os.getenv('ERROR_DATA_TOPIC', 'error_data_stream')

    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
    PROCESSING_INTERVAL = int(os.getenv('PROCESSING_INTERVAL', '60'))

    VALIDATION_RULES = {
        'user_activity':{
            'required_fields': ['user_id', 'activity_type', 'timestamp'],
            'field_types': {
                'user_id': str,
                'activity_type': str,
                'timestamp': str,
            },
            'value_ranges': {}
        },
        'transaction':{
            'required_fields': ['transaction_id',  'amount', 'currency', 'timestamp'],
            'field_types': {
                'transaction_id': str,
                'amount': float,
                'currency': str,
                'timestamp': str
            },
            'value_ranges': {'value': {'min': -100, 'max': 1000000}}
        },
        'sensor_data': {
            'required_fields': ['sensor_id', 'sensor_type', 'value', 'timestamp'],
            'field_types': {'sensor_id': str, 'value': float, 'timestamp': str, 'sensor_type': str },
            'value_ranges': {'value': {'min': -100, 'max': 250}}
        }

    }

    ENRICHMENT_CONFIG = {
        'lookup_tables': {
            'user': 'data/lookup/users.csv',
            'product': 'data/lookup/products.csv',
            'locations': 'data/lookup/locations.csv'
        },
        'calculated_fields': [
            'total_value_with_tax',
            'location_region',
            'risk_category',
            'anomaly_score'
        ]
    }

    AGGREGATION_CONFIG = {
        'time_windows': ['1 hour', '1 day'],
        'aggregation_functions': ['count', 'sum', 'avg', 'min', 'max'],
        'group_by_fields': {
            'user_activity': ['user_id', 'action'],
            'transaction': ['merchant_id', 'currency'],
            'sensor_data': ['sensor_id', 'sensor_type']
        }
    }

    LOG_TO_BLOCKCHAIN = os.getenv('LOG_TO_BLOCKCHAIN', 'true').lower() == 'true'
    BLOCKCHAIN_BATCH_SIZE = int(os.getenv('BLOCKCHAIN_BATCH_SIZE', '500'))

    QUALITY_THRESHOLDS = {
        'min_completeness': 0.95,
        'max_error_rate': 0.02,
        'min_accuracy': 0.98
    }

config = PipelineConfig()