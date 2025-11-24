import json
import hashlib
from typing import Any, Dict, List, Optional
from datetime import datetime
import logging
from pyspark.sql import DataFrame
from blockchain.python.services.blockchain_client import BlockchainProvenanceClient

logger = logging.getLogger(__name__)

class ProvenanceTracker:
    def __init__(self, enable_blockchain: bool = True):
        self.enable_blockchain = enable_blockchain
        self.blockchain_client = None
        self.provenance_records = []

        if self.enable_blockchain:
            try:
                self.blockchain_client = BlockchainProvenanceClient()
                logger.info("Blockchain provenance client initialized successfully.")
            except Exception as e:
                logger.error(f"Failed to initialize blockchain client: {e}")
                self.enable_blockchain = False

    def compute_dataframe_hash(self, df: DataFrame) -> str:
        sample_size = min(1000, df.count())
        sample_data = df.limit(sample_size).toPandas().to_json()

        hash_object = hashlib.sha256(sample_data.encode('utf-8'))
        return hash_object.hexdigest()
    
    def track_transformation(self, data_id: str, input_df: DataFrame, output_df: DataFrame,
        transformation_type: str, source_data_id: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:

        output_hash = self.compute_dataframe_hash(output_df)

        transform_metadata = {
            'transformation_type': transformation_type,
            'input_row_count': input_df.count(),
            'output_row_count': output_df.count(),
            'input_columns': input_df.columns,
            'output_columns': output_df.columns,
            'timestamp': datetime.utcnow().isoformat(),
            **(metadata or {})
        }

        record = {
            'data_id': data_id,
            'data_hash': output_hash,
            'source_data_id': source_data_id,
            'transformation_type': transformation_type,
            'metadata': transform_metadata
        }
        self.provenance_records.append(record)

        if self.enable_blockchain and self.blockchain_client:
            try:
                result = self.blockchain_client.log_transformation(
                    self,
                    data_id= data_id,
                    data = output_hash,
                    transformation_type = transformation_type,
                    source_data_id = source_data_id,
                    metadata = transform_metadata
                )
                record['blockchain_txn'] = result.get('txt_hash')
                logger.info(f"Logged transformation to blockchain with txn hash: {result.get('txt_hash')}")

            except Exception as e:
                logger.error(f"Failed to log transformation to blockchain: {e}")

        logger.info(f"Tracked transformation: {transformation_type} for data_id: {data_id}")
        return record
    
    def flush_batch_to_blockchain(self) -> Optional[Dict[str, Any]]:
        if not self.enable_blockchain or not self.provenance_records:
            return None
        
        if not self.blockchain_client:
            return None
        
        try:
            batch = self.provenance_records[:50]
            
            result = self.blockchain_client.log_batch(batch)

            self.provenance_records = self.provenance_records[50:]
            
            logger.info(f"Flushed {len(batch)} records to blockchain")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to flush batch to blockchain: {e}")
            return None
    
    def get_local_provenance(self) -> List[Dict[str, Any]]:
        return self.provenance_records.copy()