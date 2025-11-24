import sys
import time
import signal
from typing import Optional
from pyspark.sql import DataFrame

from core.spark_session import SparkSessionManager
from core.provenance_tracker import ProvenanceTracker
from consumers.kafka_consumer import KafkaStreamConsumer
from producers.kafka_producer import KafkaStreamProducer
from stages.validation_stage import ValidationStage
from stages.enrichment_stage import EnrichmentStage
from stages.aggregation_stage import AggregationStage
from config.pipeline_config import PipelineConfig as config
from config.spark_config import SparkConfig as spark_config

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataProcessingPipeline:    
    def __init__(self):
        self.spark = SparkSessionManager.get_session()

        self.provenance_tracker = ProvenanceTracker(enable_blockchain=config.LOG_TO_BLOCKCHAIN)
        
        self.consumer = KafkaStreamConsumer(self.spark)
        self.producer = KafkaStreamProducer()
 
        self.validation_stage = ValidationStage(self.provenance_tracker)
        self.enrichment_stage = EnrichmentStage(self.provenance_tracker)
        self.aggregation_stage = AggregationStage(self.provenance_tracker)
        
        self.running = True
        self.queries = []
        

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        logger.info("Shutdown signal received...")
        self.running = False
    
    def process_batch(self, df: DataFrame, data_type: str = 'unknown') -> DataFrame:
        logger.info(f"\\n{'='*70}")
        logger.info(f"Processing batch: {data_type}")
        logger.info(f"Input records: {df.count()}")
        logger.info(f"{'='*70}")

        batch_id = f"{data_type}_{int(time.time())}"

        logger.info("\\n--- STAGE 1: VALIDATION ---")
        validated_df = self.validation_stage.execute(
            df, data_id=f"{batch_id}_validated", source_data_id=batch_id, data_type=data_type, key_columns=['message_id'])

        logger.info("\\n--- STAGE 2: ENRICHMENT ---")
        enriched_df = self.enrichment_stage.execute(
            validated_df,
            data_id=f"{batch_id}_enriched",
            source_data_id=f"{batch_id}_validated",
            data_type=data_type,
            numeric_columns=['amount'] if 'amount' in validated_df.columns else []
        )

        if config.AGGREGATION_CONFIG['group_by_fields'].get(data_type):
            logger.info("\\n--- STAGE 3: AGGREGATION ---")
            aggregated_df = self.aggregation_stage.execute(
                enriched_df,
                data_id=f"{batch_id}_aggregated",
                source_data_id=f"{batch_id}_enriched",
                data_type=data_type
            )
            final_df = aggregated_df
        else:
            final_df = enriched_df
        
        # Flush provenance to blockchain
        if config.LOG_TO_BLOCKCHAIN:
            self.provenance_tracker.flush_batch_to_blockchain()
        
        logger.info(f"\\n{'='*70}")
        logger.info(f"Batch processing complete")
        logger.info(f"Output records: {final_df.count()}")
        logger.info(f"{'='*70}\\n")
        
        return final_df
    
    def run_batch_mode(self):
        logger.info("Starting pipeline in BATCH mode")
        
        try:
            logger.info(f"Reading from topic: {config.RAW_DATA_TOPIC}")
            raw_df = self.consumer.read_batch(config.RAW_DATA_TOPIC, batch_size=config.BATCH_SIZE)

            parsed_df = self.consumer.parse_kafka_message(raw_df)

            data_type = 'transaction'  # Could be extracted from data

            processed_df = self.process_batch(parsed_df, data_type)

            logger.info(f"Writing to topic: {config.PROCESSED_DATA_TOPIC}")
            self.producer.write_batch(processed_df, config.PROCESSED_DATA_TOPIC)
            
            logger.info("Batch processing complete!")
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}", exc_info=True)
            raise
    
    def run_streaming_mode(self):
        logger.info("Starting pipeline in STREAMING mode")
        
        try:
            logger.info(f"Creating stream from: {config.RAW_DATA_TOPIC}")
            raw_stream = self.consumer.create_stream(config.RAW_DATA_TOPIC)

            parsed_stream = self.consumer.parse_kafka_message(raw_stream)

            def process_batch_fn(batch_df, batch_id):
                if batch_df.count() > 0:
                    data_type = 'transaction' 
                    processed_df = self.process_batch(batch_df, data_type)

                    self.producer.write_batch(processed_df, config.PROCESSED_DATA_TOPIC)

            query = parsed_stream \
                .writeStream \
                .foreachBatch(process_batch_fn) \
                .option('checkpointLocation', f'{spark_config.CHECKPOINT_DIR}/main') \
                .trigger(processingTime=f'{config.PROCESSING_INTERVAL} seconds') \
                .start()
            
            self.queries.append(query)
            
            logger.info("Streaming pipeline started!")
            logger.info("Press Ctrl+C to stop...")

            while self.running:
                time.sleep(1)

            for query in self.queries:
                query.stop()
            
            logger.info("Streaming pipeline stopped")
            
        except Exception as e:
            logger.error(f"Streaming processing failed: {e}", exc_info=True)
            raise
    
    def run(self, mode: str = 'batch'):
        logger.info(f"\\n{'='*70}")
        logger.info("DATA PROCESSING PIPELINE")
        logger.info(f"{'='*70}")
        logger.info(f"Mode: {mode.upper()}")
        logger.info(f"Spark Version: {self.spark.version}")
        logger.info(f"Input Topic: {config.RAW_DATA_TOPIC}")
        logger.info(f"Output Topic: {config.PROCESSED_DATA_TOPIC}")
        logger.info(f"Blockchain Logging: {config.LOG_TO_BLOCKCHAIN}")
        logger.info(f"{'='*70}\\n")
        
        try:
            if mode == 'batch':
                self.run_batch_mode()
            elif mode == 'streaming':
                self.run_streaming_mode()
            else:
                raise ValueError(f"Invalid mode: {mode}")
        
        finally:
            self.shutdown()
    
    def shutdown(self):
        logger.info("Shutting down pipeline...")
        
        spark_config.stop()
        
        logger.info("Pipeline shutdown complete")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Data Processing Pipeline')
    parser.add_argument('--mode', choices=['batch', 'streaming'],
        default='batch', help='Processing mode')
    
    args = parser.parse_args()

    pipeline = DataProcessingPipeline()
    pipeline.run(mode=args.mode)

if __name__ == '__main__':
    main()