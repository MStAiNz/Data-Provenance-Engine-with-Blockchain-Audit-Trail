from pyspark.sql import DataFrame
from pyspark.sql.functions import to_json, struct, col
from config.pipeline_config import config
import logging

logger = logging.getLogger(__name__)

class KafkaStreamProducer:    
    def __init__(self):
        self.bootstrap_servers = config.KAFKA_BOOTSTRAP_SERVERS
    
    def write_stream(self, df: DataFrame, topic: str, checkpoint_location: str, output_mode: str = 'append', trigger_interval: str = '10 seconds'):
        logger.info(f"Writing stream to topic: {topic}")

        kafka_df = df.select(col('message_id').cast('string').alias('key'),
            to_json(struct(*df.columns)).alias('value'))
        
        query = kafka_df \
            .writeStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', self.bootstrap_servers) \
            .option('topic', topic) \
            .option('checkpointLocation', checkpoint_location) \
            .outputMode(output_mode) \
            .trigger(processingTime=trigger_interval) \
            .start()
        
        logger.info(f"Stream started to {topic}")
        
        return query
    
    def write_batch(self, df: DataFrame, topic: str, mode: str = 'append'):
        logger.info(f"Writing batch to topic: {topic}")

        kafka_df = df.select(col('message_id').cast('string').alias('key'), to_json(struct(*df.columns)).alias('value'))
        
        kafka_df.write \
            .format('kafka') \
            .option('kafka.bootstrap.servers', self.bootstrap_servers) \
            .option('topic', topic) \
            .mode(mode) \
            .save()
        
        logger.info(f"Batch written to {topic}: {df.count()} records")
    
    def write_to_console(self, df: DataFrame, output_mode: str = 'append', truncate: bool = False):
   
        query = df \
            .writeStream \
            .format('console') \
            .outputMode(output_mode) \
            .option('truncate', truncate) \
            .start()
        
        return query