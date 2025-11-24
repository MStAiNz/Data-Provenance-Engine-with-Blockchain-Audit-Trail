from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, MapType
from config.pipeline_config import config
import logging

logger = logging.getLogger(__name__)

class KafkaStreamConsumer:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.bootstrap_servers = config.KAFKA_BOOTSTRAP_SERVERS
    
    def create_stream(self, topic: str, starting_offsets: str = 'latest') -> DataFrame:
        logger.info(f"Creating stream from topic: {topic}")
        
        df = self.spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', self.bootstrap_servers) \
        .option('subscribe', topic) \
        .option('startingOffsets', starting_offsets) \
        .option('failOnDataLoss', 'false') \
        .load()
        
        logger.info(f"Stream created from {topic}")
        
        return df
    
    def parse_kafka_message(self, df: DataFrame, schema: StructType = None) -> DataFrame:

        if schema is None:
            schema = StructType([
                StructField('message_id', StringType(), True),
                StructField('source', StringType(), True),
                StructField('schema_version', StringType(), True),
                StructField('timestamp', StringType(), True),
                StructField('data', MapType(StringType(), StringType()), True)
            ])

        parsed_df = df.select(col('key').cast('string').alias('kafka_key'),
            col('topic'), col('partition'), col('offset'), col('timestamp').alias('kafka_timestamp'),
            from_json(col('value').cast('string'), schema).alias('message')
        )

        parsed_df = parsed_df.select(
            'kafka_key',
            'topic',
            'partition',
            'offset',
            'kafka_timestamp',
            'message.*'
        )
        
        return parsed_df
    
    def read_batch(self, topic: str, batch_size: int = 1000) -> DataFrame:

        df = self.spark \
            .read \
            .format('kafka') \
            .option('kafka.bootstrap.servers', self.bootstrap_servers) \
            .option('subscribe', topic) \
            .option('startingOffsets', 'earliest') \
            .option('endingOffsets', 'latest') \
            .load() \
            .limit(batch_size)
        
        return df