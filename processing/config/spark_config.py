import os
from typing import Dict, Any

class SparkConfig:

    APP_NAME = os.getenv('SPARK_APP_NAME', 'DataProvenanceEngine')
    MASTER = os.getenv('SPARK_MASTER', 'local[*]')

    DRIVER_MEMORY = os.getenv('SPARK_DRIVER_MEMORY', '4g')
    EXECUTOR_MEMORY = os.getenv('SPARK_EXECUTOR_MEMORY', '4g')

    DEFAULT_PARALLELISM = int(os.getenv('SPARK_DEFAULT_PARALLELISM', '8'))
    SQL_SHUFFLE_PARTITIONS = int(os.getenv('SPARK_SQL_SHUFFLE_PARTITIONS', '200'))

    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    CHECKPOINT_DIR = os.getenv('SPARK_CHECKPOINT_DIR', '/tmp/spark-checkpoints')

    LOG_LEVEL = os.getenv('SPARK_LOG_LEVEL', 'WARN')

    @classmethod
    def get_spark_conf(cls) -> Dict[str, Any]:
        return {
            'spark.app.name': cls.APP_NAME,
            'spark.master': cls.MASTER,
            'spark.driver.memory': cls.DRIVER_MEMORY,
            'spark.executor.memory': cls.EXECUTOR_MEMORY,
            'spark.default.parallelism': cls.DEFAULT_PARALLELISM,
            'spark.sql.shuffle.partitions': cls.SQL_SHUFFLE_PARTITIONS,
            'spark.kafka.bootstrap.servers': cls.KAFKA_BOOTSTRAP_SERVERS,
            'spark.checkpoint.dir': cls.CHECKPOINT_DIR,
            'spark.log.level': cls.LOG_LEVEL,
        }
    
config = SparkConfig()