from pyspark.sql import SparkSession
from pyspark import SparkConf
from config.spark_config import SparkConfig
import logging

logger = logging.getLogger(__name__)

class SparkSessionManager:
    _instance = None
    _spark = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SparkSessionManager, cls).__new__(cls)
        return cls._instance
    
    def get_session(self) -> SparkSession:
        if self._spark is None:
            self._spark = self.create_spark_session()
        return self._spark

    def create_spark_session(self) -> SparkSession:
        logger.info("Creating Spark Session...")

        conf = SparkConf() 
        for key, value in SparkConfig.get_spark_conf().items():
            conf.set(key, value)

        spark = SparkSession.builder.config(conf=conf).getOrCreate()

        spark.sparkContext.setLogLevel(SparkConfig.LOG_LEVEL)
        
        logger.info(f"Spark session created: {spark.version}")
        logger.info(f"Master: {spark.sparkContext.master}")

        return spark

    def stop_spark_session(self):
        if self._spark is not None:
            logger.info("Stopping Spark session...")    
            self._spark.stop()
            self._spark = None
            logger.info("Spark session stopped.")

spark_session_manager = SparkSessionManager()