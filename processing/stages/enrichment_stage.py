from pyspark.sql import DataFrame
from stages.base_stage import BaseProcessingStage
from transformations.enrichers import DataEnricher
from config.pipeline_config import config
import logging

logger = logging.getLogger(__name__)

class EnrichmentStage(BaseProcessingStage):
    
    def __init__(self, provenance_tracker):
        super().__init__('ENRICHMENT', provenance_tracker)
        self.enricher = DataEnricher()
    
    def process(self, df: DataFrame, **kwargs) -> DataFrame:
        data_type = kwargs.get('data_type', 'unknown')
        
        logger.info(f"Starting enrichment for data type: {data_type}")

        if data_type == 'transaction':

            if 'amount' in df.columns:
                df = self.enricher.calculate_total_with_tax(df)
                logger.info("Added total_with_tax field")

            if 'amount' in df.columns:
                df = self.enricher.categorize_risk(df)
                logger.info("Added risk_category field")

        if 'timestamp' in df.columns:
            df = self.enricher.add_temporal_features(df)
            logger.info("Added temporal features")

        if 'latitude' in df.columns and 'longitude' in df.columns:
            df = self.enricher.add_geolocation_features(df)
            logger.info("Added geolocation features")

        numeric_columns = kwargs.get('numeric_columns', [])
        if numeric_columns:
            df = self.enricher.calculate_anomaly_score(df, numeric_columns)
            logger.info("Added anomaly scores")

        calculations = kwargs.get('calculations', {})
        if calculations:
            df = self.enricher.add_calculated_fields(df, calculations)
            logger.info(f"Added {len(calculations)} calculated fields")
        
        logger.info(f"Enrichment complete. Records: {df.count()}")
        
        return df