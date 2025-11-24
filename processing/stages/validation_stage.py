from pyspark.sql import DataFrame
from stages.base_stage import BaseProcessingStage
from transformations.validators import DataValidator
from transformations.cleaners import DataCleaner
from config.pipeline_config import config
import logging

logger = logging.getLogger(__name__)

class ValidationStage(BaseProcessingStage):
    
    def __init__(self, provenance_tracker):
        super().__init__('VALIDATION', provenance_tracker)
        self.validator = DataValidator()
        self.cleaner = DataCleaner()
    
    def process(self, df: DataFrame, **kwargs) -> DataFrame:
        data_type = kwargs.get('data_type', 'unknown')
        logger.info(f"Starting validation for data type: {data_type}")

        rules = config.VALIDATION_RULES.get(data_type, {})

        if 'required_fields' in rules:
            df = self.validator.validate_required_fields(df, rules['required_fields'])
            logger.info(f"Validated required fields: {rules['required_fields']}")
        
        if 'field_types' in rules:
            df = self.validator.validate_field_types(df, rules['field_types'])
            logger.info("Validated field types")
        
        if 'value_ranges' in rules:
            df = self.validator.validate_value_ranges(df, rules['value_ranges'])
            logger.info("Validated value ranges")
        
        if 'validation_passed' in df.columns:
            invalid_count = df.filter(~df['validation_passed']).count()
            logger.info(f"Invalid records: {invalid_count}")

            df = df.filter(df['validation_passed'])

        key_columns = kwargs.get('key_columns', ['message_id'])
        df = self.validator.remove_duplicates(df, key_columns)
        logger.info("Removed duplicates")

        null_strategy = kwargs.get('null_strategy', 'drop')
        df = self.validator.handle_nulls(df, strategy=null_strategy)
        logger.info(f"Handled nulls with strategy: {null_strategy}")

        string_columns = [col for col in df.columns 
            if df.schema[col].dataType.typeName() == 'string']
        df = self.cleaner.clean_strings(df, string_columns)
        logger.info(f"Cleaned {len(string_columns)} string columns")

        numeric_columns = [col for col in df.columns 
            if df.schema[col].dataType.typeName() in ['int', 'long', 'float', 'double']]
        if numeric_columns:
            df = self.cleaner.cap_outliers(df, numeric_columns[:5]) 
            logger.info("Capped outliers")
        
        logger.info(f"Validation complete. Records: {df.count()}")
        
        return df