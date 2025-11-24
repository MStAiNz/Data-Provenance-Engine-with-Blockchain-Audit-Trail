from pyspark.sql.functions import col, when, lit, size, array
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType, FloatType
from pyspark.sql import DataFrame   
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class DataValidator:

    @staticmethod
    def validate_required_fields(df: DataFrame, required_fields: List[str]) -> DataFrame:

        validation_expr = lit(True)
        for field in required_fields:
            if field in required_fields:
                validation_expr = validation_expr & col(field).isNotNull()
            else:
                logger.warning(f'Required field missing: {field}')
                validation_expr = lit(False)

        return df.withColumn('validation_passed', validation_expr)
    
    @staticmethod
    def validate_field_types(df: DataFrame, field_types: Dict[str, Any]) -> DataFrame:
        for field_name, range_config in field_types.items():
            if field_name not in df.columns:
                logger.warning(f'Field {field_name} not found in DataFrame columns.')
                continue

            actual_type = df.schema[field_name].dataType
            pass

        return df
    
    @staticmethod
    def validate_value_ranges(df: DataFrame, value_ranges: Dict[str, Dict[float]]) -> DataFrame:
            
        for field_name, range_config in value_ranges.items():
            if field_name not in df.columns:
                continue

            min_val = range_config.get('min')
            max_val = range_config.get('max')

            range_validation = lit(True)

            if min_val is not None:
                range_validation = range_validation & (col(field_name) >= min_val)
            if max_val is not None:
                range_validation = range_validation & (col(field_name) <= max_val)

            if 'validation_passed' in df.columns:
                df = df.withColumn('validation_passed', col('validation_passed') & range_validation)
            else:
                df = df.withColumn('validation_passed', range_validation)

        return df
    
    @staticmethod
    def remove_duplicates(df: DataFrame, key_columns: List[str]) -> DataFrame:
        return df.dropDuplicates(key_columns)
    
    @staticmethod
    def handle_nulls(df: DataFrame, strategy: str = 'drop', fill_values: Dict[str, Any] = None) -> DataFrame:
        if strategy == 'drop':
            return df.na.drop()
        elif strategy == 'fill' and fill_values:
            return df.na.fill(fill_values)
        elif strategy == 'flag':
            for col_name in df.columns:
                df = df.withColumn(f'{col_name}_is_null', when(col(col_name).isNull()))
            return df
        else:
            logger.warning('Invalid null handling strategy or missing fill values.')
            return df

