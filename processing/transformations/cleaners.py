from pyspark.sql.functions import col, when, lit, trim, lower, upper, regexp_replace, to_timestamp, to_date
from pyspark.sql import DataFrame
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class DataCleaner:
    @staticmethod
    def clean_strings(df: DataFrame, string_columns: List[str], operations: Dict[str] = ['trim', 'lower']) -> DataFrame:

        for col_name in string_columns:
            if col_name not in df.columns:
                logger.warning(f"Column {col_name} does not exist in DataFrame.")
                continue

            cleaned_col = col(col_name)

            if 'trim' in operations:
                cleaned_col = trim(cleaned_col)
            if 'lower' in operations:
                cleaned_col = lower(cleaned_col)
            elif 'upper' in operations:
                cleaned_col = upper(cleaned_col)

                df = df.withColumn(col_name, cleaned_col)
        return df
    
    @staticmethod
    def remove_special_characters(df: DataFrame, columns: List[str], pattern: str = '[^a-zA-Z0-9 ]') -> DataFrame:
        for col_name in columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, regexp_replace(col(col_name), pattern, ''))
            else:
                logger.warning(f"Column {col_name} does not exist in DataFrame.")

        return df
    
    @staticmethod
    def standardize_dates(df: DataFrame, date_columns: List[str], date_format: str = 'yyyy-MM-dd') -> DataFrame:
        for col_name in date_columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, to_date(col(col_name), date_format))
            else:
                logger.warning(f"Column {col_name} does not exist in DataFrame.")

        return df
    
    @staticmethod
    def normalize_numbers(df: DataFrame, numeric_columns: List[str], method: str = 'min-max') -> DataFrame:
        
        from pyspark.ml.feature import MinMaxScaler, StandardScaler
        from pyspark.ml.linalg import Vectors
        from pyspark.sql.functions import udf
        from pyspark.sql.types import DoubleType
        
        return df
    
    @staticmethod
    def cap_outliers(df: DataFrame, numeric_columns: List[str], lower_quantile: float = 0.01, upper_quantile: float = 0.99) -> DataFrame:

        for col_name in numeric_columns:
            if col_name not in df.columns:
                logger.warning(f"Column {col_name} does not exist in DataFrame.")
                continue

            quantiles = df.approxQuantile(col_name, [lower_quantile, upper_quantile], 0.01)
            
            if len(quantiles) == 2:
                lower_bound, upper_bound = quantiles
                
                df = df.withColumn(col_name, when(col(col_name) < lower_bound, lower_bound).when(col(col_name) > upper_bound, upper_bound).otherwise(col(col_name)))
        
        return df