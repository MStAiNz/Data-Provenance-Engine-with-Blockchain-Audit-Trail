from pyspark.sql.functions import col, when, lit, udf, concat, coalesce, datediff, current_date, expr, broadcast
from pyspark.sql.types import StringType, IntegerType, FloatType
from pyspark.sql import DataFrame
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

class DataEnricher:
    @staticmethod
    def join_lookup_table(df: DataFrame, lookup_df: DataFrame, join_column: str, join_type: str, select_columns: list = None) -> DataFrame:

        if lookup_df.count() < 10000:
            lookup_df = broadcast(lookup_df)

        enriched_df = df.join(lookup_df, df[join_column] == lookup_df[join_column], join_type)
        
        if select_columns:
            enriched_df = enriched_df.select(df['*'], *[lookup_df[col] for col in select_columns])
        
        return enriched_df
    
    @staticmethod
    def add_calculated_fields(df: DataFrame, calculations: Dict[str, str]) -> DataFrame:
        for col_name, expression in calculations.items():
            df = df.withColumn(col_name, expr(expression))

        return df
    
    @staticmethod
    def calculate_total_with_tax(df: DataFrame, amount_column: str = 'amount', tax_rate: float = 0.10) -> DataFrame:
        return df.withColumn('total_with_tax', col(amount_column) * (1 + tax_rate))
    
    @staticmethod
    def categorized_risk(df: DataFrame, amount_column: str = 'amount', thresholds: Dict[str, float] = None) -> DataFrame:
        if thresholds is None:
           thresholds = {'low': 1000, 'medium': 5000, 'high': 10000}
        risk_category = when(col(amount_column) < thresholds['low'], 'low').when(
            col(amount_column) < thresholds['medium'], 'medium').when(col(amount_column) < thresholds['high'], 'high').otherwise('critical')
        
        return df.withColumn('risk_category', risk_category)
    
    @staticmethod
    def calculate_anomaly_score(df: DataFrame, numeric_columns: list, method: str = 'z-score') -> DataFrame:
        anomaly_scores = []
        
        for col_name in numeric_columns:
            if col_name not in df.columns:
                continue

            stats = df.select(col(col_name)).summary('mean', 'stddev').collect()
            
            if len(stats) >= 2:
                mean = float(stats[0][1])
                stddev = float(stats[1][1])
                
                if stddev > 0:
                    z_score = (col(col_name) - mean) / stddev
                    anomaly_scores.append(z_score * z_score)

        if anomaly_scores:
            combined_score = anomaly_scores[0]
            for score in anomaly_scores[1:]:
                combined_score = combined_score + score
            
            df = df.withColumn('anomaly_score', combined_score)
        else:
            df = df.withColumn('anomaly_score', lit(0.0))
        
        return df
    
    @staticmethod
    def add_temporal_features(df: DataFrame, timestamp_column: str = 'timestamp') -> DataFrame:
        
        from pyspark.sql.functions import hour, dayofweek, dayofmonth, month, year, quarter
        
        df = df.withColumn('hour_of_day', hour(col(timestamp_column)))
        df = df.withColumn('day_of_week', dayofweek(col(timestamp_column)))
        df = df.withColumn('day_of_month', dayofmonth(col(timestamp_column)))
        df = df.withColumn('month', month(col(timestamp_column)))
        df = df.withColumn('year', year(col(timestamp_column)))
        df = df.withColumn('quarter', quarter(col(timestamp_column)))
        
        return df
    
    @staticmethod
    def add_geolocation_features(df: DataFrame, lat_column: str = 'latitude', lon_column: str = 'longitude') -> DataFrame:

        @udf(StringType())
        def simple_geohash(lat, lon):
            if lat is None or lon is None:
                return None

            return f"{int(lat)}_{int(lon)}"
        
        df = df.withColumn('geohash', simple_geohash(col(lat_column), col(lon_column)))
        
        return df