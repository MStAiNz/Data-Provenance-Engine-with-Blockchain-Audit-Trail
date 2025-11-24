from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (col, count, sum as spark_sum, avg, min as spark_min, 
    max as spark_max, stddev, window, current_timestamp,lag, lead, row_number, rank, dense_rank)
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class DataAggregator:
    
    @staticmethod
    def aggregate_by_group(df: DataFrame, group_by_columns: List[str], agg_functions: Dict[str, List[str]]) -> DataFrame:

        agg_exprs = []
        
        for col_name, functions in agg_functions.items():
            for func in functions:
                if func == 'count':
                    agg_exprs.append(count(col(col_name)).alias(f'{col_name}_{func}'))
                elif func == 'sum':
                    agg_exprs.append(spark_sum(col(col_name)).alias(f'{col_name}_{func}'))
                elif func == 'avg':
                    agg_exprs.append(avg(col(col_name)).alias(f'{col_name}_{func}'))
                elif func == 'min':
                    agg_exprs.append(spark_min(col(col_name)).alias(f'{col_name}_{func}'))
                elif func == 'max':
                    agg_exprs.append(spark_max(col(col_name)).alias(f'{col_name}_{func}'))
                elif func == 'stddev':
                    agg_exprs.append(stddev(col(col_name)).alias(f'{col_name}_{func}'))
        
        return df.groupBy(*group_by_columns).agg(*agg_exprs)
    
    @staticmethod
    def time_window_aggregation(df: DataFrame, timestamp_column: str, window_duration: str, group_by_columns: List[str], agg_columns: Dict[str, str]) -> DataFrame:

        windowed_df = df.withColumn('window', window(col(timestamp_column), window_duration))

        group_cols = ['window'] + group_by_columns
        
        agg_exprs = []
        for col_name, func in agg_columns.items():
            if func == 'count':
                agg_exprs.append(count(col(col_name)).alias(f'{col_name}_count'))
            elif func == 'sum':
                agg_exprs.append(spark_sum(col(col_name)).alias(f'{col_name}_sum'))
            elif func == 'avg':
                agg_exprs.append(avg(col(col_name)).alias(f'{col_name}_avg'))
        
        return windowed_df.groupBy(*group_cols).agg(*agg_exprs)
    
    @staticmethod
    def rolling_window_aggregation(df: DataFrame, partition_columns: List[str], order_column: str, window_size: int, agg_column: str, agg_function: str = 'avg') -> DataFrame:
       
        window_spec = Window.partitionBy(*partition_columns).orderBy(col(order_column)).rowsBetween(-window_size + 1, 0)
        
        if agg_function == 'avg':
            df = df.withColumn(f'rolling_{agg_function}_{window_size}', avg(col(agg_column)).over(window_spec))
        
        elif agg_function == 'sum':
            df = df.withColumn(f'rolling_{agg_function}_{window_size}', spark_sum(col(agg_column)).over(window_spec))
            
        elif agg_function == 'count':
            df = df.withColumn(f'rolling_{agg_function}_{window_size}', count(col(agg_column)).over(window_spec))
        
        return df
    
    @staticmethod
    def rank_by_group(df: DataFrame, partition_columns: List[str], order_column: str, ascending: bool = False) -> DataFrame:
        
        window_spec = Window.partitionBy(*partition_columns).orderBy(col(order_column).asc() if ascending else col(order_column).desc())
        
        df = df.withColumn('rank', row_number().over(window_spec))
        df = df.withColumn('dense_rank', dense_rank().over(window_spec))
        
        return df
    
    @staticmethod
    def calculate_running_totals(df: DataFrame, partition_columns: List[str], order_column: str, sum_column: str) -> DataFrame:

        window_spec = Window.partitionBy(*partition_columns).orderBy(col(order_column)).rowsBetween(Window.unboundedPreceding, 0)
        
        return df.withColumn('running_total', spark_sum(col(sum_column)).over(window_spec))