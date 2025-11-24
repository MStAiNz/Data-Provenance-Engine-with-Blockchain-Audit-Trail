from pyspark.sql import DataFrame
from base_stage import BaseProcessingStage
from transformations.aggregators import DataAggregator
from config.pipeline_config import config
import logging

logger = logging.getLogger(__name__)

class AggregationStage(BaseProcessingStage):
    
    def __init__(self, provenance_tracker):
        super().__init__('AGGREGATION', provenance_tracker)
        self.aggregator = DataAggregator()
    
    def process(self, df: DataFrame, **kwargs) -> DataFrame:

        data_type = kwargs.get('data_type', 'unknown')
        
        logger.info(f"Starting aggregation for data type: {data_type}")

        agg_config = config.AGGREGATION_CONFIG
        
        group_by_fields = agg_config['group_by_fields'].get(data_type, [])
        
        if group_by_fields and all(field in df.columns for field in group_by_fields):

            agg_functions = {}
            
            for col_name in df.columns:
                if col_name not in group_by_fields:
                    col_type = df.schema[col_name].dataType.typeName()
                    
                    if col_type in ['int', 'long', 'float', 'double']:
                        agg_functions[col_name] = ['count', 'sum', 'avg']
                    else:
                        agg_functions[col_name] = ['count']
            
            if agg_functions:
                aggregated_df = self.aggregator.aggregate_by_group(df, group_by_fields, agg_functions)
                logger.info(f"Group aggregation complete: {aggregated_df.count()} groups")

                return aggregated_df

        if 'timestamp' in df.columns and kwargs.get('time_window'):
            time_window = kwargs['time_window']
            
            agg_columns = {}
            for col_name in df.columns:
                if col_name != 'timestamp':
                    col_type = df.schema[col_name].dataType.typeName()
                    if col_type in ['int', 'long', 'float', 'double']:
                        agg_columns[col_name] = 'sum'
            
            if agg_columns:
                windowed_df = self.aggregator.time_window_aggregation(df, 'timestamp', time_window, group_by_fields[:1] if group_by_fields else [], agg_columns)
                logger.info(f"Time window aggregation complete")
                return windowed_df

        if kwargs.get('rolling_window'):
            partition_cols = kwargs.get('partition_columns', group_by_fields[:1])
            order_col = kwargs.get('order_column', 'timestamp')
            window_size = kwargs['rolling_window']
            agg_col = kwargs.get('agg_column', 'amount')
            
            if partition_cols and order_col in df.columns and agg_col in df.columns:
                df = self.aggregator.rolling_window_aggregation(df, partition_cols, order_col, window_size, agg_col)
                logger.info(f"Rolling window aggregation complete")
        
        logger.info(f"Aggregation complete. Records: {df.count()}")
        
        return df