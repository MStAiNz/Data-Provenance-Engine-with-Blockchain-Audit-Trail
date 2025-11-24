from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from pyspark.sql import DataFrame
from core.provenance_tracker import ProvenanceTracker
import logging


logger = logging.getLogger(__name__)

class BaseProcessingStage(ABC):

    def __init__(self, stage_name: str, provenance_tracker: ProvenanceTracker):
        self.stage_name = stage_name
        self.provenance_tracker = provenance_tracker
        self.metrics = {
            'records_processed': 0,
            'processing_time_ms': 0,
            'records_failed': 0
        }

    @abstractmethod
    def process(self, input_df: DataFrame, **kwargs) -> DataFrame:
        pass

    def execute(self, df: DataFrame, data_id: str, source_data_id: Optional[str] = None, **kwargs) -> DataFrame:
        import time
        start_time = time.time()
        logger.info(f"Starting stage: {self.stage_name}")
        logger.info(f"Input record count: {df.count()}")

        try:
            output_df = self.process(df, **kwargs)

            processing_time = (time.time() - start_time) * 1000  
            self.metrics['records_processed'] = output_df.count()
            self.metrics['processing_time_ms'] = processing_time

            logger.info(f"Output record count: {self.metrics['records_processed']}")
            logger.info(f"Processing time (ms): {processing_time: .2f}ms")

            self.provenance_tracker.track_transformation(
                data_id=data_id,
                input_df=df,
                output_df=output_df,
                transformation_type=self.stage_name,
                source_data_id=source_data_id,
                metadata={'metrics': self.metrics, 'stage_config': kwargs}
            )

            return output_df

        except Exception as e:
            self.metrics['records_failed'] += df.count()
            logger.error(f"Error in stage {self.stage_name}: {e}")
            raise 

    def get_metrics(self) -> Dict[str, Any]:
        return self.metrics.copy()
        