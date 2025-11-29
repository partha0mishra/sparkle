from typing import Dict, Any
from pyspark.sql import SparkSession
from ..base import BaseTask
from ingestors.factory import get_ingestor

class IngestTask(BaseTask):
    """
    Executes any registered ingestor with parameters.
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        ingestor_name = self.config.get("ingestor")
        ingestor_config = self.config.get("ingestor_config", {})
        
        self.logger.info(f"Running IngestTask with ingestor: {ingestor_name}")
        
        ingestor = get_ingestor(ingestor_name)
        df = ingestor.ingest(spark, ingestor_config)
        
        return df
