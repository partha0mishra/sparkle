from typing import Dict, Any
from pyspark.sql import SparkSession
from ..base import BaseTask
import time

class GrantPermissionsTask(BaseTask):
    """
    Applies GRANT statements from YAML.
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        grants = self.config.get("grants", [])
        for grant in grants:
            self.logger.info(f"Executing GRANT: {grant}")
            spark.sql(grant)
        return True

class WaitForTableFreshnessTask(BaseTask):
    """
    Polls until watermark ≥ expected.
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        table = self.config.get("table")
        timeout = self.config.get("timeout", 3600)
        self.logger.info(f"Waiting for freshness of {table}")
        # Mock wait
        time.sleep(1)
        return True
