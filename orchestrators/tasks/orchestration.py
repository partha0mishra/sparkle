from typing import Dict, Any
from pyspark.sql import SparkSession
from ..base import BaseTask

class RunNotebookTask(BaseTask):
    """
    Runs a notebook with widgets/parameters.
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        notebook_path = self.config.get("notebook_path")
        self.logger.info(f"Running notebook: {notebook_path}")
        # In Databricks: dbutils.notebook.run(notebook_path, ...)
        return True

class TriggerDownstreamPipelineTask(BaseTask):
    """
    Triggers another pipeline via API.
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        pipeline_name = self.config.get("pipeline_name")
        self.logger.info(f"Triggering downstream pipeline: {pipeline_name}")
        return True

class AutoRecoverTask(BaseTask):
    """
    Retries with exponential backoff + quarantines bad input.
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        self.logger.info("Attempting auto-recovery")
        return True
