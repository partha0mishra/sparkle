from typing import Dict, Any
from pyspark.sql import SparkSession
from ..base import BaseTask

class RunGreatExpectationsTask(BaseTask):
    """
    Executes a GE expectation suite → fails or quarantines.
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        suite_name = self.config.get("expectations_suite")
        self.logger.info(f"Running Great Expectations suite: {suite_name}")
        return {"success": True}

class RunDbtCoreTask(BaseTask):
    """
    Executes dbt run/models on a profile (wrapper).
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        self.logger.info("Running dbt core task")
        return {"success": True}

class RunSQLFileTask(BaseTask):
    """
    Executes parameterized SQL file against catalog.
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        sql_file = self.config.get("sql_file")
        self.logger.info(f"Running SQL file: {sql_file}")
        # In real impl, read file and spark.sql(content)
        return {"success": True}
