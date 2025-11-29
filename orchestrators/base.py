"""
Base classes for orchestration pipelines and tasks.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
import logging


@dataclass
class PipelineConfig:
    """
    Pipeline configuration loaded from JSON/YAML.
    """
    pipeline_name: str
    pipeline_type: str
    env: str

    # Source configuration
    source_system: Optional[str] = None
    source_connection: Optional[str] = None
    source_table: Optional[str] = None
    source_tables: Optional[List[str]] = None

    # Destination configuration
    destination_catalog: Optional[str] = None
    destination_schema: Optional[str] = None
    destination_table: Optional[str] = None

    # Key columns
    primary_key: Optional[List[str]] = None
    business_key: Optional[List[str]] = None
    partition_columns: Optional[List[str]] = None
    watermark_column: Optional[str] = None

    # Processing configuration
    transformers: Optional[List[Dict[str, Any]]] = None
    ingestor: Optional[str] = None
    write_mode: str = "append"

    # Checkpointing for streaming
    checkpoint_location: Optional[str] = None

    # ML configuration
    model_name: Optional[str] = None
    feature_columns: Optional[List[str]] = None
    target_column: Optional[str] = None

    # Quality configuration
    expectations_suite: Optional[str] = None
    data_contract: Optional[Dict[str, Any]] = None

    # Scheduling
    schedule: Optional[str] = None
    dependencies: Optional[List[str]] = None

    # Observability
    tags: Dict[str, str] = field(default_factory=dict)
    alert_channels: Optional[List[str]] = None

    # Additional config
    extra_config: Dict[str, Any] = field(default_factory=dict)


class BaseTask(ABC):
    """
    Abstract base class for all tasks.
    """

    def __init__(self, task_name: str, config: Dict[str, Any]):
        self.task_name = task_name
        self.config = config
        self.logger = logging.getLogger(f"orchestration.{task_name}")

    @abstractmethod
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        """
        Execute the task.

        Args:
            spark: SparkSession
            context: Runtime context with shared state

        Returns:
            Task result (DataFrame, metrics, etc.)
        """
        pass

    def on_failure(self, error: Exception, context: Dict[str, Any]):
        """
        Handle task failure.

        Args:
            error: Exception that caused failure
            context: Runtime context
        """
        self.logger.error(f"Task {self.task_name} failed: {error}")

    def on_success(self, result: Any, context: Dict[str, Any]):
        """
        Handle task success.

        Args:
            result: Task result
            context: Runtime context
        """
        self.logger.info(f"Task {self.task_name} completed successfully")


class BasePipeline(ABC):
    """
    Abstract base class for all pipelines.
    """

    def __init__(self, pipeline_config: PipelineConfig, spark: Optional[SparkSession] = None):
        self.config = pipeline_config
        self.spark = spark
        self.logger = logging.getLogger(f"orchestration.{pipeline_config.pipeline_name}")
        self.tasks: List[BaseTask] = []
        self.metrics: Dict[str, Any] = {}
        self.context: Dict[str, Any] = {
            "pipeline_name": pipeline_config.pipeline_name,
            "env": pipeline_config.env,
            "start_time": None,
            "end_time": None
        }

    @abstractmethod
    def build(self) -> 'BasePipeline':
        """
        Build the pipeline by adding tasks.

        Returns:
            Self for chaining
        """
        pass

    def add_task(self, task: BaseTask) -> 'BasePipeline':
        """
        Add a task to the pipeline.

        Args:
            task: Task to add

        Returns:
            Self for chaining
        """
        self.tasks.append(task)
        return self

    def run(self) -> Dict[str, Any]:
        """
        Execute the pipeline by running all tasks in order.

        Returns:
            Pipeline execution metrics
        """
        self.context["start_time"] = datetime.utcnow()

        try:
            self.logger.info(f"Starting pipeline: {self.config.pipeline_name}")

            for task in self.tasks:
                self.logger.info(f"Executing task: {task.task_name}")

                try:
                    result = task.execute(self.spark, self.context)
                    task.on_success(result, self.context)

                    # Store result in context for downstream tasks
                    self.context[task.task_name] = result

                except Exception as e:
                    task.on_failure(e, self.context)
                    raise

            self.context["end_time"] = datetime.utcnow()
            self.context["status"] = "success"

            self.logger.info(f"Pipeline completed successfully: {self.config.pipeline_name}")

            return self.context

        except Exception as e:
            self.context["end_time"] = datetime.utcnow()
            self.context["status"] = "failed"
            self.context["error"] = str(e)

            self.logger.error(f"Pipeline failed: {self.config.pipeline_name} - {e}")
            raise

    def deploy(self, orchestrator: str, output_path: Optional[str] = None) -> str:
        """
        Generate deployment artifacts for the specified orchestrator.

        Args:
            orchestrator: Target orchestrator (databricks, airflow, dagster, prefect, mage)
            output_path: Path to write deployment files

        Returns:
            Path to generated deployment artifacts
        """
        from .adapters import get_adapter

        adapter = get_adapter(orchestrator)
        return adapter.generate_deployment(self, output_path)

    def validate(self) -> List[str]:
        """
        Validate pipeline configuration.

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        # Validate required fields based on pipeline type
        if not self.config.pipeline_name:
            errors.append("pipeline_name is required")

        if not self.config.pipeline_type:
            errors.append("pipeline_type is required")

        return errors

    def get_lineage(self) -> Dict[str, Any]:
        """
        Generate data lineage information.

        Returns:
            Lineage metadata in OpenLineage format
        """
        lineage = {
            "pipeline": self.config.pipeline_name,
            "inputs": [],
            "outputs": [],
            "tasks": [task.task_name for task in self.tasks]
        }

        # Add source tables
        if self.config.source_table:
            lineage["inputs"].append({
                "namespace": self.config.source_system,
                "name": self.config.source_table
            })

        if self.config.source_tables:
            for table in self.config.source_tables:
                lineage["inputs"].append({
                    "namespace": self.config.source_system,
                    "name": table
                })

        # Add destination table
        if self.config.destination_table:
            lineage["outputs"].append({
                "namespace": f"{self.config.destination_catalog}.{self.config.destination_schema}",
                "name": self.config.destination_table
            })

        return lineage

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get pipeline execution metrics.

        Returns:
            Metrics dictionary
        """
        if self.context.get("start_time") and self.context.get("end_time"):
            duration = (self.context["end_time"] - self.context["start_time"]).total_seconds()
            self.metrics["duration_seconds"] = duration

        self.metrics["status"] = self.context.get("status", "unknown")
        self.metrics["num_tasks"] = len(self.tasks)

        return self.metrics
