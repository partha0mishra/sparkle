"""
Factory for creating pipeline instances from configuration.
"""

from typing import Dict, Type, Optional
from pyspark.sql import SparkSession
from .base import BasePipeline, PipelineConfig
from .config_loader import load_pipeline_config
import logging

logger = logging.getLogger(__name__)


class PipelineFactory:
    """
    Factory for creating pipeline instances.
    """

    _registry: Dict[str, Type[BasePipeline]] = {}

    @classmethod
    def register(cls, pipeline_type: str):
        """
        Decorator to register a pipeline class.

        Args:
            pipeline_type: Type identifier for the pipeline

        Example:
            @PipelineFactory.register("bronze_raw_ingestion")
            class BronzeRawIngestionPipeline(BasePipeline):
                ...
        """
        def decorator(pipeline_class: Type[BasePipeline]):
            cls._registry[pipeline_type] = pipeline_class
            logger.debug(f"Registered pipeline type: {pipeline_type}")
            return pipeline_class
        return decorator

    @classmethod
    def create(
        cls,
        pipeline_name: str,
        env: str = "prod",
        spark: Optional[SparkSession] = None,
        config: Optional[PipelineConfig] = None
    ) -> BasePipeline:
        """
        Create a pipeline instance from configuration.

        Args:
            pipeline_name: Name of the pipeline
            env: Environment (dev, qa, prod)
            spark: SparkSession (optional, will be created if not provided)
            config: Override configuration (optional, will be loaded from file if not provided)

        Returns:
            Pipeline instance

        Raises:
            ValueError: If pipeline type is not registered
        """
        # Load config if not provided
        if config is None:
            config = load_pipeline_config(pipeline_name, env)

        # Get pipeline class from registry
        pipeline_type = config.pipeline_type
        pipeline_class = cls._registry.get(pipeline_type)

        if pipeline_class is None:
            available_types = ", ".join(cls._registry.keys())
            raise ValueError(
                f"Pipeline type '{pipeline_type}' not registered. "
                f"Available types: {available_types}"
            )

        # Create SparkSession if not provided
        if spark is None and hasattr(pipeline_class, "requires_spark") and pipeline_class.requires_spark:
            spark = SparkSession.builder \
                .appName(f"sparkle_{pipeline_name}") \
                .getOrCreate()

        # Instantiate pipeline
        pipeline = pipeline_class(config, spark)

        logger.info(f"Created pipeline: {pipeline_name} (type: {pipeline_type}, env: {env})")

        return pipeline

    @classmethod
    def list_available(cls) -> list:
        """
        List all registered pipeline types.

        Returns:
            Sorted list of pipeline type names
        """
        return sorted(cls._registry.keys())


class Pipeline:
    """
    Unified pipeline interface for easy access.
    """

    @staticmethod
    def get(
        pipeline_name: str,
        env: str = "prod",
        spark: Optional[SparkSession] = None,
        config: Optional[PipelineConfig] = None
    ) -> BasePipeline:
        """
        Get a pipeline instance.

        Args:
            pipeline_name: Name of the pipeline
            env: Environment (dev, qa, prod)
            spark: SparkSession (optional)
            config: Override configuration (optional)

        Returns:
            Pipeline instance

        Example:
            >>> from sparkle.orchestration import Pipeline
            >>> pipeline = Pipeline.get("customer_silver_daily", env="prod")
            >>> pipeline.build().run()
        """
        return PipelineFactory.create(pipeline_name, env, spark, config)

    @staticmethod
    def list() -> list:
        """
        List all available pipeline types.

        Returns:
            Sorted list of pipeline type names
        """
        return PipelineFactory.list_available()
