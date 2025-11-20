"""
Ingestor factory for creating and managing ingestor instances.

Provides registry pattern for ingestors similar to connections layer.
"""

from typing import Dict, Any, Type, Optional
from pyspark.sql import SparkSession
from .base import BaseIngestor
import logging


class IngestorFactory:
    """
    Factory for creating ingestor instances.

    Registry pattern allows dynamic registration and lookup of ingestors.
    """

    _registry: Dict[str, Type[BaseIngestor]] = {}
    _logger = logging.getLogger("sparkle.ingestors.factory")

    @classmethod
    def register(cls, name: str, ingestor_class: Type[BaseIngestor]):
        """
        Register an ingestor type.

        Args:
            name: Ingestor name (e.g., 'salesforce_incremental')
            ingestor_class: Ingestor class to register
        """
        cls._registry[name] = ingestor_class
        cls._logger.debug(f"Registered ingestor: {name}")

    @classmethod
    def create(
        cls,
        name: str,
        spark: SparkSession,
        env: str = "prod",
        config: Optional[Dict[str, Any]] = None
    ) -> BaseIngestor:
        """
        Create an ingestor instance.

        Args:
            name: Ingestor name
            spark: SparkSession instance
            env: Environment (dev/qa/prod)
            config: Optional config override (otherwise loads from file)

        Returns:
            Initialized ingestor instance

        Raises:
            ValueError: If ingestor not found
        """
        if name not in cls._registry:
            available = ", ".join(sorted(cls._registry.keys()))
            raise ValueError(
                f"Ingestor '{name}' not found. Available: {available}"
            )

        ingestor_class = cls._registry[name]

        # Load config if not provided
        if config is None:
            from .config_loader import load_ingestor_config
            config = load_ingestor_config(name, env)

        # Instantiate ingestor
        ingestor = ingestor_class(
            spark=spark,
            config=config,
            ingestor_name=name,
            env=env
        )

        cls._logger.info(f"Created ingestor: {name} (env={env})")

        return ingestor

    @classmethod
    def list_available(cls) -> list:
        """
        List all registered ingestor names.

        Returns:
            Sorted list of ingestor names
        """
        return sorted(cls._registry.keys())

    @classmethod
    def get_ingestor_class(cls, name: str) -> Type[BaseIngestor]:
        """
        Get the ingestor class for a given name.

        Args:
            name: Ingestor name

        Returns:
            Ingestor class

        Raises:
            ValueError: If ingestor not found
        """
        if name not in cls._registry:
            raise ValueError(f"Ingestor '{name}' not found")

        return cls._registry[name]


def register_ingestor(name: str):
    """
    Decorator to register an ingestor.

    Usage:
        @register_ingestor("my_ingestor")
        class MyIngestor(BaseBatchIngestor):
            pass
    """
    def decorator(cls: Type[BaseIngestor]):
        IngestorFactory.register(name, cls)
        return cls
    return decorator


# Alias for cleaner API: Ingestor.get()
class Ingestor:
    """
    Unified ingestor interface for Sparkle.

    Provides clean API for creating ingestors:
        >>> from sparkle.ingestors import Ingestor
        >>> ing = Ingestor.get("salesforce_incremental", spark, env="prod")
        >>> ing.run()
    """

    @staticmethod
    def get(
        name: str,
        spark: SparkSession,
        env: str = "prod",
        config: Optional[Dict[str, Any]] = None
    ) -> BaseIngestor:
        """
        Get an ingestor instance by name.

        Args:
            name: Ingestor type (e.g., 'partitioned_parquet', 'salesforce_incremental')
            spark: SparkSession instance
            env: Environment (dev/qa/prod)
            config: Optional config override

        Returns:
            Initialized ingestor instance

        Example:
            >>> from sparkle.ingestors import Ingestor
            >>> ing = Ingestor.get("salesforce_incremental", spark, env="prod")
            >>> result = ing.run()
            >>> print(result)
        """
        return IngestorFactory.create(name, spark, env=env, config=config)

    @staticmethod
    def list() -> list:
        """
        List all available ingestor types.

        Returns:
            Sorted list of ingestor names

        Example:
            >>> from sparkle.ingestors import Ingestor
            >>> print(Ingestor.list())
            ['daily_file_drop', 'kafka_topic_raw', 'salesforce_incremental', ...]
        """
        return IngestorFactory.list_available()

    @staticmethod
    def register(name: str, ingestor_class: Type[BaseIngestor]) -> None:
        """
        Register a new ingestor type.

        Args:
            name: Ingestor name
            ingestor_class: Ingestor class to register

        Example:
            >>> from sparkle.ingestors import Ingestor
            >>> Ingestor.register("my_ingestor", MyIngestorClass)
        """
        IngestorFactory.register(name, ingestor_class)
