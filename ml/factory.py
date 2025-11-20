"""
Factory for creating ML components.

Provides registry pattern for ML components similar to connections and ingestors.
"""

from typing import Dict, Any, Type, Optional
from pyspark.sql import SparkSession
from .base import BaseMLComponent
import logging


logger = logging.getLogger("sparkle.ml.factory")


class MLComponentFactory:
    """Factory for creating ML component instances."""

    _registry: Dict[str, Type[BaseMLComponent]] = {}

    @classmethod
    def register(cls, name: str, component_class: Type[BaseMLComponent]):
        """Register an ML component type."""
        cls._registry[name] = component_class
        logger.debug(f"Registered ML component: {name}")

    @classmethod
    def create(
        cls,
        name: str,
        spark: SparkSession,
        env: str = "prod",
        config: Optional[Dict[str, Any]] = None
    ) -> BaseMLComponent:
        """
        Create an ML component instance.

        Args:
            name: Component name (e.g., 'churn_model_v3')
            spark: SparkSession
            env: Environment (dev/qa/prod)
            config: Optional config override

        Returns:
            Initialized ML component

        Raises:
            ValueError: If component not found
        """
        if name not in cls._registry:
            available = ", ".join(sorted(cls._registry.keys()))
            raise ValueError(
                f"ML component '{name}' not found. Available: {available}"
            )

        component_class = cls._registry[name]

        # Load config if not provided
        if config is None:
            from .config_loader import load_ml_config
            config = load_ml_config(name, env)

        # Instantiate component
        component = component_class(
            spark=spark,
            config=config,
            component_name=name,
            env=env
        )

        logger.info(f"Created ML component: {name} (env={env})")

        return component

    @classmethod
    def list_available(cls) -> list:
        """List all registered ML component names."""
        return sorted(cls._registry.keys())


def register_ml_component(name: str):
    """
    Decorator to register an ML component.

    Usage:
        @register_ml_component("my_feature_engineer")
        class MyFeatureEngineer(BaseFeatureEngineer):
            pass
    """
    def decorator(cls: Type[BaseMLComponent]):
        MLComponentFactory.register(name, cls)
        return cls
    return decorator


# Alias for cleaner API
class MLComponent:
    """
    Unified ML component interface.

    Usage:
        >>> from sparkle.ml import MLComponent
        >>> component = MLComponent.get("churn_model_v3", spark, env="prod")
        >>> result = component.run()
    """

    @staticmethod
    def get(
        name: str,
        spark: SparkSession,
        env: str = "prod",
        config: Optional[Dict[str, Any]] = None
    ) -> BaseMLComponent:
        """Get an ML component instance by name."""
        return MLComponentFactory.create(name, spark, env=env, config=config)

    @staticmethod
    def list() -> list:
        """List all available ML component types."""
        return MLComponentFactory.list_available()

    @staticmethod
    def register(name: str, component_class: Type[BaseMLComponent]) -> None:
        """Register a new ML component type."""
        MLComponentFactory.register(name, component_class)
