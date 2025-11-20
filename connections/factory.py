"""
Factory for creating Sparkle connections.

Provides unified interface to instantiate any connection by name:
    conn = ConnectionFactory.create("postgres", spark, env="prod")

All connections are registered and discovered automatically.
"""

from typing import Dict, Any, Type, Optional
from pyspark.sql import SparkSession
import logging

from .base import SparkleConnection
from .config_loader import ConfigLoader

logger = logging.getLogger("sparkle.connections.factory")


class ConnectionFactory:
    """
    Factory for creating connection instances.

    Automatically discovers and registers all SparkleConnection subclasses.

    Example:
        >>> from sparkle.connections import ConnectionFactory
        >>> conn = ConnectionFactory.create("postgres", spark, env="prod")
        >>> df = conn.read_table("customers")

        >>> # Or use get() shorthand
        >>> from sparkle.connections import get_connection
        >>> conn = get_connection("s3", spark, env="prod")
    """

    _registry: Dict[str, Type[SparkleConnection]] = {}
    _config_loader = ConfigLoader()

    @classmethod
    def register(cls, name: str, connection_class: Type[SparkleConnection]) -> None:
        """
        Register a connection class.

        Args:
            name: Connection name (e.g., 'postgres', 's3')
            connection_class: Connection class to register
        """
        cls._registry[name.lower()] = connection_class
        logger.debug(f"Registered connection: {name}")

    @classmethod
    def create(
        cls,
        name: str,
        spark: SparkSession,
        env: str = "dev",
        config: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> SparkleConnection:
        """
        Create a connection instance.

        Args:
            name: Connection name (e.g., 'postgres', 's3')
            spark: SparkSession instance
            env: Environment (dev/qa/prod)
            config: Optional config dict (if None, loads from config files)
            **kwargs: Additional arguments passed to connection constructor

        Returns:
            Initialized connection instance

        Raises:
            ValueError: If connection name not registered

        Example:
            >>> spark = SparkSession.builder.getOrCreate()
            >>> conn = ConnectionFactory.create("postgres", spark, env="prod")
            >>> conn.test()
            True
        """
        name_lower = name.lower()

        if name_lower not in cls._registry:
            available = ", ".join(sorted(cls._registry.keys()))
            raise ValueError(
                f"Connection '{name}' not registered. "
                f"Available connections: {available}"
            )

        # Load config if not provided
        if config is None:
            config = cls._config_loader.load(name_lower, env=env)

        # Instantiate connection
        connection_class = cls._registry[name_lower]
        instance = connection_class(spark, config, env=env, **kwargs)

        logger.info(f"Created connection: {name} (env={env})")
        return instance

    @classmethod
    def list_available(cls) -> list:
        """
        List all registered connection types.

        Returns:
            Sorted list of connection names
        """
        return sorted(cls._registry.keys())

    @classmethod
    def get_connection_class(cls, name: str) -> Type[SparkleConnection]:
        """
        Get connection class by name.

        Args:
            name: Connection name

        Returns:
            Connection class

        Raises:
            ValueError: If connection not registered
        """
        name_lower = name.lower()
        if name_lower not in cls._registry:
            raise ValueError(f"Connection '{name}' not registered")
        return cls._registry[name_lower]


# Convenience function
def get_connection(
    name: str,
    spark: SparkSession,
    env: str = "dev",
    config: Optional[Dict[str, Any]] = None,
    **kwargs
) -> SparkleConnection:
    """
    Convenience function to create a connection.

    Args:
        name: Connection name
        spark: SparkSession instance
        env: Environment
        config: Optional config override
        **kwargs: Additional arguments

    Returns:
        Connection instance

    Example:
        >>> from sparkle.connections import get_connection
        >>> conn = get_connection("postgres", spark, env="prod")
    """
    return ConnectionFactory.create(name, spark, env=env, config=config, **kwargs)


def register_connection(name: str):
    """
    Decorator to register a connection class.

    Example:
        >>> from sparkle.connections import register_connection
        >>> from sparkle.connections.base import JDBCConnection
        >>>
        >>> @register_connection("mydb")
        >>> class MyDatabaseConnection(JDBCConnection):
        ...     pass
    """
    def decorator(cls: Type[SparkleConnection]):
        ConnectionFactory.register(name, cls)
        return cls
    return decorator


# Alias for cleaner API: Connection.get()
class Connection:
    """
    Unified connection interface for Sparkle.

    Provides clean API for creating connections:
        >>> from sparkle.connections import Connection
        >>> conn = Connection.get("postgres", spark, env="prod")
        >>> df = conn.read(table="customers")
    """

    @staticmethod
    def get(
        name: str,
        spark: SparkSession,
        env: str = "prod",
        config: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> SparkleConnection:
        """
        Get a connection instance by name.

        Args:
            name: Connection type (e.g., 'postgres', 's3', 'kafka')
            spark: SparkSession instance
            env: Environment (dev/qa/prod)
            config: Optional config override
            **kwargs: Additional connection-specific parameters

        Returns:
            Initialized connection instance

        Example:
            >>> from sparkle.connections import Connection
            >>> conn = Connection.get("snowflake", spark, env="prod")
            >>> df = conn.read(table="CUSTOMERS")
        """
        return ConnectionFactory.create(name, spark, env=env, config=config, **kwargs)

    @staticmethod
    def list() -> list:
        """
        List all available connection types.

        Returns:
            Sorted list of connection names

        Example:
            >>> from sparkle.connections import Connection
            >>> print(Connection.list())
            ['bigquery', 'cassandra', 'delta', 'kafka', ...]
        """
        return ConnectionFactory.list_available()

    @staticmethod
    def register(name: str, connection_class: Type[SparkleConnection]) -> None:
        """
        Register a new connection type.

        Args:
            name: Connection name
            connection_class: Connection class to register

        Example:
            >>> from sparkle.connections import Connection
            >>> Connection.register("mydb", MyDatabaseConnection)
        """
        ConnectionFactory.register(name, connection_class)
