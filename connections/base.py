"""
Base connection classes for Project Sparkle.

All connections inherit from SparkleConnection and provide:
- Config-driven initialization
- Connection pooling and reuse
- Health checks
- Lineage integration
- Graceful error handling
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from datetime import datetime
import logging


class SparkleConnection(ABC):
    """
    Abstract base class for all Sparkle connections.

    Every connection must:
    1. Initialize from config dictionary
    2. Provide test() method for health checks
    3. Support get_connection() for actual usage
    4. Track lineage metadata
    5. Handle cleanup via close()

    Example:
        >>> config = {
        ...     "type": "jdbc",
        ...     "driver": "org.postgresql.Driver",
        ...     "url": "jdbc:postgresql://host:5432/db"
        ... }
        >>> conn = PostgreSQLConnection(spark, config, env="prod")
        >>> conn.test()
        >>> df = conn.read_table("schema.table")
        >>> conn.close()
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        env: str = "dev",
        lineage_enabled: bool = True
    ):
        """
        Initialize connection with config.

        Args:
            spark: Active SparkSession
            config: Configuration dictionary from config/{connection_type}/{env}.json
            env: Environment (dev/qa/prod)
            lineage_enabled: Whether to emit OpenLineage events
        """
        self.spark = spark
        self.config = config
        self.env = env
        self.lineage_enabled = lineage_enabled
        self.logger = logging.getLogger(f"sparkle.connections.{self.__class__.__name__}")
        self._connection = None
        self._created_at = datetime.utcnow()

    @abstractmethod
    def test(self) -> bool:
        """
        Test connection health.

        Returns:
            True if connection is healthy, False otherwise
        """
        pass

    @abstractmethod
    def get_connection(self) -> Any:
        """
        Get underlying connection object.

        Returns:
            Connection object (varies by type)
        """
        pass

    def close(self) -> None:
        """Close connection and cleanup resources."""
        if self._connection:
            try:
                self._close_implementation()
            except Exception as e:
                self.logger.warning(f"Error closing connection: {e}")
            finally:
                self._connection = None

    def _close_implementation(self) -> None:
        """Override this for connection-specific cleanup."""
        pass

    def emit_lineage(self, operation: str, metadata: Dict[str, Any]) -> None:
        """
        Emit OpenLineage event.

        Args:
            operation: Operation type (read/write/create)
            metadata: Additional metadata for lineage
        """
        if not self.lineage_enabled:
            return

        lineage_event = {
            "timestamp": datetime.utcnow().isoformat(),
            "connection_type": self.__class__.__name__,
            "operation": operation,
            "env": self.env,
            "metadata": metadata
        }

        # Integration point for OpenLineage
        self.logger.debug(f"Lineage event: {lineage_event}")

    def get_spark_options(self) -> Dict[str, str]:
        """
        Get Spark-compatible options from config.

        Returns:
            Dictionary of Spark options
        """
        return {}

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False


class JDBCConnection(SparkleConnection):
    """
    Base class for all JDBC-based connections.

    Handles:
    - JDBC driver configuration
    - Connection pooling
    - Read/write with pushdown predicates
    - Parallel partitioning

    Example:
        >>> config = {
        ...     "driver": "org.postgresql.Driver",
        ...     "url": "jdbc:postgresql://localhost:5432/mydb",
        ...     "user": "${POSTGRES_USER}",
        ...     "password": "${POSTGRES_PASSWORD}",
        ...     "properties": {
        ...         "fetchsize": "10000",
        ...         "batchsize": "10000"
        ...     }
        ... }
        >>> conn = JDBCConnection(spark, config)
        >>> df = conn.read_table("schema.table", partition_column="id", num_partitions=10)
    """

    def test(self) -> bool:
        """Test JDBC connection by executing simple query."""
        try:
            test_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.config["url"]) \
                .option("driver", self.config["driver"]) \
                .option("user", self.config.get("user", "")) \
                .option("password", self.config.get("password", "")) \
                .option("query", "SELECT 1 AS test") \
                .load()

            count = test_df.count()
            return count == 1
        except Exception as e:
            self.logger.error(f"JDBC connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Return JDBC connection options."""
        options = {
            "url": self.config["url"],
            "driver": self.config["driver"],
            "user": self.config.get("user", ""),
            "password": self.config.get("password", "")
        }

        # Merge additional properties
        if "properties" in self.config:
            options.update(self.config["properties"])

        return options

    def read_table(
        self,
        table: str,
        partition_column: Optional[str] = None,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None,
        num_partitions: int = 10,
        predicates: Optional[list] = None
    ):
        """
        Read table from JDBC source with partitioning.

        Args:
            table: Fully qualified table name
            partition_column: Column for parallel partitioning
            lower_bound: Lower bound for partitioning
            upper_bound: Upper bound for partitioning
            num_partitions: Number of parallel partitions
            predicates: List of WHERE clause predicates for partition pruning

        Returns:
            Spark DataFrame
        """
        reader = self.spark.read.format("jdbc")

        for key, value in self.get_connection().items():
            reader = reader.option(key, value)

        reader = reader.option("dbtable", table)

        if partition_column:
            reader = reader.option("partitionColumn", partition_column) \
                           .option("lowerBound", lower_bound) \
                           .option("upperBound", upper_bound) \
                           .option("numPartitions", num_partitions)

        if predicates:
            reader = reader.option("predicates", predicates)

        self.emit_lineage("read", {"table": table, "partitions": num_partitions})

        return reader.load()

    def write_table(
        self,
        df,
        table: str,
        mode: str = "append",
        batch_size: int = 10000
    ) -> None:
        """
        Write DataFrame to JDBC table.

        Args:
            df: Spark DataFrame to write
            table: Target table name
            mode: Write mode (append/overwrite/error/ignore)
            batch_size: Batch size for inserts
        """
        writer = df.write.format("jdbc")

        for key, value in self.get_connection().items():
            writer = writer.option(key, value)

        writer.option("dbtable", table) \
              .option("batchsize", batch_size) \
              .mode(mode) \
              .save()

        self.emit_lineage("write", {"table": table, "mode": mode, "rows": df.count()})


class CloudStorageConnection(SparkleConnection):
    """
    Base class for cloud storage connections (S3, ADLS, GCS).

    Handles:
    - Authentication (IAM, SAS, service accounts)
    - Path resolution
    - Lifecycle management
    - Multi-region support
    """

    def get_base_path(self) -> str:
        """Get base path for storage."""
        return self.config.get("base_path", "")

    def resolve_path(self, relative_path: str) -> str:
        """
        Resolve relative path to full cloud path.

        Args:
            relative_path: Relative path from base

        Returns:
            Full cloud path
        """
        base = self.get_base_path().rstrip("/")
        relative = relative_path.lstrip("/")
        return f"{base}/{relative}"


class StreamingConnection(SparkleConnection):
    """
    Base class for streaming connections (Kafka, Kinesis, etc.).

    Handles:
    - Stream source/sink configuration
    - Checkpointing
    - Offset management
    - Exactly-once semantics
    """

    @abstractmethod
    def get_read_stream_options(self) -> Dict[str, str]:
        """Get options for readStream."""
        pass

    @abstractmethod
    def get_write_stream_options(self) -> Dict[str, str]:
        """Get options for writeStream."""
        pass


class APIConnection(SparkleConnection):
    """
    Base class for API connections (REST, GraphQL, SOAP).

    Handles:
    - Authentication (OAuth, API keys, tokens)
    - Rate limiting
    - Pagination
    - Retry logic
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        super().__init__(spark, config, env, **kwargs)
        self.base_url = config.get("base_url", "")
        self.auth_type = config.get("auth_type", "none")
        self.rate_limit = config.get("rate_limit", 100)

    @abstractmethod
    def get_headers(self) -> Dict[str, str]:
        """Get HTTP headers including auth."""
        pass
