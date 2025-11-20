"""
Base connection classes for Project Sparkle.

All connections inherit from SparkleConnection and provide:
- Config-driven initialization with dbutils.secrets integration
- read() and write() methods for batch operations
- read_stream() and write_stream() for streaming (where applicable)
- Connection pooling and reuse
- Health checks
- Lineage integration
- Retry logic and graceful error handling
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Union
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import DataStreamReader, DataStreamWriter
from datetime import datetime
import logging
import time


class SparkleConnection(ABC):
    """
    Abstract base class for all Sparkle connections.

    Every connection must implement:
    1. test() - Health check
    2. read() - Batch read operation
    3. write() - Batch write operation
    4. read_stream() - Streaming read (if applicable)
    5. write_stream() - Streaming write (if applicable)

    Example:
        >>> conn = Connection.get("postgres", spark, env="prod")
        >>> df = conn.read(table="customers")
        >>> conn.write(df, table="customers_gold", mode="overwrite")
        >>> conn.close()
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[Dict[str, Any]] = None,
        env: str = "prod",
        lineage_enabled: bool = True
    ):
        """
        Initialize connection with config.

        Args:
            spark: Active SparkSession
            config: Configuration dictionary (optional if loading from config files)
            env: Environment (dev/qa/prod)
            lineage_enabled: Whether to emit OpenLineage events
        """
        self.spark = spark
        self.config = config or {}
        self.env = env
        self.lineage_enabled = lineage_enabled
        self.logger = logging.getLogger(f"sparkle.connections.{self.__class__.__name__}")
        self._connection = None
        self._created_at = datetime.utcnow()

        # Resolve secrets in config
        self._resolve_secrets()

    def _resolve_secrets(self) -> None:
        """Resolve secrets from config using dbutils.secrets or spark.conf."""
        # Recursively resolve secrets in config
        self.config = self._resolve_dict_secrets(self.config)

    def _resolve_dict_secrets(self, obj: Any) -> Any:
        """Recursively resolve secret references in dictionaries."""
        if isinstance(obj, dict):
            return {k: self._resolve_dict_secrets(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._resolve_dict_secrets(item) for item in obj]
        elif isinstance(obj, str):
            return self._resolve_secret_string(obj)
        return obj

    def _resolve_secret_string(self, value: str) -> str:
        """
        Resolve secret string patterns:
        - ${ENV_VAR} -> os.environ
        - secret://scope/key -> dbutils.secrets.get()
        - conf://key -> spark.conf.get()
        """
        import re
        import os

        # Pattern: ${ENV_VAR}
        env_pattern = re.compile(r'\$\{([A-Z_][A-Z0-9_]*)\}')
        value = env_pattern.sub(
            lambda m: os.environ.get(m.group(1), f"${{MISSING:{m.group(1)}}}"),
            value
        )

        # Pattern: secret://scope/key (Databricks secrets)
        secret_pattern = re.compile(r'secret://([^/]+)/(.+)')
        match = secret_pattern.match(value)
        if match:
            scope, key = match.groups()
            try:
                # Try Databricks dbutils
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(self.spark)
                return dbutils.secrets.get(scope=scope, key=key)
            except:
                self.logger.warning(f"Could not resolve secret://{scope}/{key}")
                return value

        # Pattern: conf://key (Spark conf)
        conf_pattern = re.compile(r'conf://(.+)')
        match = conf_pattern.match(value)
        if match:
            key = match.group(1)
            return self.spark.conf.get(key, value)

        return value

    def get_secret(self, scope: str, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get secret from Databricks secrets or fallback to env vars.

        Args:
            scope: Secret scope
            key: Secret key
            default: Default value if not found

        Returns:
            Secret value or default
        """
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.spark)
            return dbutils.secrets.get(scope=scope, key=key)
        except:
            # Fallback to environment variable
            import os
            env_key = f"{scope}_{key}".upper().replace("-", "_").replace(".", "_")
            return os.environ.get(env_key, default)

    def get_conf(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get value from Spark configuration.

        Args:
            key: Config key
            default: Default value if not found

        Returns:
            Config value or default
        """
        return self.spark.conf.get(key, default)

    @abstractmethod
    def test(self) -> bool:
        """
        Test connection health.

        Returns:
            True if connection is healthy, False otherwise
        """
        pass

    @abstractmethod
    def read(self, **kwargs) -> DataFrame:
        """
        Read data from source (batch).

        Args:
            **kwargs: Source-specific read parameters

        Returns:
            Spark DataFrame

        Example:
            >>> df = conn.read(table="customers")
            >>> df = conn.read(path="s3://bucket/data/")
            >>> df = conn.read(query="SELECT * FROM customers")
        """
        pass

    @abstractmethod
    def write(self, df: DataFrame, **kwargs) -> None:
        """
        Write DataFrame to destination (batch).

        Args:
            df: Spark DataFrame to write
            **kwargs: Destination-specific write parameters

        Example:
            >>> conn.write(df, table="customers", mode="overwrite")
            >>> conn.write(df, path="s3://bucket/output/", format="parquet")
        """
        pass

    def read_stream(self, **kwargs) -> DataStreamReader:
        """
        Read streaming data from source.

        Args:
            **kwargs: Source-specific streaming parameters

        Returns:
            DataStreamReader or DataFrame

        Raises:
            NotImplementedError: If streaming not supported for this connection

        Example:
            >>> stream_df = conn.read_stream(topic="events")
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support streaming reads"
        )

    def write_stream(self, df: DataFrame, **kwargs) -> DataStreamWriter:
        """
        Write streaming DataFrame to destination.

        Args:
            df: Streaming DataFrame
            **kwargs: Destination-specific streaming parameters

        Returns:
            DataStreamWriter

        Raises:
            NotImplementedError: If streaming not supported for this connection

        Example:
            >>> query = conn.write_stream(
            ...     stream_df,
            ...     checkpoint_location="/tmp/checkpoint",
            ...     output_mode="append"
            ... )
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support streaming writes"
        )

    def get_connection(self) -> Any:
        """
        Get underlying connection object/options.

        Returns:
            Connection object or options dictionary (varies by type)
        """
        return self.config

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

    def retry(
        self,
        func,
        max_attempts: int = 3,
        delay: int = 1,
        backoff: int = 2,
        exceptions: tuple = (Exception,)
    ):
        """
        Retry a function with exponential backoff.

        Args:
            func: Function to retry
            max_attempts: Maximum retry attempts
            delay: Initial delay in seconds
            backoff: Backoff multiplier
            exceptions: Tuple of exceptions to catch

        Returns:
            Function result

        Raises:
            Last exception if all attempts fail
        """
        last_exception = None

        for attempt in range(max_attempts):
            try:
                return func()
            except exceptions as e:
                last_exception = e
                if attempt < max_attempts - 1:
                    wait_time = delay * (backoff ** attempt)
                    self.logger.warning(
                        f"Attempt {attempt + 1}/{max_attempts} failed: {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"All {max_attempts} attempts failed")

        raise last_exception

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
    """

    def test(self) -> bool:
        """Test JDBC connection by executing simple query."""
        try:
            test_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.config["url"]) \
                .option("driver", self.config.get("driver", "")) \
                .option("user", self.config.get("user", "")) \
                .option("password", self.config.get("password", "")) \
                .option("query", "SELECT 1 AS test") \
                .load()

            return test_df.count() == 1
        except Exception as e:
            self.logger.error(f"JDBC connection test failed: {e}")
            return False

    def get_jdbc_options(self) -> Dict[str, str]:
        """Return JDBC connection options."""
        options = {
            "url": self.config["url"],
            "driver": self.config.get("driver", ""),
            "user": self.config.get("user", ""),
            "password": self.config.get("password", "")
        }

        # Merge additional properties
        if "properties" in self.config:
            options.update(self.config["properties"])

        return {k: str(v) for k, v in options.items() if v}

    def read(
        self,
        table: Optional[str] = None,
        query: Optional[str] = None,
        partition_column: Optional[str] = None,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None,
        num_partitions: int = 10,
        predicates: Optional[list] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read from JDBC source.

        Args:
            table: Table name (mutually exclusive with query)
            query: SQL query (mutually exclusive with table)
            partition_column: Column for parallel partitioning
            lower_bound: Lower bound for partitioning
            upper_bound: Upper bound for partitioning
            num_partitions: Number of parallel partitions
            predicates: List of WHERE clause predicates
            **kwargs: Additional Spark read options

        Returns:
            Spark DataFrame
        """
        if not table and not query:
            raise ValueError("Either 'table' or 'query' must be provided")

        reader = self.spark.read.format("jdbc")

        for key, value in self.get_jdbc_options().items():
            reader = reader.option(key, value)

        if table:
            reader = reader.option("dbtable", table)
        else:
            reader = reader.option("query", query)

        if partition_column:
            reader = reader.option("partitionColumn", partition_column) \
                           .option("lowerBound", lower_bound or 0) \
                           .option("upperBound", upper_bound or 1000000) \
                           .option("numPartitions", num_partitions)

        if predicates:
            reader = reader.option("predicates", predicates)

        # Apply additional options
        for key, value in kwargs.items():
            reader = reader.option(key, value)

        self.emit_lineage("read", {"table": table, "query": query, "partitions": num_partitions})

        return reader.load()

    def write(
        self,
        df: DataFrame,
        table: str,
        mode: str = "append",
        batch_size: int = 10000,
        **kwargs
    ) -> None:
        """
        Write DataFrame to JDBC table.

        Args:
            df: Spark DataFrame to write
            table: Target table name
            mode: Write mode (append/overwrite/error/ignore)
            batch_size: Batch size for inserts
            **kwargs: Additional Spark write options
        """
        writer = df.write.format("jdbc")

        for key, value in self.get_jdbc_options().items():
            writer = writer.option(key, value)

        writer = writer.option("dbtable", table) \
                       .option("batchsize", batch_size) \
                       .mode(mode)

        # Apply additional options
        for key, value in kwargs.items():
            writer = writer.option(key, value)

        writer.save()

        self.emit_lineage("write", {"table": table, "mode": mode, "rows": df.count()})


class CloudStorageConnection(SparkleConnection):
    """
    Base class for cloud storage connections (S3, ADLS, GCS).

    Handles:
    - Authentication (IAM, SAS, service accounts)
    - Path resolution
    - Multi-format support
    - Multi-region support
    """

    def get_base_path(self) -> str:
        """Get base path for storage."""
        return self.config.get("base_path", "")

    def resolve_path(self, relative_path: str = "") -> str:
        """
        Resolve relative path to full cloud path.

        Args:
            relative_path: Relative path from base

        Returns:
            Full cloud path
        """
        base = self.get_base_path().rstrip("/")
        if not relative_path:
            return base
        relative = relative_path.lstrip("/")
        return f"{base}/{relative}"

    def read(
        self,
        path: Optional[str] = None,
        format: str = "parquet",
        schema: Optional[Any] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read from cloud storage.

        Args:
            path: Path (relative to base_path or absolute)
            format: File format (parquet/delta/csv/json/avro/orc)
            schema: Optional schema
            **kwargs: Additional Spark read options

        Returns:
            Spark DataFrame
        """
        full_path = self.resolve_path(path) if path else self.get_base_path()

        reader = self.spark.read.format(format)

        if schema:
            reader = reader.schema(schema)

        for key, value in kwargs.items():
            reader = reader.option(key, value)

        self.emit_lineage("read", {"path": full_path, "format": format})

        return reader.load(full_path)

    def write(
        self,
        df: DataFrame,
        path: Optional[str] = None,
        format: str = "parquet",
        mode: str = "append",
        partition_by: Optional[list] = None,
        **kwargs
    ) -> None:
        """
        Write to cloud storage.

        Args:
            df: Spark DataFrame to write
            path: Path (relative to base_path or absolute)
            format: File format (parquet/delta/csv/json/avro/orc)
            mode: Write mode (append/overwrite/error/ignore)
            partition_by: Partition columns
            **kwargs: Additional Spark write options
        """
        full_path = self.resolve_path(path) if path else self.get_base_path()

        writer = df.write.format(format).mode(mode)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        for key, value in kwargs.items():
            writer = writer.option(key, value)

        writer.save(full_path)

        self.emit_lineage("write", {"path": full_path, "format": format, "mode": mode})


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

    def read_stream(
        self,
        **kwargs
    ) -> DataFrame:
        """
        Read streaming data.

        Args:
            **kwargs: Streaming-specific options

        Returns:
            Streaming DataFrame
        """
        options = self.get_read_stream_options()
        options.update(kwargs)

        reader = self.spark.readStream.format(self.get_stream_format())

        for key, value in options.items():
            reader = reader.option(key, str(value))

        self.emit_lineage("read_stream", {"options": list(kwargs.keys())})

        return reader.load()

    def write_stream(
        self,
        df: DataFrame,
        checkpoint_location: str,
        output_mode: str = "append",
        trigger: Optional[Dict] = None,
        **kwargs
    ):
        """
        Write streaming DataFrame.

        Args:
            df: Streaming DataFrame
            checkpoint_location: Checkpoint directory
            output_mode: Output mode (append/update/complete)
            trigger: Trigger configuration
            **kwargs: Additional streaming options

        Returns:
            StreamingQuery
        """
        options = self.get_write_stream_options()
        options.update(kwargs)

        writer = df.writeStream.format(self.get_stream_format()) \
                                .outputMode(output_mode) \
                                .option("checkpointLocation", checkpoint_location)

        if trigger:
            if "processingTime" in trigger:
                writer = writer.trigger(processingTime=trigger["processingTime"])
            elif "once" in trigger:
                writer = writer.trigger(once=True)
            elif "continuous" in trigger:
                writer = writer.trigger(continuous=trigger["continuous"])

        for key, value in options.items():
            writer = writer.option(key, str(value))

        self.emit_lineage("write_stream", {"checkpoint": checkpoint_location, "mode": output_mode})

        return writer.start()

    @abstractmethod
    def get_stream_format(self) -> str:
        """Get streaming format name (e.g., 'kafka', 'kinesis')."""
        pass


class APIConnection(SparkleConnection):
    """
    Base class for API connections (REST, GraphQL, SOAP, SaaS).

    Handles:
    - Authentication (OAuth2, API keys, tokens)
    - Rate limiting
    - Pagination
    - Retry logic
    """

    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None, env: str = "prod", **kwargs):
        super().__init__(spark, config, env, **kwargs)
        self.base_url = config.get("base_url", "")
        self.auth_type = config.get("auth_type", "none")
        self.rate_limit = config.get("rate_limit", 100)
        self.max_retries = config.get("max_retries", 3)

    @abstractmethod
    def get_headers(self) -> Dict[str, str]:
        """Get HTTP headers including authentication."""
        pass

    def read(self, **kwargs) -> DataFrame:
        """
        Read data from API and return as DataFrame.

        Subclasses must implement this to handle API-specific logic.
        """
        raise NotImplementedError("Subclass must implement read()")

    def write(self, df: DataFrame, **kwargs) -> None:
        """
        Write DataFrame to API.

        Subclasses must implement this to handle API-specific logic.
        """
        raise NotImplementedError("Subclass must implement write()")
