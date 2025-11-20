"""
Table format connections for Delta Lake, Apache Iceberg, Apache Hudi.

These connections handle lakehouse table formats with ACID transactions.
"""

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame

from .base import SparkleConnection
from .factory import register_connection


@register_connection("delta")
@register_connection("delta_lake")
class DeltaLakeConnection(SparkleConnection):
    """
    Delta Lake table format connection.

    Example config (config/connections/delta/prod.json):
        {
            "catalog": "spark_catalog",
            "base_path": "s3a://my-lakehouse/delta",
            "properties": {
                "delta.autoOptimize.autoCompact": "true",
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.enableChangeDataFeed": "true"
            }
        }

    Usage:
        >>> conn = get_connection("delta", spark, env="prod")
        >>> df = conn.read_table("customers", version=10)
        >>> conn.write_table(df, "customers_gold", mode="overwrite", optimize=True)
    """

    def test(self) -> bool:
        """Test Delta Lake connection."""
        try:
            # Check if Delta Lake is available
            from delta import DeltaTable
            return True
        except ImportError:
            self.logger.error("Delta Lake not available. Install: pip install delta-spark")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Delta Lake configuration."""
        return {
            "catalog": self.config.get("catalog", "spark_catalog"),
            "base_path": self.config.get("base_path", "")
        }

    def read_table(
        self,
        table_path: str,
        version: Optional[int] = None,
        timestamp: Optional[str] = None
    ) -> DataFrame:
        """
        Read Delta table with time travel support.

        Args:
            table_path: Path or table name
            version: Specific version to read
            timestamp: Timestamp for time travel (e.g., '2024-01-01')

        Returns:
            DataFrame
        """
        reader = self.spark.read.format("delta")

        if version is not None:
            reader = reader.option("versionAsOf", version)
        elif timestamp is not None:
            reader = reader.option("timestampAsOf", timestamp)

        self.emit_lineage("read", {"table": table_path, "version": version})

        return reader.load(table_path)

    def write_table(
        self,
        df: DataFrame,
        table_path: str,
        mode: str = "append",
        optimize: bool = False,
        partition_by: Optional[list] = None
    ) -> None:
        """
        Write DataFrame to Delta table.

        Args:
            df: DataFrame to write
            table_path: Target path or table name
            mode: Write mode (append/overwrite)
            optimize: Whether to optimize after write
            partition_by: Partition columns
        """
        writer = df.write.format("delta").mode(mode)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(table_path)

        # Run OPTIMIZE if requested
        if optimize:
            self.optimize_table(table_path)

        self.emit_lineage("write", {"table": table_path, "mode": mode})

    def optimize_table(self, table_path: str, zorder_by: Optional[list] = None) -> None:
        """
        Optimize Delta table (compact files).

        Args:
            table_path: Table path or name
            zorder_by: Columns for Z-ordering
        """
        optimize_cmd = f"OPTIMIZE delta.`{table_path}`"

        if zorder_by:
            zorder_cols = ", ".join(zorder_by)
            optimize_cmd += f" ZORDER BY ({zorder_cols})"

        self.spark.sql(optimize_cmd)

    def vacuum_table(self, table_path: str, retention_hours: int = 168) -> None:
        """
        Vacuum Delta table (remove old files).

        Args:
            table_path: Table path or name
            retention_hours: Retention period in hours (default: 7 days)
        """
        self.spark.sql(f"VACUUM delta.`{table_path}` RETAIN {retention_hours} HOURS")


@register_connection("iceberg")
@register_connection("apache_iceberg")
class IcebergConnection(SparkleConnection):
    """
    Apache Iceberg table format connection.

    Example config:
        {
            "catalog_name": "iceberg_catalog",
            "catalog_type": "hive",
            "warehouse": "s3a://my-lakehouse/iceberg",
            "properties": {
                "iceberg.engine.hive.enabled": "true"
            }
        }

    Or with Glue catalog:
        {
            "catalog_name": "iceberg_catalog",
            "catalog_type": "glue",
            "warehouse": "s3a://my-lakehouse/iceberg",
            "io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        super().__init__(spark, config, env, **kwargs)
        self._configure_spark()

    def _configure_spark(self):
        """Configure Spark for Iceberg."""
        conf = self.spark.sparkContext._conf
        catalog_name = self.config.get("catalog_name", "iceberg_catalog")

        # Set catalog implementation
        conf.set(
            f"spark.sql.catalog.{catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog"
        )

        catalog_type = self.config.get("catalog_type", "hive")

        if catalog_type == "hive":
            conf.set(f"spark.sql.catalog.{catalog_name}.type", "hive")
        elif catalog_type == "glue":
            conf.set(
                f"spark.sql.catalog.{catalog_name}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog"
            )
            conf.set(f"spark.sql.catalog.{catalog_name}.warehouse", self.config["warehouse"])

        # Apply additional properties
        for key, value in self.config.get("properties", {}).items():
            conf.set(f"spark.sql.catalog.{catalog_name}.{key}", str(value))

    def test(self) -> bool:
        """Test Iceberg connection."""
        try:
            catalog_name = self.config.get("catalog_name", "iceberg_catalog")
            # Try to show namespaces
            self.spark.sql(f"SHOW NAMESPACES IN {catalog_name}").collect()
            return True
        except Exception as e:
            self.logger.error(f"Iceberg connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Iceberg configuration."""
        return {
            "catalog_name": self.config.get("catalog_name", "iceberg_catalog"),
            "warehouse": self.config.get("warehouse", "")
        }

    def read_table(
        self,
        table: str,
        snapshot_id: Optional[int] = None,
        timestamp: Optional[str] = None
    ) -> DataFrame:
        """
        Read Iceberg table with time travel.

        Args:
            table: Fully qualified table name (catalog.schema.table)
            snapshot_id: Specific snapshot to read
            timestamp: Timestamp for time travel

        Returns:
            DataFrame
        """
        if snapshot_id:
            query = f"SELECT * FROM {table} VERSION AS OF {snapshot_id}"
        elif timestamp:
            query = f"SELECT * FROM {table} TIMESTAMP AS OF '{timestamp}'"
        else:
            query = f"SELECT * FROM {table}"

        self.emit_lineage("read", {"table": table, "snapshot": snapshot_id})

        return self.spark.sql(query)

    def write_table(
        self,
        df: DataFrame,
        table: str,
        mode: str = "append"
    ) -> None:
        """
        Write DataFrame to Iceberg table.

        Args:
            df: DataFrame to write
            table: Fully qualified table name
            mode: Write mode
        """
        df.writeTo(table).using("iceberg").mode(mode).create()

        self.emit_lineage("write", {"table": table, "mode": mode})


@register_connection("hudi")
@register_connection("apache_hudi")
class HudiConnection(SparkleConnection):
    """
    Apache Hudi table format connection.

    Example config:
        {
            "base_path": "s3a://my-lakehouse/hudi",
            "table_type": "COPY_ON_WRITE",
            "properties": {
                "hoodie.datasource.write.recordkey.field": "id",
                "hoodie.datasource.write.precombine.field": "updated_at",
                "hoodie.datasource.write.partitionpath.field": "date",
                "hoodie.table.name": "customers"
            }
        }
    """

    def test(self) -> bool:
        """Test Hudi connection."""
        try:
            # Check if Hudi is available
            self.spark.read.format("hudi")
            return True
        except Exception as e:
            self.logger.error(f"Hudi connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Hudi configuration."""
        config = {
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.datasource.write.table.type": self.config.get("table_type", "COPY_ON_WRITE")
        }

        config.update(self.config.get("properties", {}))

        return config

    def read_table(
        self,
        table_path: str,
        query_type: str = "snapshot"
    ) -> DataFrame:
        """
        Read Hudi table.

        Args:
            table_path: Path to Hudi table
            query_type: Query type (snapshot/incremental/read_optimized)

        Returns:
            DataFrame
        """
        reader = self.spark.read.format("hudi")

        if query_type != "snapshot":
            reader = reader.option("hoodie.datasource.query.type", query_type)

        self.emit_lineage("read", {"table": table_path, "query_type": query_type})

        return reader.load(table_path)

    def write_table(
        self,
        df: DataFrame,
        table_path: str,
        table_name: str,
        operation: str = "upsert"
    ) -> None:
        """
        Write DataFrame to Hudi table.

        Args:
            df: DataFrame to write
            table_path: Target path
            table_name: Table name
            operation: Hudi operation (upsert/insert/bulk_insert)
        """
        hudi_options = self.get_connection()
        hudi_options["hoodie.datasource.write.operation"] = operation
        hudi_options["hoodie.table.name"] = table_name

        df.write \
            .format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(table_path)

        self.emit_lineage("write", {"table": table_path, "operation": operation})


@register_connection("parquet")
class ParquetConnection(SparkleConnection):
    """
    Parquet file format connection.

    Example config:
        {
            "base_path": "s3a://my-lakehouse/parquet",
            "compression": "snappy",
            "properties": {
                "parquet.block.size": "134217728"
            }
        }
    """

    def test(self) -> bool:
        """Test Parquet connection."""
        return True

    def get_connection(self) -> Dict[str, str]:
        """Get Parquet configuration."""
        return {
            "base_path": self.config.get("base_path", ""),
            "compression": self.config.get("compression", "snappy")
        }

    def read_table(self, path: str) -> DataFrame:
        """Read Parquet files."""
        self.emit_lineage("read", {"path": path})
        return self.spark.read.parquet(path)

    def write_table(
        self,
        df: DataFrame,
        path: str,
        mode: str = "append",
        partition_by: Optional[list] = None
    ) -> None:
        """Write DataFrame to Parquet."""
        writer = df.write.mode(mode).option("compression", self.get_connection()["compression"])

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.parquet(path)

        self.emit_lineage("write", {"path": path, "mode": mode})
