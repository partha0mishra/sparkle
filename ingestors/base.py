"""
Base classes for Sparkle ingestors.

Provides abstract base classes with watermark tracking, audit logging,
and schema drift detection.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import logging


class BaseIngestor(ABC):
    """
    Abstract base class for all Sparkle ingestors.

    Provides common functionality:
    - Watermark tracking in system.ingestion_watermarks
    - Audit logging in system.ingestion_audit_log
    - Schema drift detection
    - Idempotent execution
    - Configuration management
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        ingestor_name: str,
        env: str = "prod"
    ):
        """
        Initialize base ingestor.

        Args:
            spark: SparkSession instance
            config: Configuration dictionary from config/{ingestor_name}/{env}.json
            ingestor_name: Name of the ingestor (e.g., 'salesforce_incremental')
            env: Environment (dev/qa/prod)
        """
        self.spark = spark
        self.config = config
        self.ingestor_name = ingestor_name
        self.env = env
        self.logger = logging.getLogger(f"sparkle.ingestors.{ingestor_name}")

        # Extract common config
        self.source_name = config.get("source_name")
        self.target_table = config.get("target_table")
        self.target_catalog = config.get("target_catalog", "bronze")
        self.target_schema = config.get("target_schema", "default")

        # Watermark config
        self.watermark_column = config.get("watermark_column")
        self.watermark_enabled = config.get("watermark_enabled", True)

        # Audit config
        self.audit_enabled = config.get("audit_enabled", True)

        # Schema drift config
        self.schema_evolution = config.get("schema_evolution", "add_new_columns")

        # Initialize watermark and audit tables
        self._init_system_tables()

    def _init_system_tables(self):
        """Initialize system watermark and audit tables if they don't exist."""

        # Create system catalog/schema if needed
        self.spark.sql("CREATE CATALOG IF NOT EXISTS system")
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS system.default")

        # Create watermark table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS system.ingestion_watermarks (
                ingestor_name STRING,
                source_name STRING,
                env STRING,
                watermark_value STRING,
                watermark_type STRING,
                last_updated TIMESTAMP,
                metadata MAP<STRING, STRING>
            )
            USING DELTA
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true'
            )
        """)

        # Create audit log table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS system.ingestion_audit_log (
                run_id STRING,
                ingestor_name STRING,
                source_name STRING,
                env STRING,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duration_seconds DOUBLE,
                status STRING,
                rows_read BIGINT,
                rows_written BIGINT,
                files_processed INT,
                bytes_processed BIGINT,
                schema_changes ARRAY<STRING>,
                watermark_before STRING,
                watermark_after STRING,
                error_message STRING,
                metadata MAP<STRING, STRING>
            )
            USING DELTA
            PARTITIONED BY (ingestor_name, env)
        """)

    def get_watermark(self, watermark_type: str = "datetime") -> Optional[str]:
        """
        Get the last watermark value for this ingestor.

        Args:
            watermark_type: Type of watermark (datetime, id, file, etc.)

        Returns:
            Last watermark value or None if first run
        """
        if not self.watermark_enabled:
            return None

        watermark_df = self.spark.sql(f"""
            SELECT watermark_value
            FROM system.ingestion_watermarks
            WHERE ingestor_name = '{self.ingestor_name}'
              AND source_name = '{self.source_name}'
              AND env = '{self.env}'
              AND watermark_type = '{watermark_type}'
            ORDER BY last_updated DESC
            LIMIT 1
        """)

        if watermark_df.count() > 0:
            return watermark_df.first()["watermark_value"]
        else:
            self.logger.info(f"No watermark found for {self.ingestor_name} - first run")
            return None

    def update_watermark(
        self,
        watermark_value: str,
        watermark_type: str = "datetime",
        metadata: Optional[Dict[str, str]] = None
    ):
        """
        Update the watermark for this ingestor.

        Args:
            watermark_value: New watermark value
            watermark_type: Type of watermark
            metadata: Additional metadata to store
        """
        if not self.watermark_enabled:
            return

        from pyspark.sql.functions import lit, current_timestamp, map_from_arrays

        watermark_data = [
            (
                self.ingestor_name,
                self.source_name,
                self.env,
                watermark_value,
                watermark_type,
                metadata or {}
            )
        ]

        watermark_df = (
            self.spark.createDataFrame(
                watermark_data,
                ["ingestor_name", "source_name", "env", "watermark_value", "watermark_type", "metadata"]
            )
            .withColumn("last_updated", current_timestamp())
        )

        # Upsert watermark
        watermark_df.write.mode("append").saveAsTable("system.ingestion_watermarks")

        self.logger.info(f"Updated watermark to: {watermark_value}")

    def log_audit(
        self,
        run_id: str,
        start_time: datetime,
        end_time: datetime,
        status: str,
        rows_read: int = 0,
        rows_written: int = 0,
        files_processed: int = 0,
        bytes_processed: int = 0,
        schema_changes: Optional[List[str]] = None,
        watermark_before: Optional[str] = None,
        watermark_after: Optional[str] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ):
        """
        Log audit metrics for this ingestion run.

        Args:
            run_id: Unique run identifier
            start_time: Start timestamp
            end_time: End timestamp
            status: success/failed/partial
            rows_read: Number of rows read from source
            rows_written: Number of rows written to target
            files_processed: Number of files processed
            bytes_processed: Bytes processed
            schema_changes: List of schema changes detected
            watermark_before: Watermark before run
            watermark_after: Watermark after run
            error_message: Error message if failed
            metadata: Additional metadata
        """
        if not self.audit_enabled:
            return

        duration = (end_time - start_time).total_seconds()

        audit_data = [(
            run_id,
            self.ingestor_name,
            self.source_name,
            self.env,
            start_time,
            end_time,
            duration,
            status,
            rows_read,
            rows_written,
            files_processed,
            bytes_processed,
            schema_changes or [],
            watermark_before,
            watermark_after,
            error_message,
            metadata or {}
        )]

        audit_df = self.spark.createDataFrame(
            audit_data,
            ["run_id", "ingestor_name", "source_name", "env", "start_time", "end_time",
             "duration_seconds", "status", "rows_read", "rows_written", "files_processed",
             "bytes_processed", "schema_changes", "watermark_before", "watermark_after",
             "error_message", "metadata"]
        )

        audit_df.write.mode("append").saveAsTable("system.ingestion_audit_log")

        self.logger.info(
            f"Audit logged: run_id={run_id}, status={status}, "
            f"rows_read={rows_read}, rows_written={rows_written}, duration={duration:.2f}s"
        )

    def detect_schema_changes(
        self,
        new_schema: StructType,
        existing_table: str
    ) -> List[str]:
        """
        Detect schema changes between new data and existing table.

        Args:
            new_schema: Schema of new incoming data
            existing_table: Fully qualified table name

        Returns:
            List of schema changes detected
        """
        changes = []

        try:
            # Get existing schema
            existing_df = self.spark.table(existing_table)
            existing_schema = existing_df.schema

            # Find new columns
            existing_cols = {f.name for f in existing_schema.fields}
            new_cols = {f.name for f in new_schema.fields}

            added_cols = new_cols - existing_cols
            if added_cols:
                changes.append(f"Added columns: {', '.join(added_cols)}")

            # Find removed columns
            removed_cols = existing_cols - new_cols
            if removed_cols:
                changes.append(f"Removed columns: {', '.join(removed_cols)}")

            # Find type changes
            for field in new_schema.fields:
                if field.name in existing_cols:
                    existing_field = existing_schema[field.name]
                    if existing_field.dataType != field.dataType:
                        changes.append(
                            f"Type change: {field.name} "
                            f"from {existing_field.dataType} to {field.dataType}"
                        )

        except Exception as e:
            self.logger.warning(f"Could not detect schema changes: {e}")

        return changes

    def apply_schema_evolution(
        self,
        df: DataFrame,
        target_table: str
    ) -> DataFrame:
        """
        Apply schema evolution strategy to DataFrame.

        Args:
            df: Input DataFrame
            target_table: Target table name

        Returns:
            DataFrame with schema evolution applied
        """
        if self.schema_evolution == "strict":
            # No schema changes allowed - will fail if schemas don't match
            return df

        elif self.schema_evolution == "add_new_columns":
            # Allow adding new columns, merge with existing
            try:
                existing_df = self.spark.table(target_table)
                existing_schema = existing_df.schema

                # Add missing columns to incoming data
                for field in existing_schema.fields:
                    if field.name not in df.columns:
                        df = df.withColumn(field.name, lit(None).cast(field.dataType))

                # Reorder columns to match existing schema
                df = df.select([field.name for field in existing_schema.fields] +
                              [col for col in df.columns if col not in existing_schema.fieldNames()])

            except Exception:
                # Table doesn't exist yet - use incoming schema
                pass

        elif self.schema_evolution == "merge_schemas":
            # Merge schemas - Spark will handle this automatically with mergeSchema option
            pass

        return df

    @abstractmethod
    def ingest(self) -> Dict[str, Any]:
        """
        Execute the ingestion.

        Returns:
            Dictionary with ingestion metrics
        """
        pass

    def run(self) -> Dict[str, Any]:
        """
        Execute the ingestion with watermark tracking and audit logging.

        Returns:
            Dictionary with run metrics
        """
        import uuid

        run_id = str(uuid.uuid4())
        start_time = datetime.utcnow()
        watermark_before = self.get_watermark() if self.watermark_enabled else None

        try:
            self.logger.info(f"Starting ingestion: {self.ingestor_name} (run_id={run_id})")

            # Execute ingestion
            metrics = self.ingest()

            end_time = datetime.utcnow()
            watermark_after = metrics.get("watermark_after", watermark_before)

            # Update watermark if changed
            if watermark_after and watermark_after != watermark_before:
                self.update_watermark(watermark_after)

            # Log success audit
            self.log_audit(
                run_id=run_id,
                start_time=start_time,
                end_time=end_time,
                status="success",
                rows_read=metrics.get("rows_read", 0),
                rows_written=metrics.get("rows_written", 0),
                files_processed=metrics.get("files_processed", 0),
                bytes_processed=metrics.get("bytes_processed", 0),
                schema_changes=metrics.get("schema_changes", []),
                watermark_before=watermark_before,
                watermark_after=watermark_after,
                metadata=metrics.get("metadata", {})
            )

            self.logger.info(f"Ingestion completed successfully: {self.ingestor_name}")

            return {
                "run_id": run_id,
                "status": "success",
                "metrics": metrics
            }

        except Exception as e:
            end_time = datetime.utcnow()
            error_message = str(e)

            self.logger.error(f"Ingestion failed: {self.ingestor_name} - {error_message}")

            # Log failure audit
            self.log_audit(
                run_id=run_id,
                start_time=start_time,
                end_time=end_time,
                status="failed",
                watermark_before=watermark_before,
                error_message=error_message
            )

            raise


class BaseBatchIngestor(BaseIngestor):
    """
    Base class for batch ingestors.

    Batch ingestors load data in discrete chunks, typically with
    watermark-based incremental loading.
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], ingestor_name: str, env: str = "prod"):
        super().__init__(spark, config, ingestor_name, env)

        # Batch-specific config
        self.batch_size = config.get("batch_size", 10000)
        self.parallel_reads = config.get("parallel_reads", 4)

    @abstractmethod
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        """
        Read a batch of data from the source.

        Args:
            watermark: Last watermark value (None for initial load)

        Returns:
            DataFrame with new data since watermark
        """
        pass

    def write_batch(self, df: DataFrame, mode: str = "append") -> int:
        """
        Write batch to target table.

        Args:
            df: DataFrame to write
            mode: Write mode (append/overwrite/merge)

        Returns:
            Number of rows written
        """
        full_table_name = f"{self.target_catalog}.{self.target_schema}.{self.target_table}"

        # Apply schema evolution
        df = self.apply_schema_evolution(df, full_table_name)

        # Detect schema changes
        schema_changes = self.detect_schema_changes(df.schema, full_table_name)

        # Write to Delta
        if mode == "append":
            df.write.mode("append").option("mergeSchema", "true").saveAsTable(full_table_name)
        elif mode == "overwrite":
            df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)
        elif mode == "merge":
            # Implement merge logic based on merge_keys
            merge_keys = self.config.get("merge_keys", [])
            if not merge_keys:
                raise ValueError("merge_keys required for merge mode")

            # Use Delta MERGE
            from delta.tables import DeltaTable

            if self.spark.catalog.tableExists(full_table_name):
                delta_table = DeltaTable.forName(self.spark, full_table_name)

                merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

                (delta_table.alias("target")
                 .merge(df.alias("source"), merge_condition)
                 .whenMatchedUpdateAll()
                 .whenNotMatchedInsertAll()
                 .execute())
            else:
                df.write.mode("overwrite").saveAsTable(full_table_name)

        row_count = df.count()

        return row_count

    def ingest(self) -> Dict[str, Any]:
        """Execute batch ingestion."""
        watermark = self.get_watermark()

        # Read batch
        df = self.read_batch(watermark)

        if df.isEmpty():
            self.logger.info("No new data to ingest")
            return {
                "rows_read": 0,
                "rows_written": 0,
                "watermark_after": watermark
            }

        rows_read = df.count()

        # Write batch
        write_mode = self.config.get("write_mode", "append")
        rows_written = self.write_batch(df, mode=write_mode)

        # Calculate new watermark
        watermark_column = self.watermark_column
        if watermark_column and watermark_column in df.columns:
            from pyspark.sql.functions import max as spark_max
            watermark_after = df.select(spark_max(watermark_column)).first()[0]
            if watermark_after:
                watermark_after = str(watermark_after)
        else:
            watermark_after = watermark

        return {
            "rows_read": rows_read,
            "rows_written": rows_written,
            "watermark_after": watermark_after
        }


class BaseStreamingIngestor(BaseIngestor):
    """
    Base class for streaming ingestors.

    Streaming ingestors process data continuously using
    Spark Structured Streaming.
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], ingestor_name: str, env: str = "prod"):
        super().__init__(spark, config, ingestor_name, env)

        # Streaming-specific config
        self.checkpoint_location = config.get(
            "checkpoint_location",
            f"/tmp/sparkle/checkpoints/{ingestor_name}/{env}"
        )
        self.trigger_interval = config.get("trigger_interval", "1 minute")
        self.output_mode = config.get("output_mode", "append")

    @abstractmethod
    def read_stream(self) -> DataFrame:
        """
        Read streaming data from source.

        Returns:
            Streaming DataFrame
        """
        pass

    def write_stream(self, df: DataFrame):
        """
        Write streaming DataFrame to target.

        Args:
            df: Streaming DataFrame

        Returns:
            StreamingQuery
        """
        full_table_name = f"{self.target_catalog}.{self.target_schema}.{self.target_table}"

        query = (
            df.writeStream
            .format("delta")
            .outputMode(self.output_mode)
            .option("checkpointLocation", self.checkpoint_location)
            .option("mergeSchema", "true")
            .trigger(processingTime=self.trigger_interval)
            .toTable(full_table_name)
        )

        return query

    def ingest(self) -> Dict[str, Any]:
        """Execute streaming ingestion."""
        # Read stream
        stream_df = self.read_stream()

        # Write stream
        query = self.write_stream(stream_df)

        return {
            "query_id": query.id,
            "status": query.status,
            "checkpoint_location": self.checkpoint_location
        }


class BaseCdcIngestor(BaseBatchIngestor):
    """
    Base class for Change Data Capture (CDC) ingestors.

    CDC ingestors handle INSERT/UPDATE/DELETE operations
    from change streams or log-based CDC sources.
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], ingestor_name: str, env: str = "prod"):
        super().__init__(spark, config, ingestor_name, env)

        # CDC-specific config
        self.cdc_operation_column = config.get("cdc_operation_column", "op")
        self.cdc_primary_keys = config.get("cdc_primary_keys", [])
        self.apply_deletes = config.get("apply_deletes", True)

    def apply_cdc(self, cdc_df: DataFrame) -> int:
        """
        Apply CDC changes (INSERT/UPDATE/DELETE) to target table.

        Args:
            cdc_df: DataFrame with CDC changes

        Returns:
            Number of rows affected
        """
        from delta.tables import DeltaTable
        from pyspark.sql.functions import col

        full_table_name = f"{self.target_catalog}.{self.target_schema}.{self.target_table}"

        if not self.cdc_primary_keys:
            raise ValueError("cdc_primary_keys required for CDC ingestion")

        # Separate INSERT/UPDATE vs DELETE
        inserts_updates = cdc_df.filter(
            col(self.cdc_operation_column).isin(["c", "u", "r", "insert", "update", "read"])
        )
        deletes = cdc_df.filter(
            col(self.cdc_operation_column).isin(["d", "delete"])
        )

        rows_affected = 0

        # Apply INSERT/UPDATE
        if not inserts_updates.isEmpty():
            if self.spark.catalog.tableExists(full_table_name):
                delta_table = DeltaTable.forName(self.spark, full_table_name)

                merge_condition = " AND ".join([
                    f"target.{k} = source.{k}" for k in self.cdc_primary_keys
                ])

                (delta_table.alias("target")
                 .merge(inserts_updates.alias("source"), merge_condition)
                 .whenMatchedUpdateAll()
                 .whenNotMatchedInsertAll()
                 .execute())
            else:
                inserts_updates.write.mode("overwrite").saveAsTable(full_table_name)

            rows_affected += inserts_updates.count()

        # Apply DELETE
        if self.apply_deletes and not deletes.isEmpty():
            if self.spark.catalog.tableExists(full_table_name):
                delta_table = DeltaTable.forName(self.spark, full_table_name)

                delete_condition = " OR ".join([
                    f"({' AND '.join([f'target.{k} = {repr(row[k])}' for k in self.cdc_primary_keys])})"
                    for row in deletes.select(self.cdc_primary_keys).distinct().collect()
                ])

                if delete_condition:
                    delta_table.delete(delete_condition)

            rows_affected += deletes.count()

        return rows_affected

    def write_batch(self, df: DataFrame, mode: str = "cdc") -> int:
        """
        Override write_batch to apply CDC logic.

        Args:
            df: DataFrame with CDC changes
            mode: Write mode (should be 'cdc')

        Returns:
            Number of rows affected
        """
        if mode == "cdc":
            return self.apply_cdc(df)
        else:
            return super().write_batch(df, mode)
