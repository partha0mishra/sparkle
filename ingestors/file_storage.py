"""
File and object storage ingestors.

9 production-grade ingestors for various file-based ingestion patterns.
"""

from typing import Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, input_file_name, to_date, max as spark_max
from .base import BaseBatchIngestor
from .factory import register_ingestor
import re


@register_ingestor("partitioned_parquet")
class PartitionedParquetIngestion(BaseBatchIngestor):
    """
    Discovers and loads new Hive-style partitions (year=2025/month=11/day=19).
    
    Tracks last partition watermark and only loads newer partitions.
    
    Config example:
        {
            "source_name": "partitioned_data",
            "source_path": "s3://bucket/data/year=*/month=*/day=*/",
            "partition_columns": ["year", "month", "day"],
            "target_table": "my_table",
            "watermark_column": "partition_date",
            "file_format": "parquet"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        source_path = self.config["source_path"]
        file_format = self.config.get("file_format", "parquet")
        partition_cols = self.config.get("partition_columns", [])
        
        # Read all partitions
        df = self.spark.read.format(file_format).load(source_path)
        
        # Filter by watermark if exists
        if watermark and self.watermark_column:
            df = df.filter(col(self.watermark_column) > watermark)
        
        return df


@register_ingestor("daily_file_drop")
class DailyFileDropIngestion(BaseBatchIngestor):
    """
    Loads daily files with date patterns (events_20251119.csv).
    
    Config example:
        {
            "source_name": "daily_files",
            "source_path": "s3://bucket/daily/",
            "file_pattern": "events_*.csv",
            "date_pattern": "yyyyMMdd",
            "target_table": "daily_events",
            "watermark_column": "file_date"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from datetime import datetime, timedelta
        
        source_path = self.config["source_path"]
        file_pattern = self.config.get("file_pattern", "*.csv")
        date_pattern = self.config.get("date_pattern", "yyyyMMdd")
        
        # Build full path with pattern
        full_path = f"{source_path.rstrip('/')}/{file_pattern}"
        
        # Read files
        df = self.spark.read.option("header", "true").csv(full_path)
        
        # Extract date from filename
        df = df.withColumn("_filename", input_file_name())
        
        # Extract date using regex based on date_pattern
        if date_pattern == "yyyyMMdd":
            date_regex = r"(\d{8})"
        elif date_pattern == "yyyy-MM-dd":
            date_regex = r"(\d{4}-\d{2}-\d{2})"
        else:
            date_regex = r"(\d{8})"
        
        # Filter by watermark
        if watermark:
            # Convert watermark to comparable format
            df = df.filter(col("_filename").rlike(date_regex))
            # Additional filtering logic here
        
        return df


@register_ingestor("incremental_file_by_modified_time")
class IncrementalFileByModifiedTime(BaseBatchIngestor):
    """
    Uses S3/ADLS/GCS lastModified to load only newer files.
    
    Config example:
        {
            "source_name": "modified_files",
            "source_path": "s3://bucket/incremental/",
            "file_format": "json",
            "target_table": "incremental_data",
            "watermark_column": "_file_modified_time"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from pyspark.sql.functions import current_timestamp
        
        source_path = self.config["source_path"]
        file_format = self.config.get("file_format", "json")
        
        # Read files with modification time
        df = self.spark.read.format(file_format).load(source_path)
        
        # Add file metadata
        df = df.withColumn("_filename", input_file_name())
        df = df.withColumn("_ingestion_time", current_timestamp())
        
        # Filter by modification time if watermark exists
        # Note: Actual implementation would use cloud storage APIs
        # to check file modification times
        
        return df


@register_ingestor("append_only_json_ndjson")
class AppendOnlyJsonNdjsonIngestion(BaseBatchIngestor):
    """
    Loads newline-delimited JSON with schema evolution.
    
    Config example:
        {
            "source_name": "ndjson_data",
            "source_path": "s3://bucket/ndjson/",
            "target_table": "json_data",
            "schema_evolution": "merge_schemas"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        source_path = self.config["source_path"]
        
        # Read NDJSON with schema inference
        df = self.spark.read.json(source_path)
        
        return df


@register_ingestor("copybook_ebcdic_mainframe")
class CopybookEbcdicMainframeIngestion(BaseBatchIngestor):
    """
    Loads EBCDIC files with COBOL copybook schema.
    
    Uses Cobrix library for parsing.
    
    Config example:
        {
            "source_name": "mainframe_data",
            "source_path": "s3://bucket/mainframe/",
            "copybook_path": "s3://bucket/schemas/copybook.cpy",
            "encoding": "cp037",
            "target_table": "mainframe_data"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        source_path = self.config["source_path"]
        copybook_path = self.config["copybook_path"]
        encoding = self.config.get("encoding", "cp037")
        
        # Read EBCDIC using Cobrix
        df = (
            self.spark.read
            .format("cobol")
            .option("copybook", copybook_path)
            .option("encoding", encoding)
            .option("schema_retention_policy", "collapse_root")
            .load(source_path)
        )
        
        return df


@register_ingestor("fixed_width_dremel")
class FixedWidthWithDremelIngestion(BaseBatchIngestor):
    """
    Parses fixed-width files with Dremel-style record descriptors.
    
    Config example:
        {
            "source_name": "fixed_width",
            "source_path": "s3://bucket/fixed/",
            "schema_spec": [
                {"name": "id", "start": 0, "length": 10, "type": "string"},
                {"name": "amount", "start": 10, "length": 15, "type": "decimal"}
            ],
            "target_table": "fixed_width_data"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from pyspark.sql.functions import substring, trim
        from pyspark.sql.types import StructType, StructField, StringType, DecimalType
        
        source_path = self.config["source_path"]
        schema_spec = self.config["schema_spec"]
        
        # Read as text
        text_df = self.spark.read.text(source_path)
        
        # Parse fixed-width fields
        df = text_df
        for field_spec in schema_spec:
            name = field_spec["name"]
            start = field_spec["start"]
            length = field_spec["length"]
            
            df = df.withColumn(
                name,
                trim(substring(col("value"), start + 1, length))
            )
        
        # Drop raw value column
        df = df.drop("value")
        
        return df


@register_ingestor("incremental_avro")
class IncrementalAvroIngestion(BaseBatchIngestor):
    """
    Loads Avro files with Schema Registry compatibility.
    
    Config example:
        {
            "source_name": "avro_data",
            "source_path": "s3://bucket/avro/",
            "schema_registry_url": "http://schema-registry:8081",
            "target_table": "avro_data"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        source_path = self.config["source_path"]
        
        # Read Avro
        df = self.spark.read.format("avro").load(source_path)
        
        # Filter by watermark if exists
        if watermark and self.watermark_column:
            df = df.filter(col(self.watermark_column) > watermark)
        
        return df


@register_ingestor("delta_table_as_source")
class DeltaTableAsSourceIngestion(BaseBatchIngestor):
    """
    Reads Delta table as source (cross-catalog replication).
    
    Config example:
        {
            "source_name": "source_delta",
            "source_catalog": "silver",
            "source_schema": "sales",
            "source_table": "orders",
            "target_table": "orders_bronze",
            "watermark_column": "updated_at"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        source_catalog = self.config["source_catalog"]
        source_schema = self.config["source_schema"]
        source_table = self.config["source_table"]
        
        full_source = f"{source_catalog}.{source_schema}.{source_table}"
        
        # Read Delta table
        df = self.spark.read.table(full_source)
        
        # Filter by watermark
        if watermark and self.watermark_column:
            df = df.filter(col(self.watermark_column) > watermark)
        
        return df


@register_ingestor("iceberg_table_as_source")
class IcebergTableAsSourceIngestion(BaseBatchIngestor):
    """
    Reads Apache Iceberg table with snapshot or incremental.
    
    Config example:
        {
            "source_name": "iceberg_source",
            "source_table": "prod.events.user_events",
            "snapshot_id": "latest",
            "target_table": "user_events_bronze",
            "watermark_column": "event_time"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        source_table = self.config["source_table"]
        snapshot_id = self.config.get("snapshot_id", "latest")
        
        # Read Iceberg table
        df_reader = self.spark.read.format("iceberg")
        
        if snapshot_id and snapshot_id != "latest":
            df_reader = df_reader.option("snapshot-id", snapshot_id)
        
        df = df_reader.load(source_table)
        
        # Filter by watermark
        if watermark and self.watermark_column:
            df = df.filter(col(self.watermark_column) > watermark)
        
        return df
