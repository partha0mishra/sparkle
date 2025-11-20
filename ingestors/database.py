"""
Database incremental and CDC ingestors.

7 production-grade database ingestion patterns.
"""

from typing import Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from .base import BaseBatchIngestor
from .factory import register_ingestor


@register_ingestor("jdbc_incremental_by_date")
class JdbcIncrementalByDateColumn(BaseBatchIngestor):
    """
    Generic JDBC incremental using date column watermark.
    
    Config example:
        {
            "source_name": "postgres_orders",
            "connection_name": "postgres",
            "connection_env": "prod",
            "table": "orders",
            "watermark_column": "updated_at",
            "target_table": "orders_bronze",
            "partition_column": "id",
            "num_partitions": 10
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn_name = self.config["connection_name"]
        conn_env = self.config.get("connection_env", self.env)
        table = self.config["table"]
        
        # Get connection
        conn = Connection.get(conn_name, self.spark, env=conn_env)
        
        # Build incremental query
        if watermark:
            query = f"(SELECT * FROM {table} WHERE {self.watermark_column} > '{watermark}') AS incremental"
        else:
            query = f"(SELECT * FROM {table}) AS full"
        
        # Read with partitioning
        partition_col = self.config.get("partition_column")
        num_partitions = self.config.get("num_partitions", 10)
        
        df = conn.read(
            query=query,
            partition_column=partition_col,
            num_partitions=num_partitions
        )
        
        return df


@register_ingestor("jdbc_incremental_by_id_range")
class JdbcIncrementalByIdRange(BaseBatchIngestor):
    """
    Incremental using monotonically increasing PK with dynamic ranges.
    
    Config example:
        {
            "source_name": "mysql_users",
            "connection_name": "mysql",
            "table": "users",
            "id_column": "id",
            "watermark_column": "id",
            "target_table": "users_bronze"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn_name = self.config["connection_name"]
        conn_env = self.config.get("connection_env", self.env)
        table = self.config["table"]
        id_column = self.config["id_column"]
        
        conn = Connection.get(conn_name, self.spark, env=conn_env)
        
        # Build range query
        if watermark:
            query = f"(SELECT * FROM {table} WHERE {id_column} > {watermark}) AS incremental"
        else:
            query = f"(SELECT * FROM {table}) AS full"
        
        df = conn.read(
            query=query,
            partition_column=id_column,
            num_partitions=self.config.get("num_partitions", 10)
        )
        
        return df


@register_ingestor("jdbc_full_table_hash_partitioning")
class JdbcFullTableWithHashPartitioning(BaseBatchIngestor):
    """
    Full table with parallel JDBC reads via hash partitioning.
    
    Config example:
        {
            "source_name": "oracle_customers",
            "connection_name": "oracle",
            "table": "customers",
            "target_table": "customers_bronze",
            "hash_column": "customer_id",
            "num_partitions": 20
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn_name = self.config["connection_name"]
        table = self.config["table"]
        hash_column = self.config["hash_column"]
        num_partitions = self.config.get("num_partitions", 20)
        
        conn = Connection.get(conn_name, self.spark, env=self.config.get("connection_env", self.env))
        
        # Use hash partitioning for parallel reads
        df = conn.read(
            table=table,
            partition_column=hash_column,
            num_partitions=num_partitions
        )
        
        return df


@register_ingestor("oracle_flashback_query")
class OracleFlashbackQueryIngestion(BaseBatchIngestor):
    """
    Oracle Flashback Query AS OF SCN/timestamp.
    
    Config example:
        {
            "source_name": "oracle_transactions",
            "connection_name": "oracle",
            "table": "transactions",
            "flashback_type": "scn",
            "target_table": "transactions_bronze",
            "watermark_column": "scn"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn_name = self.config["connection_name"]
        table = self.config["table"]
        flashback_type = self.config.get("flashback_type", "scn")
        
        conn = Connection.get(conn_name, self.spark, env=self.config.get("connection_env", self.env))
        
        if watermark:
            if flashback_type == "scn":
                query = f"(SELECT * FROM {table} AS OF SCN {watermark}) AS flashback"
            else:
                query = f"(SELECT * FROM {table} AS OF TIMESTAMP TO_TIMESTAMP('{watermark}', 'YYYY-MM-DD HH24:MI:SS')) AS flashback"
        else:
            query = f"(SELECT * FROM {table}) AS full"
        
        df = conn.read(query=query)
        
        return df


@register_ingestor("db2_cdc_infosphere")
class Db2CdcViaInfoSphere(BaseBatchIngestor):
    """
    IBM InfoSphere CDC -> Kafka -> Delta.
    
    Config example:
        {
            "source_name": "db2_cdc",
            "connection_name": "kafka",
            "topic": "db2.cdc.customers",
            "target_table": "customers_cdc",
            "cdc_operation_column": "__op"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn_name = self.config["connection_name"]
        topic = self.config["topic"]
        
        conn = Connection.get(conn_name, self.spark, env=self.config.get("connection_env", self.env))
        
        # Read from Kafka
        df = conn.read(topic=topic)
        
        # Parse CDC payload (implementation depends on InfoSphere format)
        # Typically JSON with operation type
        
        return df


@register_ingestor("teradata_fastexport_parallel")
class TeradataFastExportParallel(BaseBatchIngestor):
    """
    Teradata FastExport parallel dump files.
    
    Config example:
        {
            "source_name": "teradata_sales",
            "dump_path": "s3://bucket/teradata/sales/",
            "target_table": "sales_bronze"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        dump_path = self.config["dump_path"]
        
        # Read FastExport dump files (typically Parquet or custom format)
        df = self.spark.read.parquet(dump_path)
        
        return df


@register_ingestor("sap_odp_extractor")
class SapOdpExtractor(BaseBatchIngestor):
    """
    SAP ODP/ODQ extraction.
    
    Config example:
        {
            "source_name": "sap_customers",
            "connection_name": "sap_odp",
            "odp_name": "0CUSTOMER_ATTR",
            "extraction_mode": "delta",
            "target_table": "sap_customers_bronze",
            "watermark_column": "request_id"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn_name = self.config["connection_name"]
        odp_name = self.config["odp_name"]
        extraction_mode = self.config.get("extraction_mode", "delta")
        
        conn = Connection.get(conn_name, self.spark, env=self.config.get("connection_env", self.env))
        
        # Read from SAP ODP
        df = conn.read(
            odp_name=odp_name,
            extraction_mode=extraction_mode,
            request_id=watermark if watermark else None
        )
        
        return df
