"""
Data warehouse connections.

Supports: Snowflake, AWS Redshift, Google BigQuery, Azure Synapse Analytics,
Databricks SQL Warehouse, ClickHouse (OLAP mode).
"""

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame

from .base import SparkleConnection
from .factory import register_connection


@register_connection("snowflake")
class SnowflakeConnection(SparkleConnection):
    """
    Snowflake connection via Spark connector.

    Example config (config/connections/snowflake/prod.json):
        {
            "sfUrl": "myaccount.snowflakecomputing.com",
            "sfUser": "${SNOWFLAKE_USER}",
            "sfPassword": "${SNOWFLAKE_PASSWORD}",
            "sfDatabase": "ANALYTICS",
            "sfSchema": "PUBLIC",
            "sfWarehouse": "COMPUTE_WH",
            "sfRole": "ANALYST",
            "properties": {
                "sfCompress": "on",
                "sfTimezone": "UTC"
            }
        }

    Usage:
        >>> conn = get_connection("snowflake", spark, env="prod")
        >>> df = conn.read_table("CUSTOMERS")
        >>> conn.write_table(df, "CUSTOMER_GOLD", mode="overwrite")
    """

    def test(self) -> bool:
        """Test Snowflake connection."""
        try:
            test_df = self.spark.read \
                .format("snowflake") \
                .options(**self.get_connection()) \
                .option("query", "SELECT 1 AS test") \
                .load()
            return test_df.count() == 1
        except Exception as e:
            self.logger.error(f"Snowflake connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Snowflake connection options."""
        options = {
            "sfUrl": self.config["sfUrl"],
            "sfUser": self.config["sfUser"],
            "sfPassword": self.config["sfPassword"],
            "sfDatabase": self.config.get("sfDatabase", ""),
            "sfSchema": self.config.get("sfSchema", "PUBLIC"),
            "sfWarehouse": self.config.get("sfWarehouse", "")
        }

        if "sfRole" in self.config:
            options["sfRole"] = self.config["sfRole"]

        # Add additional properties
        options.update(self.config.get("properties", {}))

        return options

    def read_table(
        self,
        table: str,
        schema: Optional[str] = None,
        database: Optional[str] = None
    ) -> DataFrame:
        """
        Read table from Snowflake.

        Args:
            table: Table name
            schema: Optional schema override
            database: Optional database override

        Returns:
            DataFrame
        """
        reader = self.spark.read.format("snowflake")

        options = self.get_connection()
        if database:
            options["sfDatabase"] = database
        if schema:
            options["sfSchema"] = schema

        for key, value in options.items():
            reader = reader.option(key, value)

        reader = reader.option("dbtable", table)

        self.emit_lineage("read", {"table": table, "database": database, "schema": schema})

        return reader.load()

    def read_query(self, query: str) -> DataFrame:
        """
        Execute arbitrary SQL query in Snowflake.

        Args:
            query: SQL query string

        Returns:
            DataFrame
        """
        reader = self.spark.read.format("snowflake")

        for key, value in self.get_connection().items():
            reader = reader.option(key, value)

        reader = reader.option("query", query)

        self.emit_lineage("read", {"query": query})

        return reader.load()

    def write_table(
        self,
        df: DataFrame,
        table: str,
        mode: str = "append",
        schema: Optional[str] = None,
        database: Optional[str] = None
    ) -> None:
        """
        Write DataFrame to Snowflake table.

        Args:
            df: DataFrame to write
            table: Target table name
            mode: Write mode (append/overwrite/error/ignore)
            schema: Optional schema override
            database: Optional database override
        """
        writer = df.write.format("snowflake")

        options = self.get_connection()
        if database:
            options["sfDatabase"] = database
        if schema:
            options["sfSchema"] = schema

        for key, value in options.items():
            writer = writer.option(key, value)

        writer.option("dbtable", table).mode(mode).save()

        self.emit_lineage("write", {"table": table, "mode": mode, "rows": df.count()})


@register_connection("redshift")
@register_connection("aws_redshift")
class RedshiftConnection(SparkleConnection):
    """
    AWS Redshift connection.

    Example config:
        {
            "url": "jdbc:redshift://redshift-cluster.region.redshift.amazonaws.com:5439/prod",
            "user": "${REDSHIFT_USER}",
            "password": "${REDSHIFT_PASSWORD}",
            "tempdir": "s3a://my-bucket/tmp/redshift/",
            "aws_iam_role": "arn:aws:iam::123456789012:role/RedshiftCopyUnload",
            "properties": {
                "forward_spark_s3_credentials": "true"
            }
        }
    """

    def test(self) -> bool:
        """Test Redshift connection."""
        try:
            test_df = self.spark.read \
                .format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", self.config["url"]) \
                .option("user", self.config["user"]) \
                .option("password", self.config["password"]) \
                .option("tempdir", self.config["tempdir"]) \
                .option("query", "SELECT 1 AS test") \
                .load()
            return test_df.count() == 1
        except Exception as e:
            self.logger.error(f"Redshift connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Redshift connection options."""
        options = {
            "url": self.config["url"],
            "user": self.config["user"],
            "password": self.config["password"],
            "tempdir": self.config["tempdir"]
        }

        if "aws_iam_role" in self.config:
            options["aws_iam_role"] = self.config["aws_iam_role"]

        options.update(self.config.get("properties", {}))

        return options

    def read_table(self, table: str) -> DataFrame:
        """Read table from Redshift."""
        reader = self.spark.read.format("io.github.spark_redshift_community.spark.redshift")

        for key, value in self.get_connection().items():
            reader = reader.option(key, value)

        reader = reader.option("dbtable", table)

        self.emit_lineage("read", {"table": table})

        return reader.load()

    def write_table(self, df: DataFrame, table: str, mode: str = "append") -> None:
        """Write DataFrame to Redshift."""
        writer = df.write.format("io.github.spark_redshift_community.spark.redshift")

        for key, value in self.get_connection().items():
            writer = writer.option(key, value)

        writer.option("dbtable", table).mode(mode).save()

        self.emit_lineage("write", {"table": table, "mode": mode})


@register_connection("bigquery")
@register_connection("gcp_bigquery")
class BigQueryConnection(SparkleConnection):
    """
    Google BigQuery connection.

    Example config:
        {
            "project_id": "my-gcp-project",
            "dataset": "analytics",
            "credentials_file": "/path/to/service-account.json",
            "temp_gcs_bucket": "my-bucket-temp",
            "properties": {
                "viewsEnabled": "true",
                "materializationDataset": "temp"
            }
        }
    """

    def test(self) -> bool:
        """Test BigQuery connection."""
        try:
            test_df = self.spark.read \
                .format("bigquery") \
                .option("credentialsFile", self.config.get("credentials_file", "")) \
                .option("query", "SELECT 1 AS test") \
                .load()
            return test_df.count() == 1
        except Exception as e:
            self.logger.error(f"BigQuery connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get BigQuery connection options."""
        options = {
            "project": self.config["project_id"]
        }

        if "credentials_file" in self.config:
            options["credentialsFile"] = self.config["credentials_file"]

        if "temp_gcs_bucket" in self.config:
            options["temporaryGcsBucket"] = self.config["temp_gcs_bucket"]

        if "dataset" in self.config:
            options["dataset"] = self.config["dataset"]

        options.update(self.config.get("properties", {}))

        return options

    def read_table(self, table: str, dataset: Optional[str] = None) -> DataFrame:
        """
        Read table from BigQuery.

        Args:
            table: Table name
            dataset: Optional dataset override

        Returns:
            DataFrame
        """
        reader = self.spark.read.format("bigquery")

        options = self.get_connection()
        if dataset:
            options["dataset"] = dataset

        for key, value in options.items():
            reader = reader.option(key, value)

        # Fully qualified table name
        project = self.config["project_id"]
        ds = dataset or self.config.get("dataset", "")
        table_ref = f"{project}.{ds}.{table}" if ds else table

        reader = reader.option("table", table_ref)

        self.emit_lineage("read", {"table": table_ref})

        return reader.load()

    def read_query(self, query: str) -> DataFrame:
        """Execute SQL query in BigQuery."""
        reader = self.spark.read.format("bigquery")

        for key, value in self.get_connection().items():
            reader = reader.option(key, value)

        reader = reader.option("query", query)

        self.emit_lineage("read", {"query": query})

        return reader.load()

    def write_table(
        self,
        df: DataFrame,
        table: str,
        mode: str = "append",
        dataset: Optional[str] = None
    ) -> None:
        """Write DataFrame to BigQuery."""
        writer = df.write.format("bigquery")

        options = self.get_connection()
        if dataset:
            options["dataset"] = dataset

        for key, value in options.items():
            writer = writer.option(key, value)

        # Fully qualified table name
        project = self.config["project_id"]
        ds = dataset or self.config.get("dataset", "")
        table_ref = f"{project}.{ds}.{table}" if ds else table

        writer.option("table", table_ref).mode(mode).save()

        self.emit_lineage("write", {"table": table_ref, "mode": mode})


@register_connection("synapse")
@register_connection("azure_synapse")
class SynapseConnection(SparkleConnection):
    """
    Azure Synapse Analytics connection.

    Example config:
        {
            "url": "jdbc:sqlserver://synapse-workspace.sql.azuresynapse.net:1433;database=prod",
            "user": "${SYNAPSE_USER}",
            "password": "${SYNAPSE_PASSWORD}",
            "tempdir": "abfss://container@storage.dfs.core.windows.net/tmp/synapse/",
            "forward_spark_azure_storage_credentials": "true"
        }
    """

    def test(self) -> bool:
        """Test Synapse connection."""
        try:
            # Uses SQL Server JDBC driver
            test_df = self.spark.read \
                .format("com.microsoft.sqlserver.jdbc.spark") \
                .option("url", self.config["url"]) \
                .option("user", self.config["user"]) \
                .option("password", self.config["password"]) \
                .option("query", "SELECT 1 AS test") \
                .load()
            return test_df.count() == 1
        except Exception as e:
            self.logger.error(f"Synapse connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Synapse connection options."""
        options = {
            "url": self.config["url"],
            "user": self.config["user"],
            "password": self.config["password"],
            "tempDir": self.config.get("tempdir", "")
        }

        options.update(self.config.get("properties", {}))

        return options

    def read_table(self, table: str) -> DataFrame:
        """Read table from Synapse."""
        reader = self.spark.read.format("com.microsoft.sqlserver.jdbc.spark")

        for key, value in self.get_connection().items():
            reader = reader.option(key, value)

        reader = reader.option("dbtable", table)

        self.emit_lineage("read", {"table": table})

        return reader.load()

    def write_table(self, df: DataFrame, table: str, mode: str = "append") -> None:
        """Write DataFrame to Synapse."""
        writer = df.write.format("com.microsoft.sqlserver.jdbc.spark")

        for key, value in self.get_connection().items():
            writer = writer.option(key, value)

        writer.option("dbtable", table).mode(mode).save()

        self.emit_lineage("write", {"table": table, "mode": mode})


@register_connection("databricks_sql")
@register_connection("databricks_warehouse")
class DatabricksSQLConnection(SparkleConnection):
    """
    Databricks SQL Warehouse connection.

    Example config:
        {
            "server_hostname": "dbc-abc123-def456.cloud.databricks.com",
            "http_path": "/sql/1.0/warehouses/abc123def456",
            "access_token": "${DATABRICKS_TOKEN}",
            "catalog": "main",
            "schema": "default"
        }
    """

    def test(self) -> bool:
        """Test Databricks SQL connection."""
        try:
            # Test using Databricks connector
            test_df = self.spark.read \
                .format("databricks") \
                .option("host", self.config["server_hostname"]) \
                .option("httpPath", self.config["http_path"]) \
                .option("personalAccessToken", self.config["access_token"]) \
                .option("query", "SELECT 1 AS test") \
                .load()
            return test_df.count() == 1
        except Exception as e:
            self.logger.error(f"Databricks SQL connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Databricks SQL connection options."""
        return {
            "host": self.config["server_hostname"],
            "httpPath": self.config["http_path"],
            "personalAccessToken": self.config["access_token"]
        }

    def read_table(self, table: str, catalog: Optional[str] = None, schema: Optional[str] = None) -> DataFrame:
        """Read table from Databricks SQL Warehouse."""
        catalog = catalog or self.config.get("catalog", "main")
        schema = schema or self.config.get("schema", "default")

        full_table = f"{catalog}.{schema}.{table}"

        reader = self.spark.read.format("databricks")

        for key, value in self.get_connection().items():
            reader = reader.option(key, value)

        reader = reader.option("query", f"SELECT * FROM {full_table}")

        self.emit_lineage("read", {"table": full_table})

        return reader.load()

    def read_query(self, query: str) -> DataFrame:
        """Execute SQL query in Databricks SQL Warehouse."""
        reader = self.spark.read.format("databricks")

        for key, value in self.get_connection().items():
            reader = reader.option(key, value)

        reader = reader.option("query", query)

        self.emit_lineage("read", {"query": query})

        return reader.load()
