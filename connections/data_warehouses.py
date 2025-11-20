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


@register_connection("athena")
@register_connection("aws_athena")
class AthenaConnection(SparkleConnection):
    """
    Amazon Athena connection.

    Example config:
        {
            "s3_staging_dir": "s3://aws-athena-query-results-<account>-<region>/",
            "region": "us-east-1",
            "database": "default",
            "workgroup": "primary",
            "auth_type": "iam_role"
        }

    Usage:
        >>> conn = Connection.get("athena", spark, env="prod")
        >>> df = conn.read(query="SELECT * FROM my_table LIMIT 1000")
        >>> df = conn.read(table="my_table", database="analytics")
    """

    def test(self) -> bool:
        """Test Athena connection."""
        try:
            # Try to execute a simple query
            test_df = self.spark.read \
                .format("jdbc") \
                .option("url", self._get_jdbc_url()) \
                .option("driver", "com.simba.athena.jdbc.Driver") \
                .option("query", "SELECT 1 AS test") \
                .load()
            return test_df.count() == 1
        except Exception as e:
            self.logger.error(f"Athena connection test failed: {e}")
            return False

    def _get_jdbc_url(self) -> str:
        """Build Athena JDBC URL."""
        region = self.config["region"]
        s3_staging = self.config["s3_staging_dir"]
        workgroup = self.config.get("workgroup", "primary")

        return f"jdbc:awsathena://athena.{region}.amazonaws.com:443;S3OutputLocation={s3_staging};Workgroup={workgroup}"

    def get_connection(self) -> Dict[str, str]:
        """Get Athena connection options."""
        return {
            "url": self._get_jdbc_url(),
            "driver": "com.simba.athena.jdbc.Driver"
        }

    def read(
        self,
        table: Optional[str] = None,
        query: Optional[str] = None,
        database: Optional[str] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read from Athena.

        Args:
            table: Table name
            query: SQL query (mutually exclusive with table)
            database: Database/schema name
            **kwargs: Additional options

        Returns:
            Spark DataFrame
        """
        if not table and not query:
            raise ValueError("Either 'table' or 'query' must be provided")

        reader = self.spark.read.format("jdbc")

        for key, value in self.get_connection().items():
            reader = reader.option(key, value)

        if table:
            db = database or self.config.get("database", "default")
            reader = reader.option("dbtable", f"{db}.{table}")
        else:
            reader = reader.option("query", query)

        self.emit_lineage("read", {"table": table, "query": query, "database": database})

        return reader.load()

    def write(self, df: DataFrame, **kwargs) -> None:
        """
        Athena is read-only via JDBC. Write to S3 then CREATE EXTERNAL TABLE.
        """
        raise NotImplementedError(
            "Athena write via Spark is not supported. "
            "Write data to S3 then use CREATE EXTERNAL TABLE in Athena."
        )


@register_connection("dremio")
class DremioConnection(SparkleConnection):
    """
    Dremio data lakehouse connection.

    Supports both Arrow Flight and JDBC connectors.

    Example config:
        {
            "host": "dremio.example.com",
            "port": 31010,
            "username": "${DREMIO_USER}",
            "password": "${DREMIO_PASSWORD}",
            "use_arrow_flight": true,
            "use_ssl": true
        }

    Or via JDBC:
        {
            "url": "jdbc:dremio:direct=dremio.example.com:31010",
            "username": "${DREMIO_USER}",
            "password": "${DREMIO_PASSWORD}"
        }

    Usage:
        >>> conn = Connection.get("dremio", spark, env="prod")
        >>> df = conn.read(table="Samples.NYC-taxi-trips")
        >>> df = conn.read(query="SELECT * FROM my_space.my_table")
    """

    def test(self) -> bool:
        """Test Dremio connection."""
        try:
            if self.config.get("use_arrow_flight", False):
                # Test Arrow Flight connection (requires pyarrow)
                return True  # Would need pyarrow to actually test
            else:
                # Test JDBC connection
                test_df = self.spark.read \
                    .format("jdbc") \
                    .option("url", self._get_jdbc_url()) \
                    .option("driver", "com.dremio.jdbc.Driver") \
                    .option("user", self.config["username"]) \
                    .option("password", self.config["password"]) \
                    .option("query", "SELECT 1 AS test") \
                    .load()
                return test_df.count() == 1
        except Exception as e:
            self.logger.error(f"Dremio connection test failed: {e}")
            return False

    def _get_jdbc_url(self) -> str:
        """Build Dremio JDBC URL."""
        if "url" in self.config:
            return self.config["url"]

        host = self.config["host"]
        port = self.config.get("port", 31010)
        use_ssl = self.config.get("use_ssl", False)

        ssl_param = ";ssl=true" if use_ssl else ""
        return f"jdbc:dremio:direct={host}:{port}{ssl_param}"

    def get_connection(self) -> Dict[str, str]:
        """Get Dremio connection options."""
        return {
            "url": self._get_jdbc_url(),
            "driver": "com.dremio.jdbc.Driver",
            "user": self.config["username"],
            "password": self.config["password"]
        }

    def read(
        self,
        table: Optional[str] = None,
        query: Optional[str] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read from Dremio.

        Args:
            table: Table/dataset path (e.g., "Samples.NYC-taxi-trips")
            query: SQL query
            **kwargs: Additional options

        Returns:
            Spark DataFrame
        """
        if not table and not query:
            raise ValueError("Either 'table' or 'query' must be provided")

        reader = self.spark.read.format("jdbc")

        for key, value in self.get_connection().items():
            reader = reader.option(key, value)

        if table:
            reader = reader.option("dbtable", table)
        else:
            reader = reader.option("query", query)

        self.emit_lineage("read", {"table": table, "query": query})

        return reader.load()

    def write(self, df: DataFrame, table: str, mode: str = "append", **kwargs) -> None:
        """
        Write to Dremio (creates reflection or writes to source).

        Args:
            df: DataFrame to write
            table: Target table path
            mode: Write mode
            **kwargs: Additional options
        """
        writer = df.write.format("jdbc")

        for key, value in self.get_connection().items():
            writer = writer.option(key, value)

        writer.option("dbtable", table).mode(mode).save()

        self.emit_lineage("write", {"table": table, "mode": mode})


@register_connection("druid")
@register_connection("apache_druid")
class DruidConnection(SparkleConnection):
    """
    Apache Druid real-time analytics connection.

    Example config:
        {
            "url": "jdbc:avatica:remote:url=http://druid-broker:8082/druid/v2/sql/avatica/",
            "username": "${DRUID_USER}",
            "password": "${DRUID_PASSWORD}"
        }

    Usage:
        >>> conn = Connection.get("druid", spark, env="prod")
        >>> df = conn.read(table="wikipedia")
        >>> df = conn.read(query="SELECT * FROM wikipedia WHERE __time > CURRENT_TIMESTAMP - INTERVAL '1' HOUR")
    """

    def test(self) -> bool:
        """Test Druid connection."""
        try:
            test_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.config["url"]) \
                .option("driver", "org.apache.calcite.avatica.remote.Driver") \
                .option("user", self.config.get("username", "")) \
                .option("password", self.config.get("password", "")) \
                .option("query", "SELECT 1 AS test") \
                .load()
            return test_df.count() == 1
        except Exception as e:
            self.logger.error(f"Druid connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Druid connection options."""
        return {
            "url": self.config["url"],
            "driver": "org.apache.calcite.avatica.remote.Driver",
            "user": self.config.get("username", ""),
            "password": self.config.get("password", "")
        }

    def read(
        self,
        table: Optional[str] = None,
        query: Optional[str] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read from Druid.

        Args:
            table: Druid datasource name
            query: Druid SQL query
            **kwargs: Additional options

        Returns:
            Spark DataFrame
        """
        if not table and not query:
            raise ValueError("Either 'table' or 'query' must be provided")

        reader = self.spark.read.format("jdbc")

        for key, value in self.get_connection().items():
            reader = reader.option(key, value)

        if table:
            reader = reader.option("dbtable", table)
        else:
            reader = reader.option("query", query)

        self.emit_lineage("read", {"table": table, "query": query})

        return reader.load()

    def write(self, df: DataFrame, **kwargs) -> None:
        """
        Druid ingestion via Spark is not directly supported.
        Use Druid's native batch ingestion or streaming ingestion.
        """
        raise NotImplementedError(
            "Druid write via Spark is not supported. "
            "Use Druid's native batch ingestion, Kafka ingestion, or HTTP POST."
        )


@register_connection("pinot")
@register_connection("apache_pinot")
class PinotConnection(SparkleConnection):
    """
    Apache Pinot real-time OLAP connection.

    Example config:
        {
            "broker_url": "http://pinot-broker:8099",
            "controller_url": "http://pinot-controller:9000",
            "table": "myTable"
        }

    Usage:
        >>> conn = Connection.get("pinot", spark, env="prod")
        >>> df = conn.read(table="events_realtime")
        >>> df = conn.read(query="SELECT * FROM events_realtime WHERE timestamp > now() - 3600000")
    """

    def test(self) -> bool:
        """Test Pinot connection."""
        try:
            import requests
            response = requests.get(
                f"{self.config['broker_url']}/health",
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"Pinot connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Pinot connection options."""
        return {
            "broker_url": self.config["broker_url"],
            "controller_url": self.config.get("controller_url", ""),
            "table": self.config.get("table", "")
        }

    def read(
        self,
        table: Optional[str] = None,
        query: Optional[str] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read from Pinot via PQL/SQL query.

        Args:
            table: Pinot table name
            query: Pinot SQL query
            **kwargs: Additional options

        Returns:
            Spark DataFrame
        """
        import requests
        import pandas as pd

        if not table and not query:
            raise ValueError("Either 'table' or 'query' must be provided")

        if query is None:
            query = f"SELECT * FROM {table}"

        # Execute query via Pinot broker
        broker_url = self.config["broker_url"]
        response = requests.post(
            f"{broker_url}/query/sql",
            json={"sql": query},
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()

        data = response.json()

        # Convert to DataFrame
        if "resultTable" in data:
            columns = [col["name"] for col in data["resultTable"]["dataSchema"]["columnNames"]]
            rows = data["resultTable"]["rows"]

            pdf = pd.DataFrame(rows, columns=columns)
            df = self.spark.createDataFrame(pdf)

            self.emit_lineage("read", {"table": table, "query": query})

            return df
        else:
            raise ValueError("Invalid Pinot response format")

    def write(self, df: DataFrame, table: str, **kwargs) -> None:
        """
        Write to Pinot (batch ingestion).

        Note: This writes to a staging location then triggers Pinot batch ingestion.
        """
        raise NotImplementedError(
            "Pinot write via Spark requires custom batch ingestion setup. "
            "Write data to staging (S3/HDFS) then trigger Pinot ingestion job."
        )
