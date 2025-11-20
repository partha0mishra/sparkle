"""
Data catalog and metadata store connections.

Supports: Unity Catalog, Hive Metastore, AWS Glue, Azure Purview,
DataHub, Apache Atlas, Amundsen.
"""

from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame

from .base import SparkleConnection
from .factory import register_connection


@register_connection("unity_catalog")
@register_connection("uc")
class UnityCatalogConnection(SparkleConnection):
    """
    Databricks Unity Catalog connection.

    Example config (config/connections/unity_catalog/prod.json):
        {
            "catalog": "main",
            "schema": "default",
            "metastore_id": "abc123-def456-ghi789",
            "workspace_url": "https://dbc-abc123.cloud.databricks.com",
            "access_token": "${DATABRICKS_TOKEN}"
        }

    Usage:
        >>> conn = get_connection("unity_catalog", spark, env="prod")
        >>> # Unity Catalog is automatically used by Spark when configured
        >>> df = spark.table("main.bronze.customers")
        >>> # Or use connection for metadata operations
        >>> tables = conn.list_tables("bronze")
    """

    def test(self) -> bool:
        """Test Unity Catalog connection."""
        try:
            # Try to list catalogs
            catalogs = self.spark.sql("SHOW CATALOGS").collect()
            return len(catalogs) > 0
        except Exception as e:
            self.logger.error(f"Unity Catalog connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Unity Catalog configuration."""
        return {
            "catalog": self.config.get("catalog", "main"),
            "schema": self.config.get("schema", "default")
        }

    def list_catalogs(self) -> List[str]:
        """List all catalogs."""
        result = self.spark.sql("SHOW CATALOGS").collect()
        return [row.catalog for row in result]

    def list_schemas(self, catalog: Optional[str] = None) -> List[str]:
        """List schemas in a catalog."""
        catalog = catalog or self.config.get("catalog", "main")
        result = self.spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()
        return [row.databaseName for row in result]

    def list_tables(
        self,
        schema: Optional[str] = None,
        catalog: Optional[str] = None
    ) -> List[str]:
        """List tables in a schema."""
        catalog = catalog or self.config.get("catalog", "main")
        schema = schema or self.config.get("schema", "default")
        result = self.spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
        return [row.tableName for row in result]

    def get_table_metadata(
        self,
        table: str,
        schema: Optional[str] = None,
        catalog: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get table metadata from Unity Catalog."""
        catalog = catalog or self.config.get("catalog", "main")
        schema = schema or self.config.get("schema", "default")
        full_name = f"{catalog}.{schema}.{table}"

        # Get table details
        details = self.spark.sql(f"DESCRIBE EXTENDED {full_name}").collect()

        metadata = {
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "full_name": full_name,
            "columns": [],
            "properties": {}
        }

        for row in details:
            if row.col_name.strip() and not row.col_name.startswith("#"):
                metadata["columns"].append({
                    "name": row.col_name,
                    "type": row.data_type
                })

        return metadata

    def set_current_catalog(self, catalog: str) -> None:
        """Set current catalog."""
        self.spark.sql(f"USE CATALOG {catalog}")

    def set_current_schema(self, schema: str, catalog: Optional[str] = None) -> None:
        """Set current schema."""
        if catalog:
            self.spark.sql(f"USE CATALOG {catalog}")
        self.spark.sql(f"USE SCHEMA {schema}")


@register_connection("hive_metastore")
@register_connection("hive")
class HiveMetastoreConnection(SparkleConnection):
    """
    Hive Metastore connection.

    Example config:
        {
            "metastore_uris": "thrift://metastore-1:9083,thrift://metastore-2:9083",
            "warehouse_dir": "hdfs://namenode:8020/user/hive/warehouse",
            "properties": {
                "hive.metastore.client.socket.timeout": "300"
            }
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        super().__init__(spark, config, env, **kwargs)
        self._configure_spark()

    def _configure_spark(self):
        """Configure Spark for Hive Metastore."""
        conf = self.spark.sparkContext._conf

        if "metastore_uris" in self.config:
            conf.set("hive.metastore.uris", self.config["metastore_uris"])

        if "warehouse_dir" in self.config:
            conf.set("spark.sql.warehouse.dir", self.config["warehouse_dir"])

        # Apply additional properties
        for key, value in self.config.get("properties", {}).items():
            conf.set(key, str(value))

    def test(self) -> bool:
        """Test Hive Metastore connection."""
        try:
            databases = self.spark.sql("SHOW DATABASES").collect()
            return len(databases) > 0
        except Exception as e:
            self.logger.error(f"Hive Metastore connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Hive Metastore configuration."""
        return {
            "metastore_uris": self.config.get("metastore_uris", "")
        }

    def list_databases(self) -> List[str]:
        """List all databases."""
        result = self.spark.sql("SHOW DATABASES").collect()
        return [row.databaseName for row in result]

    def list_tables(self, database: str = "default") -> List[str]:
        """List tables in a database."""
        result = self.spark.sql(f"SHOW TABLES IN {database}").collect()
        return [row.tableName for row in result]


@register_connection("glue")
@register_connection("aws_glue")
class GlueCatalogConnection(SparkleConnection):
    """
    AWS Glue Data Catalog connection.

    Example config:
        {
            "region": "us-east-1",
            "database": "analytics",
            "catalog_id": "123456789012",
            "auth_type": "iam_role"
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        super().__init__(spark, config, env, **kwargs)
        self._configure_spark()

    def _configure_spark(self):
        """Configure Spark for Glue Catalog."""
        conf = self.spark.sparkContext._conf

        conf.set("spark.sql.catalogImplementation", "hive")
        conf.set(
            "hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        )

        if "region" in self.config:
            conf.set("aws.region", self.config["region"])

        if "catalog_id" in self.config:
            conf.set("aws.glue.catalog.id", self.config["catalog_id"])

    def test(self) -> bool:
        """Test Glue Catalog connection."""
        try:
            databases = self.spark.sql("SHOW DATABASES").collect()
            return len(databases) > 0
        except Exception as e:
            self.logger.error(f"Glue Catalog connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Glue Catalog configuration."""
        return {
            "region": self.config.get("region", "us-east-1"),
            "database": self.config.get("database", "default")
        }

    def list_databases(self) -> List[str]:
        """List all databases."""
        result = self.spark.sql("SHOW DATABASES").collect()
        return [row.databaseName for row in result]

    def list_tables(self, database: Optional[str] = None) -> List[str]:
        """List tables in a database."""
        database = database or self.config.get("database", "default")
        result = self.spark.sql(f"SHOW TABLES IN {database}").collect()
        return [row.tableName for row in result]


@register_connection("purview")
@register_connection("azure_purview")
class PurviewConnection(SparkleConnection):
    """
    Azure Purview data catalog connection.

    Example config:
        {
            "account_name": "my-purview-account",
            "tenant_id": "${AZURE_TENANT_ID}",
            "client_id": "${AZURE_CLIENT_ID}",
            "client_secret": "${AZURE_CLIENT_SECRET}",
            "endpoint": "https://my-purview-account.purview.azure.com"
        }
    """

    def test(self) -> bool:
        """Test Purview connection."""
        try:
            required = ["account_name", "tenant_id", "client_id", "client_secret"]
            return all(k in self.config for k in required)
        except Exception as e:
            self.logger.error(f"Purview connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Purview configuration."""
        return {
            "account_name": self.config["account_name"],
            "endpoint": self.config.get(
                "endpoint",
                f"https://{self.config['account_name']}.purview.azure.com"
            )
        }


@register_connection("datahub")
class DataHubConnection(SparkleConnection):
    """
    DataHub metadata platform connection.

    Example config:
        {
            "gms_url": "http://datahub-gms:8080",
            "token": "${DATAHUB_TOKEN}"
        }
    """

    def test(self) -> bool:
        """Test DataHub connection."""
        try:
            import requests
            response = requests.get(
                f"{self.config['gms_url']}/health",
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"DataHub connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get DataHub configuration."""
        return {
            "gms_url": self.config["gms_url"],
            "token": self.config.get("token", "")
        }


@register_connection("atlas")
@register_connection("apache_atlas")
class AtlasConnection(SparkleConnection):
    """
    Apache Atlas metadata catalog connection.

    Example config:
        {
            "atlas_url": "http://atlas.example.com:21000",
            "username": "${ATLAS_USER}",
            "password": "${ATLAS_PASSWORD}"
        }
    """

    def test(self) -> bool:
        """Test Atlas connection."""
        try:
            import requests
            from requests.auth import HTTPBasicAuth

            response = requests.get(
                f"{self.config['atlas_url']}/api/atlas/admin/version",
                auth=HTTPBasicAuth(
                    self.config["username"],
                    self.config["password"]
                ),
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"Atlas connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Atlas configuration."""
        return {
            "atlas_url": self.config["atlas_url"],
            "username": self.config["username"],
            "password": self.config["password"]
        }


@register_connection("amundsen")
class AmundsenConnection(SparkleConnection):
    """
    Amundsen metadata platform connection.

    Example config:
        {
            "metadata_url": "http://amundsen-metadata:5000",
            "search_url": "http://amundsen-search:5001"
        }
    """

    def test(self) -> bool:
        """Test Amundsen connection."""
        try:
            import requests
            response = requests.get(
                f"{self.config['metadata_url']}/healthcheck",
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"Amundsen connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Amundsen configuration."""
        return {
            "metadata_url": self.config["metadata_url"],
            "search_url": self.config.get("search_url", "")
        }
