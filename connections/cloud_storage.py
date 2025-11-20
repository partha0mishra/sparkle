"""
Cloud storage connections for S3, Azure Data Lake, Google Cloud Storage.

Handles authentication, path resolution, and Spark configuration.
"""

from typing import Dict, Any
from pyspark.sql import SparkSession

from .base import CloudStorageConnection
from .factory import register_connection


@register_connection("s3")
@register_connection("aws_s3")
class S3Connection(CloudStorageConnection):
    """
    Amazon S3 connection.

    Supports:
    - IAM role authentication (recommended)
    - Access key/secret key
    - Session tokens
    - S3-compatible storage (MinIO, etc.)

    Example config (config/connections/s3/prod.json):
        {
            "base_path": "s3a://my-lakehouse-bucket",
            "aws_region": "us-east-1",
            "auth_type": "iam_role",
            "properties": {
                "fs.s3a.endpoint": "s3.us-east-1.amazonaws.com",
                "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "fs.s3a.connection.ssl.enabled": "true",
                "fs.s3a.fast.upload": "true",
                "fs.s3a.multipart.size": "104857600"
            }
        }

    Or with explicit credentials:
        {
            "base_path": "s3a://my-lakehouse-bucket",
            "auth_type": "access_key",
            "aws_access_key_id": "${AWS_ACCESS_KEY_ID}",
            "aws_secret_access_key": "${AWS_SECRET_ACCESS_KEY}",
            "aws_session_token": "${AWS_SESSION_TOKEN}",
            "aws_region": "us-east-1"
        }

    Usage:
        >>> conn = get_connection("s3", spark, env="prod")
        >>> full_path = conn.resolve_path("bronze/customers/")
        >>> # Returns: s3a://my-lakehouse-bucket/bronze/customers/
        >>> df = spark.read.parquet(full_path)
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        super().__init__(spark, config, env, **kwargs)
        self._configure_spark()

    def _configure_spark(self):
        """Configure Spark session with S3 settings."""
        conf = self.spark.sparkContext._jsc.hadoopConfiguration()

        auth_type = self.config.get("auth_type", "iam_role")

        if auth_type == "access_key":
            # Explicit credentials
            if "aws_access_key_id" in self.config:
                conf.set("fs.s3a.access.key", self.config["aws_access_key_id"])
            if "aws_secret_access_key" in self.config:
                conf.set("fs.s3a.secret.key", self.config["aws_secret_access_key"])
            if "aws_session_token" in self.config:
                conf.set("fs.s3a.session.token", self.config["aws_session_token"])
        elif auth_type == "iam_role":
            # Use IAM role
            conf.set(
                "fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider"
            )

        # Set region
        if "aws_region" in self.config:
            conf.set("fs.s3a.endpoint.region", self.config["aws_region"])

        # Apply additional properties
        for key, value in self.config.get("properties", {}).items():
            conf.set(key, str(value))

    def test(self) -> bool:
        """Test S3 connection by listing base path."""
        try:
            base_path = self.get_base_path()
            # Try to list the bucket
            files = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem \
                .get(self.spark.sparkContext._jsc.hadoopConfiguration()) \
                .listStatus(self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(base_path))
            return True
        except Exception as e:
            self.logger.error(f"S3 connection test failed: {e}")
            return False

    def get_connection(self) -> str:
        """Return base S3 path."""
        return self.get_base_path()


@register_connection("adls")
@register_connection("adls_gen2")
@register_connection("azure_datalake")
class ADLSGen2Connection(CloudStorageConnection):
    """
    Azure Data Lake Storage Gen2 connection.

    Supports:
    - Managed identity (recommended in Azure)
    - Service principal
    - Account key
    - SAS token

    Example config:
        {
            "base_path": "abfss://container@storageaccount.dfs.core.windows.net",
            "auth_type": "service_principal",
            "tenant_id": "${AZURE_TENANT_ID}",
            "client_id": "${AZURE_CLIENT_ID}",
            "client_secret": "${AZURE_CLIENT_SECRET}",
            "storage_account": "mystorageaccount",
            "container": "lakehouse"
        }

    Or with account key:
        {
            "base_path": "abfss://container@storageaccount.dfs.core.windows.net",
            "auth_type": "account_key",
            "storage_account": "mystorageaccount",
            "account_key": "${AZURE_STORAGE_ACCOUNT_KEY}"
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        super().__init__(spark, config, env, **kwargs)
        self._configure_spark()

    def _configure_spark(self):
        """Configure Spark session with ADLS settings."""
        conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        auth_type = self.config.get("auth_type", "managed_identity")
        storage_account = self.config.get("storage_account")

        if auth_type == "service_principal":
            # OAuth with service principal
            conf.set(
                f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
                "OAuth"
            )
            conf.set(
                f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
            )
            conf.set(
                f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
                self.config["client_id"]
            )
            conf.set(
                f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
                self.config["client_secret"]
            )
            conf.set(
                f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
                f"https://login.microsoftonline.com/{self.config['tenant_id']}/oauth2/token"
            )
        elif auth_type == "account_key":
            # Account key
            conf.set(
                f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
                self.config["account_key"]
            )
        elif auth_type == "sas_token":
            # SAS token
            conf.set(
                f"fs.azure.sas.{self.config['container']}.{storage_account}.dfs.core.windows.net",
                self.config["sas_token"]
            )

        # Apply additional properties
        for key, value in self.config.get("properties", {}).items():
            conf.set(key, str(value))

    def test(self) -> bool:
        """Test ADLS connection."""
        try:
            base_path = self.get_base_path()
            files = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem \
                .get(self.spark.sparkContext._jsc.hadoopConfiguration()) \
                .listStatus(self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(base_path))
            return True
        except Exception as e:
            self.logger.error(f"ADLS connection test failed: {e}")
            return False

    def get_connection(self) -> str:
        """Return base ADLS path."""
        return self.get_base_path()


@register_connection("gcs")
@register_connection("google_cloud_storage")
class GCSConnection(CloudStorageConnection):
    """
    Google Cloud Storage connection.

    Supports:
    - Service account key file
    - Application default credentials
    - Workload identity (GKE)

    Example config:
        {
            "base_path": "gs://my-lakehouse-bucket",
            "project_id": "my-gcp-project",
            "auth_type": "service_account",
            "service_account_key_file": "/path/to/keyfile.json"
        }

    Or with inline credentials:
        {
            "base_path": "gs://my-lakehouse-bucket",
            "project_id": "my-gcp-project",
            "auth_type": "service_account_json",
            "service_account_json": "${GCP_SERVICE_ACCOUNT_JSON}"
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        super().__init__(spark, config, env, **kwargs)
        self._configure_spark()

    def _configure_spark(self):
        """Configure Spark session with GCS settings."""
        conf = self.spark.sparkContext._jsc.hadoopConfiguration()

        # Set GCS file system implementation
        conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

        # Set project ID
        if "project_id" in self.config:
            conf.set("fs.gs.project.id", self.config["project_id"])

        auth_type = self.config.get("auth_type", "application_default")

        if auth_type == "service_account":
            # Service account key file
            conf.set(
                "google.cloud.auth.service.account.enable",
                "true"
            )
            conf.set(
                "google.cloud.auth.service.account.json.keyfile",
                self.config["service_account_key_file"]
            )
        elif auth_type == "service_account_json":
            # Inline JSON credentials
            conf.set(
                "google.cloud.auth.service.account.enable",
                "true"
            )
            # Write to temp file or use environment variable
            import os
            os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"] = self.config["service_account_json"]

        # Apply additional properties
        for key, value in self.config.get("properties", {}).items():
            conf.set(key, str(value))

    def test(self) -> bool:
        """Test GCS connection."""
        try:
            base_path = self.get_base_path()
            files = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem \
                .get(self.spark.sparkContext._jsc.hadoopConfiguration()) \
                .listStatus(self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(base_path))
            return True
        except Exception as e:
            self.logger.error(f"GCS connection test failed: {e}")
            return False

    def get_connection(self) -> str:
        """Return base GCS path."""
        return self.get_base_path()


@register_connection("hdfs")
class HDFSConnection(CloudStorageConnection):
    """
    HDFS connection for on-premise Hadoop clusters.

    Example config:
        {
            "base_path": "hdfs://namenode:8020/user/sparkle",
            "namenode": "namenode.example.com",
            "port": 8020,
            "properties": {
                "dfs.replication": "3",
                "dfs.blocksize": "134217728"
            }
        }
    """

    def test(self) -> bool:
        """Test HDFS connection."""
        try:
            base_path = self.get_base_path()
            files = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem \
                .get(self.spark.sparkContext._jsc.hadoopConfiguration()) \
                .listStatus(self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(base_path))
            return True
        except Exception as e:
            self.logger.error(f"HDFS connection test failed: {e}")
            return False

    def get_connection(self) -> str:
        """Return base HDFS path."""
        return self.get_base_path()


@register_connection("local")
@register_connection("file")
class LocalFileSystemConnection(CloudStorageConnection):
    """
    Local filesystem connection (for development/testing).

    Example config:
        {
            "base_path": "file:///tmp/sparkle_data"
        }
    """

    def test(self) -> bool:
        """Test local filesystem connection."""
        try:
            import os
            base_path = self.get_base_path().replace("file://", "")
            return os.path.exists(base_path) or os.path.exists(os.path.dirname(base_path))
        except Exception as e:
            self.logger.error(f"Local filesystem connection test failed: {e}")
            return False

    def get_connection(self) -> str:
        """Return base local path."""
        return self.get_base_path()


@register_connection("s3_compatible")
@register_connection("minio")
class S3CompatibleConnection(CloudStorageConnection):
    """
    S3-compatible storage (MinIO, Ceph, etc.).

    Example config:
        {
            "base_path": "s3a://my-bucket",
            "endpoint": "http://minio.example.com:9000",
            "access_key": "${MINIO_ACCESS_KEY}",
            "secret_key": "${MINIO_SECRET_KEY}",
            "properties": {
                "fs.s3a.path.style.access": "true",
                "fs.s3a.connection.ssl.enabled": "false"
            }
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        super().__init__(spark, config, env, **kwargs)
        self._configure_spark()

    def _configure_spark(self):
        """Configure Spark for S3-compatible storage."""
        conf = self.spark.sparkContext._jsc.hadoopConfiguration()

        conf.set("fs.s3a.endpoint", self.config["endpoint"])
        conf.set("fs.s3a.access.key", self.config["access_key"])
        conf.set("fs.s3a.secret.key", self.config["secret_key"])
        conf.set("fs.s3a.path.style.access", "true")
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # Apply additional properties
        for key, value in self.config.get("properties", {}).items():
            conf.set(key, str(value))

    def test(self) -> bool:
        """Test S3-compatible connection."""
        try:
            base_path = self.get_base_path()
            files = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem \
                .get(self.spark.sparkContext._jsc.hadoopConfiguration()) \
                .listStatus(self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(base_path))
            return True
        except Exception as e:
            self.logger.error(f"S3-compatible connection test failed: {e}")
            return False

    def get_connection(self) -> str:
        """Return base path."""
        return self.get_base_path()


@register_connection("databricks_volumes")
@register_connection("dbfs")
@register_connection("unity_volumes")
class DatabricksVolumesConnection(CloudStorageConnection):
    """
    Databricks Volumes (Unity Catalog) and DBFS connection.

    Supports:
    - Unity Catalog Volumes (/Volumes/catalog/schema/volume/)
    - DBFS (dbfs:/)
    - Local file system in Databricks (file:/)

    Example config:
        {
            "base_path": "/Volumes/main/default/landing",
            "catalog": "main",
            "schema": "default",
            "volume": "landing"
        }

    Or for DBFS:
        {
            "base_path": "dbfs:/mnt/data"
        }

    Usage:
        >>> conn = Connection.get("databricks_volumes", spark, env="prod")
        >>> df = conn.read(path="raw/customers/", format="parquet")
        >>> conn.write(df, path="processed/customers/", format="delta")
    """

    def test(self) -> bool:
        """Test Databricks Volumes/DBFS connection."""
        try:
            base_path = self.get_base_path()

            # Try dbutils first (Databricks environment)
            try:
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(self.spark)
                dbutils.fs.ls(base_path)
                return True
            except:
                # Fallback: try Spark file listing
                files = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem \
                    .get(self.spark.sparkContext._jsc.hadoopConfiguration()) \
                    .listStatus(self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(base_path))
                return True
        except Exception as e:
            self.logger.error(f"Databricks Volumes connection test failed: {e}")
            return False

    def get_connection(self) -> str:
        """Return base path."""
        return self.get_base_path()

    def create_volume(
        self,
        catalog: str,
        schema: str,
        volume: str,
        volume_type: str = "MANAGED",
        location: Optional[str] = None
    ) -> None:
        """
        Create Unity Catalog Volume.

        Args:
            catalog: Catalog name
            schema: Schema name
            volume: Volume name
            volume_type: MANAGED or EXTERNAL
            location: External location (for EXTERNAL volumes)
        """
        if volume_type == "EXTERNAL" and not location:
            raise ValueError("EXTERNAL volumes require location parameter")

        if volume_type == "MANAGED":
            sql = f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}"
        else:
            sql = f"CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{schema}.{volume} LOCATION '{location}'"

        self.spark.sql(sql)
        self.logger.info(f"Created volume: {catalog}.{schema}.{volume}")

    def list_volumes(self, catalog: str, schema: str) -> list:
        """List volumes in a schema."""
        result = self.spark.sql(f"SHOW VOLUMES IN {catalog}.{schema}").collect()
        return [row.volume_name for row in result]


@register_connection("cloudflare_r2")
@register_connection("r2")
class CloudflareR2Connection(S3CompatibleConnection):
    """
    Cloudflare R2 connection (S3-compatible).

    Example config:
        {
            "base_path": "s3a://my-r2-bucket",
            "endpoint": "https://<account-id>.r2.cloudflarestorage.com",
            "access_key": "${R2_ACCESS_KEY}",
            "secret_key": "${R2_SECRET_KEY}",
            "properties": {
                "fs.s3a.path.style.access": "true"
            }
        }

    Usage:
        >>> conn = Connection.get("r2", spark, env="prod")
        >>> df = conn.read(path="data/", format="parquet")
    """
    pass


@register_connection("wasabi")
class WasabiConnection(S3CompatibleConnection):
    """
    Wasabi Cloud Storage connection (S3-compatible).

    Example config:
        {
            "base_path": "s3a://my-wasabi-bucket",
            "endpoint": "https://s3.wasabisys.com",
            "access_key": "${WASABI_ACCESS_KEY}",
            "secret_key": "${WASABI_SECRET_KEY}",
            "properties": {
                "fs.s3a.path.style.access": "true"
            }
        }

    Usage:
        >>> conn = Connection.get("wasabi", spark, env="prod")
        >>> df = conn.read(path="archive/", format="parquet")
    """
    pass


@register_connection("backblaze_b2")
@register_connection("b2")
class BackblazeB2Connection(S3CompatibleConnection):
    """
    Backblaze B2 connection (S3-compatible).

    Example config:
        {
            "base_path": "s3a://my-b2-bucket",
            "endpoint": "https://s3.us-west-001.backblazeb2.com",
            "access_key": "${B2_ACCESS_KEY}",
            "secret_key": "${B2_SECRET_KEY}",
            "properties": {
                "fs.s3a.path.style.access": "true"
            }
        }

    Usage:
        >>> conn = Connection.get("b2", spark, env="prod")
        >>> df = conn.read(path="backups/", format="delta")
    """
    pass
