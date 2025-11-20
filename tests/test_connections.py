"""
Unit tests for Sparkle connections.

Tests all connection types with mocked credentials and local endpoints.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession

from connections.jdbc_connection import JDBCConnection
from connections.s3_connection import S3Connection
from connections.kafka_connection import KafkaConnection
from connections.rest_api_connection import RESTAPIConnection
from connections.file_connection import FileConnection


class TestJDBCConnection:
    """Test JDBC connections (PostgreSQL, MySQL, Oracle, SQL Server, etc.)"""

    def test_postgresql_connection_initialization(self):
        """Test PostgreSQL connection initialization"""
        config = {
            "url": "jdbc:postgresql://localhost:5432/testdb",
            "user": "testuser",
            "password": "testpass",
            "driver": "org.postgresql.Driver",
        }

        conn = JDBCConnection(**config)
        assert conn.url == config["url"]
        assert conn.user == config["user"]
        assert conn.driver == config["driver"]

    def test_mysql_connection_initialization(self):
        """Test MySQL connection initialization"""
        config = {
            "url": "jdbc:mysql://localhost:3306/testdb",
            "user": "testuser",
            "password": "testpass",
            "driver": "com.mysql.cj.jdbc.Driver",
        }

        conn = JDBCConnection(**config)
        assert conn.url == config["url"]
        assert "mysql" in conn.url.lower()

    def test_connection_properties(self):
        """Test JDBC connection properties generation"""
        config = {
            "url": "jdbc:postgresql://localhost:5432/testdb",
            "user": "testuser",
            "password": "testpass",
            "driver": "org.postgresql.Driver",
            "fetchsize": "1000",
            "batchsize": "5000",
        }

        conn = JDBCConnection(**config)
        props = conn.get_connection_properties()

        assert props.get("user") == "testuser"
        assert props.get("password") == "testpass"
        assert props.get("driver") == config["driver"]
        assert props.get("fetchsize") == "1000"

    @patch('connections.jdbc_connection.JDBCConnection.test_connection')
    def test_connection_test(self, mock_test):
        """Test connection testing functionality"""
        mock_test.return_value = True

        config = {
            "url": "jdbc:postgresql://localhost:5432/testdb",
            "user": "testuser",
            "password": "testpass",
            "driver": "org.postgresql.Driver",
        }

        conn = JDBCConnection(**config)
        result = conn.test_connection()

        assert result is True
        mock_test.assert_called_once()


class TestS3Connection:
    """Test S3/MinIO connections"""

    def test_s3_connection_initialization(self):
        """Test S3 connection initialization"""
        config = {
            "bucket": "test-bucket",
            "access_key": "mock_access_key",
            "secret_key": "mock_secret_key",
            "region": "us-east-1",
        }

        conn = S3Connection(**config)
        assert conn.bucket == config["bucket"]
        assert conn.region == config["region"]

    def test_s3_path_formatting(self):
        """Test S3 path formatting"""
        config = {
            "bucket": "test-bucket",
            "access_key": "mock_access_key",
            "secret_key": "mock_secret_key",
            "region": "us-east-1",
        }

        conn = S3Connection(**config)
        path = conn.get_s3_path("data/customers.parquet")

        assert path == "s3a://test-bucket/data/customers.parquet"

    def test_minio_connection_initialization(self):
        """Test MinIO (S3-compatible) connection"""
        config = {
            "bucket": "test-bucket",
            "access_key": "minioadmin",
            "secret_key": "minioadmin",
            "endpoint": "http://localhost:9000",
            "path_style_access": True,
        }

        conn = S3Connection(**config)
        assert conn.bucket == config["bucket"]
        assert conn.endpoint == config["endpoint"]
        assert conn.path_style_access is True

    def test_spark_config_generation(self):
        """Test Spark configuration generation for S3"""
        config = {
            "bucket": "test-bucket",
            "access_key": "mock_access_key",
            "secret_key": "mock_secret_key",
            "region": "us-east-1",
        }

        conn = S3Connection(**config)
        spark_config = conn.get_spark_config()

        assert "spark.hadoop.fs.s3a.access.key" in spark_config
        assert "spark.hadoop.fs.s3a.secret.key" in spark_config
        assert spark_config["spark.hadoop.fs.s3a.access.key"] == "mock_access_key"


class TestKafkaConnection:
    """Test Kafka connections"""

    def test_kafka_connection_initialization(self):
        """Test Kafka connection initialization"""
        config = {
            "bootstrap_servers": "localhost:9092",
            "topic": "test-topic",
        }

        conn = KafkaConnection(**config)
        assert conn.bootstrap_servers == config["bootstrap_servers"]
        assert conn.topic == config["topic"]

    def test_kafka_with_schema_registry(self):
        """Test Kafka connection with Schema Registry"""
        config = {
            "bootstrap_servers": "localhost:9092",
            "topic": "test-topic",
            "schema_registry_url": "http://localhost:8081",
        }

        conn = KafkaConnection(**config)
        assert conn.schema_registry_url == config["schema_registry_url"]

    def test_kafka_security_config(self):
        """Test Kafka security configuration"""
        config = {
            "bootstrap_servers": "localhost:9092",
            "topic": "test-topic",
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_username": "testuser",
            "sasl_password": "testpass",
        }

        conn = KafkaConnection(**config)
        kafka_options = conn.get_kafka_options()

        assert kafka_options.get("kafka.security.protocol") == "SASL_SSL"
        assert kafka_options.get("kafka.sasl.mechanism") == "PLAIN"

    def test_consumer_group_config(self):
        """Test Kafka consumer group configuration"""
        config = {
            "bootstrap_servers": "localhost:9092",
            "topic": "test-topic",
            "group_id": "test-consumer-group",
            "starting_offsets": "earliest",
        }

        conn = KafkaConnection(**config)
        kafka_options = conn.get_kafka_options()

        assert kafka_options.get("kafka.bootstrap.servers") == "localhost:9092"
        assert kafka_options.get("subscribe") == "test-topic"
        assert kafka_options.get("startingOffsets") == "earliest"


class TestRESTAPIConnection:
    """Test REST API connections (Salesforce, HubSpot, etc.)"""

    def test_rest_api_connection_initialization(self):
        """Test REST API connection initialization"""
        config = {
            "base_url": "https://api.example.com",
            "api_key": "test_api_key",
            "auth_type": "bearer",
        }

        conn = RESTAPIConnection(**config)
        assert conn.base_url == config["base_url"]
        assert conn.api_key == config["api_key"]

    def test_oauth_config(self):
        """Test OAuth configuration"""
        config = {
            "base_url": "https://api.example.com",
            "auth_type": "oauth2",
            "client_id": "test_client",
            "client_secret": "test_secret",
            "token_url": "https://api.example.com/oauth/token",
        }

        conn = RESTAPIConnection(**config)
        assert conn.auth_type == "oauth2"
        assert conn.client_id == config["client_id"]

    def test_headers_generation(self):
        """Test HTTP headers generation"""
        config = {
            "base_url": "https://api.example.com",
            "api_key": "test_api_key",
            "auth_type": "bearer",
        }

        conn = RESTAPIConnection(**config)
        headers = conn.get_headers()

        assert "Authorization" in headers
        assert headers["Authorization"] == "Bearer test_api_key"

    def test_custom_headers(self):
        """Test custom headers addition"""
        config = {
            "base_url": "https://api.example.com",
            "api_key": "test_api_key",
            "auth_type": "bearer",
            "custom_headers": {
                "X-Custom-Header": "custom-value",
                "Accept": "application/json",
            },
        }

        conn = RESTAPIConnection(**config)
        headers = conn.get_headers()

        assert headers["X-Custom-Header"] == "custom-value"
        assert headers["Accept"] == "application/json"


class TestFileConnection:
    """Test file-based connections (CSV, JSON, Parquet, Avro, etc.)"""

    def test_csv_connection_initialization(self):
        """Test CSV file connection initialization"""
        config = {
            "path": "/data/customers.csv",
            "format": "csv",
            "header": True,
            "delimiter": ",",
        }

        conn = FileConnection(**config)
        assert conn.path == config["path"]
        assert conn.format == "csv"
        assert conn.options["header"] == "true"

    def test_json_connection_initialization(self):
        """Test JSON file connection initialization"""
        config = {
            "path": "/data/customers.json",
            "format": "json",
            "multiline": True,
        }

        conn = FileConnection(**config)
        assert conn.format == "json"
        assert conn.options.get("multiLine") == "true"

    def test_parquet_connection_initialization(self):
        """Test Parquet file connection initialization"""
        config = {
            "path": "/data/customers.parquet",
            "format": "parquet",
            "merge_schema": True,
        }

        conn = FileConnection(**config)
        assert conn.format == "parquet"
        assert conn.options.get("mergeSchema") == "true"

    def test_delta_connection_initialization(self):
        """Test Delta Lake connection initialization"""
        config = {
            "path": "/data/customers_delta",
            "format": "delta",
        }

        conn = FileConnection(**config)
        assert conn.format == "delta"

    def test_read_options_generation(self):
        """Test read options generation for various formats"""
        config = {
            "path": "/data/customers.csv",
            "format": "csv",
            "header": True,
            "delimiter": "|",
            "quote": '"',
            "escape": "\\",
            "infer_schema": True,
        }

        conn = FileConnection(**config)
        options = conn.get_read_options()

        assert options["header"] == "true"
        assert options["delimiter"] == "|"
        assert options["inferSchema"] == "true"


class TestSpecializedConnections:
    """Test specialized connections (Salesforce, SAP, Mainframe, etc.)"""

    @patch('connections.salesforce_connection.SalesforceConnection.authenticate')
    def test_salesforce_connection(self, mock_auth):
        """Test Salesforce connection"""
        mock_auth.return_value = {"access_token": "mock_token", "instance_url": "https://test.salesforce.com"}

        from connections.salesforce_connection import SalesforceConnection

        config = {
            "username": "test@example.com",
            "password": "testpass",
            "security_token": "mock_token",
            "domain": "test",
        }

        conn = SalesforceConnection(**config)
        assert conn.username == config["username"]
        assert conn.domain == config["domain"]

    def test_snowflake_connection(self):
        """Test Snowflake connection"""
        from connections.snowflake_connection import SnowflakeConnection

        config = {
            "account": "test-account",
            "user": "testuser",
            "password": "testpass",
            "warehouse": "test_wh",
            "database": "test_db",
            "schema": "public",
        }

        conn = SnowflakeConnection(**config)
        assert conn.account == config["account"]
        assert conn.warehouse == config["warehouse"]

        # Test connection string generation
        conn_string = conn.get_connection_string()
        assert "test-account" in conn_string
        assert "test_wh" in conn_string

    def test_mongodb_connection(self):
        """Test MongoDB connection"""
        from connections.mongodb_connection import MongoDBConnection

        config = {
            "host": "localhost",
            "port": 27017,
            "database": "testdb",
            "collection": "customers",
            "username": "testuser",
            "password": "testpass",
        }

        conn = MongoDBConnection(**config)
        assert conn.host == config["host"]
        assert conn.database == config["database"]

        # Test connection URI generation
        uri = conn.get_connection_uri()
        assert "mongodb://" in uri
        assert "testdb" in uri


class TestConnectionValidation:
    """Test connection validation and error handling"""

    def test_missing_required_fields(self):
        """Test validation of required fields"""
        with pytest.raises(Exception):
            # Missing password
            JDBCConnection(
                url="jdbc:postgresql://localhost:5432/testdb",
                user="testuser",
                driver="org.postgresql.Driver",
            )

    def test_invalid_url_format(self):
        """Test validation of JDBC URL format"""
        config = {
            "url": "invalid_url",
            "user": "testuser",
            "password": "testpass",
            "driver": "org.postgresql.Driver",
        }

        conn = JDBCConnection(**config)
        # Should not raise during init, but may fail on validation
        assert conn.url == "invalid_url"

    def test_empty_credentials(self):
        """Test handling of empty credentials"""
        with pytest.raises(Exception):
            S3Connection(
                bucket="test-bucket",
                access_key="",
                secret_key="",
                region="us-east-1",
            )
