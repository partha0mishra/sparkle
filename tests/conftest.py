"""
Pytest configuration and fixtures for Sparkle testing.

Provides local Spark session with Delta Lake support for in-memory testing.
"""

import os
import shutil
import tempfile
from pathlib import Path
from typing import Generator, Dict, Any

import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """
    Create a local Spark session with Delta Lake support for testing.

    Uses in-memory Derby metastore and local file system for Delta tables.
    """
    # Create temporary directories
    warehouse_dir = tempfile.mkdtemp(prefix="spark_warehouse_")
    derby_dir = tempfile.mkdtemp(prefix="derby_")
    checkpoint_dir = tempfile.mkdtemp(prefix="checkpoint_")

    builder = (
        SparkSession.builder
        .appName("sparkle-tests")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={derby_dir}/metastore_db;create=true")
        .config("spark.sql.streaming.checkpointLocation", checkpoint_dir)
    )

    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")

    yield spark_session

    # Cleanup
    spark_session.stop()
    shutil.rmtree(warehouse_dir, ignore_errors=True)
    shutil.rmtree(derby_dir, ignore_errors=True)
    shutil.rmtree(checkpoint_dir, ignore_errors=True)


@pytest.fixture(scope="function")
def temp_delta_table(spark: SparkSession) -> Generator[str, None, None]:
    """
    Create a temporary Delta table location for testing.

    Args:
        spark: Spark session fixture

    Yields:
        Path to temporary Delta table directory
    """
    temp_dir = tempfile.mkdtemp(prefix="delta_table_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="function")
def sample_df(spark: SparkSession):
    """
    Create a sample DataFrame for testing.

    Returns:
        DataFrame with sample customer data
    """
    data = [
        (1, "John Doe", "john@example.com", "2023-01-15", 1500.50, "Premium", "USA"),
        (2, "Jane Smith", "jane@example.com", "2023-02-20", 2300.75, "Premium", "Canada"),
        (3, "Bob Johnson", "", "2023-03-10", 450.25, "Basic", "USA"),
        (4, "Alice Williams", "alice@example.com", "2023-04-05", 3200.00, "Enterprise", "UK"),
        (5, "Charlie Brown", None, "2023-05-12", 0.00, "Basic", "USA"),
        (6, "Diana Prince", "diana@example.com", "2023-06-18", 5400.90, "Enterprise", "Germany"),
        (7, "Eve Anderson", "eve@INVALID", "2023-07-22", -100.00, "Premium", "France"),
        (8, "Frank Miller", "frank@example.com", "2023-08-30", 1200.50, "Premium", "USA"),
        (1, "John Doe", "john@example.com", "2023-01-15", 1500.50, "Premium", "USA"),  # Duplicate
        (9, "Grace Lee", "grace@example.com", "2023-09-14", 2800.00, "Enterprise", "Japan"),
    ]

    columns = ["customer_id", "name", "email", "signup_date", "total_spend", "tier", "country"]
    return spark.createDataFrame(data, columns)


@pytest.fixture(scope="function")
def sample_streaming_df(spark: SparkSession):
    """
    Create a sample streaming DataFrame for testing.

    Returns:
        Streaming DataFrame with sample event data
    """
    temp_dir = tempfile.mkdtemp(prefix="stream_source_")

    # Write some sample files
    data = [
        (1, "page_view", "2023-01-15T10:30:00", "user_1", "/home"),
        (2, "click", "2023-01-15T10:31:00", "user_1", "/products"),
        (3, "page_view", "2023-01-15T10:32:00", "user_2", "/home"),
        (4, "purchase", "2023-01-15T10:33:00", "user_1", "/checkout"),
    ]

    columns = ["event_id", "event_type", "timestamp", "user_id", "page"]
    df = spark.createDataFrame(data, columns)
    df.write.mode("overwrite").json(temp_dir)

    # Create streaming DataFrame
    streaming_df = (
        spark.readStream
        .schema(df.schema)
        .json(temp_dir)
    )

    return streaming_df


@pytest.fixture(scope="function")
def mock_connection_config() -> Dict[str, Any]:
    """
    Mock connection configuration for testing.

    Returns:
        Dictionary with mock connection parameters
    """
    return {
        "jdbc": {
            "url": "jdbc:postgresql://localhost:5432/testdb",
            "user": "testuser",
            "password": "testpass",
            "driver": "org.postgresql.Driver",
        },
        "s3": {
            "bucket": "test-bucket",
            "access_key": "mock_access_key",
            "secret_key": "mock_secret_key",
            "region": "us-east-1",
        },
        "kafka": {
            "bootstrap_servers": "localhost:9092",
            "topic": "test-topic",
            "schema_registry_url": "http://localhost:8081",
        },
        "salesforce": {
            "username": "test@example.com",
            "password": "testpass",
            "security_token": "mock_token",
            "domain": "test",
        },
        "snowflake": {
            "account": "test-account",
            "user": "testuser",
            "password": "testpass",
            "warehouse": "test_wh",
            "database": "test_db",
            "schema": "public",
        },
    }


@pytest.fixture(scope="function")
def sample_ml_df(spark: SparkSession):
    """
    Create a sample DataFrame for ML testing.

    Returns:
        DataFrame with features and labels for classification
    """
    data = []

    # Generate 1000 synthetic records for binary classification
    import random
    random.seed(42)

    for i in range(1000):
        feature1 = random.uniform(0, 100)
        feature2 = random.uniform(0, 100)
        feature3 = random.uniform(0, 100)
        feature4 = random.random()

        # Simple decision boundary: label = 1 if feature1 + feature2 > 100
        label = 1 if (feature1 + feature2 > 100) else 0

        data.append((
            i,
            feature1,
            feature2,
            feature3,
            feature4,
            random.choice(["A", "B", "C"]),
            label
        ))

    columns = ["id", "feature1", "feature2", "feature3", "feature4", "category", "label"]
    return spark.createDataFrame(data, columns)


@pytest.fixture(scope="function")
def temp_config_dir() -> Generator[Path, None, None]:
    """
    Create a temporary config directory for testing orchestration.

    Yields:
        Path to temporary config directory
    """
    temp_dir = Path(tempfile.mkdtemp(prefix="config_"))
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(autouse=True)
def reset_environment():
    """
    Reset environment variables before each test.
    """
    # Store original env vars
    original_env = os.environ.copy()

    yield

    # Restore original env vars
    os.environ.clear()
    os.environ.update(original_env)


# Test data constants
SAMPLE_CSV_DATA = """customer_id,name,email,total_spend
1,John Doe,john@example.com,1500.50
2,Jane Smith,jane@example.com,2300.75
3,Bob Johnson,bob@example.com,450.25
"""

SAMPLE_JSON_DATA = """
{"customer_id": 1, "name": "John Doe", "email": "john@example.com", "total_spend": 1500.50}
{"customer_id": 2, "name": "Jane Smith", "email": "jane@example.com", "total_spend": 2300.75}
{"customer_id": 3, "name": "Bob Johnson", "email": "bob@example.com", "total_spend": 450.25}
"""

SAMPLE_PARQUET_SCHEMA = {
    "type": "struct",
    "fields": [
        {"name": "customer_id", "type": "long", "nullable": True},
        {"name": "name", "type": "string", "nullable": True},
        {"name": "email", "type": "string", "nullable": True},
        {"name": "total_spend", "type": "double", "nullable": True},
    ]
}
