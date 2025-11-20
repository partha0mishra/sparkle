"""
JDBC Incremental Ingestion by Date Column.
Supports watermark-based incremental loading from any JDBC source.
"""
from pyspark.sql import SparkSession, DataFrame
from sparkle.config_schema import config_schema, Field, BaseConfigSchema
from enum import Enum


class JdbcDriver(str, Enum):
    """Supported JDBC drivers."""
    POSTGRESQL = "org.postgresql.Driver"
    MYSQL = "com.mysql.cj.jdbc.Driver"
    SQLSERVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ORACLE = "oracle.jdbc.OracleDriver"


@config_schema
class JdbcIncrementalConfig(BaseConfigSchema):
    """Configuration for JDBC incremental ingestion."""

    # Connection
    jdbc_url: str = Field(
        ...,
        description="JDBC connection URL",
        examples=[
            "jdbc:postgresql://localhost:5432/mydb",
            "jdbc:mysql://localhost:3306/mydb",
            "jdbc:sqlserver://localhost:1433;databaseName=mydb",
        ],
        group="Connection",
        order=1,
    )
    driver: str = Field(
        JdbcDriver.POSTGRESQL.value,
        description="JDBC driver class name",
        widget="dropdown",
        examples=[d.value for d in JdbcDriver],
        group="Connection",
        order=2,
    )
    username: str = Field(
        ...,
        description="Database username",
        group="Connection",
        order=3,
    )
    password: str = Field(
        ...,
        description="Database password",
        widget="password",
        group="Connection",
        order=4,
    )

    # Query
    table_name: str = Field(
        ...,
        description="Table name to ingest",
        examples=["public.users", "sales.orders"],
        group="Query",
        order=10,
    )
    custom_query: str = Field(
        None,
        description="Custom SQL query (overrides table_name)",
        widget="textarea",
        group="Query",
        order=11,
        help_text="Use {watermark} placeholder for incremental column filter",
    )

    # Incremental
    watermark_column: str = Field(
        "updated_at",
        description="Column for incremental loading (timestamp or date)",
        examples=["updated_at", "created_at", "modified_date"],
        group="Incremental",
        order=20,
    )
    watermark_column_type: str = Field(
        "timestamp",
        description="Type of watermark column",
        widget="dropdown",
        examples=["timestamp", "date", "integer"],
        group="Incremental",
        order=21,
    )

    # Performance
    num_partitions: int = Field(
        4,
        ge=1,
        le=100,
        description="Number of partitions for parallel reading",
        group="Performance",
        order=30,
    )
    fetch_size: int = Field(
        10000,
        ge=100,
        le=100000,
        description="JDBC fetch size (rows per batch)",
        group="Performance",
        order=31,
    )


class JdbcIncrementalByDateColumn:
    """
    Incremental JDBC ingestion using a date/timestamp column.

    Tags: jdbc, postgresql, mysql, incremental, database
    Category: Database
    Icon: database
    """

    def __init__(self, config: dict):
        self.config = JdbcIncrementalConfig(**config) if isinstance(config, dict) else config

    @staticmethod
    def config_schema() -> dict:
        return JdbcIncrementalConfig.config_schema()

    @staticmethod
    def sample_config() -> dict:
        return {
            "jdbc_url": "jdbc:postgresql://localhost:5432/mydb",
            "driver": "org.postgresql.Driver",
            "username": "user",
            "password": "password",
            "table_name": "public.users",
            "watermark_column": "updated_at",
            "watermark_column_type": "timestamp",
            "num_partitions": 4,
            "fetch_size": 10000,
        }

    def run(self, spark: SparkSession, watermark: str = None) -> DataFrame:
        """Execute JDBC ingestion."""
        # Build query
        if self.config.custom_query:
            query = self.config.custom_query
        else:
            query = f"SELECT * FROM {self.config.table_name}"

        # Add watermark filter
        if watermark:
            query += f" WHERE {self.config.watermark_column} > '{watermark}'"

        # Read from JDBC
        df = (
            spark.read.format("jdbc")
            .option("url", self.config.jdbc_url)
            .option("driver", self.config.driver)
            .option("user", self.config.username)
            .option("password", self.config.password)
            .option("query", query)
            .option("numPartitions", self.config.num_partitions)
            .option("fetchsize", self.config.fetch_size)
            .load()
        )

        return df
