"""
Salesforce Incremental Ingestion using OAuth and SOQL.
Supports watermark-based incremental loading.
"""
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, timedelta
from sparkle.config_schema import config_schema, Field, BaseConfigSchema


@config_schema
class SalesforceIncrementalConfig(BaseConfigSchema):
    """Configuration for Salesforce incremental ingestion."""

    # OAuth credentials
    client_id: str = Field(
        ...,
        description="Salesforce OAuth client ID (Consumer Key)",
        widget="password",
        group="Authentication",
        order=1,
    )
    client_secret: str = Field(
        ...,
        description="Salesforce OAuth client secret (Consumer Secret)",
        widget="password",
        group="Authentication",
        order=2,
    )
    refresh_token: str = Field(
        ...,
        description="Salesforce refresh token",
        widget="password",
        group="Authentication",
        order=3,
    )
    instance_url: str = Field(
        "https://login.salesforce.com",
        description="Salesforce instance URL",
        examples=["https://login.salesforce.com", "https://test.salesforce.com"],
        group="Authentication",
        order=4,
    )

    # Query configuration
    query: str = Field(
        "SELECT Id, Name, CreatedDate, LastModifiedDate FROM Account",
        description="SOQL query to execute",
        widget="textarea",
        group="Query",
        order=10,
        help_text="Use standard SOQL syntax. Watermark column will be filtered automatically.",
    )

    # Incremental configuration
    watermark_column: str = Field(
        "LastModifiedDate",
        description="Column name for incremental loading (must be in SELECT clause)",
        examples=["LastModifiedDate", "CreatedDate", "SystemModstamp"],
        group="Incremental",
        order=20,
    )
    lookback_days: int = Field(
        7,
        ge=0,
        le=90,
        description="Number of days to look back from last watermark",
        group="Incremental",
        order=21,
    )

    # Advanced options
    batch_size: int = Field(
        2000,
        ge=200,
        le=10000,
        description="Number of records per batch",
        group="Advanced",
        order=30,
    )
    api_version: str = Field(
        "v57.0",
        description="Salesforce API version",
        pattern="^v\\d+\\.\\d+$",
        group="Advanced",
        order=31,
    )


class SalesforceIncrementalIngestion:
    """
    Ingest data from Salesforce using OAuth and incremental watermark.

    Tags: salesforce, crm, incremental, oauth
    Category: CRM
    Icon: salesforce
    """

    def __init__(self, config: dict):
        self.config = SalesforceIncrementalConfig(**config) if isinstance(config, dict) else config

    @staticmethod
    def config_schema() -> dict:
        """Return JSON Schema for configuration."""
        return SalesforceIncrementalConfig.config_schema()

    @staticmethod
    def sample_config() -> dict:
        """Return sample configuration."""
        return {
            "client_id": "YOUR_CLIENT_ID",
            "client_secret": "YOUR_CLIENT_SECRET",
            "refresh_token": "YOUR_REFRESH_TOKEN",
            "instance_url": "https://login.salesforce.com",
            "query": "SELECT Id, Name, Email, CreatedDate FROM Contact",
            "watermark_column": "CreatedDate",
            "lookback_days": 7,
            "batch_size": 2000,
            "api_version": "v57.0",
        }

    def run(self, spark: SparkSession, watermark: str = None) -> DataFrame:
        """
        Execute Salesforce ingestion.

        Args:
            spark: SparkSession
            watermark: Last watermark value (ISO datetime string)

        Returns:
            DataFrame with Salesforce data
        """
        # Placeholder implementation - real version would use simple-salesforce
        # and handle OAuth, pagination, etc.
        import pandas as pd

        # Mock data for demo
        data = {
            "Id": ["001xx000003DHEIAA4", "001xx000003DHEQAA4"],
            "Name": ["Acme Corp", "Global Industries"],
            "CreatedDate": ["2024-01-15T10:30:00Z", "2024-01-16T14:20:00Z"],
        }

        return spark.createDataFrame(pd.DataFrame(data))
