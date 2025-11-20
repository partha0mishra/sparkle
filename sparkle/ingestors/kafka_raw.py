"""
Kafka topic raw ingestion for streaming data.
"""
from pyspark.sql import SparkSession, DataFrame
from sparkle.config_schema import config_schema, Field, BaseConfigSchema
from enum import Enum


class StartingOffset(str, Enum):
    """Kafka starting offset options."""
    EARLIEST = "earliest"
    LATEST = "latest"


class SecurityProtocol(str, Enum):
    """Kafka security protocols."""
    PLAINTEXT = "PLAINTEXT"
    SSL = "SSL"
    SASL_PLAINTEXT = "SASL_PLAINTEXT"
    SASL_SSL = "SASL_SSL"


@config_schema
class KafkaRawConfig(BaseConfigSchema):
    """Configuration for Kafka raw topic ingestion."""

    # Connection
    bootstrap_servers: str = Field(
        ...,
        description="Kafka bootstrap servers (comma-separated)",
        examples=["localhost:9092", "broker1:9092,broker2:9092"],
        group="Connection",
        order=1,
    )
    topic: str = Field(
        ...,
        description="Kafka topic name",
        examples=["events", "user_activity", "transactions"],
        group="Connection",
        order=2,
    )

    # Security
    security_protocol: str = Field(
        SecurityProtocol.PLAINTEXT.value,
        description="Security protocol for Kafka connection",
        widget="dropdown",
        examples=[p.value for p in SecurityProtocol],
        group="Security",
        order=10,
    )
    sasl_mechanism: str = Field(
        None,
        description="SASL mechanism (required for SASL protocols)",
        widget="dropdown",
        examples=["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"],
        group="Security",
        order=11,
    )
    sasl_username: str = Field(
        None,
        description="SASL username",
        group="Security",
        order=12,
    )
    sasl_password: str = Field(
        None,
        description="SASL password",
        widget="password",
        group="Security",
        order=13,
    )

    # Consumption
    starting_offset: str = Field(
        StartingOffset.LATEST.value,
        description="Starting offset for consumption",
        widget="dropdown",
        examples=[o.value for o in StartingOffset],
        group="Consumption",
        order=20,
    )
    max_offsets_per_trigger: int = Field(
        None,
        ge=1,
        description="Max offsets to process per trigger (rate limiting)",
        group="Consumption",
        order=21,
    )
    consumer_group_id: str = Field(
        None,
        description="Consumer group ID (for tracking)",
        group="Consumption",
        order=22,
    )

    # Schema
    value_format: str = Field(
        "json",
        description="Message value format",
        widget="dropdown",
        examples=["json", "avro", "string", "binary"],
        group="Schema",
        order=30,
    )
    schema_registry_url: str = Field(
        None,
        description="Schema Registry URL (for Avro)",
        examples=["http://localhost:8081"],
        group="Schema",
        order=31,
    )

    # Processing
    include_metadata: bool = Field(
        True,
        description="Include Kafka metadata (partition, offset, timestamp)",
        group="Processing",
        order=40,
    )
    include_headers: bool = Field(
        False,
        description="Include Kafka message headers",
        group="Processing",
        order=41,
    )


class KafkaTopicRawIngestion:
    """
    Ingest raw messages from Kafka topic (batch or streaming).

    Tags: kafka, streaming, real-time, messaging
    Category: Streaming
    Icon: kafka
    """

    def __init__(self, config: dict):
        self.config = KafkaRawConfig(**config) if isinstance(config, dict) else config

    @staticmethod
    def config_schema() -> dict:
        return KafkaRawConfig.config_schema()

    @staticmethod
    def sample_config() -> dict:
        return {
            "bootstrap_servers": "localhost:9092",
            "topic": "events",
            "security_protocol": "PLAINTEXT",
            "starting_offset": "latest",
            "max_offsets_per_trigger": 10000,
            "consumer_group_id": "sparkle_consumer",
            "value_format": "json",
            "include_metadata": True,
            "include_headers": False,
        }

    def run_batch(self, spark: SparkSession) -> DataFrame:
        """
        Read Kafka topic in batch mode.

        Args:
            spark: SparkSession

        Returns:
            DataFrame with Kafka messages
        """
        options = {
            "kafka.bootstrap.servers": self.config.bootstrap_servers,
            "subscribe": self.config.topic,
            "startingOffsets": self.config.starting_offset,
        }

        # Add security options
        if self.config.security_protocol != SecurityProtocol.PLAINTEXT.value:
            options["kafka.security.protocol"] = self.config.security_protocol
            if self.config.sasl_mechanism:
                options["kafka.sasl.mechanism"] = self.config.sasl_mechanism
            if self.config.sasl_username and self.config.sasl_password:
                options["kafka.sasl.jaas.config"] = (
                    f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                    f'username="{self.config.sasl_username}" '
                    f'password="{self.config.sasl_password}";'
                )

        df = spark.read.format("kafka").options(**options).load()

        # Parse value based on format
        from pyspark.sql import functions as F

        if self.config.value_format == "json":
            df = df.withColumn("value", F.col("value").cast("string"))
            # Will parse JSON in next step
        elif self.config.value_format == "string":
            df = df.withColumn("value", F.col("value").cast("string"))

        # Select columns based on config
        select_cols = ["key", "value"]
        if self.config.include_metadata:
            select_cols.extend(["partition", "offset", "timestamp", "timestampType"])
        if self.config.include_headers:
            select_cols.append("headers")

        return df.select(*select_cols)

    def run_streaming(self, spark: SparkSession) -> DataFrame:
        """
        Read Kafka topic in streaming mode.

        Args:
            spark: SparkSession

        Returns:
            Streaming DataFrame
        """
        options = {
            "kafka.bootstrap.servers": self.config.bootstrap_servers,
            "subscribe": self.config.topic,
            "startingOffsets": self.config.starting_offset,
        }

        if self.config.max_offsets_per_trigger:
            options["maxOffsetsPerTrigger"] = self.config.max_offsets_per_trigger

        # Add security options (same as batch)
        if self.config.security_protocol != SecurityProtocol.PLAINTEXT.value:
            options["kafka.security.protocol"] = self.config.security_protocol
            if self.config.sasl_mechanism:
                options["kafka.sasl.mechanism"] = self.config.sasl_mechanism
            if self.config.sasl_username and self.config.sasl_password:
                options["kafka.sasl.jaas.config"] = (
                    f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                    f'username="{self.config.sasl_username}" '
                    f'password="{self.config.sasl_password}";'
                )

        df = spark.readStream.format("kafka").options(**options).load()

        # Same processing as batch
        from pyspark.sql import functions as F

        if self.config.value_format == "json":
            df = df.withColumn("value", F.col("value").cast("string"))
        elif self.config.value_format == "string":
            df = df.withColumn("value", F.col("value").cast("string"))

        select_cols = ["key", "value"]
        if self.config.include_metadata:
            select_cols.extend(["partition", "offset", "timestamp", "timestampType"])
        if self.config.include_headers:
            select_cols.append("headers")

        return df.select(*select_cols)
