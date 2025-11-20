"""
Streaming data source connections.

Supports: Kafka, AWS Kinesis, Azure Event Hubs, Google Pub/Sub,
Apache Pulsar, RabbitMQ, Amazon SQS, Azure Service Bus.
"""

from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame

from .base import StreamingConnection
from .factory import register_connection


@register_connection("kafka")
class KafkaConnection(StreamingConnection):
    """
    Apache Kafka connection for streaming and batch reads.

    Example config (config/connections/kafka/prod.json):
        {
            "bootstrap_servers": "kafka-1:9092,kafka-2:9092,kafka-3:9092",
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_username": "${KAFKA_USERNAME}",
            "sasl_password": "${KAFKA_PASSWORD}",
            "consumer_group": "sparkle-prod",
            "properties": {
                "session.timeout.ms": "30000",
                "enable.auto.commit": "false",
                "auto.offset.reset": "earliest"
            }
        }

    Usage:
        >>> conn = get_connection("kafka", spark, env="prod")
        >>> # Streaming read
        >>> stream_df = spark.readStream \\
        ...     .format("kafka") \\
        ...     .options(**conn.get_read_stream_options()) \\
        ...     .option("subscribe", "customer-events") \\
        ...     .load()
        >>> # Batch read
        >>> batch_df = conn.read_topic("customer-events", start_offset="earliest")
    """

    def test(self) -> bool:
        """Test Kafka connection by listing topics."""
        try:
            # Try to read from a non-existent topic to test connectivity
            test_df = self.spark.read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.config["bootstrap_servers"]) \
                .option("subscribe", "__non_existent_test_topic__") \
                .option("startingOffsets", "earliest") \
                .option("endingOffsets", "earliest") \
                .load()
            return True
        except Exception as e:
            error_msg = str(e).lower()
            # Connection successful if we get topic-not-found error rather than connection error
            if "unknown topic" in error_msg or "doesn't exist" in error_msg:
                return True
            self.logger.error(f"Kafka connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Kafka connection options."""
        return self.get_read_stream_options()

    def get_read_stream_options(self) -> Dict[str, str]:
        """Get options for Kafka readStream."""
        options = {
            "kafka.bootstrap.servers": self.config["bootstrap_servers"]
        }

        # Security settings
        if "security_protocol" in self.config:
            options["kafka.security.protocol"] = self.config["security_protocol"]

        if "sasl_mechanism" in self.config:
            options["kafka.sasl.mechanism"] = self.config["sasl_mechanism"]

        if "sasl_username" in self.config and "sasl_password" in self.config:
            options["kafka.sasl.jaas.config"] = (
                f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                f'username="{self.config["sasl_username"]}" '
                f'password="{self.config["sasl_password"]}";'
            )

        # Consumer group
        if "consumer_group" in self.config:
            options["kafka.group.id"] = self.config["consumer_group"]

        # Additional properties
        for key, value in self.config.get("properties", {}).items():
            options[f"kafka.{key}"] = str(value)

        return options

    def get_write_stream_options(self) -> Dict[str, str]:
        """Get options for Kafka writeStream."""
        options = {
            "kafka.bootstrap.servers": self.config["bootstrap_servers"]
        }

        # Security settings (same as read)
        if "security_protocol" in self.config:
            options["kafka.security.protocol"] = self.config["security_protocol"]

        if "sasl_mechanism" in self.config:
            options["kafka.sasl.mechanism"] = self.config["sasl_mechanism"]

        if "sasl_username" in self.config and "sasl_password" in self.config:
            options["kafka.sasl.jaas.config"] = (
                f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                f'username="{self.config["sasl_username"]}" '
                f'password="{self.config["sasl_password"]}";'
            )

        return options

    def read_topic(
        self,
        topic: str,
        start_offset: str = "earliest",
        end_offset: str = "latest"
    ) -> DataFrame:
        """
        Batch read from Kafka topic.

        Args:
            topic: Topic name
            start_offset: Starting offset (earliest/latest or JSON offset spec)
            end_offset: Ending offset (latest or JSON offset spec)

        Returns:
            DataFrame with Kafka messages
        """
        reader = self.spark.read.format("kafka")

        for key, value in self.get_read_stream_options().items():
            reader = reader.option(key, value)

        df = reader \
            .option("subscribe", topic) \
            .option("startingOffsets", start_offset) \
            .option("endingOffsets", end_offset) \
            .load()

        self.emit_lineage("read", {"topic": topic, "type": "batch"})
        return df


@register_connection("kinesis")
@register_connection("aws_kinesis")
class KinesisConnection(StreamingConnection):
    """
    AWS Kinesis connection for streaming data.

    Example config:
        {
            "stream_name": "customer-events",
            "region": "us-east-1",
            "endpoint_url": "https://kinesis.us-east-1.amazonaws.com",
            "auth_type": "iam_role"
        }

    Or with explicit credentials:
        {
            "stream_name": "customer-events",
            "region": "us-east-1",
            "aws_access_key_id": "${AWS_ACCESS_KEY_ID}",
            "aws_secret_access_key": "${AWS_SECRET_ACCESS_KEY}"
        }
    """

    def test(self) -> bool:
        """Test Kinesis connection."""
        try:
            # Would need AWS SDK to properly test
            # For now, just validate config
            required = ["stream_name", "region"]
            return all(k in self.config for k in required)
        except Exception as e:
            self.logger.error(f"Kinesis connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Kinesis connection options."""
        return self.get_read_stream_options()

    def get_read_stream_options(self) -> Dict[str, str]:
        """Get options for Kinesis readStream."""
        options = {
            "streamName": self.config["stream_name"],
            "region": self.config["region"],
            "endpointUrl": self.config.get(
                "endpoint_url",
                f"https://kinesis.{self.config['region']}.amazonaws.com"
            )
        }

        if "aws_access_key_id" in self.config:
            options["awsAccessKeyId"] = self.config["aws_access_key_id"]
            options["awsSecretKey"] = self.config["aws_secret_access_key"]

        return options

    def get_write_stream_options(self) -> Dict[str, str]:
        """Get options for Kinesis writeStream."""
        return self.get_read_stream_options()


@register_connection("eventhub")
@register_connection("azure_eventhub")
class EventHubConnection(StreamingConnection):
    """
    Azure Event Hubs connection.

    Example config:
        {
            "namespace": "my-eventhub-ns",
            "eventhub_name": "customer-events",
            "connection_string": "${EVENTHUB_CONNECTION_STRING}",
            "consumer_group": "$Default",
            "properties": {
                "eventhubs.prefetchCount": "1000",
                "eventhubs.maxRatePerPartition": "10000"
            }
        }
    """

    def test(self) -> bool:
        """Test Event Hub connection."""
        try:
            required = ["eventhub_name", "connection_string"]
            return all(k in self.config for k in required)
        except Exception as e:
            self.logger.error(f"Event Hub connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Event Hub connection options."""
        return self.get_read_stream_options()

    def get_read_stream_options(self) -> Dict[str, str]:
        """Get options for Event Hub readStream."""
        connection_string = self.config["connection_string"]

        # Append EntityPath if not in connection string
        if "EntityPath=" not in connection_string:
            connection_string += f";EntityPath={self.config['eventhub_name']}"

        options = {
            "eventhubs.connectionString": connection_string
        }

        if "consumer_group" in self.config:
            options["eventhubs.consumerGroup"] = self.config["consumer_group"]

        # Additional properties
        for key, value in self.config.get("properties", {}).items():
            options[key] = str(value)

        return options

    def get_write_stream_options(self) -> Dict[str, str]:
        """Get options for Event Hub writeStream."""
        return self.get_read_stream_options()


@register_connection("pubsub")
@register_connection("google_pubsub")
class PubSubConnection(StreamingConnection):
    """
    Google Cloud Pub/Sub connection.

    Example config:
        {
            "project_id": "my-gcp-project",
            "subscription_id": "sparkle-subscription",
            "credentials_file": "/path/to/service-account.json"
        }
    """

    def test(self) -> bool:
        """Test Pub/Sub connection."""
        try:
            required = ["project_id", "subscription_id"]
            return all(k in self.config for k in required)
        except Exception as e:
            self.logger.error(f"Pub/Sub connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Pub/Sub connection options."""
        return self.get_read_stream_options()

    def get_read_stream_options(self) -> Dict[str, str]:
        """Get options for Pub/Sub readStream."""
        options = {
            "subscriptionId": f"projects/{self.config['project_id']}/subscriptions/{self.config['subscription_id']}"
        }

        if "credentials_file" in self.config:
            options["credentialsFile"] = self.config["credentials_file"]

        return options

    def get_write_stream_options(self) -> Dict[str, str]:
        """Get options for Pub/Sub writeStream."""
        options = {
            "topicId": f"projects/{self.config['project_id']}/topics/{self.config.get('topic_id', '')}"
        }

        if "credentials_file" in self.config:
            options["credentialsFile"] = self.config["credentials_file"]

        return options


@register_connection("pulsar")
class PulsarConnection(StreamingConnection):
    """
    Apache Pulsar connection.

    Example config:
        {
            "service_url": "pulsar://pulsar.example.com:6650",
            "admin_url": "http://pulsar.example.com:8080",
            "topic": "persistent://public/default/customer-events",
            "subscription_name": "sparkle-prod"
        }
    """

    def test(self) -> bool:
        """Test Pulsar connection."""
        try:
            required = ["service_url", "topic"]
            return all(k in self.config for k in required)
        except Exception as e:
            self.logger.error(f"Pulsar connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Pulsar connection options."""
        return self.get_read_stream_options()

    def get_read_stream_options(self) -> Dict[str, str]:
        """Get options for Pulsar readStream."""
        options = {
            "service.url": self.config["service_url"],
            "topic": self.config["topic"]
        }

        if "admin_url" in self.config:
            options["admin.url"] = self.config["admin_url"]

        if "subscription_name" in self.config:
            options["subscription.name"] = self.config["subscription_name"]

        return options

    def get_write_stream_options(self) -> Dict[str, str]:
        """Get options for Pulsar writeStream."""
        return {
            "service.url": self.config["service_url"],
            "topic": self.config["topic"]
        }


@register_connection("rabbitmq")
class RabbitMQConnection(StreamingConnection):
    """
    RabbitMQ connection.

    Example config:
        {
            "host": "rabbitmq.example.com",
            "port": 5672,
            "virtual_host": "/",
            "username": "${RABBITMQ_USER}",
            "password": "${RABBITMQ_PASSWORD}",
            "queue": "customer-events",
            "exchange": "events",
            "routing_key": "customer.*"
        }
    """

    def test(self) -> bool:
        """Test RabbitMQ connection."""
        try:
            required = ["host", "username", "password"]
            return all(k in self.config for k in required)
        except Exception as e:
            self.logger.error(f"RabbitMQ connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get RabbitMQ connection options."""
        return self.get_read_stream_options()

    def get_read_stream_options(self) -> Dict[str, str]:
        """Get options for RabbitMQ readStream."""
        return {
            "host": self.config["host"],
            "port": str(self.config.get("port", 5672)),
            "virtualHost": self.config.get("virtual_host", "/"),
            "username": self.config["username"],
            "password": self.config["password"],
            "queue": self.config.get("queue", "")
        }

    def get_write_stream_options(self) -> Dict[str, str]:
        """Get options for RabbitMQ writeStream."""
        return self.get_read_stream_options()


@register_connection("sqs")
@register_connection("aws_sqs")
class SQSConnection(StreamingConnection):
    """
    Amazon SQS connection.

    Example config:
        {
            "queue_url": "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
            "region": "us-east-1",
            "auth_type": "iam_role"
        }
    """

    def test(self) -> bool:
        """Test SQS connection."""
        try:
            required = ["queue_url", "region"]
            return all(k in self.config for k in required)
        except Exception as e:
            self.logger.error(f"SQS connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get SQS connection options."""
        return self.get_read_stream_options()

    def get_read_stream_options(self) -> Dict[str, str]:
        """Get options for SQS readStream."""
        options = {
            "queueUrl": self.config["queue_url"],
            "region": self.config["region"]
        }

        if "aws_access_key_id" in self.config:
            options["awsAccessKeyId"] = self.config["aws_access_key_id"]
            options["awsSecretKey"] = self.config["aws_secret_access_key"]

        return options

    def get_write_stream_options(self) -> Dict[str, str]:
        """Get options for SQS writeStream."""
        return self.get_read_stream_options()


@register_connection("servicebus")
@register_connection("azure_servicebus")
class ServiceBusConnection(StreamingConnection):
    """
    Azure Service Bus connection.

    Example config:
        {
            "namespace": "my-servicebus-ns",
            "queue_name": "customer-events",
            "connection_string": "${SERVICEBUS_CONNECTION_STRING}"
        }
    """

    def test(self) -> bool:
        """Test Service Bus connection."""
        try:
            required = ["connection_string"]
            return all(k in self.config for k in required)
        except Exception as e:
            self.logger.error(f"Service Bus connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Service Bus connection options."""
        return self.get_read_stream_options()

    def get_read_stream_options(self) -> Dict[str, str]:
        """Get options for Service Bus readStream."""
        connection_string = self.config["connection_string"]

        if "queue_name" in self.config and "EntityPath=" not in connection_string:
            connection_string += f";EntityPath={self.config['queue_name']}"
        elif "topic_name" in self.config and "EntityPath=" not in connection_string:
            connection_string += f";EntityPath={self.config['topic_name']}"

        return {"connectionString": connection_string}

    def get_write_stream_options(self) -> Dict[str, str]:
        """Get options for Service Bus writeStream."""
        return self.get_read_stream_options()
