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
    Sub-Group: Streaming & Messaging

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
    Sub-Group: Streaming & Messaging

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
    Sub-Group: Streaming & Messaging

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
    Sub-Group: Streaming & Messaging

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
    Sub-Group: Streaming & Messaging

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
    Sub-Group: Streaming & Messaging

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
    Sub-Group: Streaming & Messaging

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
    Sub-Group: Streaming & Messaging

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


@register_connection("mqtt")
class MQTTConnection(StreamingConnection):
    """
    MQTT (Message Queuing Telemetry Transport) connection for IoT streaming.
    Sub-Group: Streaming & Messaging
    
    Uses Bahir MQTT connector or Kafka bridge pattern.
    
    Example config:
        mqtt_prod:
          type: mqtt
          broker_url: tcp://mqtt.example.com:1883
          username: secret://mqtt/username
          password: secret://mqtt/password
          client_id: sparkle-mqtt-client
          qos: 1
          clean_session: true
    
    Usage:
        >>> conn = Connection.get("mqtt", spark, env="prod")
        >>> stream_df = conn.read_stream(topic="sensors/temperature")
        >>> # Write stream
        >>> query = conn.write_stream(
        ...     df,
        ...     topic="actuators/control",
        ...     checkpoint_location="/tmp/mqtt_checkpoint"
        ... )
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.broker_url = self.config["broker_url"]
        self.username = self.config.get("username")
        self.password = self.config.get("password")
        self.client_id = self.config.get("client_id", "sparkle-mqtt")
        self.qos = self.config.get("qos", 1)
        self.clean_session = self.config.get("clean_session", True)
    
    def test(self) -> bool:
        """Test MQTT connection."""
        try:
            import paho.mqtt.client as mqtt
            
            def on_connect(client, userdata, flags, rc):
                if rc == 0:
                    self.logger.info("✓ MQTT connection successful")
                    client.disconnect()
                else:
                    self.logger.error(f"✗ MQTT connection failed with code {rc}")
            
            client = mqtt.Client(client_id=self.client_id, clean_session=self.clean_session)
            
            if self.username and self.password:
                client.username_pw_set(self.username, self.password)
            
            client.on_connect = on_connect
            
            # Parse broker URL
            protocol, rest = self.broker_url.split("://")
            host, port = rest.split(":")
            
            client.connect(host, int(port), 60)
            client.loop_start()
            import time
            time.sleep(2)
            client.loop_stop()
            
            return True
        except Exception as e:
            self.logger.error(f"✗ MQTT connection test failed: {e}")
            return False
    
    def read_stream(self, topic: str, **kwargs) -> DataFrame:
        """
        Read MQTT stream using Bahir connector.
        
        Args:
            topic: MQTT topic to subscribe (supports wildcards like 'sensors/#')
        
        Returns:
            Streaming DataFrame with columns: topic, payload, timestamp
        """
        options = {
            "brokerUrl": self.broker_url,
            "topic": topic,
            "clientId": self.client_id,
            "qos": str(self.qos),
            "cleanSession": str(self.clean_session).lower()
        }
        
        if self.username and self.password:
            options["username"] = self.username
            options["password"] = self.password
        
        # Merge with additional kwargs
        options.update(kwargs)
        
        self.logger.info(f"Starting MQTT stream on topic: {topic}")
        
        return (
            self.spark
            .readStream
            .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
            .options(**options)
            .load()
        )
    
    def write_stream(
        self,
        df: DataFrame,
        topic: str,
        checkpoint_location: str,
        output_mode: str = "append",
        **kwargs
    ):
        """
        Write DataFrame to MQTT topic.
        
        Args:
            df: Streaming DataFrame to write
            topic: MQTT topic to publish to
            checkpoint_location: Checkpoint directory for fault tolerance
            output_mode: Streaming output mode (append/update/complete)
        
        Returns:
            StreamingQuery object
        """
        options = {
            "brokerUrl": self.broker_url,
            "topic": topic,
            "clientId": f"{self.client_id}-writer",
            "qos": str(self.qos),
            "cleanSession": str(self.clean_session).lower()
        }
        
        if self.username and self.password:
            options["username"] = self.username
            options["password"] = self.password
        
        options.update(kwargs)
        
        self.logger.info(f"Starting MQTT write stream to topic: {topic}")
        
        query = (
            df.writeStream
            .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSinkProvider")
            .outputMode(output_mode)
            .options(**options)
            .option("checkpointLocation", checkpoint_location)
            .start()
        )
        
        return query
    
    def get_read_stream_options(self) -> Dict[str, str]:
        """Get base options for MQTT readStream."""
        options = {
            "brokerUrl": self.broker_url,
            "clientId": self.client_id,
            "qos": str(self.qos),
            "cleanSession": str(self.clean_session).lower()
        }
        
        if self.username and self.password:
            options["username"] = self.username
            options["password"] = self.password
        
        return options
    
    def get_write_stream_options(self) -> Dict[str, str]:
        """Get base options for MQTT writeStream."""
        options = self.get_read_stream_options()
        options["clientId"] = f"{self.client_id}-writer"
        return options


@register_connection("nats")
@register_connection("nats_jetstream")
class NATSJetStreamConnection(StreamingConnection):
    """
    NATS JetStream connection for cloud-native messaging.
    Sub-Group: Streaming & Messaging
    
    NATS is a simple, secure, and high-performance messaging system.
    JetStream adds streaming, persistence, and exactly-once semantics.
    
    Example config:
        nats_prod:
          type: nats_jetstream
          servers: nats://nats1.example.com:4222,nats://nats2.example.com:4222
          username: secret://nats/username
          password: secret://nats/password
          stream_name: EVENTS
          subject: events.>
          durable_name: sparkle-consumer
          tls_enabled: true
    
    Usage:
        >>> conn = Connection.get("nats", spark, env="prod")
        >>> stream_df = conn.read_stream(subject="events.orders")
        >>> # Write stream
        >>> query = conn.write_stream(
        ...     df,
        ...     subject="events.processed",
        ...     checkpoint_location="/tmp/nats_checkpoint"
        ... )
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.servers = self.config["servers"]
        self.username = self.config.get("username")
        self.password = self.config.get("password")
        self.stream_name = self.config.get("stream_name", "EVENTS")
        self.subject = self.config.get("subject", "events.>")
        self.durable_name = self.config.get("durable_name", "sparkle-consumer")
        self.tls_enabled = self.config.get("tls_enabled", False)
    
    def test(self) -> bool:
        """Test NATS JetStream connection."""
        try:
            import nats
            from nats.errors import Error as NATSError
            import asyncio
            
            async def test_connect():
                try:
                    nc = await nats.connect(
                        servers=self.servers.split(","),
                        user=self.username if self.username else None,
                        password=self.password if self.password else None,
                        tls=self.tls_enabled
                    )
                    
                    # Get JetStream context
                    js = nc.jetstream()
                    
                    # List streams to verify JetStream is available
                    streams = await js.streams_info()
                    
                    await nc.close()
                    self.logger.info(f"✓ NATS JetStream connection successful")
                    return True
                except NATSError as e:
                    self.logger.error(f"✗ NATS connection failed: {e}")
                    return False
            
            return asyncio.run(test_connect())
        except Exception as e:
            self.logger.error(f"✗ NATS connection test failed: {e}")
            return False
    
    def read_stream(self, subject: Optional[str] = None, **kwargs) -> DataFrame:
        """
        Read NATS JetStream as Spark Structured Stream.
        
        Note: This uses a custom foreachBatch pattern since there's no official
        NATS Spark connector. For production, consider using Kafka bridge.
        
        Args:
            subject: NATS subject to subscribe (defaults to config subject)
        
        Returns:
            Streaming DataFrame
        """
        subject = subject or self.subject
        
        # For NATS, we'll use a custom streaming source pattern
        # This is a placeholder - production implementation would need:
        # 1. Custom Spark Streaming Source
        # 2. Or NATS -> Kafka bridge
        # 3. Or microBatch pattern with foreachBatch
        
        self.logger.warning(
            "NATS JetStream doesn't have native Spark connector. "
            "Consider using NATS->Kafka bridge or implement custom source."
        )
        
        # Implement via rate source + custom logic for demo
        # Production would use custom StreamSourceProvider
        rate_stream = (
            self.spark
            .readStream
            .format("rate")
            .option("rowsPerSecond", 1)
            .load()
        )
        
        # User would need to implement custom foreachBatch to pull from NATS
        return rate_stream
    
    def write_stream(
        self,
        df: DataFrame,
        subject: Optional[str] = None,
        checkpoint_location: str = "/tmp/nats_checkpoint",
        output_mode: str = "append",
        **kwargs
    ):
        """
        Write DataFrame to NATS JetStream.
        
        Uses foreachBatch pattern to publish messages to NATS.
        
        Args:
            df: Streaming DataFrame
            subject: NATS subject to publish to
            checkpoint_location: Checkpoint directory
            output_mode: Streaming output mode
        
        Returns:
            StreamingQuery object
        """
        subject = subject or self.subject
        
        def write_batch_to_nats(batch_df, batch_id):
            """Write each micro-batch to NATS JetStream."""
            import nats
            import asyncio
            import json
            
            async def publish_batch():
                nc = await nats.connect(
                    servers=self.servers.split(","),
                    user=self.username if self.username else None,
                    password=self.password if self.password else None,
                    tls=self.tls_enabled
                )
                
                js = nc.jetstream()
                
                # Convert batch to JSON and publish
                rows = batch_df.collect()
                for row in rows:
                    message = json.dumps(row.asDict()).encode()
                    await js.publish(subject, message)
                
                await nc.close()
            
            asyncio.run(publish_batch())
            self.logger.info(f"Published batch {batch_id} to NATS subject {subject}")
        
        query = (
            df.writeStream
            .foreachBatch(write_batch_to_nats)
            .outputMode(output_mode)
            .option("checkpointLocation", checkpoint_location)
            .start()
        )
        
        return query
    
    def get_read_stream_options(self) -> Dict[str, str]:
        """Get base options for NATS readStream."""
        options = {
            "servers": self.servers,
            "stream": self.stream_name,
            "subject": self.subject,
            "durable": self.durable_name
        }
        
        if self.username:
            options["username"] = self.username
        if self.password:
            options["password"] = self.password
        if self.tls_enabled:
            options["tls"] = "true"
        
        return options
    
    def get_write_stream_options(self) -> Dict[str, str]:
        """Get base options for NATS writeStream."""
        return self.get_read_stream_options()
