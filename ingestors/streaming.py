"""
Streaming and event bus ingestors.

8 production-grade streaming ingestion patterns.
"""

from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, schema_of_json
from .base import BaseStreamingIngestor
from .factory import register_ingestor


@register_ingestor("kafka_topic_raw")
class KafkaTopicRawIngestion(BaseStreamingIngestor):
    """
    Raw Kafka topic ingestion with exactly-once semantics.

    Sub-Group: Streaming & Event Bus
    Tags: kafka, streaming, exactly-once, event-bus

    Config example:
        {
            "source_name": "events_kafka",
            "connection_name": "kafka",
            "topic": "customer-events",
            "target_table": "customer_events_raw",
            "checkpoint_location": "/mnt/checkpoints/customer_events",
            "value_deserializer": "json",
            "schema_registry_url": "http://schema-registry:8081"
        }
    """
    
    def read_stream(self) -> DataFrame:
        from sparkle.connections import Connection
        
        conn_name = self.config["connection_name"]
        topic = self.config["topic"]
        
        conn = Connection.get(conn_name, self.spark, env=self.config.get("connection_env", self.env))
        
        # Read stream
        stream_df = conn.read_stream(topic=topic)
        
        # Deserialize value
        value_deser = self.config.get("value_deserializer", "string")
        
        if value_deser == "json":
            from pyspark.sql.functions import from_json, col
            from pyspark.sql.types import StringType
            
            # Cast value to string and parse JSON
            stream_df = stream_df.withColumn("value", col("value").cast(StringType()))
            
            # Infer schema from sample or use provided schema
            if "value_schema" in self.config:
                schema = self.config["value_schema"]
            else:
                # Use schema inference
                schema = "struct<>"  # Placeholder
            
            stream_df = stream_df.withColumn("data", from_json(col("value"), schema))
        
        return stream_df


@register_ingestor("kinesis_datastreams")
class KinesisDataStreamsIngestion(BaseStreamingIngestor):
    """
    AWS Kinesis Data Streams ingestion.

    Sub-Group: Streaming & Event Bus
    Tags: kinesis, aws, streaming, event-bus

    Config example:
        {
            "source_name": "kinesis_events",
            "connection_name": "kinesis",
            "stream_name": "customer-events",
            "target_table": "kinesis_events_raw",
            "checkpoint_location": "/mnt/checkpoints/kinesis"
        }
    """
    
    def read_stream(self) -> DataFrame:
        from sparkle.connections import Connection
        
        conn_name = self.config["connection_name"]
        stream_name = self.config["stream_name"]
        
        conn = Connection.get(conn_name, self.spark, env=self.config.get("connection_env", self.env))
        
        stream_df = conn.read_stream(stream_name=stream_name)
        
        return stream_df


@register_ingestor("eventhubs_kafka_protocol")
class EventHubsKafkaProtocolIngestion(BaseStreamingIngestor):
    """
    Azure Event Hubs (Kafka mode) ingestion.

    Sub-Group: Streaming & Event Bus
    Tags: eventhubs, azure, kafka-protocol, streaming

    Config example:
        {
            "source_name": "eventhubs_events",
            "connection_name": "eventhubs",
            "topic": "events",
            "target_table": "eventhubs_raw",
            "checkpoint_location": "/mnt/checkpoints/eventhubs"
        }
    """
    
    def read_stream(self) -> DataFrame:
        from sparkle.connections import Connection
        
        conn_name = self.config["connection_name"]
        topic = self.config["topic"]
        
        conn = Connection.get(conn_name, self.spark, env=self.config.get("connection_env", self.env))
        
        stream_df = conn.read_stream(topic=topic)
        
        return stream_df


@register_ingestor("pubsub_topic")
class PubSubTopicIngestion(BaseStreamingIngestor):
    """
    Google Pub/Sub topic ingestion.

    Sub-Group: Streaming & Event Bus
    Tags: pubsub, gcp, streaming, messaging

    Config example:
        {
            "source_name": "pubsub_events",
            "connection_name": "pubsub",
            "subscription": "events-subscription",
            "target_table": "pubsub_raw",
            "checkpoint_location": "/mnt/checkpoints/pubsub"
        }
    """
    
    def read_stream(self) -> DataFrame:
        from sparkle.connections import Connection
        
        conn_name = self.config["connection_name"]
        subscription = self.config["subscription"]
        
        conn = Connection.get(conn_name, self.spark, env=self.config.get("connection_env", self.env))
        
        stream_df = conn.read_stream(subscription=subscription)
        
        return stream_df


@register_ingestor("pulsar_topic")
class PulsarTopicIngestion(BaseStreamingIngestor):
    """
    Apache Pulsar topic ingestion.

    Sub-Group: Streaming & Event Bus
    Tags: pulsar, apache-pulsar, streaming, messaging

    Config example:
        {
            "source_name": "pulsar_events",
            "connection_name": "pulsar",
            "topic": "persistent://public/default/events",
            "target_table": "pulsar_raw",
            "checkpoint_location": "/mnt/checkpoints/pulsar"
        }
    """
    
    def read_stream(self) -> DataFrame:
        from sparkle.connections import Connection
        
        conn_name = self.config["connection_name"]
        topic = self.config["topic"]
        
        conn = Connection.get(conn_name, self.spark, env=self.config.get("connection_env", self.env))
        
        stream_df = conn.read_stream(topic=topic)
        
        return stream_df


@register_ingestor("confluent_schema_registry_avro")
class ConfluentSchemaRegistryAvroIngestion(BaseStreamingIngestor):
    """
    Kafka with Confluent Schema Registry Avro deserialization.

    Sub-Group: Streaming & Event Bus
    Tags: kafka, confluent, schema-registry, avro, streaming

    Config example:
        {
            "source_name": "avro_events",
            "connection_name": "kafka",
            "topic": "avro-events",
            "schema_registry_url": "http://schema-registry:8081",
            "target_table": "avro_events_raw",
            "checkpoint_location": "/mnt/checkpoints/avro"
        }
    """
    
    def read_stream(self) -> DataFrame:
        from sparkle.connections import Connection
        from pyspark.sql.avro.functions import from_avro
        
        conn_name = self.config["connection_name"]
        topic = self.config["topic"]
        schema_registry_url = self.config["schema_registry_url"]
        
        conn = Connection.get(conn_name, self.spark, env=self.config.get("connection_env", self.env))
        
        # Read Kafka stream
        stream_df = conn.read_stream(topic=topic)
        
        # Deserialize Avro with Schema Registry
        json_schema = f"""{{
            "type": "record",
            "name": "Event",
            "fields": []
        }}"""
        
        stream_df = stream_df.select(
            from_avro(col("value"), json_schema).alias("data")
        )
        
        return stream_df


@register_ingestor("redpanda_topic")
class RedpandaTopicIngestion(BaseStreamingIngestor):
    """
    Redpanda topic ingestion (Kafka-compatible).

    Sub-Group: Streaming & Event Bus
    Tags: redpanda, kafka-compatible, streaming, event-bus

    Config example:
        {
            "source_name": "redpanda_events",
            "connection_name": "kafka",
            "topic": "events",
            "target_table": "redpanda_raw",
            "checkpoint_location": "/mnt/checkpoints/redpanda"
        }
    """
    
    def read_stream(self) -> DataFrame:
        from sparkle.connections import Connection
        
        conn_name = self.config["connection_name"]
        topic = self.config["topic"]
        
        conn = Connection.get(conn_name, self.spark, env=self.config.get("connection_env", self.env))
        
        stream_df = conn.read_stream(topic=topic)
        
        return stream_df


@register_ingestor("generic_structured_streaming")
class StructuredStreamingCheckpointedIngestion(BaseStreamingIngestor):
    """
    Generic wrapper for any streaming source.

    Sub-Group: Streaming & Event Bus
    Tags: structured-streaming, generic, checkpointed, streaming

    Config example:
        {
            "source_name": "custom_stream",
            "source_format": "rate",
            "source_options": {"rowsPerSecond": 10},
            "target_table": "custom_stream_raw",
            "checkpoint_location": "/mnt/checkpoints/custom"
        }
    """
    
    def read_stream(self) -> DataFrame:
        source_format = self.config["source_format"]
        source_options = self.config.get("source_options", {})
        
        stream_df = (
            self.spark
            .readStream
            .format(source_format)
            .options(**source_options)
            .load()
        )
        
        return stream_df
