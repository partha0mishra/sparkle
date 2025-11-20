"""
NoSQL database connections.

Supports: MongoDB, Cassandra, DynamoDB, CosmosDB, Elasticsearch,
HBase, Couchbase, Neo4j, Redis, ScyllaDB.
"""

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame

from .base import SparkleConnection
from .factory import register_connection


@register_connection("mongodb")
@register_connection("mongo")
class MongoDBConnection(SparkleConnection):
    """
    MongoDB connection via Spark connector.

    Example config (config/connections/mongodb/prod.json):
        {
            "connection_string": "mongodb://user:password@host1:27017,host2:27017,host3:27017/",
            "database": "analytics",
            "auth_source": "admin",
            "properties": {
                "readPreference.name": "secondaryPreferred",
                "retryWrites": "true"
            }
        }

    Or with individual parameters:
        {
            "hosts": "host1:27017,host2:27017,host3:27017",
            "username": "${MONGO_USER}",
            "password": "${MONGO_PASSWORD}",
            "database": "analytics",
            "auth_source": "admin"
        }

    Usage:
        >>> conn = get_connection("mongodb", spark, env="prod")
        >>> df = conn.read_collection("customers")
        >>> conn.write_collection(df, "customers_gold", mode="overwrite")
    """

    def test(self) -> bool:
        """Test MongoDB connection."""
        try:
            # Try to read from a collection (will fail if connection is bad)
            test_df = self.spark.read \
                .format("mongodb") \
                .options(**self.get_connection()) \
                .option("collection", "system.profile") \
                .load() \
                .limit(1)
            test_df.count()
            return True
        except Exception as e:
            self.logger.error(f"MongoDB connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get MongoDB connection options."""
        if "connection_string" in self.config:
            # Use connection string
            options = {
                "connection.uri": self.config["connection_string"]
            }
            if "database" in self.config:
                options["database"] = self.config["database"]
        else:
            # Build connection string from components
            hosts = self.config["hosts"]
            username = self.config.get("username", "")
            password = self.config.get("password", "")

            if username and password:
                uri = f"mongodb://{username}:{password}@{hosts}/"
            else:
                uri = f"mongodb://{hosts}/"

            options = {
                "connection.uri": uri,
                "database": self.config["database"]
            }

        if "auth_source" in self.config:
            options["authSource"] = self.config["auth_source"]

        # Add additional properties
        options.update(self.config.get("properties", {}))

        return options

    def read_collection(
        self,
        collection: str,
        pipeline: Optional[list] = None,
        sample_size: Optional[int] = None
    ) -> DataFrame:
        """
        Read collection from MongoDB.

        Args:
            collection: Collection name
            pipeline: Optional aggregation pipeline
            sample_size: Optional sample size for schema inference

        Returns:
            DataFrame
        """
        reader = self.spark.read.format("mongodb")

        for key, value in self.get_connection().items():
            reader = reader.option(key, value)

        reader = reader.option("collection", collection)

        if pipeline:
            import json
            reader = reader.option("pipeline", json.dumps(pipeline))

        if sample_size:
            reader = reader.option("sampleSize", sample_size)

        self.emit_lineage("read", {"collection": collection})

        return reader.load()

    def write_collection(
        self,
        df: DataFrame,
        collection: str,
        mode: str = "append"
    ) -> None:
        """Write DataFrame to MongoDB collection."""
        writer = df.write.format("mongodb")

        for key, value in self.get_connection().items():
            writer = writer.option(key, value)

        writer.option("collection", collection).mode(mode).save()

        self.emit_lineage("write", {"collection": collection, "mode": mode})


@register_connection("cassandra")
class CassandraConnection(SparkleConnection):
    """
    Apache Cassandra connection.

    Example config:
        {
            "host": "cassandra-1.example.com,cassandra-2.example.com,cassandra-3.example.com",
            "port": 9042,
            "keyspace": "analytics",
            "username": "${CASSANDRA_USER}",
            "password": "${CASSANDRA_PASSWORD}",
            "properties": {
                "spark.cassandra.connection.timeout_ms": "30000",
                "spark.cassandra.read.timeout_ms": "120000"
            }
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        super().__init__(spark, config, env, **kwargs)
        self._configure_spark()

    def _configure_spark(self):
        """Configure Spark for Cassandra."""
        conf = self.spark.sparkContext._conf

        conf.set("spark.cassandra.connection.host", self.config["host"])
        if "port" in self.config:
            conf.set("spark.cassandra.connection.port", str(self.config["port"]))

        if "username" in self.config:
            conf.set("spark.cassandra.auth.username", self.config["username"])
            conf.set("spark.cassandra.auth.password", self.config["password"])

        # Apply additional properties
        for key, value in self.config.get("properties", {}).items():
            conf.set(key, str(value))

    def test(self) -> bool:
        """Test Cassandra connection."""
        try:
            # Try to read from system.local
            test_df = self.spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .option("keyspace", "system") \
                .option("table", "local") \
                .load() \
                .limit(1)
            test_df.count()
            return True
        except Exception as e:
            self.logger.error(f"Cassandra connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Cassandra connection options."""
        return {
            "keyspace": self.config.get("keyspace", "")
        }

    def read_table(self, table: str, keyspace: Optional[str] = None) -> DataFrame:
        """Read table from Cassandra."""
        keyspace = keyspace or self.config.get("keyspace")

        reader = self.spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", keyspace) \
            .option("table", table)

        self.emit_lineage("read", {"keyspace": keyspace, "table": table})

        return reader.load()

    def write_table(
        self,
        df: DataFrame,
        table: str,
        keyspace: Optional[str] = None,
        mode: str = "append"
    ) -> None:
        """Write DataFrame to Cassandra table."""
        keyspace = keyspace or self.config.get("keyspace")

        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", keyspace) \
            .option("table", table) \
            .mode(mode) \
            .save()

        self.emit_lineage("write", {"keyspace": keyspace, "table": table, "mode": mode})


@register_connection("dynamodb")
@register_connection("aws_dynamodb")
class DynamoDBConnection(SparkleConnection):
    """
    AWS DynamoDB connection.

    Example config:
        {
            "region": "us-east-1",
            "auth_type": "iam_role",
            "properties": {
                "dynamodb.throughput.read.percent": "0.5",
                "dynamodb.throughput.write.percent": "0.5"
            }
        }
    """

    def test(self) -> bool:
        """Test DynamoDB connection."""
        try:
            # Would need AWS SDK to properly test
            required = ["region"]
            return all(k in self.config for k in required)
        except Exception as e:
            self.logger.error(f"DynamoDB connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get DynamoDB connection options."""
        options = {
            "region": self.config["region"]
        }

        if "aws_access_key_id" in self.config:
            options["accessKeyId"] = self.config["aws_access_key_id"]
            options["secretAccessKey"] = self.config["aws_secret_access_key"]

        options.update(self.config.get("properties", {}))

        return options

    def read_table(self, table: str) -> DataFrame:
        """Read table from DynamoDB."""
        reader = self.spark.read.format("dynamodb")

        for key, value in self.get_connection().items():
            reader = reader.option(key, value)

        reader = reader.option("tableName", table)

        self.emit_lineage("read", {"table": table})

        return reader.load()

    def write_table(self, df: DataFrame, table: str, mode: str = "append") -> None:
        """Write DataFrame to DynamoDB table."""
        writer = df.write.format("dynamodb")

        for key, value in self.get_connection().items():
            writer = writer.option(key, value)

        writer.option("tableName", table).mode(mode).save()

        self.emit_lineage("write", {"table": table, "mode": mode})


@register_connection("cosmosdb")
@register_connection("azure_cosmosdb")
class CosmosDBConnection(SparkleConnection):
    """
    Azure Cosmos DB connection.

    Example config:
        {
            "endpoint": "https://myaccount.documents.azure.com:443/",
            "master_key": "${COSMOS_MASTER_KEY}",
            "database": "analytics",
            "preferred_regions": "East US,West US"
        }
    """

    def test(self) -> bool:
        """Test Cosmos DB connection."""
        try:
            required = ["endpoint", "master_key", "database"]
            return all(k in self.config for k in required)
        except Exception as e:
            self.logger.error(f"Cosmos DB connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Cosmos DB connection options."""
        options = {
            "spark.cosmos.accountEndpoint": self.config["endpoint"],
            "spark.cosmos.accountKey": self.config["master_key"],
            "spark.cosmos.database": self.config["database"]
        }

        if "preferred_regions" in self.config:
            options["spark.cosmos.preferredRegions"] = self.config["preferred_regions"]

        options.update(self.config.get("properties", {}))

        return options

    def read_container(self, container: str) -> DataFrame:
        """Read container from Cosmos DB."""
        reader = self.spark.read.format("cosmos.oltp")

        for key, value in self.get_connection().items():
            reader = reader.option(key, value)

        reader = reader.option("spark.cosmos.container", container)

        self.emit_lineage("read", {"container": container})

        return reader.load()

    def write_container(self, df: DataFrame, container: str, mode: str = "append") -> None:
        """Write DataFrame to Cosmos DB container."""
        writer = df.write.format("cosmos.oltp")

        for key, value in self.get_connection().items():
            writer = writer.option(key, value)

        writer.option("spark.cosmos.container", container).mode(mode).save()

        self.emit_lineage("write", {"container": container, "mode": mode})


@register_connection("elasticsearch")
@register_connection("elastic")
class ElasticsearchConnection(SparkleConnection):
    """
    Elasticsearch connection.

    Example config:
        {
            "nodes": "es-1.example.com,es-2.example.com,es-3.example.com",
            "port": "9200",
            "protocol": "https",
            "username": "${ES_USER}",
            "password": "${ES_PASSWORD}",
            "properties": {
                "es.nodes.wan.only": "true",
                "es.batch.size.entries": "10000"
            }
        }
    """

    def test(self) -> bool:
        """Test Elasticsearch connection."""
        try:
            required = ["nodes"]
            return all(k in self.config for k in required)
        except Exception as e:
            self.logger.error(f"Elasticsearch connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Elasticsearch connection options."""
        options = {
            "es.nodes": self.config["nodes"],
            "es.port": str(self.config.get("port", "9200"))
        }

        if "protocol" in self.config:
            options["es.net.ssl"] = "true" if self.config["protocol"] == "https" else "false"

        if "username" in self.config:
            options["es.net.http.auth.user"] = self.config["username"]
            options["es.net.http.auth.pass"] = self.config["password"]

        options.update(self.config.get("properties", {}))

        return options

    def read_index(self, index: str, query: Optional[str] = None) -> DataFrame:
        """
        Read index from Elasticsearch.

        Args:
            index: Index name
            query: Optional query DSL

        Returns:
            DataFrame
        """
        reader = self.spark.read.format("es")

        for key, value in self.get_connection().items():
            reader = reader.option(key, value)

        if query:
            reader = reader.option("es.query", query)

        self.emit_lineage("read", {"index": index})

        return reader.load(index)

    def write_index(self, df: DataFrame, index: str, mode: str = "append") -> None:
        """Write DataFrame to Elasticsearch index."""
        writer = df.write.format("es")

        for key, value in self.get_connection().items():
            writer = writer.option(key, value)

        writer.mode(mode).save(index)

        self.emit_lineage("write", {"index": index, "mode": mode})


@register_connection("hbase")
class HBaseConnection(SparkleConnection):
    """
    Apache HBase connection.

    Example config:
        {
            "zookeeper_quorum": "zk-1:2181,zk-2:2181,zk-3:2181",
            "zookeeper_parent": "/hbase",
            "properties": {
                "hbase.rpc.timeout": "60000"
            }
        }
    """

    def test(self) -> bool:
        """Test HBase connection."""
        try:
            required = ["zookeeper_quorum"]
            return all(k in self.config for k in required)
        except Exception as e:
            self.logger.error(f"HBase connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get HBase connection options."""
        return {
            "hbase.zookeeper.quorum": self.config["zookeeper_quorum"],
            "zookeeper.znode.parent": self.config.get("zookeeper_parent", "/hbase")
        }


@register_connection("couchbase")
class CouchbaseConnection(SparkleConnection):
    """
    Couchbase connection.

    Example config:
        {
            "nodes": "cb-1.example.com,cb-2.example.com",
            "bucket": "analytics",
            "username": "${COUCHBASE_USER}",
            "password": "${COUCHBASE_PASSWORD}"
        }
    """

    def test(self) -> bool:
        """Test Couchbase connection."""
        try:
            required = ["nodes", "bucket"]
            return all(k in self.config for k in required)
        except Exception as e:
            self.logger.error(f"Couchbase connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Couchbase connection options."""
        return {
            "spark.couchbase.nodes": self.config["nodes"],
            "spark.couchbase.bucket": self.config["bucket"],
            "spark.couchbase.username": self.config.get("username", ""),
            "spark.couchbase.password": self.config.get("password", "")
        }


@register_connection("neo4j")
class Neo4jConnection(SparkleConnection):
    """
    Neo4j graph database connection.

    Example config:
        {
            "url": "neo4j://neo4j.example.com:7687",
            "username": "${NEO4J_USER}",
            "password": "${NEO4J_PASSWORD}",
            "database": "neo4j"
        }
    """

    def test(self) -> bool:
        """Test Neo4j connection."""
        try:
            required = ["url", "username", "password"]
            return all(k in self.config for k in required)
        except Exception as e:
            self.logger.error(f"Neo4j connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Neo4j connection options."""
        return {
            "url": self.config["url"],
            "authentication.basic.username": self.config["username"],
            "authentication.basic.password": self.config["password"],
            "database": self.config.get("database", "neo4j")
        }

    def read_query(self, query: str) -> DataFrame:
        """Execute Cypher query and return DataFrame."""
        reader = self.spark.read.format("org.neo4j.spark.DataSource")

        for key, value in self.get_connection().items():
            reader = reader.option(key, value)

        reader = reader.option("query", query)

        self.emit_lineage("read", {"query": query})

        return reader.load()


@register_connection("redis")
class RedisConnection(SparkleConnection):
    """
    Redis connection.

    Example config:
        {
            "host": "redis.example.com",
            "port": 6379,
            "auth": "${REDIS_PASSWORD}",
            "db": 0
        }
    """

    def test(self) -> bool:
        """Test Redis connection."""
        try:
            required = ["host"]
            return all(k in self.config for k in required)
        except Exception as e:
            self.logger.error(f"Redis connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Redis connection options."""
        options = {
            "host": self.config["host"],
            "port": str(self.config.get("port", 6379)),
            "db": str(self.config.get("db", 0))
        }

        if "auth" in self.config:
            options["auth"] = self.config["auth"]

        return options


@register_connection("scylladb")
class ScyllaDBConnection(CassandraConnection):
    """
    ScyllaDB connection (uses Cassandra connector).

    Example config:
        {
            "host": "scylla-1.example.com,scylla-2.example.com",
            "port": 9042,
            "keyspace": "analytics",
            "username": "${SCYLLA_USER}",
            "password": "${SCYLLA_PASSWORD}"
        }
    """
    pass
