# Sparkle Connections Layer

**78+ Production-Ready, Config-Driven Connection Factories**

The Sparkle connections layer provides a unified, enterprise-grade interface to connect Spark to any data source, data warehouse, streaming platform, API, catalog, or ML platform.

## Philosophy

1. **100% Config-Driven** - Zero hard-coded credentials, endpoints, or connection strings
2. **Uniform Interface** - Same pattern for all 78+ connection types
3. **Environment Aware** - Dev/QA/Prod configs with overlays
4. **Secret Management** - `${ENV_VAR}` interpolation from environment
5. **Lineage Ready** - OpenLineage event emission built-in
6. **Health Checks** - Every connection has `.test()` method
7. **Context Management** - Use with `with` statement for automatic cleanup

## Quick Start

```python
from pyspark.sql import SparkSession
from sparkle.connections import get_connection

spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Load connection from config/connections/postgres/prod.json
conn = get_connection("postgres", spark, env="prod")

# Test connectivity
assert conn.test(), "Connection failed"

# Read data with automatic partitioning
df = conn.read_table(
    "public.customers",
    partition_column="customer_id",
    lower_bound=0,
    upper_bound=1000000,
    num_partitions=20
)

# Write data
conn.write_table(df, "public.customers_gold", mode="overwrite")

# Cleanup
conn.close()
```

## Connection Types

### JDBC Databases (16)
- **PostgreSQL** (`postgresql`, `postgres`) - Full JDBC support with partitioning
- **MySQL** (`mysql`) - MySQL 5.7+ and MySQL 8.0+
- **MariaDB** (`mariadb`) - MariaDB 10.x
- **Oracle** (`oracle`) - Oracle Database 11g+
- **SQL Server** (`sqlserver`, `mssql`) - Microsoft SQL Server 2012+
- **DB2** (`db2`) - IBM DB2
- **Teradata** (`teradata`) - Teradata Database
- **SAP HANA** (`saphana`, `hana`) - SAP HANA
- **Vertica** (`vertica`) - Vertica Analytics
- **Greenplum** (`greenplum`) - Greenplum Database
- **Netezza** (`netezza`) - IBM Netezza
- **Sybase** (`sybase`) - Sybase ASE
- **SQLite** (`sqlite`) - SQLite for testing
- **Presto** (`presto`) - Presto SQL
- **Trino** (`trino`) - Trino (formerly PrestoSQL)
- **ClickHouse** (`clickhouse`) - ClickHouse OLAP

### Cloud Storage (7)
- **Amazon S3** (`s3`, `aws_s3`) - S3 with IAM or access keys
- **Azure Data Lake Gen2** (`adls`, `adls_gen2`) - ADLS with service principal or managed identity
- **Google Cloud Storage** (`gcs`) - GCS with service accounts
- **HDFS** (`hdfs`) - Hadoop Distributed File System
- **Local Filesystem** (`local`, `file`) - For dev/testing
- **S3-Compatible** (`s3_compatible`, `minio`) - MinIO, Ceph, etc.

### Streaming (8)
- **Apache Kafka** (`kafka`) - Kafka with SASL/SSL support
- **AWS Kinesis** (`kinesis`, `aws_kinesis`) - Kinesis Data Streams
- **Azure Event Hubs** (`eventhub`, `azure_eventhub`) - Event Hubs
- **Google Pub/Sub** (`pubsub`, `google_pubsub`) - Cloud Pub/Sub
- **Apache Pulsar** (`pulsar`) - Pulsar messaging
- **RabbitMQ** (`rabbitmq`) - RabbitMQ message queue
- **Amazon SQS** (`sqs`, `aws_sqs`) - SQS queue
- **Azure Service Bus** (`servicebus`, `azure_servicebus`) - Service Bus

### Data Warehouses (5)
- **Snowflake** (`snowflake`) - Snowflake via Spark connector
- **AWS Redshift** (`redshift`, `aws_redshift`) - Redshift with S3 staging
- **Google BigQuery** (`bigquery`, `gcp_bigquery`) - BigQuery
- **Azure Synapse** (`synapse`, `azure_synapse`) - Synapse Analytics
- **Databricks SQL** (`databricks_sql`, `databricks_warehouse`) - Databricks SQL Warehouse

### NoSQL Databases (10)
- **MongoDB** (`mongodb`, `mongo`) - MongoDB with replica sets
- **Apache Cassandra** (`cassandra`) - Cassandra/DSE
- **AWS DynamoDB** (`dynamodb`, `aws_dynamodb`) - DynamoDB
- **Azure Cosmos DB** (`cosmosdb`, `azure_cosmosdb`) - Cosmos DB
- **Elasticsearch** (`elasticsearch`, `elastic`) - Elasticsearch/OpenSearch
- **Apache HBase** (`hbase`) - HBase on Hadoop
- **Couchbase** (`couchbase`) - Couchbase Server
- **Neo4j** (`neo4j`) - Neo4j graph database
- **Redis** (`redis`) - Redis key-value store
- **ScyllaDB** (`scylladb`) - ScyllaDB

### APIs (6)
- **REST API** (`rest`, `rest_api`) - Generic REST with OAuth/Bearer/API Key
- **GraphQL** (`graphql`) - GraphQL with variables
- **SOAP** (`soap`) - SOAP web services
- **gRPC** (`grpc`) - gRPC services
- **Webhook** (`webhook`) - Webhook sender
- **HTTP** (`http`) - Generic HTTP client

### Data Catalogs (7)
- **Unity Catalog** (`unity_catalog`, `uc`) - Databricks Unity Catalog
- **Hive Metastore** (`hive_metastore`, `hive`) - Hive Metastore
- **AWS Glue** (`glue`, `aws_glue`) - Glue Data Catalog
- **Azure Purview** (`purview`, `azure_purview`) - Purview
- **DataHub** (`datahub`) - LinkedIn DataHub
- **Apache Atlas** (`atlas`, `apache_atlas`) - Atlas
- **Amundsen** (`amundsen`) - Lyft Amundsen

### ML Platforms (7)
- **MLflow** (`mlflow`) - MLflow tracking and registry
- **AWS SageMaker** (`sagemaker`, `aws_sagemaker`) - SageMaker
- **Google Vertex AI** (`vertex_ai`, `gcp_vertex_ai`) - Vertex AI
- **Azure ML** (`azure_ml`, `azureml`) - Azure Machine Learning
- **Feature Store** (`feature_store`, `databricks_feature_store`) - Databricks Feature Store
- **Feast** (`feast`) - Feast feature store
- **Tecton** (`tecton`) - Tecton feature platform

### File Formats (4)
- **Delta Lake** (`delta`, `delta_lake`) - Delta with time travel
- **Apache Iceberg** (`iceberg`, `apache_iceberg`) - Iceberg tables
- **Apache Hudi** (`hudi`, `apache_hudi`) - Hudi tables
- **Parquet** (`parquet`) - Parquet files

### Specialized (8)
- **SFTP** (`sftp`) - Secure file transfer
- **FTP** (`ftp`) - FTP client
- **SMTP** (`smtp`, `email`) - Email sender
- **LDAP** (`ldap`) - LDAP directory
- **Salesforce** (`salesforce`) - Salesforce Bulk API
- **SAP** (`sap`) - SAP ERP connector
- **Jira** (`jira`) - Jira REST API
- **Slack** (`slack`) - Slack notifications

## Configuration

### Directory Structure

```
config/connections/
├── postgres/
│   ├── common.json       # Shared across envs
│   ├── dev.json          # Dev-specific
│   ├── qa.json           # QA-specific
│   └── prod.json         # Prod-specific
├── s3/
│   └── prod.json
└── kafka/
    └── prod.json
```

### Example: PostgreSQL

**config/connections/postgres/common.json**
```json
{
  "driver": "org.postgresql.Driver",
  "properties": {
    "fetchsize": "10000",
    "ssl": "true"
  }
}
```

**config/connections/postgres/prod.json**
```json
{
  "url": "jdbc:postgresql://prod-db:5432/analytics",
  "user": "${POSTGRES_USER}",
  "password": "${POSTGRES_PASSWORD}"
}
```

## Advanced Usage

### Context Manager

```python
with get_connection("postgres", spark, env="prod") as conn:
    df = conn.read_table("customers")
    # Automatic cleanup on exit
```

### Override Config

```python
# Skip config files, provide config directly
config = {
    "url": "jdbc:postgresql://localhost:5432/test",
    "user": "test",
    "password": "test"
}
conn = get_connection("postgres", spark, config=config)
```

### List Available Connections

```python
from sparkle.connections import ConnectionFactory

available = ConnectionFactory.list_available()
print(f"Available: {len(available)} connection types")
# Available: 78 connection types
```

### Custom Connection

```python
from sparkle.connections import register_connection, JDBCConnection

@register_connection("mydb")
class MyDatabaseConnection(JDBCConnection):
    def __init__(self, spark, config, env="dev", **kwargs):
        if "driver" not in config:
            config["driver"] = "com.mydb.Driver"
        super().__init__(spark, config, env, **kwargs)
```

## Architecture

```
SparkleConnection (base)
├── JDBCConnection
│   ├── PostgreSQLConnection
│   ├── MySQLConnection
│   └── ... (14 more)
├── CloudStorageConnection
│   ├── S3Connection
│   ├── ADLSGen2Connection
│   └── ... (5 more)
├── StreamingConnection
│   ├── KafkaConnection
│   └── ... (7 more)
└── APIConnection
    ├── RESTAPIConnection
    └── ... (5 more)
```

## Features

### Automatic Health Checks

Every connection implements `.test()`:

```python
conn = get_connection("postgres", spark, env="prod")
if conn.test():
    print("✓ Connection healthy")
else:
    print("✗ Connection failed")
```

### Lineage Tracking

Built-in OpenLineage event emission:

```python
conn.read_table("customers")
# Emits lineage event:
# {
#   "operation": "read",
#   "connection_type": "PostgreSQLConnection",
#   "metadata": {"table": "customers", ...}
# }
```

### Connection Pooling

JDBC connections use Spark's built-in connection pooling.

### Parallel Reads

```python
# 20 parallel Spark tasks reading from PostgreSQL
df = conn.read_table(
    "customers",
    partition_column="id",
    lower_bound=0,
    upper_bound=1000000,
    num_partitions=20
)
```

## Best Practices

1. **Always use environment variables for secrets**
   ```json
   {"password": "${DB_PASSWORD}"}
   ```

2. **Test connections before use**
   ```python
   assert conn.test(), "Connection failed"
   ```

3. **Use context managers for cleanup**
   ```python
   with get_connection(...) as conn:
       ...
   ```

4. **Partition large tables**
   ```python
   conn.read_table(..., partition_column="id", num_partitions=20)
   ```

5. **Enable connection pooling**
   ```json
   {"properties": {"maxPoolSize": "10"}}
   ```

## Dependencies

Core dependencies (included with Spark 3.5+):
- PySpark 3.5+
- Delta Lake 3.x (for Delta connections)

Optional dependencies (install as needed):
```bash
# JDBC drivers
pip install psycopg2-binary  # PostgreSQL Python client

# Cloud SDKs
pip install boto3             # AWS
pip install azure-storage-blob # Azure
pip install google-cloud-storage # GCP

# NoSQL
pip install pymongo           # MongoDB
pip install cassandra-driver  # Cassandra

# APIs
pip install requests graphql-core

# ML
pip install mlflow sagemaker databricks-feature-store
```

## Performance

- **Connection reuse**: Connections are cached per SparkSession
- **Parallel reads**: Automatic partitioning for JDBC sources
- **Pushdown predicates**: WHERE clauses pushed to source
- **Batch writes**: Configurable batch sizes

## Troubleshooting

### Connection timeouts

Increase timeout in config:
```json
{"properties": {"connectTimeout": "60000"}}
```

### Missing JDBC driver

Add JAR to Spark:
```bash
spark-submit --jars postgresql-42.6.0.jar ...
```

### Environment variable not found

Check `.env` file or export:
```bash
export POSTGRES_PASSWORD=secret123
```

## Version

Sparkle Connections v1.0.0

## License

Apache 2.0
