"""
Sparkle Connections Layer

82+ production-ready, config-driven connection factories for:
- Cloud storage (S3, ADLS, GCS, HDFS, Databricks Volumes, Cloudflare R2, Wasabi, Backblaze B2)
- Lakehouse formats (Delta Lake, Iceberg, Hudi)
- JDBC databases (PostgreSQL, MySQL, Oracle, SQL Server, Teradata, DB2, SAP HANA, etc.)
- Data warehouses (Snowflake, Redshift, BigQuery, Synapse, Athena, Dremio, Druid, Pinot)
- NoSQL (MongoDB, Cassandra, DynamoDB, CosmosDB, Elasticsearch, Neo4j, Redis, Couchbase)
- SaaS platforms (Salesforce, Netsuite, Workday, ServiceNow, HubSpot, Shopify, Stripe, etc.)
- Streaming (Kafka, Kinesis, Event Hubs, Pub/Sub, Pulsar, RabbitMQ, MQTT, NATS)
- REST & GraphQL APIs (Generic REST, GraphQL with OAuth2)
- Mainframe/Legacy (IBM i, EBCDIC/VSAM, SAP ODP, SAP Datasphere, Oracle EBS, PeopleSoft)
- Industry protocols (HL7/FHIR, X12 EDI, FIX, SWIFT, AWS HealthLake)
- Catalogs (Unity Catalog, Hive, Glue, Purview)
- ML platforms (MLflow, SageMaker, Vertex AI, Azure ML)

Usage:
    >>> from sparkle.connections import Connection
    >>> conn = Connection.get("postgres", spark, env="prod")
    >>> df = conn.read(table="customers")

    >>> from sparkle.connections import ConnectionFactory
    >>> available = ConnectionFactory.list_available()
    >>> print(available)
"""

# Core infrastructure
from .base import (
    SparkleConnection,
    JDBCConnection,
    CloudStorageConnection,
    StreamingConnection,
    APIConnection
)

from .factory import (
    ConnectionFactory,
    Connection,
    get_connection,
    register_connection
)

from .config_loader import (
    ConfigLoader,
    load_connection_config
)

# JDBC connections
from .jdbc_connections import (
    PostgreSQLConnection,
    MySQLConnection,
    MariaDBConnection,
    OracleConnection,
    SQLServerConnection,
    DB2Connection,
    TeradataConnection,
    SAPHANAConnection,
    VerticaConnection,
    GreenplumConnection,
    NetezzaConnection,
    SybaseConnection,
    SQLiteConnection,
    PrestoConnection,
    TrinoConnection,
    ClickHouseConnection
)

# Cloud storage
from .cloud_storage import (
    S3Connection,
    ADLSGen2Connection,
    GCSConnection,
    HDFSConnection,
    LocalFileSystemConnection,
    S3CompatibleConnection,
    DatabricksVolumesConnection,
    CloudflareR2Connection,
    WasabiConnection,
    BackblazeB2Connection
)

# Streaming
from .streaming import (
    KafkaConnection,
    KinesisConnection,
    EventHubConnection,
    PubSubConnection,
    PulsarConnection,
    RabbitMQConnection,
    SQSConnection,
    ServiceBusConnection,
    MQTTConnection,
    NATSJetStreamConnection
)

# Data warehouses
from .data_warehouses import (
    SnowflakeConnection,
    RedshiftConnection,
    BigQueryConnection,
    SynapseConnection,
    DatabricksSQLConnection,
    AthenaConnection,
    DremioConnection,
    DruidConnection,
    PinotConnection
)

# NoSQL
from .nosql import (
    MongoDBConnection,
    CassandraConnection,
    DynamoDBConnection,
    CosmosDBConnection,
    ElasticsearchConnection,
    HBaseConnection,
    CouchbaseConnection,
    Neo4jConnection,
    RedisConnection,
    ScyllaDBConnection
)

# APIs
from .api import (
    RESTAPIConnection,
    GraphQLConnection,
    SOAPConnection,
    gRPCConnection,
    WebhookConnection,
    HTTPConnection
)

# Catalogs
from .catalogs import (
    UnityCatalogConnection,
    HiveMetastoreConnection,
    GlueCatalogConnection,
    PurviewConnection,
    DataHubConnection,
    AtlasConnection,
    AmundsenConnection
)

# ML platforms
from .ml_platforms import (
    MLflowConnection,
    SageMakerConnection,
    VertexAIConnection,
    AzureMLConnection,
    FeatureStoreConnection,
    FeastConnection,
    TectonConnection
)

# File formats
from .file_formats import (
    DeltaLakeConnection,
    IcebergConnection,
    HudiConnection,
    ParquetConnection
)

# Specialized
from .specialized import (
    SFTPConnection,
    FTPConnection,
    SMTPConnection,
    LDAPConnection,
    SalesforceConnection,
    SAPConnection,
    JiraConnection,
    SlackConnection
)

# SaaS Platforms
from .saas_platforms import (
    SalesforceCDCConnection,
    NetsuiteConnection,
    WorkdayPrismConnection,
    ServiceNowConnection,
    MarketoConnection,
    HubSpotConnection,
    ZendeskConnection,
    ShopifyConnection,
    StripeConnection,
    ZuoraConnection,
    GoogleAnalytics4Connection,
    GoogleAdsConnection,
    FacebookAdsConnection,
    LinkedInAdsConnection,
    AmplitudeConnection,
    SegmentConnection
)

# Mainframe & Legacy Systems
from .mainframe_legacy import (
    IBMiConnection,
    MainframeEBCDICConnection,
    SAPODPConnection,
    SAPDatasphereConnection,
    OracleEBSConnection,
    PeopleSoftConnection
)

# Industry-Specific Protocols
from .industry_protocols import (
    FHIRConnection,
    X12EDIConnection,
    FIXProtocolConnection,
    SWIFTConnection,
    AWSHealthLakeConnection
)


__all__ = [
    # Core
    "SparkleConnection",
    "JDBCConnection",
    "CloudStorageConnection",
    "StreamingConnection",
    "APIConnection",
    "ConnectionFactory",
    "Connection",
    "get_connection",
    "register_connection",
    "ConfigLoader",
    "load_connection_config",

    # JDBC
    "PostgreSQLConnection",
    "MySQLConnection",
    "MariaDBConnection",
    "OracleConnection",
    "SQLServerConnection",
    "DB2Connection",
    "TeradataConnection",
    "SAPHANAConnection",
    "VerticaConnection",
    "GreenplumConnection",
    "NetezzaConnection",
    "SybaseConnection",
    "SQLiteConnection",
    "PrestoConnection",
    "TrinoConnection",
    "ClickHouseConnection",

    # Cloud Storage
    "S3Connection",
    "ADLSGen2Connection",
    "GCSConnection",
    "HDFSConnection",
    "LocalFileSystemConnection",
    "S3CompatibleConnection",
    "DatabricksVolumesConnection",
    "CloudflareR2Connection",
    "WasabiConnection",
    "BackblazeB2Connection",

    # Streaming
    "KafkaConnection",
    "KinesisConnection",
    "EventHubConnection",
    "PubSubConnection",
    "PulsarConnection",
    "RabbitMQConnection",
    "SQSConnection",
    "ServiceBusConnection",
    "MQTTConnection",
    "NATSJetStreamConnection",

    # Data Warehouses
    "SnowflakeConnection",
    "RedshiftConnection",
    "BigQueryConnection",
    "SynapseConnection",
    "DatabricksSQLConnection",
    "AthenaConnection",
    "DremioConnection",
    "DruidConnection",
    "PinotConnection",

    # NoSQL
    "MongoDBConnection",
    "CassandraConnection",
    "DynamoDBConnection",
    "CosmosDBConnection",
    "ElasticsearchConnection",
    "HBaseConnection",
    "CouchbaseConnection",
    "Neo4jConnection",
    "RedisConnection",
    "ScyllaDBConnection",

    # APIs
    "RESTAPIConnection",
    "GraphQLConnection",
    "SOAPConnection",
    "gRPCConnection",
    "WebhookConnection",
    "HTTPConnection",

    # Catalogs
    "UnityCatalogConnection",
    "HiveMetastoreConnection",
    "GlueCatalogConnection",
    "PurviewConnection",
    "DataHubConnection",
    "AtlasConnection",
    "AmundsenConnection",

    # ML Platforms
    "MLflowConnection",
    "SageMakerConnection",
    "VertexAIConnection",
    "AzureMLConnection",
    "FeatureStoreConnection",
    "FeastConnection",
    "TectonConnection",

    # File Formats
    "DeltaLakeConnection",
    "IcebergConnection",
    "HudiConnection",
    "ParquetConnection",

    # Specialized
    "SFTPConnection",
    "FTPConnection",
    "SMTPConnection",
    "LDAPConnection",
    "SalesforceConnection",
    "SAPConnection",
    "JiraConnection",
    "SlackConnection",

    # SaaS Platforms
    "SalesforceCDCConnection",
    "NetsuiteConnection",
    "WorkdayPrismConnection",
    "ServiceNowConnection",
    "MarketoConnection",
    "HubSpotConnection",
    "ZendeskConnection",
    "ShopifyConnection",
    "StripeConnection",
    "ZuoraConnection",
    "GoogleAnalytics4Connection",
    "GoogleAdsConnection",
    "FacebookAdsConnection",
    "LinkedInAdsConnection",
    "AmplitudeConnection",
    "SegmentConnection",

    # Mainframe & Legacy
    "IBMiConnection",
    "MainframeEBCDICConnection",
    "SAPODPConnection",
    "SAPDatasphereConnection",
    "OracleEBSConnection",
    "PeopleSoftConnection",

    # Industry Protocols
    "FHIRConnection",
    "X12EDIConnection",
    "FIXProtocolConnection",
    "SWIFTConnection",
    "AWSHealthLakeConnection",
]


# Version
__version__ = "1.0.0"


def list_connections():
    """
    List all available connection types.

    Returns:
        List of connection names

    Example:
        >>> from sparkle.connections import list_connections
        >>> connections = list_connections()
        >>> print(f"Available: {len(connections)} connection types")
    """
    return ConnectionFactory.list_available()


def get_connection_help(name: str) -> str:
    """
    Get help for a specific connection type.

    Args:
        name: Connection name

    Returns:
        Connection docstring

    Example:
        >>> from sparkle.connections import get_connection_help
        >>> help_text = get_connection_help("postgres")
        >>> print(help_text)
    """
    try:
        conn_class = ConnectionFactory.get_connection_class(name)
        return conn_class.__doc__ or "No documentation available"
    except ValueError:
        return f"Connection '{name}' not found"
