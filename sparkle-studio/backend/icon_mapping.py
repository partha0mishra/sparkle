"""
Icon/Logo mapping for Sparkle components.

Maps component names to their corresponding logo icons using simple-icons naming convention.
These can be rendered using:
- Simple Icons library (simpleicons.org)
- SVG files from CDN (e.g., cdn.simpleicons.org)
- Custom icon components

For connections not in simple-icons, we use generic category icons.
"""

from typing import Dict

# Connection icon mappings
# Format: connection_name -> icon_name (from simple-icons or lucide-react)
CONNECTION_ICONS: Dict[str, str] = {
    # ========================================
    # Data Warehouses
    # ========================================
    "snowflake": "snowflake",  # ✅ Simple Icons
    "redshift": "database",  # Lucide: Database (AWS icons not in Simple Icons)
    "aws_redshift": "database",
    "bigquery": "googlebigquery",  # ✅ Simple Icons
    "google_bigquery": "googlebigquery",
    "synapse": "database",  # Lucide: Database (Microsoft icons not in Simple Icons)
    "azure_synapse": "database",
    "databricks_sql": "databricks",  # ✅ Simple Icons
    "databricks": "databricks",
    "athena": "database",  # Lucide: Database (AWS icons not in Simple Icons)
    "aws_athena": "database",
    "dremio": "database",  # Lucide: Database (no Simple Icon)
    "druid": "apachedruid",  # ✅ Simple Icons
    "apache_druid": "apachedruid",
    "pinot": "database",  # Lucide: Database (no Simple Icon)
    "apache_pinot": "database",
    "delta_lake": "delta",  # ✅ Simple Icons
    "delta": "delta",
    "iceberg": "database",  # Lucide: Database (apacheiceberg not in Simple Icons)
    "apache_iceberg": "database",
    "hudi": "database",  # Lucide: Database (apachehudi not in Simple Icons)
    "apache_hudi": "database",

    # ========================================
    # Relational Databases (JDBC)
    # ========================================
    "postgresql": "postgresql",  # ✅ Simple Icons
    "postgres": "postgresql",
    "mysql": "mysql",  # ✅ Simple Icons
    "mariadb": "mariadb",  # ✅ Simple Icons
    "oracle": "database",  # Lucide: Database (oracle not in Simple Icons)
    "sqlserver": "database",  # Lucide: Database (Microsoft icons not in Simple Icons)
    "mssql": "database",
    "db2": "database",  # Lucide: Database (IBM icons not in Simple Icons)
    "ibm_db2": "database",
    "teradata": "teradata",  # ✅ Simple Icons
    "saphana": "sap",  # ✅ Simple Icons
    "hana": "sap",
    "vertica": "database",  # Lucide: Database
    "greenplum": "database",
    "netezza": "database",  # Lucide: Database (IBM icons not in Simple Icons)
    "sybase": "database",
    "sqlite": "sqlite",  # ✅ Simple Icons
    "presto": "presto",  # ✅ Simple Icons
    "trino": "trino",  # ✅ Simple Icons
    "clickhouse": "clickhouse",  # ✅ Simple Icons

    # ========================================
    # Cloud Object Storage
    # ========================================
    "s3": "cloud",  # Lucide: Cloud (AWS icons not in Simple Icons)
    "aws_s3": "cloud",
    "amazon_s3": "cloud",
    "adls": "cloud",  # Lucide: Cloud (Microsoft icons not in Simple Icons)
    "adls_gen2": "cloud",
    "azure_datalake": "cloud",
    "azure_data_lake": "cloud",
    "gcs": "googlecloud",  # ✅ Simple Icons
    "google_cloud_storage": "googlecloud",
    "hdfs": "apachehadoop",  # ✅ Simple Icons
    "hadoop": "apachehadoop",
    "local": "folder",  # Lucide: Folder
    "file": "folder",
    "s3_compatible": "cloud",  # Lucide: Cloud
    "minio": "minio",  # ✅ Simple Icons
    "databricks_volumes": "databricks",  # ✅ Simple Icons
    "dbfs": "databricks",
    "unity_volumes": "databricks",
    "cloudflare_r2": "cloudflare",  # ✅ Simple Icons
    "r2": "cloudflare",
    "wasabi": "cloud",  # Lucide: Cloud
    "backblaze_b2": "backblaze",  # ✅ Simple Icons
    "b2": "backblaze",

    # ========================================
    # NoSQL & Document Stores
    # ========================================
    "mongodb": "mongodb",  # ✅ Simple Icons
    "mongo": "mongodb",
    "cassandra": "apachecassandra",  # ✅ Simple Icons
    "apache_cassandra": "apachecassandra",
    "dynamodb": "database",  # Lucide: Database (AWS icons not in Simple Icons)
    "aws_dynamodb": "database",
    "cosmosdb": "database",  # Lucide: Database (Microsoft icons not in Simple Icons)
    "azure_cosmosdb": "database",
    "elasticsearch": "elasticsearch",  # ✅ Simple Icons
    "elastic": "elastic",  # ✅ Simple Icons
    "hbase": "apachehbase",  # ✅ Simple Icons
    "apache_hbase": "apachehbase",
    "couchbase": "couchbase",  # ✅ Simple Icons
    "neo4j": "neo4j",  # ✅ Simple Icons
    "redis": "redis",  # ✅ Simple Icons
    "scylladb": "database",  # Lucide: Database
    "arangodb": "arangodb",  # ✅ Simple Icons
    "couchdb": "apachecouchdb",

    # ========================================
    # Streaming & Messaging
    # ========================================
    "kafka": "apachekafka",
    "apache_kafka": "apachekafka",
    "kinesis": "cloud",  # Lucide: Cloud
    "aws_kinesis": "cloud",  # Lucide: Cloud
    "event_hubs": "cloud",  # Lucide: Cloud (Microsoft icons not in Simple Icons)
    "azure_event_hubs": "cloud",
    "pubsub": "googlecloud",  # ✅ Simple Icons
    "google_pubsub": "googlecloud",
    "pulsar": "apachepulsar",  # ✅ Simple Icons
    "apache_pulsar": "apachepulsar",
    "rabbitmq": "rabbitmq",  # ✅ Simple Icons
    "sqs": "cloud",  # Lucide: Cloud (AWS icons not in Simple Icons)
    "amazon_sqs": "cloud",
    "service_bus": "cloud",  # Lucide: Cloud (Microsoft icons not in Simple Icons)
    "azure_service_bus": "cloud",
    "mqtt": "mqtt",
    "jms": "java",
    "activemq": "apacheactivemq",
    "apache_activemq": "apacheactivemq",

    # ========================================
    # Data Catalogs & Metadata
    # ========================================
    "unity_catalog": "databricks",
    "uc": "databricks",
    "hive_metastore": "apachehive",
    "hive": "apachehive",
    "glue": "cloud",  # Lucide: Cloud
    "aws_glue": "cloud",  # Lucide: Cloud
    "purview": "database",  # Lucide: Database (Microsoft icons not in Simple Icons)
    "azure_purview": "database",
    "datahub": "database",
    "apache_atlas": "apache",
    "amundsen": "database",

    # ========================================
    # APIs (use Lucide icons for generic protocols)
    # ========================================
    "rest": "api",  # Lucide: Globe
    "rest_api": "api",  # Lucide: Globe
    "graphql": "graphql",  # Simple Icons has GraphQL
    "soap": "soap",  # Lucide: Send
    "grpc": "grpc",  # Lucide: Send
    "webhook": "webhook",  # Lucide: Send
    "http": "http",  # Lucide: Globe
    "https": "https",  # Lucide: Globe

    # ========================================
    # SaaS Platforms
    # ========================================
    "salesforce": "salesforce",  # ✅ Simple Icons
    "servicenow": "plug",  # Lucide: Plug (servicenow not in Simple Icons)
    "okta": "okta",  # ✅ Simple Icons
    "hubspot": "hubspot",  # ✅ Simple Icons
    "stripe": "stripe",  # ✅ Simple Icons
    "twilio": "twilio",  # ✅ Simple Icons
    "segment": "plug",  # Lucide: Plug (segment not in Simple Icons)
    "amplitude": "plug",  # Lucide: Plug (amplitude not in Simple Icons)
    "mixpanel": "mixpanel",  # ✅ Simple Icons
    "intercom": "intercom",  # ✅ Simple Icons
    "zendesk": "zendesk",  # ✅ Simple Icons
    "jira": "jira",  # ✅ Simple Icons
    "confluence": "confluence",  # ✅ Simple Icons
    "slack": "slack",  # ✅ Simple Icons
    "microsoft_teams": "plug",  # Lucide: Plug (Microsoft icons not in Simple Icons)
    "teams": "plug",
    "google_analytics": "googleanalytics",
    "ga4": "googleanalytics",
    "adobe_analytics": "adobe",
    "marketo": "adobemarketo",
    "pardot": "salesforce",

    # ========================================
    # ML Platforms
    # ========================================
    "sagemaker": "cloud",  # Lucide: Cloud
    "aws_sagemaker": "cloud",  # Lucide: Cloud
    "vertex_ai": "googlecloud",  # ✅ Simple Icons
    "google_vertex_ai": "googlecloud",
    "azure_ml": "brain",  # Lucide: Brain (Microsoft icons not in Simple Icons)
    "mlflow": "mlflow",  # ✅ Simple Icons
    "wandb": "weightsandbiases",
    "weights_and_biases": "weightsandbiases",
    "comet": "comet",
    "neptune": "neptune",
    "huggingface": "huggingface",
    "openai": "openai",
    "anthropic": "anthropic",

    # ========================================
    # Industry Protocols
    # ========================================
    "hl7_fhir": "hl7",  # Lucide: Activity
    "fhir": "hl7",  # Lucide: Activity
    "x12_edi": "file-text",
    "edi_837": "file-text",
    "edi_835": "file-text",
    "fix_protocol": "trending-up",
    "fix": "trending-up",
    "swift": "bank",
    "swift_mt": "bank",
    "swift_mx": "bank",
    "aws_healthlake": "cloud",  # Lucide: Cloud
    "healthlake": "cloud",  # Lucide: Cloud

    # ========================================
    # Mainframe & Legacy
    # ========================================
    "mainframe": "server",
    "as400": "ibm",
    "ibm_as400": "ibm",
    "cobol": "file-code",
    "cics": "ibm",
    "ims": "ibm",
    "vsam": "database",
    "jcl": "file-code",
    "mq_series": "ibm",
    "ibm_mq": "ibm",
    "tuxedo": "oracle",
    "informix": "ibm",
    "adabas": "database",
    "natural": "database",
    "idms": "database",

    # ========================================
    # File Formats
    # ========================================
    "parquet": "file-spreadsheet",
    "csv": "file-spreadsheet",
    "json": "braces",
    "avro": "apache",
    "orc": "file-archive",
    "xml": "file-code",
    "yaml": "file-code",
    "protobuf": "buffer",
    "thrift": "apache",

    # ========================================
    # Lakehouse Formats
    # ========================================
    "delta_table": "delta",
    "iceberg_table": "apacheiceberg",
    "hudi_table": "apachehudi",
}

# Category default icons (fallback when specific icon not found)
CATEGORY_DEFAULT_ICONS: Dict[str, str] = {
    "connection": "plug",
    "ingestor": "download",
    "transformer": "zap",
    "ml": "brain",
    "sink": "upload",
    "orchestrator": "workflow",
}

# Icon provider URLs (for SVG/image rendering)
ICON_PROVIDERS = {
    "simple_icons_cdn": "https://cdn.simpleicons.org/{icon}",
    "simple_icons_svg": "https://unpkg.com/simple-icons@latest/icons/{icon}.svg",
    "lucide": "lucide-react",  # Use lucide-react package
}


def get_connection_icon(connection_name: str) -> str:
    """
    Get icon name for a connection.

    Args:
        connection_name: Name of the connection (e.g., 'postgres', 's3')

    Returns:
        Icon name (simple-icons or lucide-react icon name)
    """
    # Try exact match
    if connection_name in CONNECTION_ICONS:
        return CONNECTION_ICONS[connection_name]

    # Try lowercase
    name_lower = connection_name.lower()
    if name_lower in CONNECTION_ICONS:
        return CONNECTION_ICONS[name_lower]

    # Try removing common prefixes/suffixes
    for prefix in ["aws_", "azure_", "google_", "apache_", "ibm_"]:
        if name_lower.startswith(prefix):
            base_name = name_lower[len(prefix):]
            if base_name in CONNECTION_ICONS:
                return CONNECTION_ICONS[base_name]

    # Default to generic database icon
    return CATEGORY_DEFAULT_ICONS["connection"]


def get_component_icon(category: str, component_name: str) -> str:
    """
    Get icon for any component type.

    Args:
        category: Component category (connection, ingestor, transformer, ml, sink, orchestrator)
        component_name: Name of the component

    Returns:
        Icon name
    """
    if category == "connection":
        return get_connection_icon(component_name)
    else:
        return CATEGORY_DEFAULT_ICONS.get(category, "component")
