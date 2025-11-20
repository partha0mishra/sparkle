"""
Sparkle Ingestors Package

61+ production-grade, idempotent, watermark-tracked, schema-drift-aware ingestors.

Categories:
- File & Object Storage (9 ingestors)
- SaaS API Incremental (18 ingestors)
- Generic API Patterns (4 ingestors)
- Change Data Capture (12 ingestors)
- Streaming & Event Bus (8 ingestors)
- Database Incremental (7 ingestors)
- Specialized / Industry (4 ingestors)

Usage:
    >>> from sparkle.ingestors import Ingestor
    >>> ing = Ingestor.get("salesforce_incremental", spark, env="prod")
    >>> result = ing.run()

Features:
- 100% config-driven (no hard-coded values)
- Watermark tracking in system.ingestion_watermarks
- Audit logging in system.ingestion_audit_log
- Schema drift detection and evolution
- Idempotent execution
- Uses sparkle.connections layer
"""

# Core classes
from .base import (
    BaseIngestor,
    BaseBatchIngestor,
    BaseStreamingIngestor,
    BaseCdcIngestor
)

from .factory import (
    IngestorFactory,
    Ingestor,
    register_ingestor
)

from .config_loader import (
    load_ingestor_config,
    save_ingestor_config,
    list_ingestors,
    get_config_example
)

# File storage ingestors (9)
from .file_storage import (
    PartitionedParquetIngestion,
    DailyFileDropIngestion,
    IncrementalFileByModifiedTime,
    AppendOnlyJsonNdjsonIngestion,
    CopybookEbcdicMainframeIngestion,
    FixedWidthWithDremelIngestion,
    IncrementalAvroIngestion,
    DeltaTableAsSourceIngestion,
    IcebergTableAsSourceIngestion
)

# Database ingestors (7)
from .database import (
    JdbcIncrementalByDateColumn,
    JdbcIncrementalByIdRange,
    JdbcFullTableWithHashPartitioning,
    OracleFlashbackQueryIngestion,
    Db2CdcViaInfoSphere,
    TeradataFastExportParallel,
    SapOdpExtractor
)

# Streaming ingestors (8)
from .streaming import (
    KafkaTopicRawIngestion,
    KinesisDataStreamsIngestion,
    EventHubsKafkaProtocolIngestion,
    PubSubTopicIngestion,
    PulsarTopicIngestion,
    ConfluentSchemaRegistryAvroIngestion,
    RedpandaTopicIngestion,
    StructuredStreamingCheckpointedIngestion
)

# CDC ingestors (12)
from .cdc import (
    DebeziumKafkaCdcIngestion,
    KafkaConnectJdbcSourceIncremental,
    KafkaConnectMongoSourceCdc,
    PostgresLogicalReplicationDirect,
    OracleGoldenGateKafkaCdc,
    SqlServerCdcViaDebezium,
    MySqlBinlogDirect,
    SnowflakeStreamIngestion,
    BigQueryChangeStreamIngestion,
    DynamoDbStreamsToDelta,
    MongoDbChangeStreamsToDelta,
    PostgresCdcViaDebezium
)

# Generic API ingestors (4)
from .generic_api import (
    PaginatedRestJsonIngestion,
    PaginatedRestCsvIngestion,
    GraphQLPaginatedIngestion,
    WebhookToDeltaIngestion
)

# SaaS API ingestors (18)
from .saas_api import (
    SalesforceBulkApiFullExtractor,
    SalesforceBulkApiIncrementalExtractor,
    SalesforceCDCStreamer,
    NetsuiteSuiteAnalyticsIncremental,
    WorkdayPrismRaaSIncremental,
    MarketoBulkExtractIncremental,
    HubSpotIncrementalIngestion,
    ZendeskIncrementalIngestion,
    ShopifyAdminApiIncremental,
    StripeIncrementalEventsIngestion,
    ZuoraAquaIncremental,
    ServiceNowTableApiIncremental,
    GoogleAnalytics4BigQueryExportDaily,
    GoogleAdsReportIncremental,
    MetaAdsInsightsIncremental,
    LinkedInAdsIncremental,
    AmplitudeEventExportIncremental,
    SegmentWarehouseSync
)

# Specialized ingestors (4)
from .specialized import (
    Hl7FhirBundleIngestion,
    X12Edi837835Ingestion,
    FixProtocolLogIngestion,
    SwiftMtMxIso20022Ingestion
)


__all__ = [
    # Core
    "BaseIngestor",
    "BaseBatchIngestor",
    "BaseStreamingIngestor",
    "BaseCdcIngestor",
    "IngestorFactory",
    "Ingestor",
    "register_ingestor",
    "load_ingestor_config",
    "save_ingestor_config",
    "list_ingestors",
    "get_config_example",

    # File Storage (9)
    "PartitionedParquetIngestion",
    "DailyFileDropIngestion",
    "IncrementalFileByModifiedTime",
    "AppendOnlyJsonNdjsonIngestion",
    "CopybookEbcdicMainframeIngestion",
    "FixedWidthWithDremelIngestion",
    "IncrementalAvroIngestion",
    "DeltaTableAsSourceIngestion",
    "IcebergTableAsSourceIngestion",

    # Database (7)
    "JdbcIncrementalByDateColumn",
    "JdbcIncrementalByIdRange",
    "JdbcFullTableWithHashPartitioning",
    "OracleFlashbackQueryIngestion",
    "Db2CdcViaInfoSphere",
    "TeradataFastExportParallel",
    "SapOdpExtractor",

    # Streaming (8)
    "KafkaTopicRawIngestion",
    "KinesisDataStreamsIngestion",
    "EventHubsKafkaProtocolIngestion",
    "PubSubTopicIngestion",
    "PulsarTopicIngestion",
    "ConfluentSchemaRegistryAvroIngestion",
    "RedpandaTopicIngestion",
    "StructuredStreamingCheckpointedIngestion",

    # CDC (12)
    "DebeziumKafkaCdcIngestion",
    "KafkaConnectJdbcSourceIncremental",
    "KafkaConnectMongoSourceCdc",
    "PostgresLogicalReplicationDirect",
    "OracleGoldenGateKafkaCdc",
    "SqlServerCdcViaDebezium",
    "MySqlBinlogDirect",
    "SnowflakeStreamIngestion",
    "BigQueryChangeStreamIngestion",
    "DynamoDbStreamsToDelta",
    "MongoDbChangeStreamsToDelta",
    "PostgresCdcViaDebezium",

    # Generic API (4)
    "PaginatedRestJsonIngestion",
    "PaginatedRestCsvIngestion",
    "GraphQLPaginatedIngestion",
    "WebhookToDeltaIngestion",

    # SaaS API (18)
    "SalesforceBulkApiFullExtractor",
    "SalesforceBulkApiIncrementalExtractor",
    "SalesforceCDCStreamer",
    "NetsuiteSuiteAnalyticsIncremental",
    "WorkdayPrismRaaSIncremental",
    "MarketoBulkExtractIncremental",
    "HubSpotIncrementalIngestion",
    "ZendeskIncrementalIngestion",
    "ShopifyAdminApiIncremental",
    "StripeIncrementalEventsIngestion",
    "ZuoraAquaIncremental",
    "ServiceNowTableApiIncremental",
    "GoogleAnalytics4BigQueryExportDaily",
    "GoogleAdsReportIncremental",
    "MetaAdsInsightsIncremental",
    "LinkedInAdsIncremental",
    "AmplitudeEventExportIncremental",
    "SegmentWarehouseSync",

    # Specialized (4)
    "Hl7FhirBundleIngestion",
    "X12Edi837835Ingestion",
    "FixProtocolLogIngestion",
    "SwiftMtMxIso20022Ingestion",
]


# Version
__version__ = "1.0.0"


def list_all_ingestors():
    """
    List all available ingestor types.

    Returns:
        Sorted list of ingestor names

    Example:
        >>> from sparkle.ingestors import list_all_ingestors
        >>> ingestors = list_all_ingestors()
        >>> print(f"Available: {len(ingestors)} ingestor types")
    """
    return Ingestor.list()


def get_ingestor_help(name: str) -> str:
    """
    Get help for a specific ingestor type.

    Args:
        name: Ingestor name

    Returns:
        Ingestor docstring

    Example:
        >>> from sparkle.ingestors import get_ingestor_help
        >>> help_text = get_ingestor_help("salesforce_incremental")
        >>> print(help_text)
    """
    try:
        ingestor_class = IngestorFactory.get_ingestor_class(name)
        return ingestor_class.__doc__ or "No documentation available"
    except ValueError:
        return f"Ingestor '{name}' not found"
