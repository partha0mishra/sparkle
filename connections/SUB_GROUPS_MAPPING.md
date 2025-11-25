# Sub-Groups Mapping for Sparkle Connections

This document provides the mapping of all 82+ connections to their sub-groups for UI organization.

## How to Add Sub-Groups

Add this line to the docstring of each connection class, right after the main description:

```python
class ExampleConnection(SparkleConnection):
    """
    Example connection description.
    Sub-Group: Category Name Here

    Example config:
        ...
    """
```

## Sub-Group Mappings

### A. Cloud Object Storage (8) - ✅ COMPLETED
- S3Connection → `Sub-Group: Cloud Object Storage`
- ADLSGen2Connection → `Sub-Group: Cloud Object Storage`
- GCSConnection → `Sub-Group: Cloud Object Storage`
- CloudflareR2Connection → `Sub-Group: Cloud Object Storage`
- WasabiConnection → `Sub-Group: Cloud Object Storage`
- BackblazeB2Connection → `Sub-Group: Cloud Object Storage`
- HDFSConnection → `Sub-Group: Cloud Object Storage`
- DatabricksVolumesConnection → `Sub-Group: Cloud Object Storage`

### B. Lakehouse Table Formats (3) - TODO
**File**: `connections/catalogs.py`

- Delta Lake classes (UnityCatalogConnection, etc.) → `Sub-Group: Lakehouse Table Formats`
- Iceberg classes → `Sub-Group: Lakehouse Table Formats`
- Hudi classes → `Sub-Group: Lakehouse Table Formats`

### C. Relational Databases – JDBC (14) - TODO
**File**: `connections/jdbc_connections.py`

- PostgreSQLConnection → `Sub-Group: Relational Databases`
- MySQLConnection → `Sub-Group: Relational Databases`
- OracleConnection → `Sub-Group: Relational Databases`
- SQLServerConnection → `Sub-Group: Relational Databases`
- TeradataConnection → `Sub-Group: Relational Databases`
- DB2Connection → `Sub-Group: Relational Databases`
- SAPHANAConnection → `Sub-Group: Relational Databases`
- VerticaConnection → `Sub-Group: Relational Databases`
- NetezzaConnection → `Sub-Group: Relational Databases`
- ClickHouseConnection → `Sub-Group: Relational Databases`
- MariaDBConnection → `Sub-Group: Relational Databases`
- AuroraConnection → `Sub-Group: Relational Databases`
- GreenplumConnection → `Sub-Group: Relational Databases`

### D. Data Warehouses & Analytical Stores (6) - TODO
**File**: `connections/data_warehouses.py`

- RedshiftConnection → `Sub-Group: Data Warehouses`
- SnowflakeConnection → `Sub-Group: Data Warehouses`
- BigQueryConnection → `Sub-Group: Data Warehouses`
- AzureSynapseConnection → `Sub-Group: Data Warehouses`
- DatabricksSQLConnection → `Sub-Group: Data Warehouses`
- AthenaConnection → `Sub-Group: Data Warehouses`
- TrinoConnection / StarburstConnection → `Sub-Group: Data Warehouses`
- DremioConnection → `Sub-Group: Data Warehouses`
- DruidConnection → `Sub-Group: Data Warehouses`
- PinotConnection → `Sub-Group: Data Warehouses`

### E. NoSQL & Document Stores (9) - TODO
**File**: `connections/nosql.py`

- MongoDBConnection → `Sub-Group: NoSQL & Document Stores`
- CassandraConnection → `Sub-Group: NoSQL & Document Stores`
- DynamoDBConnection → `Sub-Group: NoSQL & Document Stores`
- CouchbaseConnection → `Sub-Group: NoSQL & Document Stores`
- RedisConnection → `Sub-Group: NoSQL & Document Stores`
- Neo4jConnection → `Sub-Group: NoSQL & Document Stores`
- ElasticsearchConnection → `Sub-Group: NoSQL & Document Stores`
- DocumentDBConnection → `Sub-Group: NoSQL & Document Stores`
- CosmosDBConnection → `Sub-Group: NoSQL & Document Stores`
- OpenSearchConnection → `Sub-Group: NoSQL & Document Stores`

### F. SaaS / Cloud Platforms (18) - TODO
**File**: `connections/saas_platforms.py`

- SalesforceConnection → `Sub-Group: SaaS Platforms`
- SalesforceCDCConnection → `Sub-Group: SaaS Platforms`
- NetsuiteConnection → `Sub-Group: SaaS Platforms`
- WorkdayConnection → `Sub-Group: SaaS Platforms`
- ServiceNowConnection → `Sub-Group: SaaS Platforms`
- MarketoConnection → `Sub-Group: SaaS Platforms`
- HubSpotConnection → `Sub-Group: SaaS Platforms`
- ZendeskConnection → `Sub-Group: SaaS Platforms`
- ShopifyConnection → `Sub-Group: SaaS Platforms`
- StripeConnection → `Sub-Group: SaaS Platforms`
- ZuoraConnection → `Sub-Group: SaaS Platforms`
- GoogleAnalyticsConnection → `Sub-Group: SaaS Platforms`
- GoogleAdsConnection → `Sub-Group: SaaS Platforms`
- FacebookAdsConnection → `Sub-Group: SaaS Platforms`
- LinkedInAdsConnection → `Sub-Group: SaaS Platforms`
- AmplitudeConnection → `Sub-Group: SaaS Platforms`
- SegmentConnection → `Sub-Group: SaaS Platforms`

### G. Streaming & Messaging Systems (11) - TODO
**File**: `connections/streaming.py`

- KafkaConnection → `Sub-Group: Streaming & Messaging`
- ConfluentConnection → `Sub-Group: Streaming & Messaging`
- MSKConnection → `Sub-Group: Streaming & Messaging`
- KinesisConnection → `Sub-Group: Streaming & Messaging`
- EventHubsConnection → `Sub-Group: Streaming & Messaging`
- PubSubConnection → `Sub-Group: Streaming & Messaging`
- PulsarConnection → `Sub-Group: Streaming & Messaging`
- RabbitMQConnection → `Sub-Group: Streaming & Messaging`
- MQTTConnection → `Sub-Group: Streaming & Messaging`
- SQSConnection → `Sub-Group: Streaming & Messaging`
- ServiceBusConnection → `Sub-Group: Streaming & Messaging`
- NATSConnection → `Sub-Group: Streaming & Messaging`
- RedpandaConnection → `Sub-Group: Streaming & Messaging`

### H. REST & GraphQL APIs (2) - TODO
**File**: `connections/api.py`

- RESTAPIConnection → `Sub-Group: APIs`
- GraphQLAPIConnection → `Sub-Group: APIs`

### I. Mainframe & Legacy Systems (6) - TODO
**File**: `connections/mainframe_legacy.py`

- MainframeConnection → `Sub-Group: Mainframe & Legacy`
- AS400Connection → `Sub-Group: Mainframe & Legacy`
- SAPConnection → `Sub-Group: Mainframe & Legacy`
- SAPDatashereConnection → `Sub-Group: Mainframe & Legacy`
- OracleEBSConnection → `Sub-Group: Mainframe & Legacy`
- PeopleSoftConnection → `Sub-Group: Mainframe & Legacy`

### J. Industry-Specific & Specialized (5) - TODO
**File**: `connections/industry_protocols.py`

- HL7Connection → `Sub-Group: Industry Protocols`
- FHIRConnection → `Sub-Group: Industry Protocols`
- EDIConnection → `Sub-Group: Industry Protocols`
- FIXConnection → `Sub-Group: Industry Protocols`
- SWIFTConnection → `Sub-Group: Industry Protocols`
- AWSHealthLakeConnection → `Sub-Group: Industry Protocols`

### K. Specialized Connections - TODO
**File**: `connections/specialized.py`

- SFTPConnection → `Sub-Group: File Transfer`
- FTPConnection → `Sub-Group: File Transfer`
- LDAPConnection → `Sub-Group: Directory Services`
- JiraConnection → `Sub-Group: Collaboration Tools`
- SlackConnection → `Sub-Group: Collaboration Tools`
- EmailConnection (SMTP/IMAP) → `Sub-Group: Communication`

## Implementation Progress

- ✅ Infrastructure: Component registry, schemas, service layer
- ✅ Cloud Object Storage (8 connections)
- ⏳ Lakehouse Table Formats (3 connections)
- ⏳ Relational Databases (14 connections)
- ⏳ Data Warehouses (10 connections)
- ⏳ NoSQL & Document Stores (10 connections)
- ⏳ SaaS Platforms (17 connections)
- ⏳ Streaming & Messaging (13 connections)
- ⏳ APIs (2 connections)
- ⏳ Mainframe & Legacy (6 connections)
- ⏳ Industry Protocols (6 connections)
- ⏳ Specialized (6 connections)

## Total: 95+ connections across 11 sub-groups

## Quick Reference Script

To bulk-add sub-groups, use this pattern:

```python
# Example for adding to a file
import re

def add_subgroup(file_path, class_pattern, subgroup_name):
    with open(file_path, 'r') as f:
        content = f.read()

    # Pattern: class ClassName...: """ Description.
    pattern = rf'(class {class_pattern}.*?:\s+"""\s+)(.*?)(\s+)'
    replacement = rf'\1\2\n    Sub-Group: {subgroup_name}\3'

    content = re.sub(pattern, replacement, content, flags=re.DOTALL)

    with open(file_path, 'w') as f:
        f.write(content)

# Usage
add_subgroup(
    'connections/data_warehouses.py',
    'SnowflakeConnection',
    'Data Warehouses'
)
```
