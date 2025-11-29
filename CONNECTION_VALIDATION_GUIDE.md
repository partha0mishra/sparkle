# Connection Validation Guide for Sparkle Studio

**Last Updated:** November 28, 2024
**Total Connections:** 177 across 12 categories
**Purpose:** Manual validation checklist for all Sparkle connection components

---

## Table of Contents

1. [Overview](#overview)
2. [How to Test Connections](#how-to-test-connections)
3. [Connection Categories & Components](#connection-categories--components)
4. [Test Button Usage](#test-button-usage)
5. [Validation Checklist Templates](#validation-checklist-templates)
6. [Troubleshooting](#troubleshooting)

---

## Overview

Sparkle Studio includes 177 pre-configured connection components organized into 12 categories. Each connection supports a **Test Connection** feature that validates connectivity and configuration.

### Connection Statistics

| Category | Count | Examples |
|----------|-------|----------|
| Relational Databases | 19 | PostgreSQL, MySQL, Oracle, SQL Server |
| Cloud Object Storage | 18 | S3, ADLS, GCS, MinIO |
| Data Warehouses | 16 | Snowflake, Redshift, BigQuery, Databricks |
| Streaming & Messaging | 16 | Kafka, Kinesis, Event Hubs, Pulsar |
| SaaS Platforms | 20 | Salesforce, ServiceNow, Hubspot |
| NoSQL & Document Stores | 14 | MongoDB, Cassandra, DynamoDB |
| Lakehouse Table Formats | 12 | Delta Lake, Iceberg, Hudi |
| Industry Protocols | 12 | HL7 FHIR, EDI, SWIFT, FIX |
| Mainframe & Legacy | 15 | AS/400, COBOL, CICS, IMS |
| Specialized | 9 | Unity Catalog, Glue, Purview |
| APIs | 6 | REST, GraphQL, SOAP, gRPC |
| Other | 20 | Various specialized connections |

---

## How to Test Connections

### Method 1: Using the Sparkle Studio UI

1. **Open Sparkle Studio**: Navigate to `http://localhost:3000`

2. **Open Sidebar**: Click on "Connections" section (177 components)

3. **Select a Connection**:
   - Click on any connection (e.g., "PostgreSQL")
   - Properties Panel opens on the right

4. **Configure Connection**:
   - Fill in required fields (host, port, database, credentials, etc.)
   - Or use JSON editor mode for advanced configuration

5. **Test Connection**:
   - Click the blue **"Test Connection"** button
   - Button shows status:
     - **"Testing..."** (gray, with spinner) - Test in progress
     - **"Success"** (green, with checkmark) - Connection successful
     - **"Failed"** (red, with X) - Connection failed
   - Success/failure message displays below the button
   - Status auto-clears after 5 seconds

6. **Review Results**:
   - **Success**: Connection is properly configured
   - **Failed**: Check error message and adjust configuration

### Method 2: Using API Directly

```bash
curl -X POST http://localhost:8000/api/v1/connections/test \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres",
    "config": {
      "host": "localhost",
      "port": 5432,
      "database": "sparkle",
      "user": "postgres",
      "password": "password"
    }
  }'
```

**Response Format:**
```json
{
  "success": true,
  "status": "success",  // or "failed", "error"
  "message": "Successfully connected to PostgreSQL Connection",
  "details": {
    "connection_type": "PostgreSQL Connection",
    "test_method": "native"
  },
  "error": null
}
```

---

## Connection Categories & Components

### 1. Relational Databases (19 connections)

#### PostgreSQL
- **Names**: `postgres`, `postgresql`
- **Icon**: PostgreSQL logo
- **Required Config**:
  ```json
  {
    "host": "localhost",
    "port": 5432,
    "database": "my_database",
    "user": "my_user",
    "password": "my_password"
  }
  ```
- **Test Method**: JDBC connection test
- **Validation Criteria**:
  - ✅ Successfully establishes JDBC connection
  - ✅ Can query `SELECT 1`
  - ✅ Credentials are valid

#### MySQL
- **Names**: `mysql`
- **Icon**: MySQL logo
- **Required Config**:
  ```json
  {
    "host": "localhost",
    "port": 3306,
    "database": "my_database",
    "user": "root",
    "password": "password"
  }
  ```
- **Test Method**: JDBC connection test

#### Oracle
- **Names**: `oracle`
- **Icon**: Oracle logo
- **Required Config**:
  ```json
  {
    "host": "localhost",
    "port": 1521,
    "service_name": "ORCL",
    "user": "system",
    "password": "oracle"
  }
  ```

#### SQL Server
- **Names**: `sqlserver`, `mssql`
- **Icon**: Microsoft SQL Server logo
- **Required Config**:
  ```json
  {
    "host": "localhost",
    "port": 1433,
    "database": "master",
    "user": "sa",
    "password": "YourStrong@Passw0rd"
  }
  ```

#### Complete List:
- postgres / postgresql
- mysql
- mariadb
- oracle
- sqlserver / mssql
- db2 / ibm_db2
- teradata
- saphana / hana
- vertica
- greenplum
- netezza
- sybase
- sqlite
- presto
- trino
- clickhouse

---

### 2. Cloud Object Storage (18 connections)

#### Amazon S3
- **Names**: `s3`, `aws_s3`, `amazon_s3`
- **Icon**: Amazon S3 logo
- **Required Config**:
  ```json
  {
    "bucket": "my-bucket",
    "region": "us-east-1",
    "access_key_id": "AKIAIOSFODNN7EXAMPLE",
    "secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  }
  ```
- **Test Method**: List objects (dry run)
- **Validation Criteria**:
  - ✅ Bucket exists and is accessible
  - ✅ Credentials are valid
  - ✅ Permissions allow read operations

#### Azure Data Lake Storage (ADLS)
- **Names**: `adls`, `adls_gen2`, `azure_datalake`
- **Icon**: Microsoft Azure logo
- **Required Config**:
  ```json
  {
    "storage_account": "mystorageaccount",
    "container": "my-container",
    "account_key": "base64_encoded_key"
  }
  ```

#### Google Cloud Storage (GCS)
- **Names**: `gcs`, `google_cloud_storage`
- **Icon**: Google Cloud logo
- **Required Config**:
  ```json
  {
    "bucket": "my-gcs-bucket",
    "project_id": "my-project",
    "credentials_json": "{...service_account_key...}"
  }
  ```

#### Complete List:
- s3 / aws_s3 / amazon_s3
- adls / adls_gen2 / azure_datalake
- gcs / google_cloud_storage
- hdfs / hadoop
- local / file
- s3_compatible / minio
- databricks_volumes / dbfs / unity_volumes
- cloudflare_r2 / r2
- wasabi
- backblaze_b2 / b2

---

### 3. Data Warehouses (16 connections)

#### Snowflake
- **Names**: `snowflake`
- **Icon**: Snowflake logo
- **Required Config**:
  ```json
  {
    "account": "myaccount.us-east-1",
    "user": "myuser",
    "password": "mypassword",
    "database": "MY_DATABASE",
    "schema": "PUBLIC",
    "warehouse": "COMPUTE_WH",
    "role": "ACCOUNTADMIN"
  }
  ```
- **Test Method**: Native connection test
- **Validation Criteria**:
  - ✅ Successfully authenticates
  - ✅ Warehouse is running
  - ✅ Can execute simple query

#### Amazon Redshift
- **Names**: `redshift`, `aws_redshift`
- **Icon**: Amazon Redshift logo
- **Required Config**:
  ```json
  {
    "host": "mycluster.abc123.us-east-1.redshift.amazonaws.com",
    "port": 5439,
    "database": "dev",
    "user": "admin",
    "password": "MyPassword123"
  }
  ```

#### Google BigQuery
- **Names**: `bigquery`, `google_bigquery`
- **Icon**: Google BigQuery logo
- **Required Config**:
  ```json
  {
    "project_id": "my-project",
    "dataset": "my_dataset",
    "credentials_json": "{...service_account_key...}"
  }
  ```

#### Complete List:
- snowflake
- redshift / aws_redshift
- bigquery / google_bigquery
- synapse / azure_synapse
- databricks_sql / databricks
- athena / aws_athena
- dremio
- druid / apache_druid
- pinot / apache_pinot
- delta_lake / delta
- iceberg / apache_iceberg
- hudi / apache_hudi

---

### 4. Streaming & Messaging (16 connections)

#### Apache Kafka
- **Names**: `kafka`, `apache_kafka`
- **Icon**: Apache Kafka logo
- **Required Config**:
  ```json
  {
    "bootstrap_servers": "localhost:9092",
    "topic": "my-topic",
    "security_protocol": "PLAINTEXT"
  }
  ```
- **Test Method**: Connection + topic validation
- **Validation Criteria**:
  - ✅ Can connect to brokers
  - ✅ Topic exists or can be created
  - ✅ Producer/consumer permissions valid

#### Complete List:
- kafka / apache_kafka
- kinesis / aws_kinesis
- event_hubs / azure_event_hubs
- pubsub / google_pubsub
- pulsar / apache_pulsar
- rabbitmq
- sqs / amazon_sqs
- service_bus / azure_service_bus
- mqtt
- jms
- activemq / apache_activemq

---

### 5. SaaS Platforms (20 connections)

#### Salesforce
- **Names**: `salesforce`
- **Icon**: Salesforce logo
- **Required Config**:
  ```json
  {
    "username": "user@example.com",
    "password": "password",
    "security_token": "token123",
    "domain": "login"
  }
  ```

#### Complete List:
- salesforce
- servicenow
- okta
- hubspot
- stripe
- twilio
- segment
- amplitude
- mixpanel
- intercom
- zendesk
- jira
- confluence
- slack
- microsoft_teams / teams
- google_analytics / ga4
- adobe_analytics
- marketo
- pardot

---

### 6. NoSQL & Document Stores (14 connections)

#### MongoDB
- **Names**: `mongodb`, `mongo`
- **Icon**: MongoDB logo
- **Required Config**:
  ```json
  {
    "connection_string": "mongodb://localhost:27017",
    "database": "my_database",
    "collection": "my_collection"
  }
  ```

#### Complete List:
- mongodb / mongo
- cassandra / apache_cassandra
- dynamodb / aws_dynamodb
- cosmosdb / azure_cosmosdb
- elasticsearch / elastic
- hbase / apache_hbase
- couchbase
- neo4j
- redis
- scylladb
- arangodb
- couchdb / apachecouchdb

---

### 7-12. Additional Categories

See full list in [ORCHESTRATOR_VALIDATION_REPORT.md](./ORCHESTRATOR_VALIDATION_REPORT.md) for:
- Lakehouse Table Formats (12)
- Industry Protocols (12)
- Mainframe & Legacy (15)
- Specialized (9)
- APIs (6)
- Other (20)

---

## Test Button Usage

### Button States

| State | Color | Icon | Description |
|-------|-------|------|-------------|
| **Idle** | Blue | - | Ready to test, click to start |
| **Testing** | Gray | Spinner | Test in progress, please wait |
| **Success** | Green | Checkmark ✓ | Connection successful |
| **Failed** | Red | X | Connection failed, check config |

### Success Messages

- "Successfully connected to PostgreSQL Connection"
- "Connection configuration validated for Snowflake"
- "Connection instance created successfully"

### Failure Messages

- "Connection test failed for MySQL Connection"
- "JDBC URL validation failed: Invalid hostname"
- "Authentication failed: Invalid credentials"
- "Timeout connecting to remote server"

---

## Validation Checklist Templates

### Template 1: Relational Database

```markdown
#### [Database Name] - [Date]

- [ ] Connection Name: _____________
- [ ] Host: _____________
- [ ] Port: _____________
- [ ] Database: _____________
- [ ] User: _____________
- [ ] Password: (hidden)
- [ ] Test Result: [ ] Success [ ] Failed
- [ ] Error Message (if failed): _____________
- [ ] Notes: _____________
```

### Template 2: Cloud Storage

```markdown
#### [Storage Service] - [Date]

- [ ] Connection Name: _____________
- [ ] Bucket/Container: _____________
- [ ] Region: _____________
- [ ] Access Key ID: _____________
- [ ] Secret Key: (hidden)
- [ ] Test Result: [ ] Success [ ] Failed
- [ ] Can list objects: [ ] Yes [ ] No
- [ ] Notes: _____________
```

### Template 3: API/SaaS

```markdown
#### [Service Name] - [Date]

- [ ] Connection Name: _____________
- [ ] API Endpoint: _____________
- [ ] API Key/Token: (hidden)
- [ ] Authentication Method: _____________
- [ ] Test Result: [ ] Success [ ] Failed
- [ ] Rate Limits Checked: [ ] Yes [ ] No
- [ ] Notes: _____________
```

---

## Troubleshooting

### Common Issues & Solutions

#### 1. "Connection test failed"

**Possible Causes:**
- Invalid credentials
- Network connectivity issues
- Firewall blocking connection
- Service is down or unreachable

**Solutions:**
- Verify credentials are correct
- Check network connectivity (`ping`, `telnet`)
- Review firewall rules
- Confirm service is running

#### 2. "Component not found"

**Cause:** Connection name doesn't exist in registry

**Solution:**
- Check spelling of connection name
- Use `curl http://localhost:8000/api/v1/components` to list all available connections
- Ensure backend has loaded all components

#### 3. "Invalid configuration"

**Cause:** Required fields are missing or malformed

**Solution:**
- Review connection-specific documentation
- Ensure all required fields are provided
- Validate JSON syntax if using JSON editor

#### 4. "Timeout"

**Cause:** Connection takes too long to establish

**Solutions:**
- Increase timeout settings
- Check network latency
- Verify remote service is responsive

---

## API Endpoint Reference

### Test Connection
```
POST /api/v1/connections/test
```

**Request Body:**
```json
{
  "name": "connection_name",
  "config": {
    "key1": "value1",
    "key2": "value2"
  }
}
```

**Response:**
```json
{
  "success": boolean,
  "status": "success" | "failed" | "error",
  "message": "Human-readable message",
  "details": {
    "connection_type": "Connection Display Name",
    "test_method": "native" | "validation" | "instantiation"
  },
  "error": "Error details (if failed)"
}
```

### List All Connections
```
GET /api/v1/components
```

Filter for connections:
```bash
curl http://localhost:8000/api/v1/components | jq '.data.groups[] | select(.category=="connection")'
```

---

## Validation Progress Tracker

Use this tracker to record your validation progress:

### Relational Databases (19)
- [ ] postgres
- [ ] mysql
- [ ] mariadb
- [ ] oracle
- [ ] sqlserver
- [ ] db2
- [ ] teradata
- [ ] saphana
- [ ] vertica
- [ ] greenplum
- [ ] netezza
- [ ] sybase
- [ ] sqlite
- [ ] presto
- [ ] trino
- [ ] clickhouse

### Cloud Object Storage (18)
- [ ] s3
- [ ] adls
- [ ] gcs
- [ ] hdfs
- [ ] minio
- [ ] databricks_volumes
- [ ] cloudflare_r2
- [ ] wasabi
- [ ] backblaze_b2

### Data Warehouses (16)
- [ ] snowflake
- [ ] redshift
- [ ] bigquery
- [ ] synapse
- [ ] databricks_sql
- [ ] athena
- [ ] dremio
- [ ] druid
- [ ] pinot

### Streaming & Messaging (16)
- [ ] kafka
- [ ] kinesis
- [ ] event_hubs
- [ ] pubsub
- [ ] pulsar
- [ ] rabbitmq
- [ ] sqs
- [ ] mqtt

### SaaS Platforms (20)
- [ ] salesforce
- [ ] servicenow
- [ ] okta
- [ ] hubspot
- [ ] stripe
- [ ] twilio
- [ ] segment
- [ ] amplitude

### NoSQL & Document (14)
- [ ] mongodb
- [ ] cassandra
- [ ] dynamodb
- [ ] cosmosdb
- [ ] elasticsearch
- [ ] hbase
- [ ] couchbase
- [ ] neo4j
- [ ] redis

### [Continue for all 177 connections...]

---

## Summary

This guide provides a comprehensive framework for validating all 177 connection components in Sparkle Studio. Use the Test Connection button in the UI or the API endpoint to systematically verify each connection type.

**Key Points:**
- ✅ Test button integrated into Properties Panel
- ✅ Visual feedback with color-coded states
- ✅ Detailed success/failure messages
- ✅ API endpoint for automated testing
- ✅ Comprehensive validation templates

For questions or issues, refer to the [Troubleshooting](#troubleshooting) section or consult the Sparkle documentation.

---

**Document Version:** 1.0
**Last Updated:** November 28, 2024
**Maintained By:** Sparkle Engineering Team
