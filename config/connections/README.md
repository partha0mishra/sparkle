# Sparkle Connection Configurations

This directory contains connection configurations for all Sparkle connection types.

## Directory Structure

```
config/connections/
├── {connection_name}/
│   ├── common.json          # Common settings across all environments
│   ├── dev.json             # Development environment
│   ├── qa.json              # QA/staging environment
│   └── prod.json            # Production environment
```

## Environment Overlay

Configs are loaded with environment overlay:
1. Load `common.json` (if exists)
2. Overlay `{env}.json` (dev/qa/prod)
3. Resolve secrets from environment variables

## Secret Interpolation

Use `${ENV_VAR}` syntax for secrets:

```json
{
  "password": "${POSTGRES_PASSWORD}",
  "api_key": "${API_KEY}"
}
```

## Usage

```python
from sparkle.connections import get_connection

# Loads config/connections/postgres/prod.json
conn = get_connection("postgres", spark, env="prod")

# Or provide config directly
config = {"url": "...", "user": "...", "password": "..."}
conn = get_connection("postgres", spark, config=config)
```

## Example Configurations

See subdirectories for example configurations for each connection type:

### JDBC Databases
- `postgresql/` - PostgreSQL
- `mysql/` - MySQL
- `oracle/` - Oracle Database
- `sqlserver/` - Microsoft SQL Server
- `snowflake/` - Snowflake (via JDBC)

### Cloud Storage
- `s3/` - Amazon S3
- `adls/` - Azure Data Lake Storage Gen2
- `gcs/` - Google Cloud Storage

### Streaming
- `kafka/` - Apache Kafka
- `kinesis/` - AWS Kinesis
- `eventhub/` - Azure Event Hubs
- `pubsub/` - Google Pub/Sub

### Data Warehouses
- `snowflake/` - Snowflake
- `redshift/` - AWS Redshift
- `bigquery/` - Google BigQuery
- `synapse/` - Azure Synapse Analytics

### NoSQL
- `mongodb/` - MongoDB
- `cassandra/` - Apache Cassandra
- `dynamodb/` - AWS DynamoDB
- `cosmosdb/` - Azure Cosmos DB
- `elasticsearch/` - Elasticsearch

### APIs
- `rest/` - Generic REST API
- `graphql/` - GraphQL API

### Catalogs
- `unity_catalog/` - Databricks Unity Catalog
- `glue/` - AWS Glue Data Catalog
- `hive/` - Hive Metastore

### ML Platforms
- `mlflow/` - MLflow
- `sagemaker/` - AWS SageMaker
- `vertex_ai/` - Google Vertex AI
- `azure_ml/` - Azure Machine Learning

### File Formats
- `delta/` - Delta Lake
- `iceberg/` - Apache Iceberg
- `hudi/` - Apache Hudi

## Creating New Connections

1. Create directory: `config/connections/{connection_name}/`
2. Add `common.json` with shared settings
3. Add environment-specific configs: `dev.json`, `qa.json`, `prod.json`
4. Use `${ENV_VAR}` for secrets
5. Never commit actual secrets to version control

## Best Practices

1. **Never hardcode secrets** - Always use environment variables
2. **Use common.json** - Share non-sensitive settings across environments
3. **Environment-specific overrides** - Only override what's different per environment
4. **Validate configs** - Test connections before deploying to production
5. **Document custom settings** - Add comments in JSON (use YAML if you need comments)
