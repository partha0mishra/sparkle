# Sparkle Orchestration - Complete Documentation

## Overview

**84 production-ready orchestration components** for lakehouse data pipelines with support for 5 orchestrators:
- Databricks Workflows
- Apache Airflow
- Dagster
- Prefect
- Mage

## Key Features

✅ **100% Config-Driven** - ZERO hard-coded values
✅ **Multi-Orchestrator** - Deploy to any platform
✅ **Production-Ready** - Error handling, monitoring, lineage
✅ **Composable** - Mix and match tasks and pipelines
✅ **Type-Safe** - Full configuration validation

## Quick Start

```python
from sparkle.orchestration import Pipeline

# Load pipeline from config
pipeline = Pipeline.get("customer_silver_daily", env="prod")

# Build and run
pipeline.build().run()

# Deploy to orchestrator
pipeline.deploy(orchestrator="databricks")
```

## CLI Usage

```bash
# Generate deployment
sparkle-orchestration generate-deploy customer_silver_daily \
    --orchestrator databricks \
    --output ./deployments

# Validate configuration
sparkle-orchestration validate customer_silver_daily --env prod

# Generate lineage
sparkle-orchestration lineage customer_silver_daily --output lineage.json

# Run locally
sparkle-orchestration run customer_silver_daily --env dev

# Initialize new pipeline
sparkle-orchestration init bronze_raw_ingestion customer_ingestion
```

## Configuration Structure

All pipelines read from: `config/orchestration/{pipeline_name}/{env}.json`

```json
{
  "pipeline_name": "customer_silver_daily",
  "pipeline_type": "silver_batch_transformation",
  "env": "prod",

  "source_system": "bronze",
  "source_table": "bronze.raw.customers",

  "destination_catalog": "${CATALOG}",
  "destination_schema": "${SCHEMA}",
  "destination_table": "customers_clean",

  "transformers": [
    {"name": "drop_exact_duplicates"},
    {"name": "standardize_nulls"},
    {"name": "add_audit_columns", "params": {"user": "etl_pipeline"}}
  ],

  "primary_key": ["customer_id"],
  "partition_columns": ["ingestion_date"],
  "watermark_column": "updated_at",

  "write_mode": "overwrite",

  "schedule": "0 2 * * *",
  "dependencies": ["customer_bronze_ingestion"],

  "tags": {
    "team": "data-engineering",
    "domain": "customer",
    "criticality": "high"
  }
}
```

## Component Categories

### A. Core Pipeline Templates (24)

#### Bronze Layer Pipelines

**1. BronzeRawIngestionPipeline**
- Ingests from any source using registered ingestor
- Adds `_ingested_at`, `_source_file`, provenance metadata
- Writes to bronze layer with partitioning

```json
{
  "pipeline_type": "bronze_raw_ingestion",
  "ingestor": "jdbc_postgres",
  "source_connection": "postgres_prod",
  "source_table": "public.customers",
  "destination_table": "customers_raw"
}
```

**2. BronzeStreamingIngestionPipeline**
- Structured Streaming with exactly-once semantics
- Checkpoint management and watermarking
- Auto-recovery on failure

```json
{
  "pipeline_type": "bronze_streaming_ingestion",
  "ingestor": "kafka_avro",
  "checkpoint_location": "/checkpoints/customer_stream",
  "trigger": "processingTime='10 seconds'"
}
```

#### Silver Layer Pipelines

**3. SilverBatchTransformationPipeline**
- Applies transformer chain from config
- Schema enforcement and validation
- Incremental processing support

**4. SilverStreamingTransformationPipeline**
- Streaming transformations with watermarking
- Late data handling
- Stateful processing support

#### Gold Layer Pipelines

**5. GoldDailyAggregatePipeline**
- Config-driven aggregations
- Partitioned by date
- Optimized for BI tools

**6. GoldRealtimeDashboardPipeline**
- Streaming aggregations
- Pre-computed rollups
- Low-latency access patterns

**7. DimensionDailySCD2Pipeline**
- SCD Type 2 historization
- Business key tracking
- Effective/end date management

**8. FactDailyIncrementalPipeline**
- Incremental fact loading
- Surrogate key generation
- Foreign key resolution

#### Feature Store Pipelines

**9. FeatureStoreDailyMaterializationPipeline**
- Materializes to Unity Catalog Feature Store
- Point-in-time correctness
- Feature versioning

**10. FeatureStoreRealtimeSyncPipeline**
- Syncs to online store (Redis/DynamoDB)
- Low-latency feature serving
- Consistency guarantees

#### ML Pipelines

**11. MLTrainingChampionChallengerPipeline**
- Trains multiple model variants
- Compares metrics automatically
- Promotes champion based on thresholds

**12. MLBatchScoringPipeline**
- Daily/weekly batch scoring
- Model versioning support
- Prediction logging

**13. MLRealtimeScoringPipeline**
- Streaming predictions
- Model caching
- Shadow mode testing

**14. ModelDriftMonitoringPipeline**
- PSI and KS drift detection
- Automated alerting
- Drift visualization

#### Quality & Governance Pipelines

**15. DataQualityExpectationsPipeline**
- Great Expectations integration
- Quarantine bad data
- Quality metrics tracking

**16. DataContractEnforcementPipeline**
- Row count validation
- Freshness checks
- Schema validation

**17. BackfillHistoricalPipeline**
- Parallel backfill execution
- Date range processing
- Progress tracking

**18. CatchUpAfterOutagePipeline**
- Detects processing lag
- Parallel catch-up
- Watermark reconciliation

**19. TableReconciliationPipeline**
- Source-target comparison
- Row hash validation
- Mismatch reporting

#### Maintenance Pipelines

**20. VacuumAndOptimizeMaintenancePipeline**
- VACUUM old files
- OPTIMIZE + ZORDER
- Statistics refresh

**21. ZOrderAndAnalyzePipeline**
- Multi-column Z-ordering
- Compute statistics
- Query optimization

**22. UnityCatalogGovernanceSyncPipeline**
- Apply grants from YAML
- Sync tags and ownership
- Audit compliance

**23. CostAttributionTaggingPipeline**
- Tag all Delta tables
- Cost tracking metadata
- Chargeback support

**24. DisasterRecoveryReplicationPipeline**
- Cross-region replication
- Backup validation
- Failover testing

### B. Multi-Orchestrator Adapters (25)

#### Databricks Adapters (5)

**25. DatabricksWorkflowsJobAdapter**
- Single-task job creation
- Cluster configuration
- Library dependencies

**26. DatabricksWorkflowsMultiTaskAdapter**
- Multi-task DAG generation
- Task dependencies
- Conditional execution

**27. DatabricksWorkflowsDeltaLiveTablesAdapter**
- DLT pipeline generation
- Expectations as code
- CDC support

**28. DatabricksWorkflowsNotebookTaskAdapter**
- Notebook execution
- Widget parameters
- Result capture

**29. DatabricksWorkflowsRepairRunAdapter**
- Failed task repair
- Partial re-execution
- State preservation

#### Airflow Adapters (5)

**30. AirflowTaskFlowDecoratorAdapter**
- @task decorated functions
- XCom passing
- Dynamic task generation

**31. AirflowDynamicTaskMappingAdapter**
- Parallel backfills
- Dynamic fan-out
- Result aggregation

**32. AirflowExternalTaskSensorAdapter**
- Cross-DAG dependencies
- Timeout handling
- Poke interval config

**33. AirflowSlackWebhookOperatorAdapter**
- Slack notifications
- Custom message formatting
- Alert channels

**34. AirflowDatabricksSubmitRunOperatorAdapter**
- Databricks job trigger
- Poll for completion
- Log retrieval

#### Dagster Adapters (5)

**35. DagsterSoftwareDefinedAssetsAdapter**
- @asset definitions
- Freshness policies
- Partition mappings

**36. DagsterOpFactoryAdapter**
- Reusable op generation
- Config schemas
- Type annotations

**37. DagsterScheduleAndSensorAdapter**
- Cron schedules
- File sensors
- Event triggers

**38. DagsterFreshnessPolicyEnforcerAdapter**
- Max age enforcement
- SLA monitoring
- Alert integration

**39. DagsterUnityCatalogIntegrationAdapter**
- Asset registration
- Lineage tracking
- Metadata sync

#### Prefect Adapters (5)

**40. PrefectFlowDecoratorAdapter**
- @flow definitions
- Parameter validation
- Result persistence

**41. PrefectDeploymentBuilderAdapter**
- Deployment YAML generation
- Work pool configuration
- Schedule definition

**42. PrefectAutomationsAlertingAdapter**
- Automation rules
- Slack/Teams integration
- PagerDuty escalation

**43. PrefectParameterisedRunAdapter**
- Ad-hoc execution
- Parameter overrides
- UI integration

**44. PrefectResultPersistenceDeltaAdapter**
- Delta table results
- Debugging support
- State management

#### Mage Adapters (5)

**45. MagePipelineBlockAdapter**
- Pipeline definition
- Block chaining
- Variable passing

**46. MageDataLoaderBlockAdapter**
- Ingestor mapping
- Source configuration
- Credential management

**47. MageTransformerBlockAdapter**
- Transformer chain
- Data validation
- Error handling

**48. MageDataExporterBlockAdapter**
- Write operations
- Format conversion
- Partitioning

**49. MageTriggerSchedulerAdapter**
- Time triggers
- Event triggers
- Manual execution

### C. Task Building Blocks (22)

**50. IngestTask** - Execute any registered ingestor
**51. TransformTask** - Apply transformer chain
**52. WriteDeltaTask** - Write to Delta with options
**53. WriteFeatureTableTask** - Write to Feature Store
**54. CreateUnityCatalogTableTask** - Create UC table
**55. TrainMLModelTask** - Train ML model
**56. BatchScoreTask** - Batch prediction
**57. RegisterModelTask** - Register to UC Model Registry
**58. PromoteModelChampionTask** - Promote champion model
**59. RunGreatExpectationsTask** - Run GE suite
**60. RunDbtCoreTask** - Execute dbt models
**61. RunSQLFileTask** - Execute SQL file
**62. RunNotebookTask** - Run notebook
**63. TriggerDownstreamPipelineTask** - Trigger pipeline
**64. AutoRecoverTask** - Retry with backoff
**65. GrantPermissionsTask** - Apply grants
**66. WaitForTableFreshnessTask** - Wait for data
**67. SendSlackAlertTask** - Slack notification
**68. SendEmailAlertTask** - Email notification
**69. NotifyOnFailureTask** - Failure callback
**70. MonteCarloDataIncidentTask** - MonteCarlo integration
**71. LightupDataSLAAlertTask** - Lightup integration

### D. Scheduling & Trigger Patterns (12)

**72. DailyAtMidnightSchedule** - Cron: 0 0 * * *
**73. HourlySchedule** - Cron: 0 * * * *
**74. Every15MinutesSchedule** - Cron: */15 * * * *
**75. EventDrivenFileArrivalTrigger** - S3/ADLS events
**76. EventDrivenKafkaLagTrigger** - Kafka lag monitoring
**77. EventDrivenTableUpdatedTrigger** - Delta CDF
**78. CronExpressionBuilder** - Complex cron builder
**79. CatchupBackfillSchedule** - Backfill mode
**80. HolidayCalendarSkip** - Holiday exclusion
**81. DependencyChainBuilder** - Task dependency wiring
**82. SLAMonitor** - SLA tracking and alerting
**83. DataFreshnessMonitor** - Freshness monitoring
**84. PipelineLineageVisualizer** - Lineage visualization

## Configuration Examples

See `config/orchestration/examples/` for complete examples:
- `bronze_customer_ingestion.json` - Bronze raw ingestion
- `silver_customer_transformation.json` - Silver transformations
- `gold_customer_aggregate.json` - Gold aggregations
- `feature_store_materialization.json` - Feature Store
- `ml_churn_prediction.json` - ML training pipeline

## Architecture

```
orchestration/
├── base.py              # Base classes (BasePipeline, BaseTask, PipelineConfig)
├── factory.py           # Pipeline factory and registry
├── config_loader.py     # Config loading with validation
├── cli.py              # Command-line interface
├── pipelines/          # 24 core pipeline templates
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   ├── feature_store.py
│   ├── ml.py
│   ├── quality.py
│   ├── maintenance.py
│   └── governance.py
├── tasks/              # 22 task building blocks
│   ├── ingest.py
│   ├── transform.py
│   ├── write.py
│   ├── ml.py
│   ├── quality.py
│   ├── governance.py
│   ├── notification.py
│   ├── orchestration.py
│   └── observability.py
├── adapters/           # 25 orchestrator adapters
│   ├── databricks.py
│   ├── airflow.py
│   ├── dagster.py
│   ├── prefect.py
│   └── mage.py
└── schedules/          # 12 scheduling patterns
    ├── cron.py
    ├── event_driven.py
    └── monitoring.py
```

## Extending

### Create Custom Pipeline

```python
from sparkle.orchestration.base import BasePipeline
from sparkle.orchestration.factory import PipelineFactory

@PipelineFactory.register("custom_pipeline")
class CustomPipeline(BasePipeline):
    def build(self):
        # Add your tasks
        self.add_task(IngestTask("ingest", self.config))
        self.add_task(TransformTask("transform", self.config))
        return self
```

### Create Custom Task

```python
from sparkle.orchestration.base import BaseTask

class CustomTask(BaseTask):
    def execute(self, spark, context):
        # Your logic here
        return result
```

## Best Practices

1. **Always use config files** - Never hard-code values
2. **Test locally first** - Use `sparkle-orchestration run --dry-run`
3. **Validate configs** - Run `sparkle-orchestration validate`
4. **Monitor lineage** - Generate lineage visualizations
5. **Tag everything** - Use tags for cost attribution
6. **Version configs** - Keep configs in git
7. **Environment-specific** - Separate dev/qa/prod configs
8. **Document dependencies** - Use `dependencies` field

## Troubleshooting

### Config not found
```bash
# Check config path
ls config/orchestration/{pipeline_name}/{env}.json

# Initialize if missing
sparkle-orchestration init {pipeline_type} {pipeline_name}
```

### Validation errors
```bash
# Validate config
sparkle-orchestration validate {pipeline_name} --env {env}
```

### Deployment issues
```bash
# Check generated deployment
sparkle-orchestration generate-deploy {pipeline_name} \
    --orchestrator {orchestrator} \
    --output ./debug
```

## Support

For issues or questions:
- Check logs in Spark UI
- Review config schema: `config/orchestration/schema.json`
- See examples: `config/orchestration/examples/`
- Review component docs: `orchestration/COMPONENTS.md`

## License

Part of Sparkle Framework - Production Data Engineering Platform
