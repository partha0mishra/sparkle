# Sparkle Orchestration - 84 Components Index

## A. Core Pipeline Templates (24)

### Bronze Layer
1. **BronzeRawIngestionPipeline** - Ingests any source via named ingestor → writes raw bronze Delta with _ingested_at + source provenance
2. **BronzeStreamingIngestionPipeline** - Same as #1 but Structured Streaming with checkpointing and exactly-once to bronze

### Silver Layer
3. **SilverBatchTransformationPipeline** - Reads bronze → applies configurable transformer chain → writes silver with schema enforcement
4. **SilverStreamingTransformationPipeline** - Streaming version of #3 with watermark tracking and late-data handling

### Gold Layer
5. **GoldDailyAggregatePipeline** - Daily/partitioned silver → gold aggregated tables (config-driven group-by + measures)
6. **GoldRealtimeDashboardPipeline** - Streaming silver → pre-aggregated gold rollups optimized for BI tools
7. **DimensionDailySCD2Pipeline** - Applies SCD Type-2 logic from silver → gold dimension using config-defined business key
8. **FactDailyIncrementalPipeline** - Incremental fact load from silver → gold fact with surrogate key generation

### Feature Store
9. **FeatureStoreDailyMaterializationPipeline** - Materializes feature views from silver/gold → Unity Catalog Feature Store (offline)
10. **FeatureStoreRealtimeSyncPipeline** - Pushes latest feature values to online store (Redis/DynamoDB) via foreachBatch

### ML Pipelines
11. **MLTrainingChampionChallengerPipeline** - Trains N models from feature table → registers → promotes winner via config threshold
12. **MLBatchScoringPipeline** - Daily/weekly batch scoring of gold/feature table → writes scored table
13. **MLRealtimeScoringPipeline** - Structured Streaming scoring with model cache and shadow mode option
14. **ModelDriftMonitoringPipeline** - Compares prediction vs feature drift daily → alerts if PSI > threshold

### Quality & Governance
15. **DataQualityExpectationsPipeline** - Runs Great Expectations suite defined in config → quarantines bad data
16. **DataContractEnforcementPipeline** - Validates row counts, freshness, schema from config → fails job on breach
17. **BackfillHistoricalPipeline** - Parallel backfill of any pipeline from start_date to end_date using dynamic task mapping
18. **CatchUpAfterOutagePipeline** - Detects lag via watermark table → runs parallel catch-up slices
19. **TableReconciliationPipeline** - Compares row counts + row hash between source and target → reports mismatches

### Maintenance
20. **VacuumAndOptimizeMaintenancePipeline** - Runs VACUUM + OPTIMIZE + ZORDER on tables listed in config
21. **ZOrderAndAnalyzePipeline** - Z-orders by config columns + ANALYZE for statistics
22. **UnityCatalogGovernanceSyncPipeline** - Applies grants, tags, and ownership from config YAML
23. **CostAttributionTaggingPipeline** - Adds job_id, pipeline_name, business_unit tags to all Delta tables written
24. **DisasterRecoveryReplicationPipeline** - Replicates configured tables to secondary region/catalog

## B. Multi-Orchestrator Adapters (25)

### Databricks (5)
25. **DatabricksWorkflowsJobAdapter** - Creates single-task Databricks job from pipeline config
26. **DatabricksWorkflowsMultiTaskAdapter** - Builds multi-task DAG with dependencies in Databricks Workflows
27. **DatabricksWorkflowsDeltaLiveTablesAdapter** - Generates DLT pipeline.py from config (expectations, target tables)
28. **DatabricksWorkflowsNotebookTaskAdapter** - Wraps any notebook path as task with parameters from config
29. **DatabricksWorkflowsRepairRunAdapter** - Adds repair-run capability to any failed pipeline

### Airflow (5)
30. **AirflowTaskFlowDecoratorAdapter** - Converts pipeline.build() into @task decorated Airflow TaskFlow DAG
31. **AirflowDynamicTaskMappingAdapter** - Maps backfill dates or partitions dynamically in Airflow
32. **AirflowExternalTaskSensorAdapter** - Waits for upstream pipeline in another DAG
33. **AirflowSlackWebhookOperatorAdapter** - Sends success/failure messages to Slack channel from config
34. **AirflowDatabricksSubmitRunOperatorAdapter** - Triggers Databricks job from Airflow

### Dagster (5)
35. **DagsterSoftwareDefinedAssetsAdapter** - Turns pipeline into @asset with freshness policies
36. **DagsterOpFactoryAdapter** - Generates reusable ops from task definitions
37. **DagsterScheduleAndSensorAdapter** - Creates cron schedules and file-arrival sensors
38. **DagsterFreshnessPolicyEnforcerAdapter** - Enforces max age on gold tables
39. **DagsterUnityCatalogIntegrationAdapter** - Registers assets in Unity Catalog lineage

### Prefect (5)
40. **PrefectFlowDecoratorAdapter** - Converts pipeline to @flow with parameters from config
41. **PrefectDeploymentBuilderAdapter** - Generates deployment YAML from config
42. **PrefectAutomationsAlertingAdapter** - Creates failure/success automations (Slack/Teams)
43. **PrefectParameterisedRunAdapter** - Allows ad-hoc runs with parameter overrides
44. **PrefectResultPersistenceDeltaAdapter** - Persists intermediate results to Delta for debugging

### Mage (5)
45. **MagePipelineBlockAdapter** - Generates Mage pipeline with blocks from config
46. **MageDataLoaderBlockAdapter** - Maps ingestors to Mage data loader blocks
47. **MageTransformerBlockAdapter** - Maps transformer chain to Mage transformer blocks
48. **MageDataExporterBlockAdapter** - Maps write tasks to Mage data exporter blocks
49. **MageTriggerSchedulerAdapter** - Adds time-based or event-based triggers in Mage

## C. Task Building Blocks (22)

### Ingest & Transform
50. **IngestTask** - Executes any registered ingestor with parameters
51. **TransformTask** - Applies a list of transformer functions in order

### Write
52. **WriteDeltaTask** - Writes DataFrame to Delta with mode, partitionBy, zOrderBy
53. **WriteFeatureTableTask** - Writes to Unity Catalog Feature Store with primary keys
54. **CreateUnityCatalogTableTask** - Creates external/managed Delta table with schema

### ML
55. **TrainMLModelTask** - Trains and logs model using ml/ training components
56. **BatchScoreTask** - Scores a table using latest/champion model
57. **RegisterModelTask** - Registers scored model to Unity Catalog Model Registry
58. **PromoteModelChampionTask** - Compares metrics → aliases "champion"

### Quality
59. **RunGreatExpectationsTask** - Executes a GE expectation suite → fails or quarantines
60. **RunDbtCoreTask** - Executes dbt run/models on a profile (wrapper)
61. **RunSQLFileTask** - Executes parameterized SQL file against catalog

### Orchestration
62. **RunNotebookTask** - Runs a notebook with widgets/parameters
63. **TriggerDownstreamPipelineTask** - Triggers another pipeline via API
64. **AutoRecoverTask** - Retries with exponential backoff + quarantines bad input

### Governance
65. **GrantPermissionsTask** - Applies GRANT statements from YAML
66. **WaitForTableFreshnessTask** - Polls until watermark ≥ expected

### Notification
67. **SendSlackAlertTask** - Sends formatted message to Slack webhook
68. **SendEmailAlertTask** - Sends HTML email via SES/SMTP
69. **NotifyOnFailureTask** - Centralised on-failure callback (Slack + email + PagerDuty)

### Observability
70. **MonteCarloDataIncidentTask** - Opens/closes incidents via MonteCarlo API
71. **LightupDataSLAAlertTask** - Calls Lightup SLA webhook on breach

## D. Scheduling & Trigger Patterns (12)

### Cron Schedules
72. **DailyAtMidnightSchedule** - Cron 0 0 * * * with catchup
73. **HourlySchedule** - Every hour on the hour
74. **Every15MinutesSchedule** - 0,15,30,45 * * * *

### Event-Driven Triggers
75. **EventDrivenFileArrivalTrigger** - S3/ADLS event or Unity Catalog table change feed
76. **EventDrivenKafkaLagTrigger** - Triggers when consumer lag > threshold
77. **EventDrivenTableUpdatedTrigger** - Reacts to new Delta commits via change data feed

### Advanced Scheduling
78. **CronExpressionBuilder** - Helper to build complex cron with holidays excluded
79. **CatchupBackfillSchedule** - Allows backfill=true without duplicating runs
80. **HolidayCalendarSkip** - Skips runs on country-specific holidays

### Monitoring
81. **DependencyChainBuilder** - Programmatically wires task dependencies
82. **SLAMonitor** - Measures end-to-end latency + row counts → alerts if breached
83. **DataFreshnessMonitor** - Compares max(event_date) vs expectations
84. **PipelineLineageVisualizer** - Generates HTML/OpenLineage JSON visualization of full config-driven DAG

## Total: 84 Production-Ready Components

All components are:
- ✅ 100% config-driven (ZERO hard-coded values)
- ✅ Read from config/{pipeline_name}/{env}.json
- ✅ Support multiple orchestrators (Databricks, Airflow, Dagster, Prefect, Mage)
- ✅ Production-ready with error handling and observability
- ✅ Full lineage tracking and monitoring
