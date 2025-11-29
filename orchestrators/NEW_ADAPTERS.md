# New Orchestrator Adapters - Complete Documentation

## Overview

Added **15 new orchestrator adapter components** across 3 platforms:

1. **dbt Cloud Adapter** (5 components) - Analytics engineering
2. **AWS Step Functions Adapter** (5 components) - Serverless workflows
3. **Argo Workflows Adapter** (5 components) - Kubernetes-native workflows

Total orchestrator support: **8 platforms** with **40 adapter components**

---

## 1. dbt Cloud Adapter (5 Components)

### Overview

Transforms Sparkle pipelines into dbt projects for analytics engineering workflows.

### Components

#### 1.1 dbtProjectAdapter

**Generates complete dbt project structure**

- `dbt_project.yml` - Project configuration
- `profiles.yml` - Connection profiles
- `models/` - SQL models
- `schema.yml` - Sources and documentation
- `README.md` - Project documentation

**Usage:**
```python
from sparkle.orchestration import Pipeline

pipeline = Pipeline.get("customer_silver_daily", env="prod")
pipeline.build()
pipeline.deploy(orchestrator="dbt")
```

**Generated Files:**
```
dbt_project_name/
├── dbt_project.yml      # Project config
├── profiles.yml         # Databricks connection
├── models/
│   ├── customers.sql    # Generated SQL model
│   └── schema.yml       # Model documentation
├── macros/              # Reusable Jinja macros
├── tests/               # Data quality tests
└── README.md            # Documentation
```

**Example dbt Model (Silver Layer):**
```sql
-- Generated from Sparkle pipeline: customer_silver_daily

{{
  config(
    materialized='incremental',
    unique_key=['customer_id'],
    partition_by=['ingestion_date'],
    tags=['customer', 'silver']
  )
}}

with source as (
    select * from {{ source('bronze', 'customers_raw') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

cleaned as (
    select
        *,
        current_timestamp() as _dbt_loaded_at,
        '{{ invocation_id }}' as _dbt_invocation_id
    from source
)

select * from cleaned
```

#### 1.2 dbtModelAdapter

**Maps Sparkle transformers to dbt SQL**

Converts transformer chains into CTE-based SQL models.

**Supported Transformers:**
- `drop_exact_duplicates` → `SELECT DISTINCT`
- `add_audit_columns` → Timestamp columns
- `add_hash_key` → `MD5()` hash generation
- `standardize_nulls` → `CASE WHEN` logic

**Example:**
```python
# Config with transformers
{
  "transformers": [
    {"name": "drop_exact_duplicates"},
    {"name": "add_audit_columns", "params": {"user": "dbt"}},
    {"name": "add_hash_key", "params": {"source_columns": ["id", "email"]}}
  ]
}

# Generated SQL
with source as (
    select distinct * from ref('source_table')
),

step_2_add_audit_columns as (
    select
        *,
        current_timestamp() as created_at,
        'dbt' as created_by
    from source
),

step_3_add_hash_key as (
    select
        *,
        md5(id || '-' || email) as hash_key
    from step_2_add_audit_columns
)

select * from step_3_add_hash_key
```

#### 1.3 dbtTestAdapter

**Generates dbt tests from quality checks**

Maps data quality expectations to dbt tests.

**Generated Tests:**
- Generic tests (unique, not_null, relationships)
- Singular tests (custom SQL checks)
- Email format validation
- Uniqueness checks
- Null value checks

**Example Test:**
```sql
-- tests/test_customers_null_checks.sql

select *
from {{ ref('customers') }}
where
    customer_id is null or
    email is null or
    registration_date is null
```

#### 1.4 dbtMacroAdapter

**Creates reusable Jinja macros**

Generates macro library for common transformations.

**Generated Macros:**

```sql
-- macros/standardize_nulls.sql
{% macro standardize_nulls(column_name, null_values=['', 'NULL', 'N/A']) -%}
    case
        {% for null_val in null_values %}
        when {{ column_name }} = '{{ null_val }}' then null
        {% endfor %}
        else {{ column_name }}
    end
{%- endmacro %}

-- macros/hash_key.sql
{% macro hash_key(columns) -%}
    md5(concat(
        {%- for col in columns -%}
            coalesce(cast({{ col }} as string), '')
            {%- if not loop.last -%}, '-', {%- endif -%}
        {%- endfor -%}
    ))
{%- endmacro %}
```

#### 1.5 dbtExposureAdapter

**Documents downstream consumers**

Creates exposure definitions for dashboards, reports, ML models.

**Example Exposure:**
```yaml
version: 2
exposures:
  - name: customer_dashboard
    type: dashboard
    maturity: high
    url: https://dashboards.company.com/customers
    description: Customer analytics dashboard
    depends_on:
      - ref('customers')
    owner:
      name: data-team
      email: data@company.com
    tags: [customer, analytics]
```

---

## 2. AWS Step Functions Adapter (5 Components)

### Overview

Generates serverless state machines for AWS Step Functions with full ASL (Amazon States Language) support.

### Components

#### 2.1 StepFunctionsStateMachineAdapter

**Generates complete state machine definition**

Creates ASL JSON with states, transitions, error handling, and retry logic.

**Generated Files:**
- `state_machine.json` - ASL definition
- `cloudformation.yaml` - CloudFormation template
- `main.tf` - Terraform configuration
- `deploy.sh` - Deployment script

**Usage:**
```python
pipeline = Pipeline.get("customer_etl", env="prod")
pipeline.build()
pipeline.deploy(orchestrator="stepfunctions")
```

**Example State Machine:**
```json
{
  "Comment": "State machine for customer_etl",
  "StartAt": "IngestData",
  "States": {
    "IngestData": {
      "Type": "Task",
      "Resource": "arn:aws:states:::databricks:runNow.sync",
      "Parameters": {
        "ClusterId.$": "$.cluster_id",
        "Notebook": {
          "Path": "/Repos/sparkle/notebooks/ingest",
          "BaseParameters": {
            "source_table": "customers",
            "destination_table": "customers_raw"
          }
        }
      },
      "Retry": [
        {
          "ErrorEquals": ["States.Timeout"],
          "IntervalSeconds": 60,
          "MaxAttempts": 3,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "ResultPath": "$.error",
          "Next": "HandleFailure"
        }
      ],
      "Next": "TransformData"
    },
    "TransformData": {
      "Type": "Task",
      "Resource": "arn:aws:states:::databricks:runNow.sync",
      "End": true
    },
    "HandleFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:us-east-1:123456789012:pipeline-failure",
        "Message.$": "$"
      },
      "End": true
    }
  }
}
```

**Deployment:**
```bash
# Using CloudFormation
cd deployments/step_functions/customer_etl
./deploy.sh prod

# Or manually
aws cloudformation deploy \
    --template-file cloudformation.yaml \
    --stack-name customer-etl-prod \
    --capabilities CAPABILITY_IAM

# Start execution
aws stepfunctions start-execution \
    --state-machine-arn arn:aws:states:us-east-1:123456789012:stateMachine:customer-etl-prod \
    --input '{"cluster_id": "0123-456789-abc123"}'
```

#### 2.2 StepFunctionsTaskAdapter

**Converts Sparkle tasks to Step Functions states**

Maps task types to appropriate state types.

**State Types:**
- **Task** - Execute Databricks job, Lambda, ECS task
- **Pass** - Pass-through state
- **Wait** - Delay execution
- **Succeed/Fail** - Terminal states

**Features:**
- Automatic retry strategies
- Error handling with Catch
- Result path manipulation
- Parameter passing

#### 2.3 StepFunctionsChoiceAdapter

**Conditional branching logic**

Creates Choice states for dynamic routing.

**Example:**
```json
{
  "CheckEnvironment": {
    "Type": "Choice",
    "Choices": [
      {
        "Variable": "$.environment",
        "StringEquals": "prod",
        "Next": "ProductionPath"
      },
      {
        "Variable": "$.environment",
        "StringEquals": "dev",
        "Next": "DevelopmentPath"
      }
    ],
    "Default": "DevelopmentPath"
  }
}
```

#### 2.4 StepFunctionsParallelAdapter

**Parallel execution support**

Enables concurrent task execution.

**Patterns:**
- **Parallel** - Fixed parallel branches
- **Map** - Dynamic parallel over array

**Example Map State:**
```json
{
  "ProcessPartitions": {
    "Type": "Map",
    "ItemsPath": "$.partitions",
    "MaxConcurrency": 10,
    "Iterator": {
      "StartAt": "ProcessPartition",
      "States": {
        "ProcessPartition": {
          "Type": "Task",
          "Resource": "arn:aws:states:::databricks:runNow.sync",
          "End": true
        }
      }
    },
    "ResultPath": "$.results"
  }
}
```

#### 2.5 StepFunctionsEventBridgeAdapter

**Event-driven triggering**

Generates EventBridge rules for state machine invocation.

**Supported Events:**
- S3 object creation
- DynamoDB streams
- CloudWatch Events
- Cron schedules
- Custom events

**Example Rule:**
```yaml
Resources:
  EventRule:
    Type: AWS::Events::Rule
    Properties:
      Name: customer-etl-trigger
      ScheduleExpression: cron(0 2 * * ? *)  # Daily at 2 AM
      State: ENABLED
      Targets:
        - Arn: !Ref StateMachine
          RoleArn: !GetAtt EventBridgeRole.Arn
```

---

## 3. Argo Workflows Adapter (5 Components)

### Overview

Kubernetes-native workflows with DAG support, parallel execution, and event-driven triggering.

### Components

#### 3.1 ArgoWorkflowTemplateAdapter

**Generates Kubernetes WorkflowTemplate CRDs**

Creates reusable workflow templates with all steps and dependencies.

**Generated Files:**
- `workflow-template.yaml` - Reusable template
- `workflow.yaml` - One-time instantiation
- `cron-workflow.yaml` - Scheduled execution
- `deploy.sh` - kubectl deployment script
- `README.md` - Usage documentation

**Usage:**
```python
pipeline = Pipeline.get("customer_etl", env="prod")
pipeline.build()
pipeline.deploy(orchestrator="argo")
```

**Example WorkflowTemplate:**
```yaml
apiVersion: argoproj.io/v1alpha1
kind: WorkflowTemplate
metadata:
  name: customer-etl
  namespace: argo
  labels:
    sparkle.io/pipeline: customer-etl
    sparkle.io/type: silver_batch_transformation
    sparkle.io/env: prod
spec:
  entrypoint: main
  arguments:
    parameters:
      - name: config
        value: '{...}'
  templates:
    - name: main
      dag:
        tasks:
          - name: ingest-data
            template: ingest-data
          - name: transform-data
            template: transform-data
            dependencies: [ingest-data]
          - name: write-to-gold
            template: write-to-gold
            dependencies: [transform-data]

    - name: ingest-data
      container:
        image: apache/spark-py:v3.5.0
        command: [python, /app/run_task.py]
        args:
          - --task-name
          - ingest_data
          - --config
          - '{{inputs.parameters.config}}'
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
          limits:
            memory: "8Gi"
            cpu: "4"
      retryStrategy:
        limit: "3"
        retryPolicy: OnFailure
        backoff:
          duration: "60s"
          factor: 2
```

**Deployment:**
```bash
cd deployments/argo/customer-etl

# Deploy template
./deploy.sh

# Or manually
kubectl apply -f workflow-template.yaml -n argo

# Submit workflow
kubectl create -f workflow.yaml -n argo

# Schedule with cron
kubectl apply -f cron-workflow.yaml -n argo

# Monitor
argo list -n argo
argo get customer-etl-xxxxx -n argo
argo logs customer-etl-xxxxx -n argo
```

#### 3.2 ArgoDagAdapter

**DAG-based workflows**

Complex dependencies with parallel execution.

**Features:**
- Sequential dependencies
- Parallel fan-out/fan-in
- Conditional execution
- Dynamic task generation

**Example:**
```yaml
- name: parallel-processing
  dag:
    tasks:
      # Fan-out: Process 10 partitions in parallel
      - name: process-partition-0
        template: process-partition
        arguments:
          parameters:
            - name: partition
              value: "0"

      - name: process-partition-1
        template: process-partition
        arguments:
          parameters:
            - name: partition
              value: "1"

      # ... (partitions 2-9)

      # Fan-in: Aggregate results
      - name: aggregate-results
        template: aggregate
        dependencies:
          - process-partition-0
          - process-partition-1
          # ... (all partitions)
```

#### 3.3 ArgoStepTemplateAdapter

**Reusable step templates**

Library of common operations.

**Template Types:**
- **Container** - Run container
- **Script** - Inline script execution
- **Resource** - Kubernetes resource manipulation
- **Suspend** - Manual approval gate

**Example Templates:**
```yaml
- name: spark-job
  container:
    image: apache/spark-py:v3.5.0
    command: [spark-submit]
    args:
      - --master
      - k8s://https://kubernetes.default.svc
      - /app/job.py

- name: python-script
  script:
    image: python:3.10
    command: [python]
    source: |
      import pandas as pd
      print("Processing data...")
      df = pd.read_parquet("s3://bucket/data.parquet")
      print(f"Loaded {len(df)} rows")

- name: create-table
  resource:
    action: create
    manifest: |
      apiVersion: v1
      kind: ConfigMap
      metadata:
        name: pipeline-config
```

#### 3.4 ArgoEventSourceAdapter

**Event-driven workflow triggering**

Integrates with Argo Events for reactive workflows.

**Supported Event Sources:**
- S3 (MinIO)
- Kafka
- Webhooks
- Calendar (cron)
- AWS SNS/SQS
- GitHub
- Slack

**Example EventSource:**
```yaml
apiVersion: argoproj.io/v1alpha1
kind: EventSource
metadata:
  name: customer-etl-event-source
  namespace: argo-events
spec:
  minio:
    example:
      bucket:
        name: data-bucket
      endpoint: s3.amazonaws.com
      events: [s3:ObjectCreated:*]
      filter:
        prefix: data/customers/
      accessKey:
        name: aws-secret
        key: accesskey
      secretKey:
        name: aws-secret
        key: secretkey
```

**Sensor (Workflow Trigger):**
```yaml
apiVersion: argoproj.io/v1alpha1
kind: Sensor
metadata:
  name: customer-etl-sensor
spec:
  dependencies:
    - name: s3-dep
      eventSourceName: customer-etl-event-source
      eventName: example
  triggers:
    - template:
        name: workflow-trigger
        argoWorkflow:
          operation: submit
          source:
            resource:
              apiVersion: argoproj.io/v1alpha1
              kind: Workflow
              spec:
                workflowTemplateRef:
                  name: customer-etl
```

#### 3.5 ArgoArtifactAdapter

**Artifact management**

Input/output artifacts between steps.

**Storage Backends:**
- S3
- GCS
- Azure Blob Storage
- Artifactory
- HTTP

**Example:**
```yaml
# Input artifact
inputs:
  artifacts:
    - name: data
      path: /input/data.parquet
      s3:
        key: data/input/2024-01-01/data.parquet

# Output artifact
outputs:
  artifacts:
    - name: results
      path: /output/results.parquet
      s3:
        key: data/output/2024-01-01/results.parquet
```

---

## Comparison Matrix

| Feature | dbt Cloud | Step Functions | Argo Workflows |
|---------|-----------|----------------|----------------|
| **Platform** | Analytics Engineering | AWS Serverless | Kubernetes |
| **Best For** | SQL transformations | AWS-native workflows | K8s-native pipelines |
| **Language** | SQL + Jinja | JSON (ASL) | YAML (K8s CRDs) |
| **Parallel Execution** | No | Yes (Parallel/Map) | Yes (DAG) |
| **Event-Driven** | Via dbt Cloud API | EventBridge | Argo Events |
| **Scheduling** | dbt Cloud scheduler | EventBridge | CronWorkflow |
| **Cost Model** | Per-seat + compute | Per state transition | K8s cluster cost |
| **Testing** | Built-in (dbt test) | Manual | Container-based |
| **Observability** | dbt Cloud UI | CloudWatch | Argo UI + Prometheus |

---

## Migration Guide

### From Databricks Workflows to Step Functions

```python
# Original config
{
  "pipeline_type": "silver_batch_transformation",
  "destination_table": "customers"
}

# Databricks deployment
pipeline.deploy(orchestrator="databricks")  # → Jobs UI

# Step Functions deployment
pipeline.deploy(orchestrator="stepfunctions")  # → State Machines
```

### From Airflow to Argo

```python
# Original Airflow DAG
pipeline.deploy(orchestrator="airflow")  # → Python DAG file

# Argo Workflows deployment
pipeline.deploy(orchestrator="argo")  # → WorkflowTemplate CRD
```

### From Dagster to dbt

```python
# Original Dagster asset
pipeline.deploy(orchestrator="dagster")  # → @asset

# dbt project
pipeline.deploy(orchestrator="dbt")  # → dbt models/
```

---

## Best Practices

### dbt Cloud

1. **Version control** - Keep generated dbt projects in git
2. **Environment separation** - Use separate profiles for dev/qa/prod
3. **Documentation** - Leverage dbt docs for data catalog
4. **Testing** - Add custom tests for business logic
5. **Incremental models** - Use for large tables

### Step Functions

1. **Error handling** - Always use Retry and Catch
2. **Timeouts** - Set reasonable TimeoutSeconds
3. **Cost optimization** - Use Express workflows for high-volume
4. **Monitoring** - Enable CloudWatch Logs
5. **Parallel limits** - Use MaxConcurrency in Map states

### Argo Workflows

1. **Resource limits** - Set memory/CPU limits
2. **Artifact storage** - Configure artifact repository
3. **Security** - Use RBAC and ServiceAccounts
4. **Retries** - Configure retry strategies
5. **Cleanup** - Use TTL strategy for old workflows

---

## Next Steps

### Additional Orchestrators to Implement

Based on enterprise adoption, consider adding:

1. **Apache NiFi** (5 components) - Visual data flows
2. **Luigi** (5 components) - Python-based pipelines
3. **Kubeflow Pipelines** (5 components) - ML workflows
4. **Temporal** (5 components) - Durable execution
5. **Azure Data Factory** (5 components) - Azure-native

Total potential: **65 orchestrator components** across **13 platforms**!

---

## Resources

### dbt Cloud

- [dbt Docs](https://docs.getdbt.com/)
- [dbt Cloud](https://www.getdbt.com/product/dbt-cloud/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)

### AWS Step Functions

- [Step Functions Developer Guide](https://docs.aws.amazon.com/step-functions/)
- [ASL Specification](https://states-language.net/)
- [Best Practices](https://docs.aws.amazon.com/step-functions/latest/dg/bp-express.html)

### Argo Workflows

- [Argo Workflows Docs](https://argoproj.github.io/argo-workflows/)
- [Argo Events](https://argoproj.github.io/argo-events/)
- [Examples](https://github.com/argoproj/argo-workflows/tree/master/examples)

---

## Support

For issues or questions:
- Review generated README in deployment directory
- Check orchestrator-specific documentation
- Validate configuration with `sparkle-orchestration validate`
- Test locally before production deployment
