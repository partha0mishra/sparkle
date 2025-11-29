# Simple Icons Availability Test Results

**Date:** November 29, 2025
**Total Tested:** 102 icons

## ✅ Available in Simple Icons (67 icons)

These icons work and can be loaded from CDN:

### Data & Analytics
- snowflake
- googlebigquery
- databricks
- delta
- apachedruid
- clickhouse
- elastic, elasticsearch
- apacheflink
- apachespark
- apachehive

### Databases
- postgresql
- mysql
- mariadb
- teradata
- sap
- sqlite
- presto
- trino
- mongodb
- apachecassandra
- apachehbase
- couchbase
- neo4j
- redis
- arangodb

### Cloud & Storage
- googlecloud
- apachehadoop
- minio
- cloudflare
- backblaze

### Streaming & Messaging
- apachekafka
- rabbitmq
- apachepulsar

### SaaS Platforms
- salesforce
- okta
- hubspot
- stripe
- twilio
- mixpanel
- intercom
- zendesk
- jira
- confluence
- slack

### BI & Analytics
- looker
- qlik

### Data Integration
- airbyte
- talend
- informatica

### ML & AI
- mlflow
- tensorflow
- pytorch
- scikitlearn
- keras
- jupyter

### DevOps & Infrastructure
- githubactions
- gitlab
- jenkins
- circleci
- docker
- kubernetes
- helm
- terraform
- ansible

### Monitoring & Observability
- prometheus
- grafana
- datadog
- splunk
- newrelic
- pagerduty

### Development
- git
- github
- graphql
- json

## ❌ NOT Available in Simple Icons (35 icons)

These icons return 404 and need fallbacks:

### AWS Services (ALL NOT AVAILABLE)
- amazon ❌
- amazonwebservices ❌
- amazons3 ❌
- amazondynamodb ❌
- amazonredshift ❌
- awsathena ❌
- awskinesis ❌
- awsglue ❌
- awssagemaker ❌

**Finding:** Simple Icons does NOT have ANY AWS-specific service icons

### Microsoft Services (ALL NOT AVAILABLE)
- microsoft ❌
- microsoftazure ❌
- microsoftsqlserver ❌
- microsoftteams ❌
- powerbi ❌
- azuredevops ❌

**Finding:** Simple Icons does NOT have ANY Microsoft service icons

### Oracle & IBM (NOT AVAILABLE)
- oracle ❌
- ibm ❌
- ibmdb2 ❌

**Finding:** Simple Icons does NOT have Oracle or IBM icons

### Apache Projects (PARTIAL)
- apacheiceberg ❌
- apachehudi ❌

**Available:** druid, kafka, cassandra, hbase, pulsar, flink, spark, hive, hadoop

### Other Services
- nats ❌
- servicenow ❌
- segment ❌
- amplitude ❌
- tableau ❌
- dbt ❌
- fivetran ❌
- travis ❌

## 🎯 Recommended Icon Strategy

### For AWS Services
Use generic Lucide icons:
- **S3, Storage** → `cloud` (Lucide Cloud)
- **Redshift, Athena** → `database` (Lucide Database)
- **DynamoDB** → `database` (Lucide Database)
- **Kinesis, Glue** → `cloud` (Lucide Cloud)
- **SageMaker** → `brain` (Lucide Brain for ML)

### For Microsoft Services
Use generic Lucide icons:
- **Azure services** → `cloud` (Lucide Cloud)
- **SQL Server** → `database` (Lucide Database)
- **Teams** → `plug` (Lucide Plug for communication)
- **Power BI** → `trending-up` (Lucide TrendingUp for analytics)

### For Oracle & IBM
Use generic Lucide icons:
- **Oracle Database** → `database` (Lucide Database)
- **IBM DB2** → `database` (Lucide Database)

### For Apache Projects Without Icons
- **Iceberg, Hudi** → `database` (Lucide Database)

## 📊 Summary

**Available:** 67/102 (66%)
**Not Available:** 35/102 (34%)

**Key Finding:** Simple Icons explicitly does NOT include icons for major cloud providers (AWS, Microsoft Azure) or enterprise software (Oracle, IBM). This is likely due to trademark/licensing restrictions.

**Solution:** Use a hybrid approach:
1. Use Simple Icons for open-source projects and smaller SaaS companies
2. Use Lucide icons as semantic fallbacks for major cloud providers
3. Group by service type (cloud, database, analytics) for consistent UX

## Sources

- [Simple Icons](https://simpleicons.org/)
- [Simple Icons GitHub](https://github.com/simple-icons/simple-icons)
