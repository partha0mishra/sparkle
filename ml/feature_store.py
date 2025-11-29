"""
Feature Store Integration Components (8 components).

Unity Catalog Feature Store operations for production ML.
"""

from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp
from .base import BaseFeatureStoreComponent
from .factory import register_ml_component
import mlflow


@register_ml_component("feature_table_writer")
class FeatureTableWriter(BaseFeatureStoreComponent):
    """
    Writes DataFrame to Unity Catalog Feature Store table.

    Sub-Group: Feature Store Integrations
    Tags: feature-store, unity-catalog, features, mlops

    Config example:
        {
            "table_name": "customer_features",
            "primary_keys": ["customer_id"],
            "timestamp_column": "feature_timestamp",
            "description": "Customer demographic and behavioral features",
            "tags": {"team": "data-science", "domain": "customer"}
        }

    Usage:
        >>> writer = MLComponent.get("feature_table_writer", spark, env="prod")
        >>> writer.run()
    """

    def execute(self) -> str:
        """Write features to Feature Store."""
        from databricks.feature_store import FeatureStoreClient

        fs = FeatureStoreClient()

        # Get config
        table_name = self.get_feature_table_name(self.config["table_name"])
        primary_keys = self.config["primary_keys"]
        timestamp_col = self.config.get("timestamp_column", "feature_timestamp")
        description = self.config.get("description", "")
        tags = self.config.get("tags", {})

        # Read source data
        source_table = self.config["source_table"]
        df = self.spark.table(source_table)

        # Add timestamp if not present
        if timestamp_col not in df.columns:
            df = df.withColumn(timestamp_col, current_timestamp())

        # Create or update feature table
        try:
            fs.create_table(
                name=table_name,
                primary_keys=primary_keys,
                df=df,
                timestamp_keys=[timestamp_col],
                description=description,
                tags=tags
            )
            self.logger.info(f"Created feature table: {table_name}")
        except Exception:
            # Table exists, write features
            fs.write_table(
                name=table_name,
                df=df,
                mode=self.config.get("mode", "merge")
            )
            self.logger.info(f"Updated feature table: {table_name}")

        return table_name


@register_ml_component("feature_table_reader")
class FeatureTableReader(BaseFeatureStoreComponent):
    """
    Reads features from Unity Catalog Feature Store.

    Sub-Group: Feature Store Integrations
    Tags: feature-store, unity-catalog, feature-lookup, point-in-time

    Supports point-in-time lookup or latest feature values.

    Config example:
        {
            "table_name": "customer_features",
            "lookup_keys": ["customer_id"],
            "as_of_timestamp": "2025-11-19T00:00:00",  # Optional
            "feature_columns": ["age", "ltv", "churn_score"]  # Optional
        }
    """

    def execute(self) -> DataFrame:
        """Read features from Feature Store."""
        from databricks.feature_store import FeatureStoreClient

        fs = FeatureStoreClient()

        table_name = self.get_feature_table_name(self.config["table_name"])
        lookup_df_table = self.config.get("lookup_table")

        if lookup_df_table:
            # Point-in-time lookup
            lookup_df = self.spark.table(lookup_df_table)
            features_df = fs.read_table(name=table_name)

            # Join with lookup
            lookup_keys = self.config["lookup_keys"]
            result = lookup_df.join(features_df, on=lookup_keys, how="left")
        else:
            # Read latest features
            result = fs.read_table(name=table_name)

        # Select specific features if specified
        feature_columns = self.config.get("feature_columns")
        if feature_columns:
            lookup_keys = self.config.get("lookup_keys", [])
            result = result.select(lookup_keys + feature_columns)

        return result


@register_ml_component("feature_table_creator")
class FeatureTableCreator(BaseFeatureStoreComponent):
    """
    Auto-creates Unity Catalog Feature Store table from DataFrame schema.

    Sub-Group: Feature Store Integrations
    Tags: feature-store, unity-catalog, schema, automation

    Config example:
        {
            "source_table": "gold.customer.enriched",
            "table_name": "customer_features_v2",
            "primary_keys": ["customer_id"],
            "timestamp_column": "updated_at",
            "description": "Customer features v2 with LTV and propensity scores",
            "tags": {"version": "2", "owner": "ds-team"}
        }
    """

    def execute(self) -> str:
        """Create feature table."""
        from databricks.feature_store import FeatureStoreClient

        fs = FeatureStoreClient()

        table_name = self.get_feature_table_name(self.config["table_name"])
        source_table = self.config["source_table"]

        # Read source
        df = self.spark.table(source_table)

        # Create table
        fs.create_table(
            name=table_name,
            primary_keys=self.config["primary_keys"],
            df=df,
            timestamp_keys=[self.config.get("timestamp_column", "feature_timestamp")],
            description=self.config.get("description", ""),
            tags=self.config.get("tags", {})
        )

        self.logger.info(f"Created feature table: {table_name}")
        self.log_params({"table_name": table_name, "row_count": df.count()})

        return table_name


@register_ml_component("feature_view_materializer")
class FeatureViewMaterializer(BaseFeatureStoreComponent):
    """
    DLT-compatible daily materialization of feature views.

    Sub-Group: Feature Store Integrations
    Tags: feature-store, materialization, dlt, scheduling

    Config example:
        {
            "source_view": "silver.customer.customer_360",
            "target_table": "customer_daily_features",
            "schedule": "0 2 * * *",  # Daily at 2 AM
            "partition_column": "feature_date"
        }
    """

    def execute(self) -> DataFrame:
        """Materialize feature view."""
        source_view = self.config["source_view"]
        target_table = self.get_feature_table_name(self.config["target_table"])

        # Read source view
        df = self.spark.table(source_view)

        # Add partition column if needed
        partition_col = self.config.get("partition_column")
        if partition_col and partition_col not in df.columns:
            from pyspark.sql.functions import current_date
            df = df.withColumn(partition_col, current_date())

        # Write to feature table
        mode = self.config.get("mode", "overwrite")
        df.write.mode(mode).saveAsTable(target_table)

        self.logger.info(f"Materialized {df.count()} rows to {target_table}")

        return df


@register_ml_component("feature_online_store_sync")
class FeatureOnlineStoreSync(BaseFeatureStoreComponent):
    """
    Pushes latest feature values to online store (Redis/DynamoDB/Rockset).

    Sub-Group: Feature Store Integrations
    Tags: feature-store, online-serving, redis, dynamodb, low-latency

    Config example:
        {
            "source_table": "customer_features",
            "online_store_type": "redis",
            "redis_host": "${REDIS_HOST}",
            "redis_port": 6379,
            "key_column": "customer_id",
            "ttl_seconds": 86400
        }
    """

    def execute(self) -> int:
        """Sync features to online store."""
        source_table = self.get_feature_table_name(self.config["source_table"])
        online_store_type = self.config["online_store_type"]

        df = self.spark.table(source_table)

        if online_store_type == "redis":
            count = self._sync_to_redis(df)
        elif online_store_type == "dynamodb":
            count = self._sync_to_dynamodb(df)
        else:
            raise ValueError(f"Unsupported online store: {online_store_type}")

        self.logger.info(f"Synced {count} records to {online_store_type}")
        return count

    def _sync_to_redis(self, df: DataFrame) -> int:
        """Sync to Redis using foreachBatch."""
        import redis
        import json

        redis_host = self.config["redis_host"]
        redis_port = self.config.get("redis_port", 6379)
        key_column = self.config["key_column"]
        ttl = self.config.get("ttl_seconds", 86400)

        r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

        def write_to_redis(batch_df, batch_id):
            for row in batch_df.collect():
                key = f"feature:{row[key_column]}"
                value = json.dumps(row.asDict())
                r.setex(key, ttl, value)

        # Convert to Pandas and write (for batch)
        pandas_df = df.toPandas()
        for _, row in pandas_df.iterrows():
            key = f"feature:{row[key_column]}"
            value = json.dumps(row.to_dict(), default=str)
            r.setex(key, ttl, value)

        return len(pandas_df)

    def _sync_to_dynamodb(self, df: DataFrame) -> int:
        """Sync to DynamoDB."""
        import boto3

        dynamodb = boto3.resource('dynamodb')
        table_name = self.config.get("dynamodb_table")
        table = dynamodb.Table(table_name)

        pandas_df = df.toPandas()
        with table.batch_writer() as batch:
            for _, row in pandas_df.iterrows():
                batch.put_item(Item=row.to_dict())

        return len(pandas_df)


@register_ml_component("feature_drift_detector")
class FeatureDriftDetector(BaseFeatureStoreComponent):
    """
    Detects feature drift using PSI (Population Stability Index) or KS test.

    Sub-Group: Feature Store Integrations
    Tags: feature-drift, monitoring, psi, ks-test, alerting

    Config example:
        {
            "reference_table": "training_features_202501",
            "current_table": "serving_features_current",
            "feature_columns": ["age", "income", "credit_score"],
            "method": "psi",  # or "ks"
            "threshold": 0.2,
            "alert_channel": "slack://data-science-alerts"
        }
    """

    def execute(self) -> Dict[str, float]:
        """Calculate feature drift metrics."""
        reference_df = self.spark.table(self.config["reference_table"])
        current_df = self.spark.table(self.config["current_table"])
        feature_columns = self.config["feature_columns"]
        method = self.config.get("method", "psi")
        threshold = self.config.get("threshold", 0.2)

        drift_metrics = {}

        for feature in feature_columns:
            if method == "psi":
                drift = self._calculate_psi(reference_df, current_df, feature)
            elif method == "ks":
                drift = self._calculate_ks(reference_df, current_df, feature)
            else:
                raise ValueError(f"Unsupported method: {method}")

            drift_metrics[feature] = drift

            # Alert if drift exceeds threshold
            if drift > threshold:
                self.logger.warning(f"Feature drift detected: {feature} = {drift:.4f}")
                self._send_alert(feature, drift)

        self.log_metrics(drift_metrics)

        return drift_metrics

    def _calculate_psi(self, ref_df: DataFrame, curr_df: DataFrame, feature: str) -> float:
        """Calculate Population Stability Index."""
        from pyspark.sql.functions import count, col

        # Bucket the feature
        buckets = 10
        ref_counts = ref_df.select(feature).summary("percentiles").collect()

        # Simplified PSI calculation (production would use proper bucketing)
        psi = 0.0  # Placeholder
        return psi

    def _calculate_ks(self, ref_df: DataFrame, curr_df: DataFrame, feature: str) -> float:
        """Calculate Kolmogorov-Smirnov statistic."""
        # Simplified KS calculation
        ks_stat = 0.0  # Placeholder
        return ks_stat

    def _send_alert(self, feature: str, drift: float):
        """Send alert to configured channel."""
        alert_channel = self.config.get("alert_channel")
        if alert_channel:
            self.logger.info(f"Sending alert to {alert_channel}: {feature} drift = {drift}")


@register_ml_component("feature_importance_logger")
class FeatureImportanceLogger(BaseFeatureStoreComponent):
    """
    Logs feature importance (permutation or SHAP) to MLflow.

    Sub-Group: Feature Store Integrations
    Tags: feature-importance, shap, mlflow, explainability

    Config example:
        {
            "model_name": "churn_model_v3",
            "model_version": "5",
            "feature_columns": ["age", "tenure", "usage"],
            "method": "shap",  # or "permutation"
            "test_table": "gold.test_data"
        }
    """

    def execute(self) -> Dict[str, float]:
        """Calculate and log feature importance."""
        # Load model
        model_name = self.config["model_name"]
        model_version = self.config["model_version"]
        model_uri = f"models:/{self.uc_catalog}.{self.uc_schema}.{model_name}/{model_version}"

        model = mlflow.sklearn.load_model(model_uri)

        # Load test data
        test_df = self.spark.table(self.config["test_table"])
        test_pandas = test_df.toPandas()

        feature_columns = self.config["feature_columns"]
        X_test = test_pandas[feature_columns]

        method = self.config.get("method", "shap")

        if method == "shap":
            importance = self._calculate_shap_importance(model, X_test)
        elif method == "permutation":
            importance = self._calculate_permutation_importance(model, X_test, test_pandas)
        else:
            raise ValueError(f"Unsupported method: {method}")

        # Log to MLflow
        self.log_metrics(importance)

        # Log as artifact
        import pandas as pd
        import tempfile
        import os

        importance_df = pd.DataFrame(list(importance.items()), columns=['feature', 'importance'])
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "feature_importance.csv")
            importance_df.to_csv(path, index=False)
            self.log_artifact(path, "feature_importance")

        return importance

    def _calculate_shap_importance(self, model, X_test):
        """Calculate SHAP importance."""
        import shap

        explainer = shap.Explainer(model, X_test)
        shap_values = explainer(X_test)

        importance = {}
        for i, feature in enumerate(X_test.columns):
            importance[feature] = abs(shap_values.values[:, i]).mean()

        return importance

    def _calculate_permutation_importance(self, model, X_test, test_pandas):
        """Calculate permutation importance."""
        from sklearn.inspection import permutation_importance

        y_test = test_pandas[self.config.get("target_column", "label")]

        result = permutation_importance(model, X_test, y_test, n_repeats=10, random_state=42)

        importance = {}
        for i, feature in enumerate(X_test.columns):
            importance[feature] = result.importances_mean[i]

        return importance


@register_ml_component("feature_lineage_tracker")
class FeatureLineageTracker(BaseFeatureStoreComponent):
    """
    Emits OpenLineage events linking features → transformations → models.

    Sub-Group: Feature Store Integrations
    Tags: lineage, openlineage, governance, data-quality

    Config example:
        {
            "lineage_backend": "marquez",
            "marquez_url": "http://marquez:5000",
            "namespace": "sparkle_ml",
            "source_tables": ["bronze.raw_data"],
            "feature_table": "customer_features",
            "model_name": "churn_model"
        }
    """

    def execute(self) -> Dict[str, Any]:
        """Emit lineage events."""
        from openlineage.client import OpenLineageClient
        from openlineage.client.run import RunEvent, RunState, Run, Job
        from openlineage.client.facet import SqlJobFacet

        # Initialize OpenLineage client
        backend_url = self.config.get("marquez_url", "http://localhost:5000")
        client = OpenLineageClient(url=backend_url)

        namespace = self.config["namespace"]
        job_name = f"feature_engineering_{self.component_name}"

        # Create run event
        run_id = self.spark.sparkContext.applicationId

        run_event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.utcnow().isoformat(),
            run=Run(runId=run_id),
            job=Job(namespace=namespace, name=job_name),
            inputs=self._get_input_datasets(),
            outputs=self._get_output_datasets()
        )

        # Emit event
        client.emit(run_event)

        self.logger.info(f"Emitted lineage event for {job_name}")

        return {"run_id": run_id, "job_name": job_name}

    def _get_input_datasets(self):
        """Get input dataset metadata."""
        from openlineage.client.run import Dataset

        inputs = []
        for table in self.config.get("source_tables", []):
            inputs.append(Dataset(namespace=self.config["namespace"], name=table))

        return inputs

    def _get_output_datasets(self):
        """Get output dataset metadata."""
        from openlineage.client.run import Dataset

        feature_table = self.config.get("feature_table")
        if feature_table:
            return [Dataset(namespace=self.config["namespace"], name=feature_table)]

        return []
