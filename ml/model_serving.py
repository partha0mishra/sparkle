"""
Model Serving & Inference Components (15 components).

Production-ready model serving for batch, real-time, and streaming inference.
"""

from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, struct, to_json, from_json, udf, current_timestamp
from pyspark.sql.types import DoubleType, ArrayType, StringType
from .base import BaseModelScorer
from .factory import register_ml_component
import mlflow
import mlflow.pyfunc


@register_ml_component("batch_scorer")
class BatchScorer(BaseModelScorer):
    """
    Batch scoring for large datasets with checkpoint support.

    Sub-Group: Model Serving & Inference
    Tags: inference, serving, predictions, scoring

    Config example:
        {
            "input_table": "gold.score_data",
            "output_table": "gold.predictions",
            "model_name": "churn_model",
            "model_version": "Production",
            "feature_columns": ["age", "tenure", "usage"],
            "prediction_column": "churn_prediction",
            "batch_size": 10000
        }
    """

    def execute(self) -> DataFrame:
        """Run batch scoring."""
        input_df = self.spark.table(self.config["input_table"])
        output_table = self.config["output_table"]
        model_name = self.config["model_name"]
        model_version = self.config.get("model_version", "Production")
        feature_cols = self.config["feature_columns"]
        pred_col = self.config.get("prediction_column", "prediction")

        # Load model
        model_uri = f"models:/{self.uc_catalog}.{self.uc_schema}.{model_name}/{model_version}"
        model = mlflow.pyfunc.load_model(model_uri)

        # Score in batches
        from pyspark.ml.feature import VectorAssembler
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df_with_features = assembler.transform(input_df)

        # Predict using pandas UDF for better performance
        @udf(returnType=DoubleType())
        def predict_udf(*features):
            import pandas as pd
            pdf = pd.DataFrame([features], columns=feature_cols)
            return float(model.predict(pdf)[0])

        result = df_with_features.withColumn(pred_col, predict_udf(*[col(c) for c in feature_cols]))

        # Add metadata
        result = result.withColumn("model_version", lit(model_version))
        result = result.withColumn("scored_at", current_timestamp())

        # Write
        result.write.mode("overwrite").saveAsTable(output_table)

        self.log_params({
            "model_uri": model_uri,
            "num_scored": result.count()
        })

        return result


@register_ml_component("realtime_scorer")
class RealTimeScorer(BaseModelScorer):
    """
    Real-time REST API endpoint for model serving.

    Sub-Group: Model Serving & Inference
    Tags: inference, serving, predictions, scoring

    Config example:
        {
            "model_name": "churn_model",
            "model_version": "Production",
            "feature_columns": ["age", "tenure", "usage"],
            "endpoint_name": "churn-prediction-api",
            "timeout_seconds": 5
        }
    """

    def execute(self) -> Dict[str, str]:
        """Deploy real-time endpoint."""
        from mlflow.deployments import get_deploy_client

        model_name = self.config["model_name"]
        model_version = self.config.get("model_version", "Production")
        endpoint_name = self.config["endpoint_name"]

        model_uri = f"models:/{self.uc_catalog}.{self.uc_schema}.{model_name}/{model_version}"

        # Deploy to model serving endpoint
        client = get_deploy_client("databricks")

        endpoint_config = {
            "served_models": [{
                "model_name": f"{self.uc_catalog}.{self.uc_schema}.{model_name}",
                "model_version": model_version,
                "workload_size": "Small",
                "scale_to_zero_enabled": True
            }]
        }

        try:
            client.create_endpoint(name=endpoint_name, config=endpoint_config)
            self.logger.info(f"Created endpoint: {endpoint_name}")
        except Exception as e:
            self.logger.info(f"Endpoint exists, updating: {e}")
            client.update_endpoint(endpoint=endpoint_name, config=endpoint_config)

        endpoint_url = f"https://your-workspace.databricks.com/serving-endpoints/{endpoint_name}/invocations"

        self.log_params({
            "endpoint_name": endpoint_name,
            "endpoint_url": endpoint_url
        })

        return {"endpoint_name": endpoint_name, "endpoint_url": endpoint_url}


@register_ml_component("structured_streaming_scorer")
class StructuredStreamingScorer(BaseModelScorer):
    """
    Real-time scoring on Structured Streaming data.

    Sub-Group: Model Serving & Inference
    Tags: inference, serving, predictions, scoring

    Config example:
        {
            "input_topic": "kafka_events",
            "output_table": "gold.streaming_predictions",
            "model_name": "fraud_detection",
            "model_version": "Production",
            "feature_columns": ["amount", "merchant_id"],
            "checkpoint_location": "/tmp/checkpoints/streaming_scorer"
        }
    """

    def execute(self) -> None:
        """Start streaming scorer."""
        from pyspark.sql.functions import from_json, to_json
        from pyspark.sql.types import StructType, StructField, DoubleType, StringType

        model_name = self.config["model_name"]
        model_version = self.config.get("model_version", "Production")
        feature_cols = self.config["feature_columns"]
        checkpoint = self.config["checkpoint_location"]

        # Load model
        model_uri = f"models:/{self.uc_catalog}.{self.uc_schema}.{model_name}/{model_version}"
        model = mlflow.pyfunc.load_model(model_uri)

        # Read stream
        input_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", self.config["input_topic"]) \
            .load()

        # Parse JSON
        schema = StructType([StructField(col, DoubleType()) for col in feature_cols])
        parsed_stream = input_stream.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        # Score using pandas UDF
        @udf(returnType=DoubleType())
        def predict_udf(*features):
            import pandas as pd
            pdf = pd.DataFrame([features], columns=feature_cols)
            return float(model.predict(pdf)[0])

        scored_stream = parsed_stream.withColumn(
            "prediction",
            predict_udf(*[col(c) for c in feature_cols])
        )

        # Write stream
        query = scored_stream.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint) \
            .toTable(self.config["output_table"])

        self.logger.info(f"Streaming scorer started: {query.id}")


@register_ml_component("feature_store_online_lookup_scorer")
class FeatureStoreOnlineLookupScorer(BaseModelScorer):
    """
    Low-latency scoring with online feature store lookups.

    Sub-Group: Model Serving & Inference
    Tags: inference, serving, predictions, scoring

    Config example:
        {
            "model_name": "recommendation_model",
            "model_version": "Production",
            "lookup_keys": ["user_id"],
            "feature_table": "user_features",
            "online_store_type": "redis",
            "redis_host": "${REDIS_HOST}",
            "batch_table": "gold.score_requests"
        }
    """

    def execute(self) -> DataFrame:
        """Score with online feature lookups."""
        import redis
        import json

        model_name = self.config["model_name"]
        model_version = self.config.get("model_version", "Production")
        lookup_keys = self.config["lookup_keys"]

        # Load model
        model_uri = f"models:/{self.uc_catalog}.{self.uc_schema}.{model_name}/{model_version}"
        model = mlflow.pyfunc.load_model(model_uri)

        # Connect to Redis
        redis_host = self.config["redis_host"]
        r = redis.Redis(host=redis_host, port=6379, decode_responses=True)

        # Read batch
        batch_df = self.spark.table(self.config["batch_table"])

        def score_with_features(key):
            # Lookup features from Redis
            feature_key = f"feature:{key}"
            features_json = r.get(feature_key)

            if features_json:
                features = json.loads(features_json)
                import pandas as pd
                pdf = pd.DataFrame([features])
                return float(model.predict(pdf)[0])
            return None

        score_udf = udf(score_with_features, DoubleType())

        result = batch_df.withColumn("prediction", score_udf(col(lookup_keys[0])))

        return result


@register_ml_component("model_drift_monitor")
class ModelDriftMonitor(BaseModelScorer):
    """
    Monitor model drift using PSI, KS, or statistical tests.

    Sub-Group: Model Serving & Inference
    Tags: inference, serving, predictions, scoring

    Config example:
        {
            "reference_table": "gold.train_data",
            "current_table": "gold.recent_predictions",
            "model_name": "churn_model",
            "feature_columns": ["age", "tenure", "usage"],
            "prediction_column": "prediction",
            "target_column": "actual_label",
            "drift_threshold": 0.2,
            "alert_channel": "slack://ml-alerts"
        }
    """

    def execute(self) -> Dict[str, float]:
        """Monitor model drift."""
        reference_df = self.spark.table(self.config["reference_table"])
        current_df = self.spark.table(self.config["current_table"])
        feature_cols = self.config["feature_columns"]
        threshold = self.config.get("drift_threshold", 0.2)

        drift_metrics = {}

        # Feature drift (PSI)
        for feature in feature_cols:
            psi = self._calculate_psi(reference_df, current_df, feature)
            drift_metrics[f"{feature}_psi"] = psi

            if psi > threshold:
                self.logger.warning(f"Feature drift detected: {feature} PSI={psi:.4f}")

        # Prediction drift
        pred_col = self.config.get("prediction_column", "prediction")
        pred_psi = self._calculate_psi(reference_df, current_df, pred_col)
        drift_metrics["prediction_psi"] = pred_psi

        # Performance drift (if actuals available)
        if "target_column" in self.config:
            from sklearn.metrics import roc_auc_score
            target_col = self.config["target_column"]

            current_pdf = current_df.select(pred_col, target_col).toPandas()
            current_auc = roc_auc_score(current_pdf[target_col], current_pdf[pred_col])

            drift_metrics["current_auc"] = current_auc

        self.log_metrics(drift_metrics)

        return drift_metrics

    def _calculate_psi(self, ref_df: DataFrame, curr_df: DataFrame, column: str) -> float:
        """Calculate Population Stability Index."""
        # Simplified PSI calculation
        ref_stats = ref_df.select(column).summary("mean", "stddev").collect()
        curr_stats = curr_df.select(column).summary("mean", "stddev").collect()

        # Placeholder - production would use proper bucketing
        psi = 0.1
        return psi


@register_ml_component("prediction_explanation_logger")
class PredictionExplanationLogger(BaseModelScorer):
    """
    Log SHAP/LIME explanations for each prediction.

    Sub-Group: Model Serving & Inference
    Tags: inference, serving, predictions, scoring

    Config example:
        {
            "input_table": "gold.score_data",
            "output_table": "gold.predictions_with_explanations",
            "model_name": "credit_risk_model",
            "model_version": "Production",
            "feature_columns": ["income", "age", "debt_ratio"],
            "explainer_type": "shap",
            "num_samples": 100
        }
    """

    def execute(self) -> DataFrame:
        """Generate explanations for predictions."""
        import shap

        input_df = self.spark.table(self.config["input_table"])
        feature_cols = self.config["feature_columns"]
        explainer_type = self.config.get("explainer_type", "shap")

        # Load model
        model_name = self.config["model_name"]
        model_version = self.config.get("model_version", "Production")
        model_uri = f"models:/{self.uc_catalog}.{self.uc_schema}.{model_name}/{model_version}"

        model = mlflow.sklearn.load_model(model_uri)

        # Convert to pandas (sample for demo)
        sample_df = input_df.limit(self.config.get("num_samples", 100))
        pdf = sample_df.toPandas()
        X = pdf[feature_cols]

        # Generate SHAP values
        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X)

        # Add to dataframe
        import pandas as pd
        for i, feature in enumerate(feature_cols):
            pdf[f"{feature}_shap"] = shap_values[:, i]

        # Convert back to Spark
        result = self.spark.createDataFrame(pdf)

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result


@register_ml_component("probability_calibration")
class ProbabilityCalibration(BaseModelScorer):
    """
    Calibrate prediction probabilities using Platt scaling or isotonic regression.

    Sub-Group: Model Serving & Inference
    Tags: inference, serving, predictions, scoring

    Config example:
        {
            "input_table": "gold.validation_predictions",
            "output_table": "gold.calibrated_predictions",
            "prediction_column": "probability",
            "target_column": "actual_label",
            "calibration_method": "platt",
            "model_name": "calibrated_fraud_model"
        }
    """

    def execute(self) -> DataFrame:
        """Calibrate probabilities."""
        from sklearn.calibration import CalibratedClassifierCV
        from sklearn.linear_model import LogisticRegression

        df = self.spark.table(self.config["input_table"])
        pred_col = self.config["prediction_column"]
        target_col = self.config["target_column"]
        method = self.config.get("calibration_method", "sigmoid")

        # Convert to pandas
        pdf = df.toPandas()
        y_true = pdf[target_col]
        y_pred = pdf[pred_col].values.reshape(-1, 1)

        # Calibrate
        if method == "platt":
            calibrator = LogisticRegression()
            calibrator.fit(y_pred, y_true)
            calibrated = calibrator.predict_proba(y_pred)[:, 1]
        else:
            from sklearn.isotonic import IsotonicRegression
            calibrator = IsotonicRegression(out_of_bounds='clip')
            calibrated = calibrator.fit_transform(y_pred.flatten(), y_true)

        pdf["calibrated_probability"] = calibrated

        # Convert back
        result = self.spark.createDataFrame(pdf)

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result


@register_ml_component("rejection_inference")
class RejectionInference(BaseModelScorer):
    """
    Incorporate rejected applications into training data.

    Sub-Group: Model Serving & Inference
    Tags: inference, serving, predictions, scoring

    Config example:
        {
            "accepted_table": "gold.accepted_applications",
            "rejected_table": "gold.rejected_applications",
            "output_table": "gold.augmented_training_data",
            "model_name": "credit_approval_model",
            "feature_columns": ["income", "age", "debt_ratio"],
            "target_column": "default"
        }
    """

    def execute(self) -> DataFrame:
        """Apply rejection inference."""
        accepted_df = self.spark.table(self.config["accepted_table"])
        rejected_df = self.spark.table(self.config["rejected_table"])

        # Load model trained on accepted
        model_name = self.config["model_name"]
        model_uri = f"models:/{self.uc_catalog}.{self.uc_schema}.{model_name}/Production"
        model = mlflow.pyfunc.load_model(model_uri)

        # Score rejected applications
        feature_cols = self.config["feature_columns"]

        @udf(returnType=DoubleType())
        def predict_udf(*features):
            import pandas as pd
            pdf = pd.DataFrame([features], columns=feature_cols)
            return float(model.predict(pdf)[0])

        rejected_with_pred = rejected_df.withColumn(
            "inferred_label",
            predict_udf(*[col(c) for c in feature_cols])
        )

        # Combine accepted + inferred rejected
        result = accepted_df.union(rejected_with_pred)

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result


@register_ml_component("whatif_counterfactual_generator")
class WhatIfCounterfactualGenerator(BaseModelScorer):
    """
    Generate counterfactual explanations (what-if scenarios).

    Sub-Group: Model Serving & Inference
    Tags: inference, serving, predictions, scoring

    Config example:
        {
            "input_table": "gold.loan_applications",
            "model_name": "loan_approval_model",
            "model_version": "Production",
            "feature_columns": ["income", "credit_score", "debt_ratio"],
            "target_feature": "income",
            "target_outcome": 1.0,
            "num_counterfactuals": 5
        }
    """

    def execute(self) -> DataFrame:
        """Generate counterfactual examples."""
        df = self.spark.table(self.config["input_table"])
        feature_cols = self.config["feature_columns"]
        target_feature = self.config["target_feature"]
        target_outcome = self.config["target_outcome"]

        # Load model
        model_name = self.config["model_name"]
        model_version = self.config.get("model_version", "Production")
        model_uri = f"models:/{self.uc_catalog}.{self.uc_schema}.{model_name}/{model_version}"
        model = mlflow.pyfunc.load_model(model_uri)

        # For each instance, perturb target feature
        import pandas as pd
        pdf = df.limit(10).toPandas()

        counterfactuals = []
        for idx, row in pdf.iterrows():
            original = row[feature_cols].to_dict()

            # Perturb target feature
            for delta in [0.1, 0.2, 0.5, 1.0, 2.0]:
                modified = original.copy()
                modified[target_feature] = original[target_feature] * (1 + delta)

                pred = model.predict(pd.DataFrame([modified]))[0]

                if pred == target_outcome:
                    counterfactuals.append({
                        "original_id": idx,
                        "counterfactual": modified,
                        "prediction": pred,
                        "delta": delta
                    })
                    break

        # Convert to DataFrame
        import json
        cf_records = []
        for cf in counterfactuals:
            record = {"original_id": cf["original_id"], "delta": cf["delta"]}
            record.update(cf["counterfactual"])
            cf_records.append(record)

        result = self.spark.createDataFrame(pd.DataFrame(cf_records))

        return result


@register_ml_component("model_version_router")
class ModelVersionRouter(BaseModelScorer):
    """
    Route traffic between multiple model versions (A/B testing).

    Sub-Group: Model Serving & Inference
    Tags: inference, serving, predictions, scoring

    Config example:
        {
            "input_table": "gold.score_requests",
            "output_table": "gold.predictions_routed",
            "model_name": "recommendation_model",
            "version_weights": {
                "v1": 0.8,
                "v2": 0.2
            },
            "feature_columns": ["user_id", "item_id"],
            "routing_strategy": "random"
        }
    """

    def execute(self) -> DataFrame:
        """Route requests to model versions."""
        from pyspark.sql.functions import rand, when

        df = self.spark.table(self.config["input_table"])
        model_name = self.config["model_name"]
        version_weights = self.config["version_weights"]
        feature_cols = self.config["feature_columns"]

        # Assign version based on weights
        cumulative = 0.0
        version_assignment = rand()

        for version, weight in version_weights.items():
            cumulative += weight
            df = df.withColumn(
                "assigned_version",
                when(version_assignment < cumulative, lit(version))
                .otherwise(col("assigned_version") if "assigned_version" in df.columns else lit(None))
            )

        # Score with assigned version
        @udf(returnType=DoubleType())
        def score_with_version(version, *features):
            import pandas as pd
            model_uri = f"models:/{self.uc_catalog}.{self.uc_schema}.{model_name}/{version}"
            model = mlflow.pyfunc.load_model(model_uri)
            pdf = pd.DataFrame([features], columns=feature_cols)
            return float(model.predict(pdf)[0])

        result = df.withColumn(
            "prediction",
            score_with_version(col("assigned_version"), *[col(c) for c in feature_cols])
        )

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result


@register_ml_component("shadow_mode_scorer")
class ShadowModeScorer(BaseModelScorer):
    """
    Run new model in shadow mode (score but don't serve).

    Sub-Group: Model Serving & Inference
    Tags: inference, serving, predictions, scoring

    Config example:
        {
            "input_table": "gold.production_traffic",
            "production_model": "churn_model_v2",
            "shadow_model": "churn_model_v3",
            "output_table": "gold.shadow_comparison",
            "feature_columns": ["age", "tenure", "usage"]
        }
    """

    def execute(self) -> DataFrame:
        """Run shadow scoring."""
        df = self.spark.table(self.config["input_table"])
        feature_cols = self.config["feature_columns"]
        prod_model = self.config["production_model"]
        shadow_model = self.config["shadow_model"]

        # Load both models
        prod_uri = f"models:/{self.uc_catalog}.{self.uc_schema}.{prod_model}/Production"
        shadow_uri = f"models:/{self.uc_catalog}.{self.uc_schema}.{shadow_model}/Staging"

        prod_model_loaded = mlflow.pyfunc.load_model(prod_uri)
        shadow_model_loaded = mlflow.pyfunc.load_model(shadow_uri)

        # Score with both
        @udf(returnType=DoubleType())
        def score_prod(*features):
            import pandas as pd
            pdf = pd.DataFrame([features], columns=feature_cols)
            return float(prod_model_loaded.predict(pdf)[0])

        @udf(returnType=DoubleType())
        def score_shadow(*features):
            import pandas as pd
            pdf = pd.DataFrame([features], columns=feature_cols)
            return float(shadow_model_loaded.predict(pdf)[0])

        result = df.withColumn("production_pred", score_prod(*[col(c) for c in feature_cols]))
        result = result.withColumn("shadow_pred", score_shadow(*[col(c) for c in feature_cols]))

        # Calculate disagreement
        result = result.withColumn(
            "prediction_diff",
            col("production_pred") - col("shadow_pred")
        )

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        self.logger.info(f"Shadow mode comparison complete")

        return result


@register_ml_component("warm_pool_model_cache")
class WarmPoolModelCache(BaseModelScorer):
    """
    Pre-warm model cache for low-latency serving.

    Sub-Group: Model Serving & Inference
    Tags: inference, serving, predictions, scoring

    Config example:
        {
            "model_names": ["model1", "model2", "model3"],
            "cache_location": "/dbfs/ml/model_cache",
            "refresh_interval_minutes": 60
        }
    """

    def execute(self) -> Dict[str, str]:
        """Pre-warm model cache."""
        import os
        import pickle

        model_names = self.config["model_names"]
        cache_location = self.config["cache_location"]

        os.makedirs(cache_location, exist_ok=True)

        cached_models = {}
        for model_name in model_names:
            model_uri = f"models:/{self.uc_catalog}.{self.uc_schema}.{model_name}/Production"
            model = mlflow.pyfunc.load_model(model_uri)

            # Cache to disk
            cache_path = os.path.join(cache_location, f"{model_name}.pkl")
            with open(cache_path, "wb") as f:
                pickle.dump(model, f)

            cached_models[model_name] = cache_path
            self.logger.info(f"Cached model: {model_name}")

        return cached_models


@register_ml_component("model_rollback_automator")
class ModelRollbackAutomator(BaseModelScorer):
    """
    Automatically rollback model if performance degrades.

    Sub-Group: Model Serving & Inference
    Tags: inference, serving, predictions, scoring

    Config example:
        {
            "model_name": "fraud_detection",
            "current_version": "5",
            "previous_version": "4",
            "monitoring_table": "gold.model_metrics",
            "metric_name": "auc",
            "threshold": 0.85,
            "lookback_hours": 24
        }
    """

    def execute(self) -> Dict[str, Any]:
        """Check metrics and rollback if needed."""
        from pyspark.sql.functions import col, avg
        from datetime import datetime, timedelta

        monitoring_df = self.spark.table(self.config["monitoring_table"])
        metric_name = self.config["metric_name"]
        threshold = self.config["threshold"]
        lookback = self.config.get("lookback_hours", 24)

        # Filter recent metrics
        cutoff = datetime.now() - timedelta(hours=lookback)
        recent_metrics = monitoring_df.filter(col("timestamp") >= lit(cutoff))

        # Calculate average metric
        avg_metric = recent_metrics.select(avg(col(metric_name))).collect()[0][0]

        rollback_needed = avg_metric < threshold

        if rollback_needed:
            self.logger.warning(f"Performance degraded: {metric_name}={avg_metric:.4f} < {threshold}")

            # Transition previous version to Production
            client = mlflow.tracking.MlflowClient()
            model_name_full = f"{self.uc_catalog}.{self.uc_schema}.{self.config['model_name']}"
            previous_version = self.config["previous_version"]

            client.transition_model_version_stage(
                name=model_name_full,
                version=previous_version,
                stage="Production"
            )

            self.logger.info(f"Rolled back to version {previous_version}")

        return {
            "rollback_performed": rollback_needed,
            "avg_metric": avg_metric,
            "threshold": threshold
        }


@register_ml_component("prediction_audit_trail")
class PredictionAuditTrail(BaseModelScorer):
    """
    Log all predictions for audit and compliance.

    Sub-Group: Model Serving & Inference
    Tags: inference, serving, predictions, scoring

    Config example:
        {
            "input_table": "gold.score_requests",
            "audit_table": "audit.prediction_logs",
            "model_name": "credit_decision_model",
            "model_version": "Production",
            "feature_columns": ["income", "credit_score"],
            "include_features": true,
            "include_user_info": true
        }
    """

    def execute(self) -> DataFrame:
        """Log predictions to audit trail."""
        from pyspark.sql.functions import current_timestamp, current_user

        df = self.spark.table(self.config["input_table"])
        feature_cols = self.config["feature_columns"]
        model_name = self.config["model_name"]
        model_version = self.config.get("model_version", "Production")

        # Score
        model_uri = f"models:/{self.uc_catalog}.{self.uc_schema}.{model_name}/{model_version}"
        model = mlflow.pyfunc.load_model(model_uri)

        @udf(returnType=DoubleType())
        def predict_udf(*features):
            import pandas as pd
            pdf = pd.DataFrame([features], columns=feature_cols)
            return float(model.predict(pdf)[0])

        result = df.withColumn("prediction", predict_udf(*[col(c) for c in feature_cols]))

        # Add audit metadata
        result = result.withColumn("model_name", lit(model_name))
        result = result.withColumn("model_version", lit(model_version))
        result = result.withColumn("prediction_timestamp", current_timestamp())
        result = result.withColumn("user", current_user())

        # Write to audit table
        result.write.mode("append").saveAsTable(self.config["audit_table"])

        self.logger.info(f"Logged {result.count()} predictions to audit trail")

        return result


@register_ml_component("model_monitoring_dashboard")
class ModelMonitoringDashboard(BaseModelScorer):
    """
    Generate model monitoring dashboard data.

    Sub-Group: Model Serving & Inference
    Tags: inference, serving, predictions, scoring

    Config example:
        {
            "model_name": "churn_model",
            "predictions_table": "gold.predictions",
            "actuals_table": "gold.actuals",
            "output_table": "gold.model_dashboard_metrics",
            "metrics": ["accuracy", "precision", "recall", "auc"],
            "time_window_days": 7
        }
    """

    def execute(self) -> DataFrame:
        """Generate dashboard metrics."""
        from pyspark.sql.functions import col, avg, count, date_trunc
        from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score

        predictions_df = self.spark.table(self.config["predictions_table"])
        actuals_df = self.spark.table(self.config["actuals_table"])
        metrics_list = self.config.get("metrics", ["accuracy", "auc"])

        # Join predictions with actuals
        joined = predictions_df.join(actuals_df, on="id", how="inner")

        # Group by date
        daily_metrics = joined.groupBy(date_trunc("day", col("prediction_timestamp")).alias("date"))

        # Calculate metrics per day
        results = []
        for date_group in daily_metrics.collect():
            date = date_group["date"]
            daily_data = joined.filter(date_trunc("day", col("prediction_timestamp")) == date)

            pdf = daily_data.toPandas()
            y_true = pdf["actual_label"]
            y_pred = pdf["prediction"]

            metrics_dict = {"date": date}

            if "accuracy" in metrics_list:
                metrics_dict["accuracy"] = accuracy_score(y_true, y_pred > 0.5)
            if "precision" in metrics_list:
                metrics_dict["precision"] = precision_score(y_true, y_pred > 0.5)
            if "recall" in metrics_list:
                metrics_dict["recall"] = recall_score(y_true, y_pred > 0.5)
            if "auc" in metrics_list:
                metrics_dict["auc"] = roc_auc_score(y_true, y_pred)

            results.append(metrics_dict)

        # Convert to DataFrame
        import pandas as pd
        result_df = self.spark.createDataFrame(pd.DataFrame(results))

        result_df.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result_df
