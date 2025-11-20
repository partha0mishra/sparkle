"""
Anomaly & Fraud Detection Components (11 components).

Production-ready anomaly detection and fraud prevention systems.
"""

from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, avg, stddev, count, sum as spark_sum, lag, datediff
from pyspark.sql.window import Window
from .base import BaseModelTrainer
from .factory import register_ml_component
import mlflow


@register_ml_component("isolation_forest_trainer")
class IsolationForestTrainer(BaseModelTrainer):
    """
    Isolation Forest for unsupervised anomaly detection.

    Config example:
        {
            "train_table": "gold.transaction_features",
            "feature_columns": ["amount", "velocity", "distance"],
            "contamination": 0.05,
            "n_estimators": 100,
            "max_samples": 256,
            "output_table": "gold.anomaly_scores",
            "model_name": "isolation_forest_anomaly"
        }
    """

    def execute(self) -> Dict[str, Any]:
        """Train Isolation Forest."""
        from sklearn.ensemble import IsolationForest

        train_df = self.spark.table(self.config["train_table"])
        feature_cols = self.config["feature_columns"]

        # Convert to pandas
        train_pdf = train_df.toPandas()
        X_train = train_pdf[feature_cols]

        # Train
        model = IsolationForest(
            contamination=self.config.get("contamination", 0.05),
            n_estimators=self.config.get("n_estimators", 100),
            max_samples=self.config.get("max_samples", 256),
            random_state=42
        )
        model.fit(X_train)

        # Score
        anomaly_scores = model.decision_function(X_train)
        train_pdf["anomaly_score"] = anomaly_scores
        train_pdf["is_anomaly"] = model.predict(X_train) == -1

        # Convert back to Spark
        result = self.spark.createDataFrame(train_pdf)
        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        # Metrics
        num_anomalies = train_pdf["is_anomaly"].sum()
        anomaly_rate = num_anomalies / len(train_pdf)

        self.log_metrics({
            "num_anomalies": int(num_anomalies),
            "anomaly_rate": float(anomaly_rate)
        })

        # Log model
        mlflow.sklearn.log_model(model, "model")
        model_uri = self.register_model_to_uc(model, self.config["model_name"])

        return {
            "model_uri": model_uri,
            "num_anomalies": int(num_anomalies),
            "anomaly_rate": float(anomaly_rate)
        }


@register_ml_component("oneclass_svm_trainer")
class OneClassSVMTrainer(BaseModelTrainer):
    """
    One-Class SVM for novelty detection.

    Config example:
        {
            "train_table": "gold.normal_transactions",
            "feature_columns": ["amount", "frequency", "recency"],
            "nu": 0.05,
            "kernel": "rbf",
            "gamma": "auto",
            "output_table": "gold.svm_anomalies",
            "model_name": "oneclass_svm"
        }
    """

    def execute(self) -> Dict[str, Any]:
        """Train One-Class SVM."""
        from sklearn.svm import OneClassSVM

        train_df = self.spark.table(self.config["train_table"])
        feature_cols = self.config["feature_columns"]

        # Convert to pandas
        train_pdf = train_df.toPandas()
        X_train = train_pdf[feature_cols]

        # Train
        model = OneClassSVM(
            nu=self.config.get("nu", 0.05),
            kernel=self.config.get("kernel", "rbf"),
            gamma=self.config.get("gamma", "auto")
        )
        model.fit(X_train)

        # Score
        predictions = model.predict(X_train)
        decision_scores = model.decision_function(X_train)

        train_pdf["is_anomaly"] = predictions == -1
        train_pdf["anomaly_score"] = decision_scores

        # Convert back
        result = self.spark.createDataFrame(train_pdf)
        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        # Log
        mlflow.sklearn.log_model(model, "model")

        return {"num_anomalies": int((predictions == -1).sum())}


@register_ml_component("autoencoder_anomaly")
class AutoencoderAnomaly(BaseModelTrainer):
    """
    Autoencoder for anomaly detection via reconstruction error.

    Config example:
        {
            "train_table": "gold.transaction_features",
            "feature_columns": ["feature1", "feature2", "feature3"],
            "encoding_dim": 16,
            "epochs": 50,
            "batch_size": 256,
            "anomaly_threshold_percentile": 95,
            "model_name": "autoencoder_anomaly"
        }
    """

    def execute(self) -> Dict[str, Any]:
        """Train autoencoder for anomaly detection."""
        import tensorflow as tf
        from tensorflow import keras
        import numpy as np

        train_df = self.spark.table(self.config["train_table"])
        feature_cols = self.config["feature_columns"]
        encoding_dim = self.config.get("encoding_dim", 16)

        # Convert to numpy
        train_pdf = train_df.toPandas()
        X_train = train_pdf[feature_cols].values

        # Normalize
        from sklearn.preprocessing import StandardScaler
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)

        input_dim = X_train_scaled.shape[1]

        # Build autoencoder
        encoder = keras.Sequential([
            keras.layers.Dense(64, activation='relu', input_shape=(input_dim,)),
            keras.layers.Dense(32, activation='relu'),
            keras.layers.Dense(encoding_dim, activation='relu')
        ])

        decoder = keras.Sequential([
            keras.layers.Dense(32, activation='relu', input_shape=(encoding_dim,)),
            keras.layers.Dense(64, activation='relu'),
            keras.layers.Dense(input_dim, activation='linear')
        ])

        autoencoder = keras.Sequential([encoder, decoder])
        autoencoder.compile(optimizer='adam', loss='mse')

        # Train
        history = autoencoder.fit(
            X_train_scaled, X_train_scaled,
            epochs=self.config.get("epochs", 50),
            batch_size=self.config.get("batch_size", 256),
            validation_split=0.2,
            verbose=0
        )

        # Calculate reconstruction errors
        reconstructions = autoencoder.predict(X_train_scaled)
        reconstruction_errors = np.mean(np.square(X_train_scaled - reconstructions), axis=1)

        # Determine threshold
        threshold_percentile = self.config.get("anomaly_threshold_percentile", 95)
        threshold = np.percentile(reconstruction_errors, threshold_percentile)

        train_pdf["reconstruction_error"] = reconstruction_errors
        train_pdf["is_anomaly"] = reconstruction_errors > threshold

        # Convert back
        result = self.spark.createDataFrame(train_pdf)
        result.write.mode("overwrite").saveAsTable(self.config.get("output_table", "gold.autoencoder_anomalies"))

        # Log
        mlflow.tensorflow.log_model(autoencoder, "model")

        self.log_metrics({
            "reconstruction_threshold": float(threshold),
            "final_loss": float(history.history['loss'][-1])
        })

        return {"threshold": float(threshold)}


@register_ml_component("prophet_anomaly_detector")
class ProphetAnomalyDetector(BaseModelTrainer):
    """
    Time-series anomaly detection using Prophet forecasting.

    Config example:
        {
            "input_table": "gold.timeseries_metrics",
            "timestamp_column": "timestamp",
            "value_column": "metric_value",
            "anomaly_threshold": 3.0,
            "output_table": "gold.timeseries_anomalies"
        }
    """

    def execute(self) -> DataFrame:
        """Detect anomalies using Prophet."""
        from prophet import Prophet

        df = self.spark.table(self.config["input_table"])
        timestamp_col = self.config["timestamp_column"]
        value_col = self.config["value_column"]

        # Convert to pandas
        pdf = df.toPandas()
        pdf = pdf.rename(columns={timestamp_col: "ds", value_col: "y"})

        # Train Prophet
        model = Prophet(interval_width=0.99)
        model.fit(pdf)

        # Forecast
        forecast = model.predict(pdf)

        # Calculate anomalies (values outside prediction interval)
        threshold = self.config.get("anomaly_threshold", 3.0)

        pdf["yhat"] = forecast["yhat"]
        pdf["yhat_lower"] = forecast["yhat_lower"]
        pdf["yhat_upper"] = forecast["yhat_upper"]

        pdf["is_anomaly"] = (pdf["y"] < pdf["yhat_lower"]) | (pdf["y"] > pdf["yhat_upper"])
        pdf["anomaly_score"] = abs(pdf["y"] - pdf["yhat"]) / (pdf["yhat_upper"] - pdf["yhat_lower"])

        # Convert back
        result = self.spark.createDataFrame(pdf)
        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        num_anomalies = pdf["is_anomaly"].sum()
        self.log_metrics({"num_anomalies": int(num_anomalies)})

        return result


@register_ml_component("transaction_rule_engine")
class TransactionRuleEngine(BaseModelTrainer):
    """
    Rule-based fraud detection engine.

    Config example:
        {
            "input_table": "gold.transactions",
            "output_table": "gold.flagged_transactions",
            "rules": [
                {"rule": "amount > 10000", "risk_score": 50},
                {"rule": "velocity_24h > 10", "risk_score": 30},
                {"rule": "distance_from_home > 1000", "risk_score": 40}
            ],
            "threshold": 70
        }
    """

    def execute(self) -> DataFrame:
        """Apply fraud detection rules."""
        df = self.spark.table(self.config["input_table"])
        rules = self.config["rules"]
        threshold = self.config.get("threshold", 70)

        # Initialize risk score
        result = df.withColumn("risk_score", lit(0))

        # Apply each rule
        for rule in rules:
            rule_condition = rule["rule"]
            risk_score = rule["risk_score"]

            result = result.withColumn(
                "risk_score",
                when(expr(rule_condition), col("risk_score") + lit(risk_score))
                .otherwise(col("risk_score"))
            )

        # Flag high-risk transactions
        result = result.withColumn("is_flagged", col("risk_score") >= threshold)

        # Write
        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        num_flagged = result.filter(col("is_flagged")).count()
        self.log_metrics({"num_flagged": num_flagged})

        return result


@register_ml_component("graphsage_fraud")
class GraphSAGEFraud(BaseModelTrainer):
    """
    Graph neural network for fraud detection.

    Config example:
        {
            "nodes_table": "gold.transaction_nodes",
            "edges_table": "gold.transaction_edges",
            "node_features": ["amount", "velocity"],
            "hidden_dim": 64,
            "num_layers": 2,
            "epochs": 100,
            "model_name": "graphsage_fraud"
        }
    """

    def execute(self) -> Dict[str, Any]:
        """Train GraphSAGE for fraud detection."""
        # Note: Simplified - production would use DGL or PyG

        nodes_df = self.spark.table(self.config["nodes_table"])
        edges_df = self.spark.table(self.config["edges_table"])

        # Convert to graph format
        nodes_pdf = nodes_df.toPandas()
        edges_pdf = edges_df.toPandas()

        self.logger.info(f"Building graph with {len(nodes_pdf)} nodes and {len(edges_pdf)} edges")

        # Placeholder for actual GraphSAGE training
        # Would use DGL or PyTorch Geometric in production

        return {
            "num_nodes": len(nodes_pdf),
            "num_edges": len(edges_pdf)
        }


@register_ml_component("velocity_link_analysis")
class VelocityLinkAnalysis(BaseModelTrainer):
    """
    Velocity checks and link analysis for fraud detection.

    Config example:
        {
            "transactions_table": "gold.transactions",
            "user_id_column": "user_id",
            "timestamp_column": "timestamp",
            "amount_column": "amount",
            "velocity_windows": [1, 24, 168],
            "output_table": "gold.velocity_features"
        }
    """

    def execute(self) -> DataFrame:
        """Calculate velocity features."""
        from pyspark.sql.functions import sum as spark_sum, count, unix_timestamp

        df = self.spark.table(self.config["transactions_table"])
        user_col = self.config["user_id_column"]
        timestamp_col = self.config["timestamp_column"]
        amount_col = self.config["amount_column"]
        velocity_windows = self.config.get("velocity_windows", [1, 24, 168])  # hours

        result = df

        # Calculate velocity features for each window
        for window_hours in velocity_windows:
            window_seconds = window_hours * 3600

            window_spec = Window \
                .partitionBy(user_col) \
                .orderBy(col(timestamp_col).cast("long")) \
                .rangeBetween(-window_seconds, 0)

            # Transaction count in window
            result = result.withColumn(
                f"txn_count_{window_hours}h",
                count("*").over(window_spec)
            )

            # Total amount in window
            result = result.withColumn(
                f"total_amount_{window_hours}h",
                spark_sum(amount_col).over(window_spec)
            )

        # Link analysis - find shared attributes
        # (Simplified - production would build full attribution graph)

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result


@register_ml_component("dbscan_clustering")
class DBSCANClustering(BaseModelTrainer):
    """
    DBSCAN clustering for outlier detection.

    Config example:
        {
            "input_table": "gold.transaction_features",
            "feature_columns": ["amount", "velocity", "distance"],
            "eps": 0.5,
            "min_samples": 5,
            "output_table": "gold.dbscan_clusters"
        }
    """

    def execute(self) -> Dict[str, Any]:
        """Run DBSCAN clustering."""
        from sklearn.cluster import DBSCAN
        from sklearn.preprocessing import StandardScaler

        df = self.spark.table(self.config["input_table"])
        feature_cols = self.config["feature_columns"]

        # Convert to pandas
        pdf = df.toPandas()
        X = pdf[feature_cols]

        # Normalize
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # DBSCAN
        dbscan = DBSCAN(
            eps=self.config.get("eps", 0.5),
            min_samples=self.config.get("min_samples", 5)
        )
        clusters = dbscan.fit_predict(X_scaled)

        pdf["cluster"] = clusters
        pdf["is_outlier"] = clusters == -1

        # Convert back
        result = self.spark.createDataFrame(pdf)
        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        num_outliers = (clusters == -1).sum()
        num_clusters = len(set(clusters)) - (1 if -1 in clusters else 0)

        self.log_metrics({
            "num_clusters": int(num_clusters),
            "num_outliers": int(num_outliers)
        })

        return {
            "num_clusters": int(num_clusters),
            "num_outliers": int(num_outliers)
        }


@register_ml_component("supervised_fraud_trainer")
class SupervisedFraudTrainer(BaseModelTrainer):
    """
    Supervised fraud detection with class imbalance handling.

    Config example:
        {
            "train_table": "gold.labeled_transactions",
            "test_table": "gold.test_transactions",
            "feature_columns": ["amount", "velocity", "distance"],
            "target_column": "is_fraud",
            "model_type": "xgboost",
            "handle_imbalance": "smote",
            "model_name": "fraud_classifier"
        }
    """

    def execute(self) -> Dict[str, Any]:
        """Train supervised fraud model."""
        import xgboost as xgb
        from sklearn.metrics import precision_recall_fscore_support, roc_auc_score

        train_df = self.spark.table(self.config["train_table"])
        test_df = self.spark.table(self.config["test_table"])

        # Convert to pandas
        train_pdf = train_df.toPandas()
        test_pdf = test_df.toPandas()

        feature_cols = self.config["feature_columns"]
        target_col = self.config["target_column"]

        X_train = train_pdf[feature_cols]
        y_train = train_pdf[target_col]
        X_test = test_pdf[feature_cols]
        y_test = test_pdf[target_col]

        # Handle class imbalance
        if self.config.get("handle_imbalance") == "smote":
            from imblearn.over_sampling import SMOTE
            smote = SMOTE(random_state=42)
            X_train, y_train = smote.fit_resample(X_train, y_train)

        # Calculate scale_pos_weight for XGBoost
        scale_pos_weight = (y_train == 0).sum() / (y_train == 1).sum()

        # Train XGBoost
        model = xgb.XGBClassifier(
            scale_pos_weight=scale_pos_weight,
            max_depth=6,
            learning_rate=0.1,
            n_estimators=100
        )
        model.fit(X_train, y_train)

        # Evaluate
        y_pred = model.predict(X_test)
        y_pred_proba = model.predict_proba(X_test)[:, 1]

        precision, recall, f1, _ = precision_recall_fscore_support(y_test, y_pred, average='binary')
        auc = roc_auc_score(y_test, y_pred_proba)

        self.log_metrics({
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "auc": auc
        })

        # Log model
        mlflow.xgboost.log_model(model, "model")
        model_uri = self.register_model_to_uc(model, self.config["model_name"])

        return {
            "model_uri": model_uri,
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "auc": auc
        }


@register_ml_component("anomaly_score_calibration")
class AnomalyScoreCalibration(BaseModelTrainer):
    """
    Calibrate anomaly scores to probabilities.

    Config example:
        {
            "scores_table": "gold.anomaly_scores",
            "labels_table": "gold.labeled_anomalies",
            "score_column": "anomaly_score",
            "label_column": "is_anomaly",
            "calibration_method": "isotonic",
            "output_table": "gold.calibrated_scores"
        }
    """

    def execute(self) -> DataFrame:
        """Calibrate anomaly scores."""
        from sklearn.calibration import CalibratedClassifierCV
        from sklearn.isotonic import IsotonicRegression

        scores_df = self.spark.table(self.config["scores_table"])
        labels_df = self.spark.table(self.config["labels_table"])

        # Join scores with labels
        df = scores_df.join(labels_df, on="id", how="inner")
        pdf = df.toPandas()

        score_col = self.config["score_column"]
        label_col = self.config["label_column"]

        scores = pdf[score_col].values.reshape(-1, 1)
        labels = pdf[label_col].values

        # Calibrate
        method = self.config.get("calibration_method", "isotonic")

        if method == "isotonic":
            calibrator = IsotonicRegression(out_of_bounds='clip')
            calibrated = calibrator.fit_transform(scores.flatten(), labels)
        else:
            from sklearn.linear_model import LogisticRegression
            calibrator = LogisticRegression()
            calibrator.fit(scores, labels)
            calibrated = calibrator.predict_proba(scores)[:, 1]

        pdf["calibrated_probability"] = calibrated

        # Convert back
        result = self.spark.createDataFrame(pdf)
        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result


@register_ml_component("fraud_case_manager")
class FraudCaseManager(BaseModelTrainer):
    """
    Manage fraud investigation cases and workflows.

    Config example:
        {
            "flagged_transactions_table": "gold.flagged_transactions",
            "cases_table": "audit.fraud_cases",
            "priority_threshold": 80,
            "auto_block_threshold": 95,
            "assignment_strategy": "round_robin"
        }
    """

    def execute(self) -> Dict[str, Any]:
        """Create and manage fraud cases."""
        from pyspark.sql.functions import current_timestamp, monotonically_increasing_id

        flagged_df = self.spark.table(self.config["flagged_transactions_table"])
        priority_threshold = self.config.get("priority_threshold", 80)
        auto_block_threshold = self.config.get("auto_block_threshold", 95)

        # Create cases
        cases = flagged_df.filter(col("risk_score") >= priority_threshold)

        cases = cases.withColumn("case_id", monotonically_increasing_id())
        cases = cases.withColumn("created_at", current_timestamp())
        cases = cases.withColumn(
            "priority",
            when(col("risk_score") >= auto_block_threshold, lit("critical"))
            .when(col("risk_score") >= 90, lit("high"))
            .when(col("risk_score") >= 80, lit("medium"))
            .otherwise(lit("low"))
        )

        # Auto-block critical cases
        cases = cases.withColumn(
            "status",
            when(col("risk_score") >= auto_block_threshold, lit("auto_blocked"))
            .otherwise(lit("pending_review"))
        )

        # Assign to investigators (simplified round-robin)
        cases = cases.withColumn(
            "assigned_to",
            when(col("priority") == "critical", lit("senior_investigator"))
            .otherwise(lit("investigator"))
        )

        # Write cases
        cases.write.mode("append").saveAsTable(self.config["cases_table"])

        num_cases = cases.count()
        num_critical = cases.filter(col("priority") == "critical").count()
        num_auto_blocked = cases.filter(col("status") == "auto_blocked").count()

        self.log_metrics({
            "num_cases": num_cases,
            "num_critical": num_critical,
            "num_auto_blocked": num_auto_blocked
        })

        return {
            "num_cases": num_cases,
            "num_critical": num_critical,
            "num_auto_blocked": num_auto_blocked
        }
