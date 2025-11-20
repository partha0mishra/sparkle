"""
Responsible AI & Governance Components (8 components).

Production-ready AI governance, fairness, and model management.
"""

from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, avg, count, current_timestamp, datediff
from .base import BaseMLComponent
from .factory import register_ml_component
import mlflow
from datetime import datetime


@register_ml_component("bias_fairness_auditor")
class BiasFairnessAuditor(BaseMLComponent):
    """
    Audit model predictions for bias across protected attributes.

    Config example:
        {
            "predictions_table": "gold.model_predictions",
            "protected_attributes": ["gender", "race", "age_group"],
            "target_column": "label",
            "prediction_column": "prediction",
            "fairness_metrics": ["demographic_parity", "equalized_odds"],
            "output_table": "audit.fairness_metrics"
        }`
    """

    def execute(self) -> Dict[str, Any]:
        """Audit model for bias."""
        from pyspark.sql.functions import sum as spark_sum

        df = self.spark.table(self.config["predictions_table"])
        protected_attrs = self.config["protected_attributes"]
        target_col = self.config["target_column"]
        pred_col = self.config["prediction_column"]

        fairness_results = {}

        for attr in protected_attrs:
            # Get unique values for this attribute
            attr_values = df.select(attr).distinct().rdd.flatMap(lambda x: x).collect()

            # Calculate metrics per group
            for value in attr_values:
                group_df = df.filter(col(attr) == value)
                total_count = group_df.count()

                if total_count == 0:
                    continue

                # Demographic Parity: P(pred=1|attr=value)
                positive_pred_rate = group_df.filter(col(pred_col) == 1).count() / total_count

                # True Positive Rate: P(pred=1|label=1,attr=value)
                positives = group_df.filter(col(target_col) == 1)
                tpr = positives.filter(col(pred_col) == 1).count() / positives.count() if positives.count() > 0 else 0

                # False Positive Rate: P(pred=1|label=0,attr=value)
                negatives = group_df.filter(col(target_col) == 0)
                fpr = negatives.filter(col(pred_col) == 1).count() / negatives.count() if negatives.count() > 0 else 0

                fairness_results[f"{attr}_{value}_positive_rate"] = positive_pred_rate
                fairness_results[f"{attr}_{value}_tpr"] = tpr
                fairness_results[f"{attr}_{value}_fpr"] = fpr

        # Log metrics
        self.log_metrics(fairness_results)

        # Save detailed report
        import pandas as pd
        report_df = pd.DataFrame([fairness_results])
        spark_report = self.spark.createDataFrame(report_df)
        spark_report.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return fairness_results


@register_ml_component("fairness_reweighting")
class FairnessReweighting(BaseMLComponent):
    """
    Reweight training data to improve fairness.

    Config example:
        {
            "train_table": "gold.train_data",
            "protected_attribute": "gender",
            "target_column": "label",
            "output_table": "gold.reweighted_train_data"
        }
    """

    def execute(self) -> DataFrame:
        """Reweight training data."""
        df = self.spark.table(self.config["train_table"])
        protected_attr = self.config["protected_attribute"]
        target_col = self.config["target_column"]

        # Calculate weights to achieve demographic parity
        # Weight = (proportion of group) / (proportion of group with label)

        total_count = df.count()

        # Get counts per group and label
        group_counts = df.groupBy(protected_attr, target_col).count()

        # Calculate overall label distribution
        label_dist = df.groupBy(target_col).count().collect()
        label_proportions = {row[target_col]: row["count"] / total_count for row in label_dist}

        # Calculate group proportions
        group_dist = df.groupBy(protected_attr).count().collect()
        group_proportions = {row[protected_attr]: row["count"] / total_count for row in group_dist}

        # Assign weights
        result = df
        for attr_value in group_proportions.keys():
            for label_value in label_proportions.keys():
                # Calculate expected count
                expected_count = total_count * group_proportions[attr_value] * label_proportions[label_value]

                # Get actual count
                actual_count = df.filter(
                    (col(protected_attr) == attr_value) & (col(target_col) == label_value)
                ).count()

                if actual_count > 0:
                    weight = expected_count / actual_count
                else:
                    weight = 1.0

                # Apply weight
                result = result.withColumn(
                    "sample_weight",
                    when(
                        (col(protected_attr) == attr_value) & (col(target_col) == label_value),
                        lit(weight)
                    ).otherwise(col("sample_weight") if "sample_weight" in result.columns else lit(1.0))
                )

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result


@register_ml_component("shap_segment_explainability")
class SHAPSegmentExplainability(BaseMLComponent):
    """
    Generate SHAP explanations per customer segment.

    Config example:
        {
            "input_table": "gold.predictions",
            "model_name": "churn_model",
            "model_version": "Production",
            "feature_columns": ["tenure", "monthly_charges", "total_charges"],
            "segment_column": "customer_segment",
            "output_table": "gold.shap_by_segment"
        }
    """

    def execute(self) -> DataFrame:
        """Generate SHAP values per segment."""
        import shap

        df = self.spark.table(self.config["input_table"])
        segment_col = self.config["segment_column"]
        feature_cols = self.config["feature_columns"]
        model_name = self.config["model_name"]
        model_version = self.config.get("model_version", "Production")

        # Load model
        model_uri = f"models:/{self.uc_catalog}.{self.uc_schema}.{model_name}/{model_version}"
        model = mlflow.sklearn.load_model(model_uri)

        # Get unique segments
        segments = df.select(segment_col).distinct().rdd.flatMap(lambda x: x).collect()

        all_shap_values = []

        for segment in segments:
            segment_df = df.filter(col(segment_col) == segment)
            segment_pdf = segment_df.toPandas()

            X = segment_pdf[feature_cols]

            # Calculate SHAP values
            explainer = shap.TreeExplainer(model)
            shap_values = explainer.shap_values(X)

            # Add to results
            for i, feature in enumerate(feature_cols):
                segment_pdf[f"{feature}_shap"] = shap_values[:, i]

            segment_pdf["segment"] = segment
            all_shap_values.append(segment_pdf)

        # Combine
        import pandas as pd
        combined_pdf = pd.concat(all_shap_values, ignore_index=True)

        result = self.spark.createDataFrame(combined_pdf)
        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result


@register_ml_component("model_card_generator")
class ModelCardGenerator(BaseMLComponent):
    """
    Generate model cards documenting model details, performance, and limitations.

    Config example:
        {
            "model_name": "fraud_detection_model",
            "model_version": "3",
            "model_description": "XGBoost classifier for transaction fraud detection",
            "use_cases": ["Real-time fraud scoring", "Batch transaction review"],
            "limitations": ["May have higher false positives for new merchants"],
            "training_data": "gold.fraud_training_v3",
            "performance_metrics": {"auc": 0.95, "precision": 0.88, "recall": 0.92},
            "fairness_metrics": {"gender_parity": 0.02, "age_parity": 0.03},
            "output_path": "/dbfs/ml/model_cards/fraud_detection_v3.json"
        }
    """

    def execute(self) -> str:
        """Generate model card."""
        import json

        model_card = {
            "model_details": {
                "name": self.config["model_name"],
                "version": self.config.get("model_version", "1"),
                "description": self.config.get("model_description", ""),
                "model_type": self.config.get("model_type", "classifier"),
                "created_date": datetime.utcnow().isoformat(),
                "created_by": self.config.get("created_by", "data-science-team")
            },
            "intended_use": {
                "primary_uses": self.config.get("use_cases", []),
                "primary_users": self.config.get("primary_users", []),
                "out_of_scope_uses": self.config.get("out_of_scope_uses", [])
            },
            "factors": {
                "relevant_factors": self.config.get("relevant_factors", []),
                "evaluation_factors": self.config.get("evaluation_factors", [])
            },
            "metrics": {
                "performance": self.config.get("performance_metrics", {}),
                "fairness": self.config.get("fairness_metrics", {})
            },
            "training_data": {
                "dataset": self.config.get("training_data", ""),
                "preprocessing": self.config.get("preprocessing_steps", [])
            },
            "ethical_considerations": {
                "risks": self.config.get("risks", []),
                "mitigations": self.config.get("mitigations", [])
            },
            "limitations": self.config.get("limitations", []),
            "recommendations": self.config.get("recommendations", [])
        }

        # Save model card
        output_path = self.config["output_path"]
        with open(output_path, "w") as f:
            json.dump(model_card, f, indent=2)

        self.logger.info(f"Model card generated at {output_path}")

        # Log as artifact
        self.log_artifact(output_path, "model_card")

        return output_path


@register_ml_component("pii_feature_redactor")
class PIIFeatureRedactor(BaseMLComponent):
    """
    Redact or encrypt PII features before model training.

    Config example:
        {
            "input_table": "gold.customer_data",
            "pii_columns": ["email", "ssn", "phone_number", "credit_card"],
            "redaction_strategy": {
                "email": "hash",
                "ssn": "mask",
                "phone_number": "mask",
                "credit_card": "encrypt"
            },
            "output_table": "gold.customer_data_redacted"
        }
    """

    def execute(self) -> DataFrame:
        """Redact PII features."""
        from pyspark.sql.functions import sha2, regexp_replace, substring

        df = self.spark.table(self.config["input_table"])
        pii_columns = self.config["pii_columns"]
        redaction_strategy = self.config["redaction_strategy"]

        result = df

        for column in pii_columns:
            strategy = redaction_strategy.get(column, "mask")

            if strategy == "hash":
                # Hash the column
                result = result.withColumn(
                    column,
                    sha2(col(column).cast("string"), 256)
                )

            elif strategy == "mask":
                # Mask with asterisks, keeping last 4 characters
                result = result.withColumn(
                    column,
                    when(col(column).isNotNull(),
                         regexp_replace(
                             col(column).cast("string"),
                             "^.*(....)",
                             "****$1"
                         )
                    ).otherwise(col(column))
                )

            elif strategy == "encrypt":
                # Simplified encryption (production would use proper key management)
                result = result.withColumn(
                    column,
                    sha2(col(column).cast("string"), 256)
                )

            elif strategy == "remove":
                # Drop the column entirely
                result = result.drop(column)

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        self.logger.info(f"Redacted {len(pii_columns)} PII columns")

        return result


@register_ml_component("model_risk_tier_classifier")
class ModelRiskTierClassifier(BaseMLComponent):
    """
    Classify models into risk tiers based on impact and usage.

    Config example:
        {
            "model_inventory_table": "audit.model_inventory",
            "risk_criteria": {
                "high_impact_domains": ["credit_decisions", "healthcare", "hiring"],
                "high_volume_threshold": 1000000,
                "sensitive_data_indicators": ["ssn", "medical", "financial"]
            },
            "output_table": "audit.model_risk_tiers"
        }
    """

    def execute(self) -> DataFrame:
        """Classify model risk tiers."""
        df = self.spark.table(self.config["model_inventory_table"])
        risk_criteria = self.config["risk_criteria"]

        high_impact_domains = risk_criteria.get("high_impact_domains", [])
        high_volume_threshold = risk_criteria.get("high_volume_threshold", 1000000)
        sensitive_indicators = risk_criteria.get("sensitive_data_indicators", [])

        # Initialize risk score
        result = df.withColumn("risk_score", lit(0))

        # Domain impact
        for domain in high_impact_domains:
            result = result.withColumn(
                "risk_score",
                when(col("domain") == domain, col("risk_score") + lit(30))
                .otherwise(col("risk_score"))
            )

        # Volume
        result = result.withColumn(
            "risk_score",
            when(col("prediction_volume") > high_volume_threshold, col("risk_score") + lit(20))
            .otherwise(col("risk_score"))
        )

        # Sensitive data
        for indicator in sensitive_indicators:
            result = result.withColumn(
                "risk_score",
                when(col("feature_list").contains(indicator), col("risk_score") + lit(25))
                .otherwise(col("risk_score"))
            )

        # Classify risk tier
        result = result.withColumn(
            "risk_tier",
            when(col("risk_score") >= 70, lit("high"))
            .when(col("risk_score") >= 40, lit("medium"))
            .otherwise(lit("low"))
        )

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        # Log distribution
        tier_counts = result.groupBy("risk_tier").count().collect()
        self.log_params({
            f"{row['risk_tier']}_count": row["count"]
            for row in tier_counts
        })

        return result


@register_ml_component("model_inventory_registry")
class ModelInventoryRegistry(BaseMLComponent):
    """
    Maintain central registry of all production models.

    Config example:
        {
            "inventory_table": "audit.model_inventory",
            "model_metadata": {
                "model_name": "fraud_detection_v3",
                "model_type": "xgboost_classifier",
                "owner": "fraud-team",
                "business_purpose": "Real-time transaction fraud detection",
                "deployment_date": "2025-01-15",
                "expected_lifetime": "6 months",
                "refresh_frequency": "monthly",
                "approval_status": "approved"
            }
        }
    """

    def execute(self) -> Dict[str, Any]:
        """Register model in inventory."""
        from pyspark.sql.functions import current_timestamp

        metadata = self.config["model_metadata"]

        # Create inventory record
        import pandas as pd
        record = {
            "model_name": metadata["model_name"],
            "model_type": metadata.get("model_type", "unknown"),
            "owner": metadata.get("owner", "unknown"),
            "business_purpose": metadata.get("business_purpose", ""),
            "deployment_date": metadata.get("deployment_date"),
            "expected_lifetime": metadata.get("expected_lifetime"),
            "refresh_frequency": metadata.get("refresh_frequency", "as_needed"),
            "approval_status": metadata.get("approval_status", "pending"),
            "last_updated": datetime.utcnow().isoformat(),
            "risk_tier": metadata.get("risk_tier", "medium"),
            "compliance_requirements": metadata.get("compliance_requirements", ""),
            "data_sources": metadata.get("data_sources", ""),
            "feature_count": metadata.get("feature_count", 0),
            "prediction_volume": metadata.get("prediction_volume", 0)
        }

        # Convert to Spark DataFrame
        record_df = self.spark.createDataFrame([record])

        # Append to inventory
        record_df.write.mode("append").saveAsTable(self.config["inventory_table"])

        self.logger.info(f"Registered model: {metadata['model_name']}")

        return record


@register_ml_component("model_retirement_workflow")
class ModelRetirementWorkflow(BaseMLComponent):
    """
    Manage model retirement lifecycle and decommissioning.

    Config example:
        {
            "inventory_table": "audit.model_inventory",
            "retirement_criteria": {
                "max_age_days": 180,
                "min_performance_threshold": 0.75,
                "replacement_available": true
            },
            "output_table": "audit.retirement_candidates"
        }
    """

    def execute(self) -> DataFrame:
        """Identify models for retirement."""
        from pyspark.sql.functions import datediff, current_date

        df = self.spark.table(self.config["inventory_table"])
        criteria = self.config["retirement_criteria"]

        max_age = criteria.get("max_age_days", 180)
        min_performance = criteria.get("min_performance_threshold", 0.75)

        # Calculate age
        result = df.withColumn(
            "age_days",
            datediff(current_date(), col("deployment_date"))
        )

        # Flag retirement candidates
        result = result.withColumn(
            "retirement_reason",
            when(col("age_days") > max_age, lit("exceeded_max_age"))
            .when(col("current_performance") < min_performance, lit("performance_degradation"))
            .when(col("approval_status") == "deprecated", lit("manual_deprecation"))
            .otherwise(lit(None))
        )

        # Filter to candidates
        retirement_candidates = result.filter(col("retirement_reason").isNotNull())

        # Add retirement workflow steps
        retirement_candidates = retirement_candidates.withColumn(
            "retirement_steps",
            array(
                lit("notify_stakeholders"),
                lit("document_replacement"),
                lit("migrate_traffic"),
                lit("archive_model"),
                lit("decommission_endpoints")
            )
        )

        retirement_candidates.write.mode("overwrite").saveAsTable(self.config["output_table"])

        num_candidates = retirement_candidates.count()
        self.logger.info(f"Identified {num_candidates} models for retirement")

        self.log_metrics({"retirement_candidates": num_candidates})

        return retirement_candidates
