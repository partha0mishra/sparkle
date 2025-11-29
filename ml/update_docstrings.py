#!/usr/bin/env python3
"""Script to add Sub-Group and Tags to ML component docstrings."""

import re
from pathlib import Path

# Define components and their metadata
COMPONENTS = {
    "feature_engineering.py": {
        "sub_group": "Feature Engineering Transformers",
        "components": [
            ("polynomial_features", "polynomial, feature-engineering, interactions, spark-ml"),
            ("bucketizer", "binning, discretization, feature-engineering, categorical"),
            ("one_hot_encoder", "encoding, categorical, one-hot, sparse"),
            ("target_encoder", "encoding, categorical, target-encoding, smoothing"),
            ("frequency_encoder", "encoding, categorical, frequency, cardinality"),
            ("hashing_trick_feature_hasher", "hashing, high-cardinality, feature-hashing, scalability"),
            ("count_vectorizer", "nlp, text, bag-of-words, vectorization"),
            ("tfidf_transformer", "nlp, text, tfidf, feature-weighting"),
            ("word2vec_embeddings", "nlp, embeddings, word2vec, semantic"),
            ("bert_sentence_embeddings", "nlp, bert, embeddings, transformers"),
            ("sql_feature_transformer", "sql, custom-features, transformations, flexibility"),
            ("rolling_window_features", "time-series, rolling-windows, aggregation, temporal"),
            ("time_since_event_features", "time-series, recency, temporal, behavioral"),
            ("ratio_features", "feature-engineering, ratios, derived-features, numerical"),
            ("interaction_features", "feature-engineering, interactions, polynomial, cross-features"),
            ("cyclical_datetime_features", "time-series, cyclical, datetime, seasonality"),
            ("haversine_distance_feature", "geospatial, distance, location, haversine"),
            ("geohash_grid_features", "geospatial, geohash, location, gridding"),
            ("missing_indicator", "missing-data, imputation, feature-engineering, quality"),
            ("outlier_capper", "outliers, robustness, data-quality, iqr"),
            ("skewness_corrector", "normalization, skewness, transformations, distribution"),
            ("robust_scaler", "scaling, robustness, normalization, outliers")
        ]
    },
    "model_training.py": {
        "sub_group": "Model Training Patterns",
        "components": [
            ("hyperparameter_sweep", "hyperparameter-tuning, grid-search, cross-validation, optimization"),
            ("cross_validator", "cross-validation, model-evaluation, k-fold, stratified"),
            ("time_series_split", "time-series, cross-validation, temporal-split, backtesting"),
            ("stratified_sampler", "sampling, stratified, train-test-split, balanced"),
            ("smote_oversampler", "imbalanced-data, smote, oversampling, class-balance"),
            ("adversarial_validation", "distribution-shift, adversarial-validation, data-leakage, quality"),
            ("model_ensemble", "ensemble, voting, stacking, model-combination"),
            ("automl_trainer", "automl, automated-ml, databricks-automl, optimization"),
            ("prophet_forecaster", "time-series, forecasting, prophet, seasonality"),
            ("arimax_trainer", "time-series, arimax, forecasting, exogenous-variables"),
            ("lstm_sequence_trainer", "deep-learning, lstm, sequences, tensorflow"),
            ("xgboost_spark_trainer", "xgboost, distributed, training, gradient-boosting"),
            ("lightgbm_spark_trainer", "lightgbm, distributed, training, gradient-boosting"),
            ("catboost_spark_trainer", "catboost, categorical-features, gradient-boosting, training"),
            ("h2o_automl_trainer", "h2o, automl, automated-ml, ensemble"),
            ("mlflow_model_wrapper", "mlflow, pyfunc, custom-models, deployment"),
            ("champion_challenger_trainer", "a-b-testing, model-comparison, champion-challenger, mlops"),
            ("model_explainability_trainer", "explainability, shap, lime, interpretability")
        ]
    },
    "model_serving.py": {
        "sub_group": "Model Serving & Inference",
        "components": [
            ("batch_scorer", "batch-inference, scoring, predictions, scale"),
            ("realtime_scorer", "realtime, rest-api, low-latency, serving"),
            ("structured_streaming_scorer", "streaming, real-time, kafka, structured-streaming"),
            ("feature_store_online_lookup_scorer", "online-features, low-latency, feature-store, redis"),
            ("model_drift_monitor", "drift-detection, monitoring, psi, model-quality"),
            ("prediction_explanation_logger", "explainability, shap, lime, interpretability"),
            ("probability_calibration", "calibration, probabilities, platt-scaling, isotonic"),
            ("rejection_inference", "credit-risk, rejection-inference, bias-correction, sampling"),
            ("whatif_counterfactual_generator", "counterfactuals, what-if, explainability, fairness"),
            ("model_version_router", "a-b-testing, traffic-routing, canary, versioning"),
            ("shadow_mode_scorer", "shadow-mode, testing, comparison, deployment"),
            ("warm_pool_model_cache", "caching, performance, low-latency, optimization"),
            ("model_rollback_automator", "rollback, automation, monitoring, reliability"),
            ("prediction_audit_trail", "audit, compliance, logging, governance"),
            ("model_monitoring_dashboard", "monitoring, metrics, dashboards, observability")
        ]
    },
    "recommendation.py": {
        "sub_group": "Recommendation & Ranking",
        "components": [
            ("als_matrix_factorization", "als, collaborative-filtering, matrix-factorization, recommendations"),
            ("implicit_als", "implicit-feedback, als, recommendations, clicks"),
            ("two_tower_model", "two-tower, neural-retrieval, embeddings, deep-learning"),
            ("ann_index_builder", "ann, approximate-nearest-neighbor, indexing, retrieval"),
            ("lightfm_trainer", "lightfm, hybrid-recommender, content-based, collaborative"),
            ("sar_recommender", "sar, smart-adaptive, recommendations, microsoft"),
            ("gru4rec_trainer", "gru, session-based, sequential, deep-learning"),
            ("ranking_feature_generator", "ranking, feature-engineering, learning-to-rank, ltr"),
            ("learning_to_rank_trainer", "learning-to-rank, lambdamart, xgboost, ranking"),
            ("contextual_bandit_trainer", "bandits, exploration-exploitation, online-learning, reinforcement")
        ]
    },
    "anomaly_detection.py": {
        "sub_group": "Anomaly & Fraud Detection",
        "components": [
            ("isolation_forest_trainer", "isolation-forest, anomaly-detection, unsupervised, outliers"),
            ("oneclass_svm_trainer", "one-class-svm, novelty-detection, anomaly-detection, unsupervised"),
            ("autoencoder_anomaly", "autoencoder, deep-learning, anomaly-detection, reconstruction"),
            ("prophet_anomaly_detector", "prophet, time-series, anomaly-detection, forecasting"),
            ("transaction_rule_engine", "rules-engine, fraud-detection, risk-scoring, business-logic"),
            ("graphsage_fraud", "graph-neural-network, fraud-detection, graphsage, network-analysis"),
            ("velocity_link_analysis", "velocity-checks, link-analysis, fraud-detection, behavioral"),
            ("dbscan_clustering", "dbscan, clustering, outlier-detection, density-based"),
            ("supervised_fraud_trainer", "fraud-detection, supervised-learning, imbalanced-data, xgboost"),
            ("anomaly_score_calibration", "calibration, anomaly-scores, probabilities, evaluation"),
            ("fraud_case_manager", "case-management, fraud-investigation, workflow, prioritization")
        ]
    },
    "forecasting.py": {
        "sub_group": "Time-Series & Forecasting",
        "components": [
            ("multi_series_prophet", "prophet, multi-series, forecasting, time-series"),
            ("hierarchical_reconciler", "hierarchical, reconciliation, forecasting, aggregation"),
            ("global_lightgbm_forecaster", "lightgbm, global-model, forecasting, time-series"),
            ("forecast_accuracy_tracker", "accuracy, metrics, mae, rmse, mape"),
            ("probabilistic_quantile_forecast", "quantile-regression, probabilistic, uncertainty, forecasting"),
            ("exogenous_feature_joiner", "exogenous-features, external-data, weather, holidays"),
            ("calendar_fourier_features", "fourier, seasonality, calendar, feature-engineering"),
            ("lag_feature_generator", "lag-features, time-series, feature-engineering, rolling-windows"),
            ("holiday_effect_encoder", "holidays, calendar, seasonality, feature-engineering"),
            ("forecast_bias_corrector", "bias-correction, calibration, forecasting, accuracy"),
            ("residual_anomaly_detector", "residuals, anomaly-detection, forecasting, quality"),
            ("forecast_version_controller", "version-control, comparison, rollback, mlops")
        ]
    },
    "nlp_generative.py": {
        "sub_group": "NLP & Generative",
        "components": [
            ("zero_shot_classifier", "zero-shot, classification, transformers, nlp"),
            ("multilingual_sentiment", "multilingual, sentiment-analysis, bert, nlp"),
            ("spark_nlp_ner", "ner, named-entity-recognition, spark-nlp, extraction"),
            ("bertopic_trainer", "topic-modeling, bertopic, clustering, nlp"),
            ("text_similarity_scorer", "similarity, embeddings, sentence-transformers, semantic"),
            ("llm_structured_extractor", "llm, extraction, structured-data, generative-ai"),
            ("prompt_template_engine", "prompts, templates, llm, generative-ai"),
            ("text_summarizer", "summarization, transformers, nlp, bart")
        ]
    },
    "responsible_ai.py": {
        "sub_group": "Responsible AI & Governance",
        "components": [
            ("bias_fairness_auditor", "fairness, bias-detection, audit, responsible-ai"),
            ("fairness_reweighting", "fairness, reweighting, bias-mitigation, preprocessing"),
            ("shap_segment_explainability", "shap, explainability, segmentation, interpretability"),
            ("model_card_generator", "model-cards, documentation, governance, transparency"),
            ("pii_feature_redactor", "pii, privacy, redaction, compliance"),
            ("model_risk_tier_classifier", "risk-management, model-governance, classification, compliance"),
            ("model_inventory_registry", "inventory, registry, governance, model-management"),
            ("model_retirement_workflow", "retirement, lifecycle, decommissioning, governance")
        ]
    }
}


def update_component_docstring(content: str, component_name: str, tags: str, sub_group: str) -> str:
    """Update a single component's docstring."""
    # Pattern to match the component class and its docstring
    pattern = rf'(@register_ml_component\("{component_name}"\)\s+class\s+\w+\([^)]+\):\s+"""[^\n]+\n)'

    def replacement(match):
        original = match.group(1)
        # Add Sub-Group and Tags after the first line of the docstring
        return f'{original}\n    Sub-Group: {sub_group}\n    Tags: {tags}\n'

    # Only replace if Sub-Group doesn't already exist
    if f'@register_ml_component("{component_name}")' in content and 'Sub-Group:' not in content[content.find(f'@register_ml_component("{component_name}")'):content.find(f'@register_ml_component("{component_name}")')+500]:
        content = re.sub(pattern, replacement, content, count=1)

    return content


def process_file(file_path: Path, sub_group: str, components: list):
    """Process a single file and update all component docstrings."""
    print(f"\nProcessing {file_path.name}...")

    content = file_path.read_text()
    original_content = content

    for component_name, tags in components:
        content = update_component_docstring(content, component_name, tags, sub_group)
        print(f"  Updated {component_name}")

    if content != original_content:
        file_path.write_text(content)
        print(f"✓ Saved {file_path.name}")
    else:
        print(f"  No changes needed for {file_path.name}")

    return len(components)


def main():
    """Main function to process all files."""
    ml_dir = Path("/Users/parthapmishra/mywork/sparkle/ml")
    total_updated = 0

    for filename, metadata in COMPONENTS.items():
        file_path = ml_dir / filename
        if file_path.exists():
            count = process_file(
                file_path,
                metadata["sub_group"],
                metadata["components"]
            )
            total_updated += count
        else:
            print(f"Warning: {file_path} not found")

    print(f"\n{'='*60}")
    print(f"Total components updated: {total_updated}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
