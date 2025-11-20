# Sparkle ML Components Index

Complete catalog of all 112 production-ready ML components.

## A. Feature Store Integrations (8 components)

1. **FeatureTableWriter** - `ml/feature_store.py`
   - Writes DataFrame to UC Feature Store with PK + timestamp
   - Config: table_name, primary_keys, timestamp_column

2. **FeatureTableReader** - `ml/feature_store.py`
   - Point-in-time or latest lookups from Feature Store
   - Config: table_name, lookup_keys, as_of_timestamp

3. **FeatureTableCreator** - `ml/feature_store.py`
   - Auto-creates UC Feature Store table from DataFrame
   - Config: table_name, primary_keys, description, tags

4. **FeatureViewMaterializer** - `ml/feature_store.py`
   - DLT-compatible daily materialization from silver/gold
   - Config: source_view, target_table, schedule

5. **FeatureOnlineStoreSync** - `ml/feature_store.py`
   - Pushes latest features to Redis/DynamoDB via foreachBatch
   - Config: online_store_type, connection_params

6. **FeatureDriftDetector** - `ml/feature_store.py`
   - Calculates PSI/KS between training and current data
   - Config: reference_table, threshold, alert_channel

7. **FeatureImportanceLogger** - `ml/feature_store.py`
   - Logs permutation/SHAP importance to MLflow
   - Config: model_name, feature_columns, method

8. **FeatureLineageTracker** - `ml/feature_store.py`
   - Emits OpenLineage events linking features → models
   - Config: lineage_backend, namespace

## B. Feature Engineering Transformers (22 components)

9-30. See `ml/feature_engineering.py`
- PolynomialFeatures, Bucketizer, OneHotEncoder, TargetEncoder
- FrequencyEncoder, HashingTrickFeatureHasher, CountVectorizer
- TFIDFTransformer, Word2VecEmbeddings, BERTSentenceEmbeddings
- SQLFeatureTransformer, RollingWindowFeatures, TimeSinceEventFeatures
- RatioFeatures, InteractionFeatures, CyclicalDateTimeFeatures
- HaversineDistanceFeature, GeohashGridFeatures, MissingIndicator
- OutlierCapper, SkewnessCorrector, RobustScaler

## C. Model Training Patterns (18 components)

31-48. See `ml/model_training.py`
- HyperparameterSweep, CrossValidator, TimeSeriesSplit
- StratifiedSampler, SMOTEOversampler, AdversarialValidation
- ModelEnsemble, AutoMLTrainer, ProphetForecaster
- ARIMAXTrainer, LSTMSequenceTrainer, XGBoostSparkTrainer
- LightGBMSparkTrainer, CatBoostSparkTrainer, H2OAutoMLTrainer
- MLflowModelWrapper, ChampionChallengerTrainer, ModelExplainabilityTrainer

## D. Model Serving & Inference (15 components)

49-63. See `ml/model_serving.py`
- BatchScorer, RealTimeScorer, StructuredStreamingScorer
- FeatureStoreOnlineLookupScorer, ModelDriftMonitor
- PredictionExplanationLogger, ProbabilityCalibration
- RejectionInference, WhatIfCounterfactualGenerator
- ModelVersionRouter, ShadowModeScorer, WarmPoolModelCache
- ModelRollbackAutomator, PredictionAuditTrail, ModelMonitoringDashboard

## E. Recommendation & Ranking (10 components)

64-73. See `ml/recommendation.py`
- ALSMatrixFactorization, ImplicitALS, TwoTowerModel
- ANNIndexBuilder, LightFMTrainer, SARRecommender
- GRU4RecTrainer, RankingFeatureGenerator, LearningToRankTrainer
- ContextualBanditTrainer

## F. Anomaly & Fraud Detection (11 components)

74-84. See `ml/anomaly_detection.py`
- IsolationForestTrainer, OneClassSVMTrainer, AutoencoderAnomaly
- ProphetAnomalyDetector, TransactionRuleEngine, GraphSAGEFraud
- VelocityLinkAnalysis, DBSCANClustering, SupervisedFraudTrainer
- AnomalyScoreCalibration, FraudCaseManager

## G. Time-Series & Forecasting (12 components)

85-96. See `ml/forecasting.py`
- MultiSeriesProphet, HierarchicalReconciler, GlobalLightGBMForecaster
- ForecastAccuracyTracker, ProbabilisticQuantileForecast
- ExogenousFeatureJoiner, CalendarFourierFeatures
- LagFeatureGenerator, HolidayEffectEncoder, ForecastBiasCorrector
- ResidualAnomalyDetector, ForecastVersionController

## H. NLP & Generative (8 components)

97-104. See `ml/nlp_generative.py`
- ZeroShotClassifier, MultilingualSentiment, SparkNLPNER
- BERTopicTrainer, TextSimilarityScorer, LLMStructuredExtractor
- PromptTemplateEngine, TextSummarizer

## I. Responsible AI & Governance (8 components)

105-112. See `ml/responsible_ai.py`
- BiasFairnessAuditor, FairnessReweighting, SHAPSegmentExplainability
- ModelCardGenerator, PIIFeatureRedactor, ModelRiskTierClassifier
- ModelInventoryRegistry, ModelRetirementWorkflow

## Usage Pattern

All components follow the same pattern:

```python
from sparkle.ml import MLComponent

# Load component by name
component = MLComponent.get("churn_model_v3", spark, env="prod")

# Run with MLflow tracking
result = component.run()
```

Configuration is 100% external (JSON/YAML in `config/ml/{component}/{env}.json`).

## Key Features

- ✅ 100% config-driven (ZERO hard-coded values)
- ✅ Auto-logs to MLflow experiments
- ✅ Auto-registers to Unity Catalog Model Registry
- ✅ Full lineage tracking (OpenLineage)
- ✅ Production-ready error handling
- ✅ Comprehensive docstrings + examples
