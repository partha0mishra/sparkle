"""
Sparkle ML Package

112 production-ready ML components for feature engineering, model training,
serving, and governance.

Categories:
- Feature Store (8): Unity Catalog Feature Store operations
- Feature Engineering (22): Transformers for feature creation
- Model Training (18): Training patterns and AutoML
- Model Serving (15): Batch, real-time, and streaming inference
- Recommendation (10): Collaborative filtering and ranking
- Anomaly Detection (11): Fraud and anomaly detection
- Forecasting (12): Time-series forecasting
- NLP & Generative (8): NLP and LLM operations
- Responsible AI (8): Governance, bias detection, model cards

All components are:
- 100% config-driven (ZERO hard-coded values)
- Auto-logged to MLflow experiments
- Auto-registered to Unity Catalog Model Registry
- Production-ready with full error handling

Usage:
    from sparkle.ml import MLComponent

    # Create component from config
    component = MLComponent.get("churn_model_v3", spark, env="prod")

    # Run with MLflow tracking
    result = component.run()
"""

# Core infrastructure
from .base import (
    BaseMLComponent,
    BaseFeatureEngineer,
    BaseModelTrainer,
    BaseModelScorer,
    BaseFeatureStoreComponent
)

from .factory import (
    MLComponentFactory,
    MLComponent,
    register_ml_component
)

from .config_loader import (
    load_ml_config,
    save_ml_config,
    get_config_template
)

# Feature Store components (8)
from .feature_store import (
    FeatureTableWriter,
    FeatureTableReader,
    FeatureTableCreator,
    FeatureViewMaterializer,
    FeatureOnlineStoreSync,
    FeatureDriftDetector,
    FeatureImportanceLogger,
    FeatureLineageTracker
)

# Feature Engineering components (22)
from .feature_engineering import (
    PolynomialFeatures,
    Bucketizer,
    OneHotEncoder,
    TargetEncoder,
    FrequencyEncoder,
    HashingTrickFeatureHasher,
    CountVectorizer,
    TFIDFTransformer,
    Word2VecEmbeddings,
    BERTSentenceEmbeddings,
    SQLFeatureTransformer,
    RollingWindowFeatures,
    TimeSinceEventFeatures,
    RatioFeatures,
    InteractionFeatures,
    CyclicalDateTimeFeatures,
    HaversineDistanceFeature,
    GeohashGridFeatures,
    MissingIndicator,
    OutlierCapper,
    SkewnessCorrector,
    RobustScaler
)

# Model Training components (18)
from .model_training import (
    HyperparameterSweep,
    CrossValidator,
    TimeSeriesSplit,
    StratifiedSampler,
    SMOTEOversampler,
    AdversarialValidation,
    ModelEnsemble,
    AutoMLTrainer,
    ProphetForecaster,
    ARIMAXTrainer,
    LSTMSequenceTrainer,
    XGBoostSparkTrainer,
    LightGBMSparkTrainer,
    CatBoostSparkTrainer,
    H2OAutoMLTrainer,
    MLflowModelWrapper,
    ChampionChallengerTrainer,
    ModelExplainabilityTrainer
)

# Model Serving components (15)
from .model_serving import (
    BatchScorer,
    RealTimeScorer,
    StructuredStreamingScorer,
    FeatureStoreOnlineLookupScorer,
    ModelDriftMonitor,
    PredictionExplanationLogger,
    ProbabilityCalibration,
    RejectionInference,
    WhatIfCounterfactualGenerator,
    ModelVersionRouter,
    ShadowModeScorer,
    WarmPoolModelCache,
    ModelRollbackAutomator,
    PredictionAuditTrail,
    ModelMonitoringDashboard
)

# Recommendation components (10)
from .recommendation import (
    ALSMatrixFactorization,
    ImplicitALS,
    TwoTowerModel,
    ANNIndexBuilder,
    LightFMTrainer,
    SARRecommender,
    GRU4RecTrainer,
    RankingFeatureGenerator,
    LearningToRankTrainer,
    ContextualBanditTrainer
)

# Anomaly Detection components (11)
from .anomaly_detection import (
    IsolationForestTrainer,
    OneClassSVMTrainer,
    AutoencoderAnomaly,
    ProphetAnomalyDetector,
    TransactionRuleEngine,
    GraphSAGEFraud,
    VelocityLinkAnalysis,
    DBSCANClustering,
    SupervisedFraudTrainer,
    AnomalyScoreCalibration,
    FraudCaseManager
)

# Forecasting components (12)
from .forecasting import (
    MultiSeriesProphet,
    HierarchicalReconciler,
    GlobalLightGBMForecaster,
    ForecastAccuracyTracker,
    ProbabilisticQuantileForecast,
    ExogenousFeatureJoiner,
    CalendarFourierFeatures,
    LagFeatureGenerator,
    HolidayEffectEncoder,
    ForecastBiasCorrector,
    ResidualAnomalyDetector,
    ForecastVersionController
)

# NLP & Generative components (8)
from .nlp_generative import (
    ZeroShotClassifier,
    MultilingualSentiment,
    SparkNLPNER,
    BERTopicTrainer,
    TextSimilarityScorer,
    LLMStructuredExtractor,
    PromptTemplateEngine,
    TextSummarizer
)

# Responsible AI components (8)
from .responsible_ai import (
    BiasFairnessAuditor,
    FairnessReweighting,
    SHAPSegmentExplainability,
    ModelCardGenerator,
    PIIFeatureRedactor,
    ModelRiskTierClassifier,
    ModelInventoryRegistry,
    ModelRetirementWorkflow
)


__all__ = [
    # Core
    "BaseMLComponent",
    "BaseFeatureEngineer",
    "BaseModelTrainer",
    "BaseModelScorer",
    "BaseFeatureStoreComponent",
    "MLComponentFactory",
    "MLComponent",
    "register_ml_component",
    "load_ml_config",
    "save_ml_config",
    "get_config_template",

    # Feature Store (8)
    "FeatureTableWriter",
    "FeatureTableReader",
    "FeatureTableCreator",
    "FeatureViewMaterializer",
    "FeatureOnlineStoreSync",
    "FeatureDriftDetector",
    "FeatureImportanceLogger",
    "FeatureLineageTracker",

    # Feature Engineering (22)
    "PolynomialFeatures",
    "Bucketizer",
    "OneHotEncoder",
    "TargetEncoder",
    "FrequencyEncoder",
    "HashingTrickFeatureHasher",
    "CountVectorizer",
    "TFIDFTransformer",
    "Word2VecEmbeddings",
    "BERTSentenceEmbeddings",
    "SQLFeatureTransformer",
    "RollingWindowFeatures",
    "TimeSinceEventFeatures",
    "RatioFeatures",
    "InteractionFeatures",
    "CyclicalDateTimeFeatures",
    "HaversineDistanceFeature",
    "GeohashGridFeatures",
    "MissingIndicator",
    "OutlierCapper",
    "SkewnessCorrector",
    "RobustScaler",

    # Model Training (18)
    "HyperparameterSweep",
    "CrossValidator",
    "TimeSeriesSplit",
    "StratifiedSampler",
    "SMOTEOversampler",
    "AdversarialValidation",
    "ModelEnsemble",
    "AutoMLTrainer",
    "ProphetForecaster",
    "ARIMAXTrainer",
    "LSTMSequenceTrainer",
    "XGBoostSparkTrainer",
    "LightGBMSparkTrainer",
    "CatBoostSparkTrainer",
    "H2OAutoMLTrainer",
    "MLflowModelWrapper",
    "ChampionChallengerTrainer",
    "ModelExplainabilityTrainer",

    # Model Serving (15)
    "BatchScorer",
    "RealTimeScorer",
    "StructuredStreamingScorer",
    "FeatureStoreOnlineLookupScorer",
    "ModelDriftMonitor",
    "PredictionExplanationLogger",
    "ProbabilityCalibration",
    "RejectionInference",
    "WhatIfCounterfactualGenerator",
    "ModelVersionRouter",
    "ShadowModeScorer",
    "WarmPoolModelCache",
    "ModelRollbackAutomator",
    "PredictionAuditTrail",
    "ModelMonitoringDashboard",

    # Recommendation (10)
    "ALSMatrixFactorization",
    "ImplicitALS",
    "TwoTowerModel",
    "ANNIndexBuilder",
    "LightFMTrainer",
    "SARRecommender",
    "GRU4RecTrainer",
    "RankingFeatureGenerator",
    "LearningToRankTrainer",
    "ContextualBanditTrainer",

    # Anomaly Detection (11)
    "IsolationForestTrainer",
    "OneClassSVMTrainer",
    "AutoencoderAnomaly",
    "ProphetAnomalyDetector",
    "TransactionRuleEngine",
    "GraphSAGEFraud",
    "VelocityLinkAnalysis",
    "DBSCANClustering",
    "SupervisedFraudTrainer",
    "AnomalyScoreCalibration",
    "FraudCaseManager",

    # Forecasting (12)
    "MultiSeriesProphet",
    "HierarchicalReconciler",
    "GlobalLightGBMForecaster",
    "ForecastAccuracyTracker",
    "ProbabilisticQuantileForecast",
    "ExogenousFeatureJoiner",
    "CalendarFourierFeatures",
    "LagFeatureGenerator",
    "HolidayEffectEncoder",
    "ForecastBiasCorrector",
    "ResidualAnomalyDetector",
    "ForecastVersionController",

    # NLP & Generative (8)
    "ZeroShotClassifier",
    "MultilingualSentiment",
    "SparkNLPNER",
    "BERTopicTrainer",
    "TextSimilarityScorer",
    "LLMStructuredExtractor",
    "PromptTemplateEngine",
    "TextSummarizer",

    # Responsible AI (8)
    "BiasFairnessAuditor",
    "FairnessReweighting",
    "SHAPSegmentExplainability",
    "ModelCardGenerator",
    "PIIFeatureRedactor",
    "ModelRiskTierClassifier",
    "ModelInventoryRegistry",
    "ModelRetirementWorkflow",
]


__version__ = "1.0.0"


def list_all_ml_components():
    """
    List all available ML component types.

    Returns:
        Sorted list of component names

    Example:
        >>> from sparkle.ml import list_all_ml_components
        >>> components = list_all_ml_components()
        >>> print(f"Available: {len(components)} ML components")
    """
    return MLComponent.list()


def get_ml_component_help(name: str) -> str:
    """
    Get help for a specific ML component.

    Args:
        name: Component name

    Returns:
        Component docstring

    Example:
        >>> from sparkle.ml import get_ml_component_help
        >>> help_text = get_ml_component_help("feature_table_writer")
        >>> print(help_text)
    """
    try:
        component_class = MLComponentFactory._registry.get(name)
        if component_class:
            return component_class.__doc__ or "No documentation available"
        return f"Component '{name}' not found"
    except Exception as e:
        return f"Error retrieving help: {e}"


# Component statistics
COMPONENT_COUNTS = {
    "Feature Store": 8,
    "Feature Engineering": 22,
    "Model Training": 18,
    "Model Serving": 15,
    "Recommendation & Ranking": 10,
    "Anomaly & Fraud Detection": 11,
    "Time-Series & Forecasting": 12,
    "NLP & Generative": 8,
    "Responsible AI & Governance": 8
}

TOTAL_COMPONENTS = sum(COMPONENT_COUNTS.values())  # 112 components


def print_component_summary():
    """Print summary of all ML components."""
    print(f"\nSparkle ML Package - {TOTAL_COMPONENTS} Components")
    print("=" * 60)
    for category, count in COMPONENT_COUNTS.items():
        print(f"  {category:.<45} {count:>3} components")
    print("=" * 60)
    print(f"\nRegistered components: {len(list_all_ml_components())}")
    print("\nUsage:")
    print("  from sparkle.ml import MLComponent")
    print('  component = MLComponent.get("component_name", spark, env="prod")')
    print("  result = component.run()")
    print()
