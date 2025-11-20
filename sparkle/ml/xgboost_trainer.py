"""
XGBoost Spark Trainer for distributed model training.
"""
from pyspark.sql import DataFrame
from sparkle.config_schema import config_schema, Field, BaseConfigSchema
from enum import Enum


class Objective(str, Enum):
    """XGBoost objectives."""
    BINARY_LOGISTIC = "binary:logistic"
    MULTI_SOFTMAX = "multi:softmax"
    MULTI_SOFTPROB = "multi:softprob"
    REG_SQUAREDERROR = "reg:squarederror"
    REG_LINEAR = "reg:linear"


class TreeMethod(str, Enum):
    """Tree construction algorithms."""
    AUTO = "auto"
    HIST = "hist"
    APPROX = "approx"
    GPU_HIST = "gpu_hist"


@config_schema
class XGBoostConfig(BaseConfigSchema):
    """Configuration for XGBoost Spark trainer."""

    # Features and target
    feature_cols: list[str] = Field(
        ...,
        description="Feature column names",
        min_length=1,
        group="Data",
        order=1,
    )
    label_col: str = Field(
        "label",
        description="Label/target column name",
        group="Data",
        order=2,
    )
    prediction_col: str = Field(
        "prediction",
        description="Output prediction column name",
        group="Data",
        order=3,
    )

    # Model parameters
    objective: str = Field(
        Objective.BINARY_LOGISTIC.value,
        description="Learning objective",
        widget="dropdown",
        examples=[obj.value for obj in Objective],
        group="Model",
        order=10,
    )
    num_round: int = Field(
        100,
        ge=1,
        le=10000,
        description="Number of boosting rounds",
        group="Model",
        order=11,
    )
    max_depth: int = Field(
        6,
        ge=1,
        le=20,
        description="Maximum tree depth",
        group="Model",
        order=12,
    )
    eta: float = Field(
        0.3,
        gt=0,
        le=1,
        description="Learning rate (step size shrinkage)",
        group="Model",
        order=13,
    )
    subsample: float = Field(
        1.0,
        gt=0,
        le=1,
        description="Subsample ratio of training instances",
        group="Model",
        order=14,
    )
    colsample_bytree: float = Field(
        1.0,
        gt=0,
        le=1,
        description="Subsample ratio of columns when constructing each tree",
        group="Model",
        order=15,
    )

    # Regularization
    alpha: float = Field(
        0.0,
        ge=0,
        description="L1 regularization term on weights",
        group="Regularization",
        order=20,
    )
    lambda_: float = Field(
        1.0,
        ge=0,
        description="L2 regularization term on weights",
        group="Regularization",
        order=21,
    )
    gamma: float = Field(
        0.0,
        ge=0,
        description="Minimum loss reduction required for split",
        group="Regularization",
        order=22,
    )

    # Training
    tree_method: str = Field(
        TreeMethod.AUTO.value,
        description="Tree construction algorithm",
        widget="dropdown",
        examples=[m.value for m in TreeMethod],
        group="Training",
        order=30,
    )
    early_stopping_rounds: int = Field(
        None,
        ge=1,
        description="Stop if no improvement for N rounds (requires eval set)",
        group="Training",
        order=31,
    )
    num_workers: int = Field(
        2,
        ge=1,
        le=100,
        description="Number of parallel workers for training",
        group="Training",
        order=32,
    )

    # Output
    model_path: str = Field(
        "/tmp/xgboost_model",
        description="Path to save trained model",
        widget="file",
        group="Output",
        order=40,
    )


class XGBoostSparkTrainer:
    """
    Train XGBoost models on Spark for distributed ML.

    Tags: xgboost, ml, training, classification, regression
    Category: Machine Learning
    Icon: ml
    """

    def __init__(self, config: dict):
        self.config = XGBoostConfig(**config) if isinstance(config, dict) else config

    @staticmethod
    def config_schema() -> dict:
        return XGBoostConfig.config_schema()

    @staticmethod
    def sample_config() -> dict:
        return {
            "feature_cols": ["feature1", "feature2", "feature3"],
            "label_col": "label",
            "prediction_col": "prediction",
            "objective": "binary:logistic",
            "num_round": 100,
            "max_depth": 6,
            "eta": 0.3,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "alpha": 0.0,
            "lambda_": 1.0,
            "gamma": 0.0,
            "tree_method": "hist",
            "early_stopping_rounds": 10,
            "num_workers": 2,
            "model_path": "/tmp/xgboost_model",
        }

    def fit(self, df: DataFrame) -> "XGBoostSparkTrainer":
        """
        Train XGBoost model on DataFrame.

        Args:
            df: Training DataFrame with features and label

        Returns:
            Trained model
        """
        # Placeholder - real implementation would use xgboost4j-spark
        print(f"Training XGBoost with {self.config.num_round} rounds...")
        return self

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply trained model to DataFrame.

        Args:
            df: DataFrame with features

        Returns:
            DataFrame with predictions
        """
        # Placeholder - real implementation would load model and predict
        from pyspark.sql import functions as F
        return df.withColumn(self.config.prediction_col, F.lit(0.5))
