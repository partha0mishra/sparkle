"""
Base classes for Sparkle ML components.

All ML components (feature engineering, training, serving) inherit from these base classes.
Provides MLflow integration, Unity Catalog registration, and config-driven execution.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame
import mlflow
import logging
from datetime import datetime


logger = logging.getLogger("sparkle.ml")


class BaseMLComponent(ABC):
    """
    Abstract base class for all ML components.

    Provides:
    - Config-driven execution (NO hard-coded values)
    - MLflow experiment tracking
    - Unity Catalog integration
    - Logging and error handling
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        component_name: str,
        env: str = "prod"
    ):
        """
        Initialize ML component.

        Args:
            spark: SparkSession
            config: Configuration dictionary from JSON/YAML
            component_name: Name of component (e.g., 'churn_model_v3')
            env: Environment (dev/qa/prod)
        """
        self.spark = spark
        self.config = config
        self.component_name = component_name
        self.env = env
        self.logger = logging.getLogger(f"sparkle.ml.{component_name}")

        # MLflow configuration
        self.mlflow_experiment = config.get("mlflow_experiment", f"/Shared/sparkle/{component_name}")
        self.mlflow_tracking_uri = config.get("mlflow_tracking_uri", "databricks")

        # Unity Catalog configuration
        self.uc_catalog = config.get("uc_catalog", "ml")
        self.uc_schema = config.get("uc_schema", "models")

        # Initialize MLflow
        self._setup_mlflow()

    def _setup_mlflow(self):
        """Configure MLflow experiment and tracking."""
        mlflow.set_tracking_uri(self.mlflow_tracking_uri)
        mlflow.set_experiment(self.mlflow_experiment)

        self.logger.info(f"MLflow experiment: {self.mlflow_experiment}")

    def log_params(self, params: Dict[str, Any]):
        """Log parameters to MLflow."""
        try:
            mlflow.log_params(params)
        except Exception as e:
            self.logger.warning(f"Failed to log params: {e}")

    def log_metrics(self, metrics: Dict[str, float], step: Optional[int] = None):
        """Log metrics to MLflow."""
        try:
            mlflow.log_metrics(metrics, step=step)
        except Exception as e:
            self.logger.warning(f"Failed to log metrics: {e}")

    def log_artifact(self, local_path: str, artifact_path: Optional[str] = None):
        """Log artifact to MLflow."""
        try:
            mlflow.log_artifact(local_path, artifact_path)
        except Exception as e:
            self.logger.warning(f"Failed to log artifact: {e}")

    @abstractmethod
    def execute(self) -> Any:
        """
        Execute the ML component.

        Returns:
            Component-specific output (DataFrame, model, metrics, etc.)
        """
        pass

    def run(self) -> Any:
        """
        Run the component with MLflow tracking.

        Returns:
            Execution result
        """
        with mlflow.start_run(run_name=f"{self.component_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"):
            # Log component info
            mlflow.set_tags({
                "component_name": self.component_name,
                "component_type": self.__class__.__name__,
                "environment": self.env,
                "sparkle_version": "1.0.0"
            })

            # Log configuration
            self.log_params(self.config)

            try:
                result = self.execute()
                self.logger.info(f"Component {self.component_name} executed successfully")
                return result
            except Exception as e:
                self.logger.error(f"Component {self.component_name} failed: {e}")
                mlflow.log_param("status", "failed")
                mlflow.log_param("error", str(e))
                raise


class BaseFeatureEngineer(BaseMLComponent):
    """
    Base class for feature engineering components.

    Feature engineers transform DataFrames by adding/modifying columns.
    """

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform DataFrame by adding/modifying features.

        Args:
            df: Input DataFrame

        Returns:
            Transformed DataFrame
        """
        pass

    def execute(self) -> DataFrame:
        """Execute feature engineering."""
        input_table = self.config.get("input_table")
        output_table = self.config.get("output_table")

        # Read input
        df = self.spark.table(input_table)

        # Transform
        transformed = self.transform(df)

        # Write output if specified
        if output_table:
            mode = self.config.get("write_mode", "overwrite")
            transformed.write.mode(mode).saveAsTable(output_table)
            self.logger.info(f"Wrote transformed data to {output_table}")

        return transformed


class BaseModelTrainer(BaseMLComponent):
    """
    Base class for model training components.

    Trainers fit models and automatically register them to MLflow + Unity Catalog.
    """

    @abstractmethod
    def train(self, train_df: DataFrame) -> Any:
        """
        Train the model.

        Args:
            train_df: Training DataFrame

        Returns:
            Trained model object
        """
        pass

    def execute(self) -> Dict[str, Any]:
        """Execute model training with full MLflow tracking."""
        train_table = self.config["train_table"]

        # Read training data
        train_df = self.spark.table(train_table)

        # Train model
        model = self.train(train_df)

        # Register to MLflow Model Registry
        model_name = self.config.get("model_name", self.component_name)
        registered_model = self._register_model(model, model_name)

        return {
            "model": model,
            "registered_model": registered_model,
            "model_name": model_name
        }

    def _register_model(self, model: Any, model_name: str) -> Any:
        """Register model to MLflow Model Registry."""
        try:
            # Log model
            mlflow.sklearn.log_model(
                model,
                "model",
                registered_model_name=f"{self.uc_catalog}.{self.uc_schema}.{model_name}"
            )

            self.logger.info(f"Registered model: {self.uc_catalog}.{self.uc_schema}.{model_name}")

            return model
        except Exception as e:
            self.logger.error(f"Failed to register model: {e}")
            return None


class BaseModelScorer(BaseMLComponent):
    """
    Base class for model scoring/inference components.

    Scorers load models and generate predictions on new data.
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], component_name: str, env: str = "prod"):
        super().__init__(spark, config, component_name, env)
        self.model = None

    def load_model(self, model_uri: Optional[str] = None):
        """
        Load model from MLflow Model Registry.

        Args:
            model_uri: Model URI (e.g., 'models:/churn_model/Production')
        """
        if model_uri is None:
            model_name = self.config["model_name"]
            model_version = self.config.get("model_version", "Production")
            model_uri = f"models:/{self.uc_catalog}.{self.uc_schema}.{model_name}/{model_version}"

        try:
            self.model = mlflow.sklearn.load_model(model_uri)
            self.logger.info(f"Loaded model from {model_uri}")
        except Exception as e:
            self.logger.error(f"Failed to load model: {e}")
            raise

    @abstractmethod
    def score(self, df: DataFrame) -> DataFrame:
        """
        Score DataFrame using loaded model.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with predictions
        """
        pass

    def execute(self) -> DataFrame:
        """Execute scoring."""
        # Load model
        self.load_model()

        # Read input data
        input_table = self.config["input_table"]
        df = self.spark.table(input_table)

        # Score
        predictions = self.score(df)

        # Write predictions
        output_table = self.config.get("output_table")
        if output_table:
            mode = self.config.get("write_mode", "overwrite")
            predictions.write.mode(mode).saveAsTable(output_table)
            self.logger.info(f"Wrote predictions to {output_table}")

        return predictions


class BaseFeatureStoreComponent(BaseMLComponent):
    """
    Base class for Unity Catalog Feature Store components.

    Handles feature store operations (read, write, lookup).
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], component_name: str, env: str = "prod"):
        super().__init__(spark, config, component_name, env)

        # Feature Store configuration
        self.fs_catalog = config.get("fs_catalog", "feature_store")
        self.fs_schema = config.get("fs_schema", "features")

    def get_feature_table_name(self, table_name: str) -> str:
        """Get fully qualified feature table name."""
        return f"{self.fs_catalog}.{self.fs_schema}.{table_name}"
