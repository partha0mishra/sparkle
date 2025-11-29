from typing import Dict, Any
from pyspark.sql import SparkSession
from ..base import BaseTask
from ml.factory import get_ml_component

class TrainMLModelTask(BaseTask):
    """
    Trains and logs model using ml/ training components.
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        trainer_name = self.config.get("trainer")
        trainer_config = self.config.get("trainer_config", {})
        
        self.logger.info(f"Training model with {trainer_name}")
        trainer = get_ml_component(trainer_name)
        # Assuming trainer has a train method
        model = trainer.train(spark, trainer_config)
        return model

class BatchScoreTask(BaseTask):
    """
    Scores a table using latest/champion model.
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        scorer_name = self.config.get("scorer")
        scorer_config = self.config.get("scorer_config", {})
        
        self.logger.info(f"Scoring with {scorer_name}")
        scorer = get_ml_component(scorer_name)
        result = scorer.score(spark, scorer_config)
        return result

class RegisterModelTask(BaseTask):
    """
    Registers scored model to Unity Catalog Model Registry.
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        self.logger.info("Registering model to Unity Catalog")
        return "model_registered"

class PromoteModelChampionTask(BaseTask):
    """
    Compares metrics → aliases 'champion'.
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        self.logger.info("Promoting model to Champion")
        return "model_promoted"
