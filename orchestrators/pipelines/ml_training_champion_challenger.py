from ..base import BasePipeline
from ..tasks.ml import TrainMLModelTask, RegisterModelTask, PromoteModelChampionTask

class MLTrainingChampionChallengerPipeline(BasePipeline):
    """
    Trains N models from feature table → registers → promotes winner via config threshold
    """
    def build(self) -> 'BasePipeline':
        # 1. Train Models
        # Assuming config defines multiple model configs
        models_config = self.config.extra_config.get("models", [])
        for i, model_conf in enumerate(models_config):
            self.add_task(TrainMLModelTask(f"train_model_{i}", {
                "trainer": model_conf.get("trainer"),
                "trainer_config": model_conf.get("config")
            }))
            
        # 2. Register Best Model
        self.add_task(RegisterModelTask("register_best_model", {
            "model_name": self.config.model_name
        }))
        
        # 3. Promote if better than current champion
        self.add_task(PromoteModelChampionTask("promote_champion", {
            "model_name": self.config.model_name,
            "metric": self.config.extra_config.get("metric"),
            "threshold": self.config.extra_config.get("threshold")
        }))
        
        return self
