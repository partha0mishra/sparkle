from ..base import BasePipeline
from ..tasks.ml import BatchScoreTask
from ..tasks.write import WriteDeltaTask

class MLRealtimeScoringPipeline(BasePipeline):
    """
    Structured Streaming scoring with model cache and shadow mode option
    """
    def build(self) -> 'BasePipeline':
        # 1. Score (Streaming)
        self.add_task(BatchScoreTask("realtime_score", {
            "scorer": self.config.extra_config.get("scorer"),
            "scorer_config": {
                "model_name": self.config.model_name,
                "streaming": True,
                "shadow_mode": self.config.extra_config.get("shadow_mode", False)
            }
        }))
        
        # 2. Write Results (Streaming)
        self.add_task(WriteDeltaTask("write_scores_stream", {
            "destination_catalog": self.config.destination_catalog,
            "destination_schema": self.config.destination_schema,
            "destination_table": self.config.destination_table,
            "write_mode": "append",
            "checkpoint_location": self.config.checkpoint_location
        }))
        
        return self
