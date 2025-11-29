from ..base import BasePipeline
from ..tasks.ml import BatchScoreTask
from ..tasks.write import WriteDeltaTask

class MLBatchScoringPipeline(BasePipeline):
    """
    Daily/weekly batch scoring of gold/feature table → writes scored table
    """
    def build(self) -> 'BasePipeline':
        # 1. Score
        self.add_task(BatchScoreTask("batch_score", {
            "scorer": self.config.extra_config.get("scorer"),
            "scorer_config": {
                "model_name": self.config.model_name,
                "input_table": f"{self.config.source_catalog}.{self.config.source_schema}.{self.config.source_table}" if self.config.source_table else None
            }
        }))
        
        # 2. Write Results
        self.add_task(WriteDeltaTask("write_scores", {
            "destination_catalog": self.config.destination_catalog,
            "destination_schema": self.config.destination_schema,
            "destination_table": self.config.destination_table,
            "write_mode": "append",
            "partition_columns": self.config.partition_columns
        }))
        
        return self
