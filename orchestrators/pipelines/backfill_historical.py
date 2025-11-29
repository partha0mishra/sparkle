from ..base import BasePipeline
from ..tasks.orchestration import TriggerDownstreamPipelineTask

class BackfillHistoricalPipeline(BasePipeline):
    """
    Parallel backfill of any pipeline from start_date to end_date using dynamic task mapping
    """
    def build(self) -> 'BasePipeline':
        # This pipeline essentially triggers other pipelines with date ranges
        # In a real implementation, this would generate multiple tasks dynamically
        start_date = self.config.extra_config.get("start_date")
        end_date = self.config.extra_config.get("end_date")
        target_pipeline = self.config.extra_config.get("target_pipeline")
        
        self.add_task(TriggerDownstreamPipelineTask("trigger_backfill", {
            "pipeline_name": target_pipeline,
            "parameters": {
                "start_date": start_date,
                "end_date": end_date,
                "is_backfill": True
            }
        }))
        return self
