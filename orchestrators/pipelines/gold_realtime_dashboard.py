from ..base import BasePipeline
from ..tasks.transform import TransformTask
from ..tasks.write import WriteDeltaTask

class GoldRealtimeDashboardPipeline(BasePipeline):
    """
    Streaming silver → pre-aggregated gold rollups optimized for BI tools
    """
    def build(self) -> 'BasePipeline':
        # 1. Aggregate (Streaming)
        self.add_task(TransformTask("aggregate_gold_stream", {
            "transformers": [
                {
                    "name": "streaming_aggregate",
                    "params": {
                        "group_by": self.config.extra_config.get("group_by"),
                        "measures": self.config.extra_config.get("measures"),
                        "window_duration": self.config.extra_config.get("window_duration")
                    }
                }
            ]
        }))
        
        # 2. Write to Gold (Streaming)
        self.add_task(WriteDeltaTask("write_gold_stream", {
            "destination_catalog": self.config.destination_catalog,
            "destination_schema": self.config.destination_schema,
            "destination_table": self.config.destination_table,
            "write_mode": "complete", # Often used for aggregations
            "checkpoint_location": self.config.checkpoint_location
        }))
        
        return self
