from ..base import BasePipeline
from ..tasks.transform import TransformTask
from ..tasks.write import WriteDeltaTask

class GoldDailyAggregatePipeline(BasePipeline):
    """
    Daily/partitioned silver → gold aggregated tables (config-driven group-by + measures)
    """
    def build(self) -> 'BasePipeline':
        # 1. Aggregate
        self.add_task(TransformTask("aggregate_gold", {
            "transformers": [
                {
                    "name": "aggregate",
                    "params": {
                        "group_by": self.config.extra_config.get("group_by"),
                        "measures": self.config.extra_config.get("measures")
                    }
                }
            ]
        }))
        
        # 2. Write to Gold
        self.add_task(WriteDeltaTask("write_gold", {
            "destination_catalog": self.config.destination_catalog,
            "destination_schema": self.config.destination_schema,
            "destination_table": self.config.destination_table,
            "write_mode": "overwrite",
            "partition_columns": self.config.partition_columns
        }))
        
        return self
