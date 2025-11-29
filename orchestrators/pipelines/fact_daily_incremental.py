from ..base import BasePipeline
from ..tasks.transform import TransformTask
from ..tasks.write import WriteDeltaTask

class FactDailyIncrementalPipeline(BasePipeline):
    """
    Incremental fact load from silver → gold fact with surrogate key generation
    """
    def build(self) -> 'BasePipeline':
        # 1. Generate Surrogate Keys
        self.add_task(TransformTask("generate_surrogate_keys", {
            "transformers": [
                {
                    "name": "surrogate_key",
                    "params": {
                        "keys": self.config.business_key
                    }
                }
            ]
        }))
        
        # 2. Write to Fact
        self.add_task(WriteDeltaTask("write_fact", {
            "destination_catalog": self.config.destination_catalog,
            "destination_schema": self.config.destination_schema,
            "destination_table": self.config.destination_table,
            "write_mode": "append",
            "partition_columns": self.config.partition_columns
        }))
        
        return self
