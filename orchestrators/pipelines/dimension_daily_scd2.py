from ..base import BasePipeline
from ..tasks.transform import TransformTask
from ..tasks.write import WriteDeltaTask

class DimensionDailySCD2Pipeline(BasePipeline):
    """
    Applies SCD Type-2 logic from silver → gold dimension using config-defined business key
    """
    def build(self) -> 'BasePipeline':
        # 1. SCD2 Logic
        self.add_task(TransformTask("apply_scd2", {
            "transformers": [
                {
                    "name": "scd2",
                    "params": {
                        "business_keys": self.config.business_key,
                        "target_table": f"{self.config.destination_catalog}.{self.config.destination_schema}.{self.config.destination_table}"
                    }
                }
            ]
        }))
        
        # 2. Write (Merge)
        # Note: SCD2 usually handles the merge internally or returns the merged DF
        # Here we assume the transformer returns the final DF to be written/merged
        self.add_task(WriteDeltaTask("write_dimension", {
            "destination_catalog": self.config.destination_catalog,
            "destination_schema": self.config.destination_schema,
            "destination_table": self.config.destination_table,
            "write_mode": "merge" 
        }))
        
        return self
