from ..base import BasePipeline
from ..tasks.write import WriteDeltaTask

class DisasterRecoveryReplicationPipeline(BasePipeline):
    """
    Replicates configured tables to secondary region/catalog
    """
    def build(self) -> 'BasePipeline':
        # 1. Read Source (Implicit via context or explicit read task needed)
        # For simplicity assuming source is available as input_df or we use a Clone command
        
        # 2. Write to DR Region (Deep Clone)
        source_table = f"{self.config.source_catalog}.{self.config.source_schema}.{self.config.source_table}"
        dest_table = f"{self.config.destination_catalog}.{self.config.destination_schema}.{self.config.destination_table}"
        
        # Using WriteDeltaTask but effectively doing a CLONE
        # In real impl, might use a specific CloneTask
        self.add_task(WriteDeltaTask("replicate_dr", {
            "destination_catalog": self.config.destination_catalog,
            "destination_schema": self.config.destination_schema,
            "destination_table": self.config.destination_table,
            "write_mode": "overwrite", # or clone
            "extra_options": {
                "clone_source": source_table
            }
        }))
        return self
