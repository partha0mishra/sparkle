from ..base import BasePipeline
from ..tasks.transform import TransformTask
from ..tasks.write import WriteDeltaTask
from ..tasks.quality import RunGreatExpectationsTask

class SilverBatchTransformationPipeline(BasePipeline):
    """
    Reads bronze → applies configurable transformer chain → writes silver with schema enforcement
    """
    def build(self) -> 'BasePipeline':
        # 1. Transform
        self.add_task(TransformTask("apply_transformations", {
            "transformers": self.config.transformers
        }))
        
        # 2. Quality Check (Optional)
        if self.config.expectations_suite:
            self.add_task(RunGreatExpectationsTask("validate_silver", {
                "expectations_suite": self.config.expectations_suite
            }))
        
        # 3. Write to Silver
        self.add_task(WriteDeltaTask("write_silver", {
            "destination_catalog": self.config.destination_catalog,
            "destination_schema": self.config.destination_schema,
            "destination_table": self.config.destination_table,
            "write_mode": self.config.write_mode,
            "partition_columns": self.config.partition_columns,
            "zorder_columns": self.config.extra_config.get("zorder_columns", [])
        }))
        
        return self
