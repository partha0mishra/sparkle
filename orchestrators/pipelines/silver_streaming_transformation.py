from ..base import BasePipeline
from ..tasks.transform import TransformTask
from ..tasks.write import WriteDeltaTask

class SilverStreamingTransformationPipeline(BasePipeline):
    """
    Streaming version of SilverBatchTransformationPipeline with watermark tracking and late-data handling
    """
    def build(self) -> 'BasePipeline':
        # 1. Transform (Streaming)
        self.add_task(TransformTask("apply_transformations_stream", {
            "transformers": self.config.transformers,
            "watermark_column": self.config.watermark_column
        }))
        
        # 2. Write to Silver (Streaming)
        self.add_task(WriteDeltaTask("write_silver_stream", {
            "destination_catalog": self.config.destination_catalog,
            "destination_schema": self.config.destination_schema,
            "destination_table": self.config.destination_table,
            "write_mode": "append",
            "partition_columns": self.config.partition_columns,
            "checkpoint_location": self.config.checkpoint_location
        }))
        
        return self
