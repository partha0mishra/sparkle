from ..base import BasePipeline
from ..tasks.transform import TransformTask
from ..tasks.write import WriteFeatureTableTask

class FeatureStoreRealtimeSyncPipeline(BasePipeline):
    """
    Pushes latest feature values to online store (Redis/DynamoDB) via foreachBatch
    """
    def build(self) -> 'BasePipeline':
        # 1. Compute Features (Streaming)
        self.add_task(TransformTask("compute_features_stream", {
            "transformers": self.config.transformers
        }))
        
        # 2. Sync to Online Store
        self.add_task(WriteFeatureTableTask("sync_online_store", {
            "destination_catalog": self.config.destination_catalog,
            "destination_schema": self.config.destination_schema,
            "destination_table": self.config.destination_table,
            "online_store": True,
            "checkpoint_location": self.config.checkpoint_location
        }))
        
        return self
