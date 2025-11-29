from ..base import BasePipeline
from ..tasks.transform import TransformTask
from ..tasks.write import WriteFeatureTableTask

class FeatureStoreDailyMaterializationPipeline(BasePipeline):
    """
    Materializes feature views from silver/gold → Unity Catalog Feature Store (offline)
    """
    def build(self) -> 'BasePipeline':
        # 1. Compute Features
        self.add_task(TransformTask("compute_features", {
            "transformers": self.config.transformers
        }))
        
        # 2. Write to Feature Store
        self.add_task(WriteFeatureTableTask("write_feature_table", {
            "destination_catalog": self.config.destination_catalog,
            "destination_schema": self.config.destination_schema,
            "destination_table": self.config.destination_table,
            "primary_key": self.config.primary_key,
            "partition_columns": self.config.partition_columns
        }))
        
        return self
