from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from ..base import BaseTask
from transformers.factory import get_transformer

class TransformTask(BaseTask):
    """
    Applies a list of transformer functions in order.
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> DataFrame:
        transformers_config = self.config.get("transformers", [])
        input_df = context.get("input_df")
        
        if input_df is None:
            # Try to find the result of the previous task
            # This is a simplification; in a real DAG we'd look up dependencies
            for key, value in context.items():
                if isinstance(value, DataFrame):
                    input_df = value
                    break
        
        if input_df is None:
            raise ValueError("No input DataFrame found for TransformTask")
            
        current_df = input_df
        
        for transformer_conf in transformers_config:
            name = transformer_conf.get("name")
            params = transformer_conf.get("params", {})
            
            self.logger.info(f"Applying transformer: {name}")
            
            transformer_func = get_transformer(name)
            current_df = transformer_func(current_df, **params)
            
        return current_df
