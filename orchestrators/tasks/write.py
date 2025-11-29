from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from ..base import BaseTask

class WriteDeltaTask(BaseTask):
    """
    Writes DataFrame to Delta with mode, partitionBy, zOrderBy.
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        input_df = context.get("input_df")
        # Fallback to finding any DataFrame in context
        if input_df is None:
            for key, value in context.items():
                if isinstance(value, DataFrame):
                    input_df = value
                    break
                    
        if input_df is None:
            raise ValueError("No input DataFrame found for WriteDeltaTask")

        catalog = self.config.get("destination_catalog")
        schema = self.config.get("destination_schema")
        table = self.config.get("destination_table")
        full_table_name = f"{catalog}.{schema}.{table}"
        
        mode = self.config.get("write_mode", "append")
        partition_cols = self.config.get("partition_columns", [])
        zorder_cols = self.config.get("zorder_columns", [])
        
        self.logger.info(f"Writing to {full_table_name} with mode={mode}")
        
        writer = input_df.write.format("delta").mode(mode)
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
            
        writer.saveAsTable(full_table_name)
        
        if zorder_cols:
            self.logger.info(f"Optimizing and Z-Ordering {full_table_name}")
            spark.sql(f"OPTIMIZE {full_table_name} ZORDER BY ({', '.join(zorder_cols)})")
            
        return full_table_name

class WriteFeatureTableTask(BaseTask):
    """
    Writes to Unity Catalog Feature Store with primary keys.
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        # Placeholder for Feature Store integration
        # In a real implementation, this would use FeatureStoreClient
        self.logger.info("Writing to Feature Store (Mock)")
        return "feature_table_written"

class CreateUnityCatalogTableTask(BaseTask):
    """
    Creates external/managed Delta table with schema.
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        catalog = self.config.get("destination_catalog")
        schema = self.config.get("destination_schema")
        table = self.config.get("destination_table")
        full_table_name = f"{catalog}.{schema}.{table}"
        
        self.logger.info(f"Creating table {full_table_name}")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table_name} (id STRING) USING DELTA")
        return full_table_name
