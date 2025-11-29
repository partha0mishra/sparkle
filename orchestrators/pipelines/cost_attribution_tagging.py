from ..base import BasePipeline
from ..tasks.quality import RunSQLFileTask

class CostAttributionTaggingPipeline(BasePipeline):
    """
    Adds job_id, pipeline_name, business_unit tags to all Delta tables written
    """
    def build(self) -> 'BasePipeline':
        # Apply tags to destination table
        table = f"{self.config.destination_catalog}.{self.config.destination_schema}.{self.config.destination_table}"
        tags = {
            "pipeline_name": self.config.pipeline_name,
            "business_unit": self.config.extra_config.get("business_unit", "unknown"),
            "env": self.config.env
        }
        
        tag_str = ", ".join([f"'{k}' = '{v}'" for k, v in tags.items()])
        sql = f"ALTER TABLE {table} SET TBLPROPERTIES ({tag_str})"
        
        self.add_task(RunSQLFileTask("apply_tags", {
            "sql_statement": sql
        }))
        return self
