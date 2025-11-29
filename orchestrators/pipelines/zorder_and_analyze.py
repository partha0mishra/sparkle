from ..base import BasePipeline
from ..tasks.quality import RunSQLFileTask

class ZOrderAndAnalyzePipeline(BasePipeline):
    """
    Z-orders by config columns + ANALYZE for statistics
    """
    def build(self) -> 'BasePipeline':
        table = f"{self.config.destination_catalog}.{self.config.destination_schema}.{self.config.destination_table}"
        zorder_cols = self.config.extra_config.get("zorder_columns", [])
        
        sql = f"OPTIMIZE {table}"
        if zorder_cols:
            sql += f" ZORDER BY ({', '.join(zorder_cols)})"
        sql += f"; ANALYZE TABLE {table} COMPUTE STATISTICS;"
        
        self.add_task(RunSQLFileTask("zorder_analyze", {
            "sql_statement": sql
        }))
        return self
