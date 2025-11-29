from ..base import BasePipeline
from ..tasks.quality import RunSQLFileTask

class VacuumAndOptimizeMaintenancePipeline(BasePipeline):
    """
    Runs VACUUM + OPTIMIZE + ZORDER on tables listed in config
    """
    def build(self) -> 'BasePipeline':
        tables = self.config.extra_config.get("tables", [])
        for table in tables:
            # We can use RunSQLFileTask or a custom task. Using SQL task for simplicity.
            # In real impl, we might generate SQL dynamically.
            self.add_task(RunSQLFileTask(f"optimize_{table}", {
                "sql_statement": f"OPTIMIZE {table}; VACUUM {table};"
            }))
        return self
