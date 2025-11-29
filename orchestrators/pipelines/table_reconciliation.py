from ..base import BasePipeline
from ..tasks.transform import TransformTask
from ..tasks.notification import SendSlackAlertTask

class TableReconciliationPipeline(BasePipeline):
    """
    Compares row counts + row hash between source and target → reports mismatches
    """
    def build(self) -> 'BasePipeline':
        # 1. Compare
        self.add_task(TransformTask("compare_tables", {
            "transformers": [
                {
                    "name": "reconciler",
                    "params": {
                        "source_table": self.config.source_table,
                        "target_table": self.config.destination_table
                    }
                }
            ]
        }))
        
        # 2. Report
        self.add_task(SendSlackAlertTask("report_mismatch", {
            "message": "Table reconciliation found mismatches"
        }))
        
        return self
