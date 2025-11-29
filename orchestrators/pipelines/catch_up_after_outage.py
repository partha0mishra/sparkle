from ..base import BasePipeline
from ..tasks.orchestration import AutoRecoverTask

class CatchUpAfterOutagePipeline(BasePipeline):
    """
    Detects lag via watermark table → runs parallel catch-up slices
    """
    def build(self) -> 'BasePipeline':
        self.add_task(AutoRecoverTask("catch_up", {
            "mode": "catchup",
            "watermark_table": self.config.extra_config.get("watermark_table")
        }))
        return self
