from ..base import BasePipeline
from ..tasks.quality import RunGreatExpectationsTask

class DataQualityExpectationsPipeline(BasePipeline):
    """
    Runs Great Expectations suite defined in config → quarantines bad data
    """
    def build(self) -> 'BasePipeline':
        self.add_task(RunGreatExpectationsTask("run_ge_suite", {
            "expectations_suite": self.config.expectations_suite,
            "quarantine_table": self.config.extra_config.get("quarantine_table")
        }))
        return self
