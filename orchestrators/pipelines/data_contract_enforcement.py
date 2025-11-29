from ..base import BasePipeline
from ..tasks.quality import RunGreatExpectationsTask

class DataContractEnforcementPipeline(BasePipeline):
    """
    Validates row counts, freshness, schema from config → fails job on breach
    """
    def build(self) -> 'BasePipeline':
        # Using GE task as it can enforce contracts
        self.add_task(RunGreatExpectationsTask("enforce_contract", {
            "data_contract": self.config.data_contract,
            "fail_on_error": True
        }))
        return self
