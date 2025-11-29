from ..base import BasePipeline
from ..tasks.governance import GrantPermissionsTask

class UnityCatalogGovernanceSyncPipeline(BasePipeline):
    """
    Applies grants, tags, and ownership from config YAML
    """
    def build(self) -> 'BasePipeline':
        self.add_task(GrantPermissionsTask("sync_governance", {
            "grants": self.config.extra_config.get("grants", []),
            "tags": self.config.tags,
            "owner": self.config.extra_config.get("owner")
        }))
        return self
