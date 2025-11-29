"""
Task Building Blocks (22 components).

Reusable tasks for orchestration pipelines.
"""

from .ingest import IngestTask
from .transform import TransformTask
from .write import (
    WriteDeltaTask,
    WriteFeatureTableTask,
    CreateUnityCatalogTableTask
)
from .ml import (
    TrainMLModelTask,
    BatchScoreTask,
    RegisterModelTask,
    PromoteModelChampionTask
)
from .quality import (
    RunGreatExpectationsTask,
    RunDbtCoreTask,
    RunSQLFileTask
)
from .governance import (
    GrantPermissionsTask,
    WaitForTableFreshnessTask
)
from .notification import (
    SendSlackAlertTask,
    SendEmailAlertTask,
    NotifyOnFailureTask
)
from .orchestration import (
    RunNotebookTask,
    TriggerDownstreamPipelineTask,
    AutoRecoverTask
)
from .observability import (
    MonteCarloDataIncidentTask,
    LightupDataSLAAlertTask
)

__all__ = [
    # Ingest (1)
    "IngestTask",

    # Transform (1)
    "TransformTask",

    # Write (3)
    "WriteDeltaTask",
    "WriteFeatureTableTask",
    "CreateUnityCatalogTableTask",

    # ML (4)
    "TrainMLModelTask",
    "BatchScoreTask",
    "RegisterModelTask",
    "PromoteModelChampionTask",

    # Quality (3)
    "RunGreatExpectationsTask",
    "RunDbtCoreTask",
    "RunSQLFileTask",

    # Governance (2)
    "GrantPermissionsTask",
    "WaitForTableFreshnessTask",

    # Notification (3)
    "SendSlackAlertTask",
    "SendEmailAlertTask",
    "NotifyOnFailureTask",

    # Orchestration (3)
    "RunNotebookTask",
    "TriggerDownstreamPipelineTask",
    "AutoRecoverTask",

    # Observability (2)
    "MonteCarloDataIncidentTask",
    "LightupDataSLAAlertTask",
]
