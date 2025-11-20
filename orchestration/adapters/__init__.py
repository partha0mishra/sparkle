"""
Multi-Orchestrator Adapters.

Adapters for deploying Sparkle pipelines to various orchestration platforms.
"""

# Existing adapters (to be implemented)
from .databricks import *
from .airflow import *
from .dagster import *
from .prefect import *
from .mage import *

# New adapters
from .dbt_cloud import (
    dbtProjectAdapter,
    dbtModelAdapter,
    dbtTestAdapter,
    dbtMacroAdapter,
    dbtExposureAdapter
)

from .step_functions import (
    StepFunctionsStateMachineAdapter,
    StepFunctionsTaskAdapter,
    StepFunctionsChoiceAdapter,
    StepFunctionsParallelAdapter,
    StepFunctionsEventBridgeAdapter
)

from .argo_workflows import (
    ArgoWorkflowTemplateAdapter,
    ArgoDagAdapter,
    ArgoStepTemplateAdapter,
    ArgoEventSourceAdapter,
    ArgoArtifactAdapter
)

__all__ = [
    # dbt Cloud (5)
    "dbtProjectAdapter",
    "dbtModelAdapter",
    "dbtTestAdapter",
    "dbtMacroAdapter",
    "dbtExposureAdapter",

    # AWS Step Functions (5)
    "StepFunctionsStateMachineAdapter",
    "StepFunctionsTaskAdapter",
    "StepFunctionsChoiceAdapter",
    "StepFunctionsParallelAdapter",
    "StepFunctionsEventBridgeAdapter",

    # Argo Workflows (5)
    "ArgoWorkflowTemplateAdapter",
    "ArgoDagAdapter",
    "ArgoStepTemplateAdapter",
    "ArgoEventSourceAdapter",
    "ArgoArtifactAdapter",
]


def get_adapter(orchestrator: str):
    """
    Get adapter for specified orchestrator.

    Args:
        orchestrator: Name of orchestrator (databricks, airflow, dagster, prefect, mage, dbt, stepfunctions, argo)

    Returns:
        Adapter instance
    """
    adapters = {
        "databricks": "DatabricksWorkflowsAdapter",
        "airflow": "AirflowAdapter",
        "dagster": "DagsterAdapter",
        "prefect": "PrefectAdapter",
        "mage": "MageAdapter",
        "dbt": "dbtProjectAdapter",
        "dbt_cloud": "dbtProjectAdapter",
        "stepfunctions": "StepFunctionsStateMachineAdapter",
        "step_functions": "StepFunctionsStateMachineAdapter",
        "argo": "ArgoWorkflowTemplateAdapter",
        "argo_workflows": "ArgoWorkflowTemplateAdapter",
    }

    adapter_name = adapters.get(orchestrator.lower())
    if not adapter_name:
        raise ValueError(f"Unknown orchestrator: {orchestrator}")

    # Return adapter class (simplified - would instantiate properly)
    return globals()[adapter_name]
