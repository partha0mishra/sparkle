from typing import Dict, Any
from ..base import BasePipeline

class PrefectFlowDecoratorAdapter:
    """
    Converts pipeline to @flow with parameters from config
    """
    def generate_deployment(self, pipeline: BasePipeline, output_path: str = None) -> str:
        flow_code = f"""
from prefect import flow, task

@flow(name="{pipeline.config.pipeline_name}")
def {pipeline.config.pipeline_name}_flow():
    # Pipeline logic
    pass
"""
        return flow_code

class PrefectDeploymentBuilderAdapter:
    """
    Generates deployment YAML from config
    """
    def generate_deployment(self, pipeline: BasePipeline, output_path: str = None) -> str:
        return "deployment_yaml"

class PrefectAutomationsAlertingAdapter:
    """
    Creates failure/success automations (Slack/Teams)
    """
    def generate_deployment(self, pipeline: BasePipeline, output_path: str = None) -> str:
        return "automation_code"

class PrefectParameterisedRunAdapter:
    """
    Allows ad-hoc runs with parameter overrides
    """
    def generate_deployment(self, pipeline: BasePipeline, output_path: str = None) -> str:
        return "parameterised_run_code"

class PrefectResultPersistenceDeltaAdapter:
    """
    Persists intermediate results to Delta for debugging
    """
    def generate_deployment(self, pipeline: BasePipeline, output_path: str = None) -> str:
        return "result_persistence_code"
