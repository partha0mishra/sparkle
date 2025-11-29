"""
Sparkle Orchestration Package

84 production-ready orchestration components for lakehouse pipelines.

Categories:
- Core Pipeline Templates (24): Bronze, Silver, Gold, Feature Store, ML, Quality, Maintenance
- Task Building Blocks (22): Ingest, Transform, Write, ML, Quality, Governance, Notification
- Multi-Orchestrator Adapters (25): Databricks, Airflow, Dagster, Prefect, Mage
- Scheduling & Trigger Patterns (12): Cron, Event-Driven, Monitoring

All components are:
- 100% config-driven (ZERO hard-coded values)
- Reads from config/{pipeline_name}/{env}.json
- Supports multi-orchestrator deployment
- Production-ready with error handling and observability

Usage:
    from orchestration import Pipeline

    # Load pipeline from config
    pipeline = Pipeline.get("customer_silver_daily", env="prod")

    # Build and execute
    pipeline.build()
    pipeline.run()

    # Generate deployment
    pipeline.deploy(orchestrator="databricks")
"""

__version__ = "1.0.0"

# Core
from .base import BasePipeline, BaseTask, PipelineConfig
from .factory import PipelineFactory
from .config_loader import ConfigLoader

# Alias for easier usage
Pipeline = PipelineFactory

__all__ = [
    "BasePipeline",
    "BaseTask",
    "PipelineConfig",
    "PipelineFactory",
    "Pipeline",
    "ConfigLoader",
]
