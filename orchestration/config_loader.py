"""
Configuration loader for orchestration pipelines.

Loads configs from config/{pipeline_name}/{env}.json with validation.
"""

import json
import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional
from .base import PipelineConfig
import logging

logger = logging.getLogger(__name__)


def load_pipeline_config(
    pipeline_name: str,
    env: str = "prod",
    config_dir: Optional[str] = None
) -> PipelineConfig:
    """
    Load pipeline configuration from JSON/YAML file.

    Args:
        pipeline_name: Name of the pipeline
        env: Environment (dev, qa, prod)
        config_dir: Override config directory path

    Returns:
        PipelineConfig object

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config is invalid
    """
    if config_dir is None:
        project_root = Path(__file__).parent.parent
        config_dir = project_root / "config" / "orchestration"

    config_dir = Path(config_dir)

    # Try JSON first, then YAML
    json_file = config_dir / pipeline_name / f"{env}.json"
    yaml_file = config_dir / pipeline_name / f"{env}.yaml"

    config_dict = None

    if json_file.exists():
        with open(json_file, "r") as f:
            config_dict = json.load(f)
        logger.info(f"Loaded config from {json_file}")

    elif yaml_file.exists():
        with open(yaml_file, "r") as f:
            config_dict = yaml.safe_load(f)
        logger.info(f"Loaded config from {yaml_file}")

    else:
        raise FileNotFoundError(
            f"Config file not found for pipeline '{pipeline_name}' in environment '{env}'. "
            f"Looked for: {json_file} or {yaml_file}"
        )

    # Resolve environment variables
    config_dict = _resolve_env_vars(config_dict)

    # Add pipeline_name and env if not present
    config_dict["pipeline_name"] = config_dict.get("pipeline_name", pipeline_name)
    config_dict["env"] = config_dict.get("env", env)

    # Convert to PipelineConfig
    config = PipelineConfig(**config_dict)

    return config


def _resolve_env_vars(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively resolve environment variables in config values.

    Args:
        config: Configuration dictionary

    Returns:
        Config with resolved environment variables
    """
    if isinstance(config, dict):
        return {k: _resolve_env_vars(v) for k, v in config.items()}
    elif isinstance(config, list):
        return [_resolve_env_vars(item) for item in config]
    elif isinstance(config, str):
        # Replace ${VAR} or ${VAR:default} with environment variable
        if config.startswith("${") and config.endswith("}"):
            var_spec = config[2:-1]

            if ":" in var_spec:
                var_name, default_value = var_spec.split(":", 1)
                return os.getenv(var_name, default_value)
            else:
                var_name = var_spec
                value = os.getenv(var_name)
                if value is None:
                    raise ValueError(f"Environment variable {var_name} is required but not set")
                return value
        return config
    else:
        return config


def validate_config(config: PipelineConfig) -> List[str]:
    """
    Validate pipeline configuration.

    Args:
        config: Pipeline configuration

    Returns:
        List of validation errors (empty if valid)
    """
    errors = []

    # Required fields
    if not config.pipeline_name:
        errors.append("pipeline_name is required")

    if not config.pipeline_type:
        errors.append("pipeline_type is required")

    # Pipeline-specific validation
    if config.pipeline_type in ["bronze_raw_ingestion", "bronze_streaming_ingestion"]:
        if not config.ingestor:
            errors.append(f"{config.pipeline_type} requires 'ingestor'")

        if not config.source_connection:
            errors.append(f"{config.pipeline_type} requires 'source_connection'")

        if not config.destination_catalog:
            errors.append(f"{config.pipeline_type} requires 'destination_catalog'")

        if not config.destination_schema:
            errors.append(f"{config.pipeline_type} requires 'destination_schema'")

        if not config.destination_table:
            errors.append(f"{config.pipeline_type} requires 'destination_table'")

    elif config.pipeline_type in ["silver_batch_transformation", "silver_streaming_transformation"]:
        if not config.source_table:
            errors.append(f"{config.pipeline_type} requires 'source_table'")

        if not config.destination_table:
            errors.append(f"{config.pipeline_type} requires 'destination_table'")

        if not config.transformers:
            errors.append(f"{config.pipeline_type} requires 'transformers' list")

    elif config.pipeline_type in ["gold_daily_aggregate", "gold_realtime_dashboard"]:
        if not config.source_table:
            errors.append(f"{config.pipeline_type} requires 'source_table'")

        if not config.destination_table:
            errors.append(f"{config.pipeline_type} requires 'destination_table'")

    elif config.pipeline_type == "dimension_daily_scd2":
        if not config.business_key:
            errors.append("dimension_daily_scd2 requires 'business_key'")

        if not config.source_table:
            errors.append("dimension_daily_scd2 requires 'source_table'")

    elif config.pipeline_type == "ml_training_champion_challenger":
        if not config.model_name:
            errors.append("ml_training_champion_challenger requires 'model_name'")

        if not config.feature_columns:
            errors.append("ml_training_champion_challenger requires 'feature_columns'")

        if not config.target_column:
            errors.append("ml_training_champion_challenger requires 'target_column'")

    # Streaming-specific validation
    if "streaming" in config.pipeline_type.lower():
        if not config.checkpoint_location:
            errors.append(f"Streaming pipeline requires 'checkpoint_location'")

    return errors


def load_config_schema() -> Dict[str, Any]:
    """
    Load the JSON schema for pipeline configurations.

    Returns:
        JSON schema dictionary
    """
    project_root = Path(__file__).parent.parent
    schema_file = project_root / "config" / "orchestration" / "schema.json"

    if schema_file.exists():
        with open(schema_file, "r") as f:
            return json.load(f)

    return {}


def save_pipeline_config(config: PipelineConfig, output_path: str):
    """
    Save pipeline configuration to JSON file.

    Args:
        config: Pipeline configuration
        output_path: Path to save config file
    """
    import dataclasses

    config_dict = dataclasses.asdict(config)

    with open(output_path, "w") as f:
        json.dump(config_dict, f, indent=2)

    logger.info(f"Saved config to {output_path}")


def merge_configs(base_config: Dict[str, Any], override_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge two configuration dictionaries.

    Args:
        base_config: Base configuration
        override_config: Override configuration

    Returns:
        Merged configuration
    """
    merged = base_config.copy()

    for key, value in override_config.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = merge_configs(merged[key], value)
        else:
            merged[key] = value

    return merged
