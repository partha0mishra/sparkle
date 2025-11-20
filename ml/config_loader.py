"""
Configuration loader for ML components.

Loads ML component configurations from JSON/YAML files with validation.
"""

import json
import yaml
import os
from typing import Dict, Any, Optional
from pathlib import Path
import logging


logger = logging.getLogger("sparkle.ml.config_loader")


def load_ml_config(
    component_name: str,
    env: str = "prod",
    config_dir: Optional[str] = None
) -> Dict[str, Any]:
    """
    Load ML component configuration from JSON/YAML.

    Looks for config file at: config/ml/{component_name}/{env}.json or .yaml

    Args:
        component_name: Name of ML component
        env: Environment (dev/qa/prod)
        config_dir: Optional custom config directory

    Returns:
        Configuration dictionary

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config is invalid
    """
    if config_dir is None:
        project_root = Path(__file__).parent.parent
        config_dir = project_root / "config" / "ml"
    else:
        config_dir = Path(config_dir)

    # Try JSON first, then YAML
    json_file = config_dir / component_name / f"{env}.json"
    yaml_file = config_dir / component_name / f"{env}.yaml"

    if json_file.exists():
        config_file = json_file
        with open(config_file, "r") as f:
            config = json.load(f)
    elif yaml_file.exists():
        config_file = yaml_file
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
    else:
        raise FileNotFoundError(
            f"Config file not found for {component_name} in {env}. "
            f"Tried: {json_file} and {yaml_file}"
        )

    logger.info(f"Loaded config from: {config_file}")

    # Resolve environment variables
    config = _resolve_env_vars(config)

    # Validate required fields
    _validate_config(config, component_name)

    return config


def _resolve_env_vars(config: Any) -> Any:
    """Recursively resolve environment variables in config."""
    import re

    if isinstance(config, dict):
        return {k: _resolve_env_vars(v) for k, v in config.items()}
    elif isinstance(config, list):
        return [_resolve_env_vars(item) for item in config]
    elif isinstance(config, str):
        pattern = re.compile(r'\$\{([A-Z_][A-Z0-9_]*)(:-([^}]*))?\}')

        def replacer(match):
            var_name = match.group(1)
            default_value = match.group(3)
            value = os.environ.get(var_name)

            if value is None:
                if default_value is not None:
                    return default_value
                else:
                    logger.warning(f"Environment variable not set: {var_name}")
                    return f"${{MISSING:{var_name}}}"

            return value

        return pattern.sub(replacer, config)
    else:
        return config


def _validate_config(config: Dict[str, Any], component_name: str):
    """Validate ML component configuration."""
    # Common required fields can be validated here
    # Component-specific validation happens in the component itself
    if not isinstance(config, dict):
        raise ValueError(f"Config for {component_name} must be a dictionary")

    logger.debug(f"Config validation passed for {component_name}")


def save_ml_config(
    component_name: str,
    env: str,
    config: Dict[str, Any],
    config_dir: Optional[str] = None,
    format: str = "json"
):
    """
    Save ML component configuration.

    Args:
        component_name: Name of ML component
        env: Environment
        config: Configuration dictionary
        config_dir: Optional custom config directory
        format: 'json' or 'yaml'
    """
    if config_dir is None:
        project_root = Path(__file__).parent.parent
        config_dir = project_root / "config" / "ml"
    else:
        config_dir = Path(config_dir)

    # Create directory
    component_config_dir = config_dir / component_name
    component_config_dir.mkdir(parents=True, exist_ok=True)

    # Write file
    if format == "json":
        config_file = component_config_dir / f"{env}.json"
        with open(config_file, "w") as f:
            json.dump(config, f, indent=2)
    elif format == "yaml":
        config_file = component_config_dir / f"{env}.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f, default_flow_style=False)
    else:
        raise ValueError(f"Unsupported format: {format}")

    logger.info(f"Saved config to: {config_file}")


def get_config_template(component_type: str) -> Dict[str, Any]:
    """
    Get configuration template for a component type.

    Args:
        component_type: Type of component (e.g., 'feature_engineer', 'model_trainer')

    Returns:
        Template configuration dictionary
    """
    templates = {
        "feature_engineer": {
            "input_table": "bronze.schema.table",
            "output_table": "silver.schema.table",
            "feature_columns": ["col1", "col2"],
            "mlflow_experiment": "/Shared/sparkle/features",
            "uc_catalog": "feature_store",
            "uc_schema": "features"
        },
        "model_trainer": {
            "train_table": "gold.schema.train_data",
            "test_table": "gold.schema.test_data",
            "target_column": "label",
            "feature_columns": ["feature1", "feature2"],
            "model_name": "my_model",
            "hyperparameters": {
                "max_depth": 10,
                "learning_rate": 0.1
            },
            "mlflow_experiment": "/Shared/sparkle/training",
            "uc_catalog": "ml",
            "uc_schema": "models"
        },
        "model_scorer": {
            "input_table": "gold.schema.score_data",
            "output_table": "gold.schema.predictions",
            "model_name": "my_model",
            "model_version": "Production",
            "feature_columns": ["feature1", "feature2"],
            "uc_catalog": "ml",
            "uc_schema": "models"
        }
    }

    return templates.get(component_type, {
        "mlflow_experiment": "/Shared/sparkle/ml",
        "uc_catalog": "ml",
        "uc_schema": "default"
    })
