"""
Configuration loader for Sparkle ingestors.

Loads ingestor configurations from config/ingestors/{ingestor_name}/{env}.json
with environment variable resolution and validation.
"""

import json
import os
import re
from typing import Dict, Any, Optional
from pathlib import Path
import logging


logger = logging.getLogger("sparkle.ingestors.config_loader")


def load_ingestor_config(
    ingestor_name: str,
    env: str = "prod",
    config_dir: Optional[str] = None
) -> Dict[str, Any]:
    """
    Load ingestor configuration from JSON file.

    Looks for config file at: config/ingestors/{ingestor_name}/{env}.json

    Args:
        ingestor_name: Name of the ingestor
        env: Environment (dev/qa/prod)
        config_dir: Optional custom config directory

    Returns:
        Configuration dictionary with env vars resolved

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config is invalid
    """
    # Determine config directory
    if config_dir is None:
        # Assume config/ is at project root
        project_root = Path(__file__).parent.parent
        config_dir = project_root / "config" / "ingestors"
    else:
        config_dir = Path(config_dir)

    # Build config file path
    config_file = config_dir / ingestor_name / f"{env}.json"

    if not config_file.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_file}. "
            f"Create config at: config/ingestors/{ingestor_name}/{env}.json"
        )

    logger.info(f"Loading config from: {config_file}")

    # Load JSON
    with open(config_file, "r") as f:
        config = json.load(f)

    # Resolve environment variables
    config = _resolve_env_vars(config)

    # Validate required fields
    _validate_config(config, ingestor_name)

    return config


def _resolve_env_vars(config: Any) -> Any:
    """
    Recursively resolve environment variables in config.

    Supports patterns:
    - ${ENV_VAR} - Replace with environment variable
    - ${ENV_VAR:-default} - Replace with env var or default value

    Args:
        config: Configuration (dict, list, or primitive)

    Returns:
        Configuration with env vars resolved
    """
    if isinstance(config, dict):
        return {k: _resolve_env_vars(v) for k, v in config.items()}

    elif isinstance(config, list):
        return [_resolve_env_vars(item) for item in config]

    elif isinstance(config, str):
        # Pattern: ${VAR} or ${VAR:-default}
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


def _validate_config(config: Dict[str, Any], ingestor_name: str):
    """
    Validate that config has required fields.

    Args:
        config: Configuration dictionary
        ingestor_name: Name of ingestor

    Raises:
        ValueError: If required fields are missing
    """
    required_fields = ["source_name", "target_table"]

    missing = [field for field in required_fields if field not in config]

    if missing:
        raise ValueError(
            f"Missing required config fields for {ingestor_name}: {', '.join(missing)}"
        )

    logger.debug(f"Config validation passed for {ingestor_name}")


def save_ingestor_config(
    ingestor_name: str,
    env: str,
    config: Dict[str, Any],
    config_dir: Optional[str] = None
):
    """
    Save ingestor configuration to JSON file.

    Args:
        ingestor_name: Name of the ingestor
        env: Environment (dev/qa/prod)
        config: Configuration dictionary
        config_dir: Optional custom config directory
    """
    # Determine config directory
    if config_dir is None:
        project_root = Path(__file__).parent.parent
        config_dir = project_root / "config" / "ingestors"
    else:
        config_dir = Path(config_dir)

    # Create directory if needed
    ingestor_config_dir = config_dir / ingestor_name
    ingestor_config_dir.mkdir(parents=True, exist_ok=True)

    # Build config file path
    config_file = ingestor_config_dir / f"{env}.json"

    logger.info(f"Saving config to: {config_file}")

    # Write JSON with pretty formatting
    with open(config_file, "w") as f:
        json.dump(config, f, indent=2)


def list_ingestors() -> list:
    """
    List all available ingestor configurations.

    Returns:
        List of ingestor names with configs
    """
    project_root = Path(__file__).parent.parent
    config_dir = project_root / "config" / "ingestors"

    if not config_dir.exists():
        return []

    ingestors = [
        d.name for d in config_dir.iterdir()
        if d.is_dir() and not d.name.startswith(".")
    ]

    return sorted(ingestors)


def get_config_example(ingestor_type: str) -> Dict[str, Any]:
    """
    Get an example configuration for an ingestor type.

    Args:
        ingestor_type: Type of ingestor

    Returns:
        Example configuration dictionary
    """
    examples = {
        "partitioned_parquet": {
            "source_name": "my_partitioned_data",
            "source_path": "s3://my-bucket/data/partitioned/",
            "partition_columns": ["year", "month", "day"],
            "target_catalog": "bronze",
            "target_schema": "default",
            "target_table": "partitioned_data",
            "watermark_enabled": True,
            "watermark_column": "partition_date",
            "file_format": "parquet",
            "schema_evolution": "add_new_columns"
        },
        "salesforce_incremental": {
            "source_name": "salesforce_prod",
            "connection_name": "salesforce",
            "connection_env": "prod",
            "sobject": "Account",
            "target_catalog": "bronze",
            "target_schema": "salesforce",
            "target_table": "accounts",
            "watermark_enabled": True,
            "watermark_column": "LastModifiedDate",
            "batch_size": 10000,
            "fields": ["Id", "Name", "Type", "Industry", "LastModifiedDate"]
        },
        "kafka_topic_raw": {
            "source_name": "events_topic",
            "connection_name": "kafka",
            "connection_env": "prod",
            "topic": "customer-events",
            "target_catalog": "bronze",
            "target_schema": "streaming",
            "target_table": "customer_events",
            "checkpoint_location": "/mnt/checkpoints/customer_events",
            "trigger_interval": "1 minute",
            "output_mode": "append",
            "value_deserializer": "json"
        }
    }

    return examples.get(ingestor_type, {
        "source_name": "example_source",
        "target_catalog": "bronze",
        "target_schema": "default",
        "target_table": "example_table",
        "watermark_enabled": True
    })
