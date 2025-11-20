"""
Configuration loader for Sparkle connections.

Loads config from JSON/YAML with:
- Environment overlays (dev/qa/prod)
- Secret interpolation (${ENV_VAR})
- Schema validation
- Default value resolution
"""

import json
import yaml
import os
import re
from typing import Dict, Any, Optional
from pathlib import Path
import logging

logger = logging.getLogger("sparkle.connections.config_loader")


class ConfigLoader:
    """
    Load and resolve connection configurations.

    Example:
        >>> loader = ConfigLoader()
        >>> config = loader.load("postgres", env="prod")
        >>> # Returns config/connections/postgres/prod.json with secrets resolved
    """

    def __init__(self, config_root: Optional[str] = None):
        """
        Initialize config loader.

        Args:
            config_root: Root directory for configs (default: config/)
        """
        if config_root is None:
            # Default to config/ at project root
            project_root = Path(__file__).parent.parent
            config_root = project_root / "config" / "connections"
        else:
            config_root = Path(config_root)

        self.config_root = config_root
        self.secret_pattern = re.compile(r'\$\{([A-Z_][A-Z0-9_]*)\}')

    def load(
        self,
        connection_name: str,
        env: str = "dev",
        overlay_common: bool = True
    ) -> Dict[str, Any]:
        """
        Load configuration for a connection.

        Args:
            connection_name: Name of connection (e.g., 'postgres', 's3')
            env: Environment (dev/qa/prod)
            overlay_common: Whether to overlay common.json first

        Returns:
            Resolved configuration dictionary

        Example:
            >>> loader = ConfigLoader()
            >>> config = loader.load("postgres", env="prod")
            >>> # Loads config/connections/postgres/prod.json
            >>> # Overlays config/connections/postgres/common.json
            >>> # Resolves ${POSTGRES_PASSWORD} from environment
        """
        config = {}

        # Load common config if exists
        if overlay_common:
            common_config = self._load_file(connection_name, "common")
            if common_config:
                config.update(common_config)

        # Load environment-specific config
        env_config = self._load_file(connection_name, env)
        if env_config:
            config.update(env_config)
        elif not config:
            raise FileNotFoundError(
                f"No config found for connection '{connection_name}' in env '{env}'"
            )

        # Resolve secrets
        config = self._resolve_secrets(config)

        logger.info(f"Loaded config for {connection_name}/{env}")
        return config

    def _load_file(self, connection_name: str, filename: str) -> Optional[Dict[str, Any]]:
        """
        Load a single config file (JSON or YAML).

        Args:
            connection_name: Connection name
            filename: File name (without extension)

        Returns:
            Config dictionary or None if not found
        """
        config_dir = self.config_root / connection_name

        # Try JSON first
        json_path = config_dir / f"{filename}.json"
        if json_path.exists():
            with open(json_path, 'r') as f:
                return json.load(f)

        # Try YAML
        yaml_path = config_dir / f"{filename}.yaml"
        if yaml_path.exists():
            with open(yaml_path, 'r') as f:
                return yaml.safe_load(f)

        # Try YML
        yml_path = config_dir / f"{filename}.yml"
        if yml_path.exists():
            with open(yml_path, 'r') as f:
                return yaml.safe_load(f)

        return None

    def _resolve_secrets(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively resolve ${ENV_VAR} references in config.

        Args:
            config: Configuration dictionary

        Returns:
            Config with secrets resolved from environment variables

        Example:
            >>> config = {"password": "${DB_PASSWORD}"}
            >>> # If DB_PASSWORD=secret123 in environment
            >>> resolved = loader._resolve_secrets(config)
            >>> # Returns {"password": "secret123"}
        """
        if isinstance(config, dict):
            return {k: self._resolve_secrets(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._resolve_secrets(item) for item in config]
        elif isinstance(config, str):
            return self._resolve_string_secrets(config)
        else:
            return config

    def _resolve_string_secrets(self, value: str) -> str:
        """
        Replace ${ENV_VAR} patterns in string with environment variable values.

        Args:
            value: String potentially containing ${ENV_VAR} patterns

        Returns:
            String with environment variables resolved
        """
        def replacer(match):
            env_var = match.group(1)
            env_value = os.environ.get(env_var)

            if env_value is None:
                logger.warning(f"Environment variable {env_var} not found, using placeholder")
                return f"${{MISSING:{env_var}}}"

            return env_value

        return self.secret_pattern.sub(replacer, value)

    def list_connections(self) -> list:
        """
        List all available connection configurations.

        Returns:
            List of connection names
        """
        if not self.config_root.exists():
            return []

        return [
            d.name for d in self.config_root.iterdir()
            if d.is_dir() and not d.name.startswith('_')
        ]

    def list_environments(self, connection_name: str) -> list:
        """
        List available environments for a connection.

        Args:
            connection_name: Name of connection

        Returns:
            List of environment names
        """
        config_dir = self.config_root / connection_name
        if not config_dir.exists():
            return []

        envs = []
        for file in config_dir.iterdir():
            if file.suffix in ['.json', '.yaml', '.yml'] and file.stem != 'common':
                envs.append(file.stem)

        return envs


def load_connection_config(connection_name: str, env: str = "dev") -> Dict[str, Any]:
    """
    Convenience function to load connection config.

    Args:
        connection_name: Name of connection
        env: Environment

    Returns:
        Configuration dictionary

    Example:
        >>> config = load_connection_config("postgres", env="prod")
    """
    loader = ConfigLoader()
    return loader.load(connection_name, env)
