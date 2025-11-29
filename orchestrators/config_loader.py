import os
import json
import yaml
from typing import Dict, Any
from .base import PipelineConfig

class ConfigLoader:
    """
    Loads and validates config with environment overlay.
    """
    def __init__(self, config_root: str = "config"):
        self.config_root = config_root

    def load_config(self, pipeline_name: str, env: str) -> PipelineConfig:
        """
        Load configuration for a specific pipeline and environment.
        """
        # 1. Load base config (optional)
        base_config = self._load_file(f"{self.config_root}/{pipeline_name}/base.json") or {}
        
        # 2. Load env config
        env_config = self._load_file(f"{self.config_root}/{pipeline_name}/{env}.json")
        if not env_config:
             # Try YAML
            env_config = self._load_file(f"{self.config_root}/{pipeline_name}/{env}.yaml")
            
        if not env_config:
            raise FileNotFoundError(f"Configuration not found for pipeline {pipeline_name} in env {env}")
            
        # 3. Merge configs (simple merge)
        final_config = {**base_config, **env_config}
        
        # 4. Convert to PipelineConfig object
        return PipelineConfig(
            pipeline_name=pipeline_name,
            env=env,
            **final_config
        )

    def _load_file(self, path: str) -> Dict[str, Any]:
        if not os.path.exists(path):
            return {}
            
        with open(path, 'r') as f:
            if path.endswith('.json'):
                return json.load(f)
            elif path.endswith('.yaml') or path.endswith('.yml'):
                return yaml.safe_load(f)
        return {}
