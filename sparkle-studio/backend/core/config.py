"""
Core configuration for Sparkle Studio Backend.
Loads environment variables and studio.yaml configuration.
"""
import os
from pathlib import Path
from typing import Optional
import yaml
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings loaded from environment and config files."""

    # API Settings
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Sparkle Studio API"
    VERSION: str = "1.0.0"

    # Environment
    SPARKLE_ENV: str = Field(default="local", env="SPARKLE_ENV")
    DEBUG: bool = Field(default=True, env="DEBUG")

    # CORS
    BACKEND_CORS_ORIGINS: list[str] = Field(
        default=["http://localhost:3000", "http://localhost:8000"],
        env="BACKEND_CORS_ORIGINS"
    )

    # Git Configuration
    GIT_REPO_PATH: str = Field(default="/app/git_repo", env="GIT_REPO_PATH")
    GIT_DEFAULT_BRANCH: str = Field(default="main", env="GIT_DEFAULT_BRANCH")
    GIT_REMOTE_URL: Optional[str] = Field(default=None, env="GIT_REMOTE_URL")
    GIT_USERNAME: Optional[str] = Field(default=None, env="GIT_USERNAME")
    GIT_TOKEN: Optional[str] = Field(default=None, env="GIT_TOKEN")

    # Spark Configuration
    SPARK_MASTER: str = Field(default="local[*]", env="SPARK_MASTER")
    SPARK_REMOTE: Optional[str] = Field(default=None, env="SPARK_REMOTE")
    SPARK_APP_NAME: str = Field(default="SparkleStudio", env="SPARK_APP_NAME")

    # Cluster Configuration (Databricks/EMR)
    CLUSTER_TYPE: Optional[str] = Field(default=None, env="CLUSTER_TYPE")  # databricks, emr, local
    CLUSTER_URL: Optional[str] = Field(default=None, env="CLUSTER_URL")
    CLUSTER_TOKEN: Optional[str] = Field(default=None, env="CLUSTER_TOKEN")

    # Studio Config Path
    STUDIO_CONFIG_PATH: str = Field(
        default="/app/config/studio.yaml",
        env="STUDIO_CONFIG_PATH"
    )

    # Sparkle Engine Path
    SPARKLE_ENGINE_PATH: str = Field(
        default="/opt/sparkle",
        env="SPARKLE_ENGINE_PATH"
    )

    # Security (stub for Phase 8)
    SECRET_KEY: str = Field(
        default="dev-secret-key-change-in-production",
        env="SECRET_KEY"
    )
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    class Config:
        env_file = ".env"
        case_sensitive = True


class StudioConfig:
    """Studio-specific configuration loaded from studio.yaml."""

    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self._config = self._load_config()

    def _load_config(self) -> dict:
        """Load studio.yaml configuration."""
        if not self.config_path.exists():
            # Return default configuration
            return self._default_config()

        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f) or self._default_config()

    def _default_config(self) -> dict:
        """Default studio configuration."""
        return {
            "git": {
                "repository": None,
                "branch_strategy": {
                    "main": "main",
                    "development": "develop",
                    "feature_prefix": "feature/"
                },
                "auto_commit": True
            },
            "cluster": {
                "default": "local",
                "local": {
                    "type": "local",
                    "spark_master": "local[*]"
                }
            },
            "pipelines": {
                "directory": "pipelines",
                "config_directory": "config"
            },
            "execution": {
                "dry_run_sample_size": 1000,
                "default_parallelism": 4
            },
            "ui": {
                "theme": "dark",
                "auto_save_interval": 30
            }
        }

    def get(self, key: str, default=None):
        """Get configuration value by dot-notation key."""
        keys = key.split('.')
        value = self._config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
            if value is None:
                return default
        return value

    def reload(self):
        """Reload configuration from file."""
        self._config = self._load_config()


# Global settings instance
settings = Settings()

# Global studio config instance
studio_config = StudioConfig(settings.STUDIO_CONFIG_PATH)
