"""
Service for managing pipelines.
Handles loading, saving, and exporting pipelines from/to Git repository.
"""
import json
from pathlib import Path
from typing import Optional
from datetime import datetime

from core.config import settings, studio_config
from schemas.pipeline import (
    Pipeline,
    PipelineListItem,
    PipelineExportResponse,
)
from .git_service import git_service


class PipelineService:
    """Service for managing pipelines."""

    def __init__(self):
        self.repo_path = Path(settings.GIT_REPO_PATH)
        self.pipelines_dir = studio_config.get("pipelines.directory", "pipelines")
        self.config_dir = studio_config.get("pipelines.config_directory", "config")

    def _get_pipelines_path(self) -> Path:
        """Get path to pipelines directory."""
        path = self.repo_path / self.pipelines_dir
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _get_pipeline_path(self, pipeline_name: str) -> Path:
        """Get path to specific pipeline directory."""
        path = self._get_pipelines_path() / pipeline_name
        return path

    def list_pipelines(self) -> list[PipelineListItem]:
        """List all pipelines in the repository."""
        pipelines_path = self._get_pipelines_path()
        pipeline_items = []

        for pipeline_dir in pipelines_path.iterdir():
            if not pipeline_dir.is_dir():
                continue

            pipeline_file = pipeline_dir / "pipeline.json"
            if not pipeline_file.exists():
                continue

            try:
                with open(pipeline_file, "r") as f:
                    pipeline_data = json.load(f)

                metadata = pipeline_data.get("metadata", {})
                nodes = pipeline_data.get("nodes", [])
                edges = pipeline_data.get("edges", [])

                # Get file modification time
                updated_at = datetime.fromtimestamp(pipeline_file.stat().st_mtime)

                pipeline_items.append(
                    PipelineListItem(
                        name=pipeline_dir.name,
                        display_name=metadata.get("display_name"),
                        description=metadata.get("description"),
                        author=metadata.get("author"),
                        updated_at=updated_at,
                        node_count=len(nodes),
                        edge_count=len(edges),
                        tags=metadata.get("tags", []),
                    )
                )
            except Exception as e:
                print(f"Error loading pipeline {pipeline_dir.name}: {e}")
                continue

        return sorted(pipeline_items, key=lambda x: x.updated_at or datetime.min, reverse=True)

    def get_pipeline(self, pipeline_name: str) -> Optional[Pipeline]:
        """Get pipeline by name."""
        pipeline_file = self._get_pipeline_path(pipeline_name) / "pipeline.json"

        if not pipeline_file.exists():
            return None

        try:
            with open(pipeline_file, "r") as f:
                pipeline_data = json.load(f)

            return Pipeline(**pipeline_data)
        except Exception as e:
            print(f"Error loading pipeline {pipeline_name}: {e}")
            return None

    def save_pipeline(
        self,
        pipeline_name: str,
        pipeline: Pipeline,
        commit_message: Optional[str] = None,
        auto_commit: bool = True,
    ) -> Pipeline:
        """
        Save pipeline to repository.
        Creates pipeline directory structure and optionally commits to Git.
        """
        pipeline_path = self._get_pipeline_path(pipeline_name)
        pipeline_path.mkdir(parents=True, exist_ok=True)

        # Update metadata
        pipeline.metadata.name = pipeline_name
        pipeline.metadata.updated_at = datetime.now()

        # Save pipeline.json
        pipeline_file = pipeline_path / "pipeline.json"
        with open(pipeline_file, "w") as f:
            json.dump(
                pipeline.model_dump(mode="json", exclude_none=True),
                f,
                indent=2,
            )

        # Save config files for each node (if they have custom config)
        config_path = pipeline_path / "config"
        config_path.mkdir(exist_ok=True)

        for node in pipeline.nodes:
            if node.data.config:
                node_config_file = config_path / f"{node.id}.json"
                with open(node_config_file, "w") as f:
                    json.dump(node.data.config, f, indent=2)

        # Auto-commit if enabled
        if auto_commit and studio_config.get("git.auto_commit", True):
            try:
                message = commit_message or f"Update pipeline: {pipeline_name}"
                git_service.commit(
                    message=message,
                    files=[str(pipeline_file.relative_to(self.repo_path))],
                )
            except Exception as e:
                print(f"Error committing pipeline: {e}")

        return pipeline

    def delete_pipeline(self, pipeline_name: str, auto_commit: bool = True) -> bool:
        """Delete pipeline from repository."""
        pipeline_path = self._get_pipeline_path(pipeline_name)

        if not pipeline_path.exists():
            return False

        # Remove directory
        import shutil
        shutil.rmtree(pipeline_path)

        # Auto-commit if enabled
        if auto_commit and studio_config.get("git.auto_commit", True):
            try:
                git_service.commit(
                    message=f"Delete pipeline: {pipeline_name}",
                )
            except Exception as e:
                print(f"Error committing deletion: {e}")

        return True

    def export_pipeline(
        self, pipeline_name: str, commit_message: Optional[str] = None
    ) -> PipelineExportResponse:
        """
        Export pipeline to Git (force commit and push).
        Returns commit information.
        """
        pipeline = self.get_pipeline(pipeline_name)
        if not pipeline:
            raise ValueError(f"Pipeline not found: {pipeline_name}")

        # Save pipeline (will auto-commit)
        self.save_pipeline(pipeline_name, pipeline, commit_message, auto_commit=True)

        # Get current branch
        status = git_service.get_status()

        # Push to remote
        try:
            git_service.push()
        except Exception as e:
            print(f"Warning: Could not push to remote: {e}")

        # Get commit info
        repo = git_service._get_repo()
        last_commit = repo.head.commit

        return PipelineExportResponse(
            commit_sha=last_commit.hexsha,
            commit_message=last_commit.message.strip(),
            branch=status.current_branch,
            files_changed=[f"{self.pipelines_dir}/{pipeline_name}/pipeline.json"],
        )

    def create_pipeline(self, pipeline_name: str, template: Optional[str] = None) -> Pipeline:
        """
        Create a new pipeline from scratch or from template.
        """
        from schemas.pipeline import PipelineMetadata, PipelineConfig

        # Create basic pipeline structure
        pipeline = Pipeline(
            metadata=PipelineMetadata(
                name=pipeline_name,
                display_name=pipeline_name.replace("_", " ").title(),
                description=f"New pipeline: {pipeline_name}",
                author="Sparkle Studio",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            ),
            nodes=[],
            edges=[],
            config=PipelineConfig(),
        )

        # Save and return
        return self.save_pipeline(pipeline_name, pipeline)


# Global pipeline service instance
pipeline_service = PipelineService()
