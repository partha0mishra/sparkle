"""
Pydantic schemas for pipeline definitions (nodes, edges, full pipeline).
"""
from typing import Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field


class Position(BaseModel):
    """Node position on canvas."""
    x: float
    y: float


class NodeData(BaseModel):
    """Data stored in a pipeline node."""
    component_type: str  # ingestor, transformer, ml, connection
    component_name: str  # e.g., "postgres_ingestor", "deduplicate_transformer"
    label: str
    config: dict[str, Any] = {}
    description: Optional[str] = None


class PipelineNode(BaseModel):
    """A node in the pipeline canvas."""
    id: str
    type: str = "sparkle_component"  # React Flow node type
    position: Position
    data: NodeData


class PipelineEdge(BaseModel):
    """An edge connecting two nodes in the pipeline."""
    id: str
    source: str  # source node id
    target: str  # target node id
    sourceHandle: Optional[str] = None
    targetHandle: Optional[str] = None
    type: str = "default"
    animated: bool = False
    label: Optional[str] = None


class PipelineMetadata(BaseModel):
    """Pipeline metadata."""
    name: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    author: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    version: str = "1.0.0"
    tags: list[str] = []


class PipelineConfig(BaseModel):
    """Global pipeline configuration."""
    schedule: Optional[str] = None  # Cron expression
    timezone: str = "UTC"
    parallelism: int = 4
    retry_policy: dict[str, Any] = {}
    notifications: dict[str, Any] = {}
    environment: dict[str, str] = {}


class Pipeline(BaseModel):
    """Complete pipeline definition."""
    metadata: PipelineMetadata
    nodes: list[PipelineNode] = []
    edges: list[PipelineEdge] = []
    config: PipelineConfig = Field(default_factory=PipelineConfig)


class PipelineListItem(BaseModel):
    """Pipeline list item (summary)."""
    name: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    author: Optional[str] = None
    updated_at: Optional[datetime] = None
    node_count: int = 0
    edge_count: int = 0
    tags: list[str] = []


class PipelineSaveRequest(BaseModel):
    """Request to save/update a pipeline."""
    pipeline: Pipeline
    commit_message: Optional[str] = None


class PipelineExportResponse(BaseModel):
    """Response from pipeline export to Git."""
    commit_sha: str
    commit_message: str
    branch: str
    files_changed: list[str] = []
