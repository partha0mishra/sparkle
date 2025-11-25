"""
Pydantic schemas for pipeline execution and monitoring.
"""
from typing import Any, Optional
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field


class ExecutionStatus(str, Enum):
    """Execution status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class DryRunRequest(BaseModel):
    """Request for dry-run execution (sample data)."""
    pipeline_name: str
    sample_size: int = Field(default=1000, ge=1, le=10000)
    node_id: Optional[str] = None  # If specified, run only up to this node


class ExecutionRequest(BaseModel):
    """Request to execute a pipeline."""
    pipeline_name: str
    parameters: dict[str, Any] = {}
    environment: dict[str, str] = {}
    dry_run: bool = False


class BackfillRequest(BaseModel):
    """Request to backfill a pipeline."""
    pipeline_name: str
    start_date: str  # ISO format date
    end_date: str  # ISO format date
    parameters: dict[str, Any] = {}


class ExecutionResponse(BaseModel):
    """Response from execution submission."""
    run_id: str
    pipeline_name: str
    status: ExecutionStatus
    submitted_at: datetime
    message: Optional[str] = None


class ExecutionDetail(BaseModel):
    """Detailed execution information."""
    run_id: str
    pipeline_name: str
    status: ExecutionStatus
    submitted_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    spark_ui_url: Optional[str] = None
    logs_url: Optional[str] = None
    error_message: Optional[str] = None
    parameters: dict[str, Any] = {}
    metrics: dict[str, Any] = {}


class DryRunResult(BaseModel):
    """Result from dry-run execution."""
    success: bool
    node_id: str
    node_name: str
    sample_data: list[dict[str, Any]] = []
    row_count: int = 0
    schema_: Optional[dict[str, Any]] = Field(default=None, alias="schema")
    execution_time_ms: float = 0
    error: Optional[str] = None

    class Config:
        populate_by_name = True


class DryRunResponse(BaseModel):
    """Response from dry-run execution."""
    run_id: str
    pipeline_name: str
    results: list[DryRunResult] = []
    total_execution_time_ms: float = 0
    success: bool
    error: Optional[str] = None


# Phase 4: Enhanced execution tracking and real-time monitoring


class NodeExecutionStatus(str, Enum):
    """Node-level execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class NodeExecutionInfo(BaseModel):
    """Execution info for a single node in the pipeline."""
    node_id: str
    node_name: str
    component_type: str
    status: NodeExecutionStatus
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    rows_processed: Optional[int] = None
    rows_written: Optional[int] = None
    error: Optional[str] = None
    progress_percent: float = 0.0


class ExecutionMetrics(BaseModel):
    """Aggregate execution metrics."""
    total_rows_processed: int = 0
    total_rows_written: int = 0
    peak_memory_mb: float = 0.0
    total_cpu_seconds: float = 0.0
    data_read_mb: float = 0.0
    data_written_mb: float = 0.0
    num_tasks: int = 0
    num_stages: int = 0


class ExecutionProgress(BaseModel):
    """Real-time execution progress."""
    run_id: str
    pipeline_name: str
    status: ExecutionStatus
    progress_percent: float = 0.0
    current_node: Optional[str] = None
    nodes: list[NodeExecutionInfo] = Field(default_factory=list)
    elapsed_seconds: float = 0.0
    estimated_remaining_seconds: Optional[float] = None


class ExecutionLogEntry(BaseModel):
    """Single log entry for WebSocket streaming."""
    timestamp: datetime
    level: str  # INFO, WARNING, ERROR, DEBUG
    node_id: Optional[str] = None
    node_name: Optional[str] = None
    message: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExecutionListItem(BaseModel):
    """Execution history list item."""
    run_id: str
    pipeline_name: str
    status: ExecutionStatus
    submitted_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    dry_run: bool = False
    error_message: Optional[str] = None


class ExecutionListResponse(BaseModel):
    """Paginated list of executions."""
    executions: list[ExecutionListItem]
    total: int
    page: int = 1
    page_size: int = 20


class ExecutionStopRequest(BaseModel):
    """Request to stop/cancel execution."""
    run_id: str
    force: bool = Field(False, description="Force kill (SIGKILL)")


class WebSocketMessage(BaseModel):
    """WebSocket message for real-time updates."""
    type: str  # "log", "progress", "status", "error", "complete"
    run_id: str
    timestamp: datetime
    data: dict[str, Any]
