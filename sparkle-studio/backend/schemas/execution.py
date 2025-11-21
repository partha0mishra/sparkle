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
