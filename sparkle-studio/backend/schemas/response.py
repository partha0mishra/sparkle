"""
Standard API response schemas.
"""
from typing import Any, Generic, TypeVar, Optional
from pydantic import BaseModel


T = TypeVar('T')


class APIResponse(BaseModel, Generic[T]):
    """Standard API response wrapper."""
    success: bool
    data: Optional[T] = None
    message: Optional[str] = None
    error: Optional[str] = None


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    version: str
    environment: str
    spark_available: bool
    git_repo_path: str
    git_repo_exists: bool
