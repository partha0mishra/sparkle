"""
Pydantic schemas for Git operations.
"""
from typing import Optional
from datetime import datetime
from pydantic import BaseModel


class GitBranch(BaseModel):
    """Git branch information."""
    name: str
    is_current: bool = False
    commit_sha: Optional[str] = None
    last_commit_message: Optional[str] = None
    last_commit_date: Optional[datetime] = None


class GitStatus(BaseModel):
    """Git repository status."""
    current_branch: str
    is_clean: bool
    modified_files: list[str] = []
    untracked_files: list[str] = []
    staged_files: list[str] = []
    ahead: int = 0  # Commits ahead of remote
    behind: int = 0  # Commits behind remote
    remote_url: Optional[str] = None


class GitCommitRequest(BaseModel):
    """Request to commit changes."""
    message: str
    files: Optional[list[str]] = None  # If None, commit all changes
    push: bool = False  # Auto-push after commit


class GitCommitResponse(BaseModel):
    """Response from commit operation."""
    commit_sha: str
    message: str
    author: str
    timestamp: datetime
    files_changed: list[str] = []
    pushed: bool = False


class GitPullRequest(BaseModel):
    """Request to pull from remote."""
    branch: Optional[str] = None  # If None, use current branch
    rebase: bool = False


class GitPullResponse(BaseModel):
    """Response from pull operation."""
    success: bool
    branch: str
    commits_pulled: int
    files_changed: list[str] = []
    message: str


class GitPRRequest(BaseModel):
    """Request to create a pull request."""
    title: str
    description: str
    source_branch: str
    target_branch: str = "main"
    draft: bool = False


class GitPRResponse(BaseModel):
    """Response from PR creation."""
    pr_number: int
    pr_url: str
    title: str
    source_branch: str
    target_branch: str
