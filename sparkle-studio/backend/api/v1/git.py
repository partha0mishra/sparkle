"""
API endpoints for Git operations.
"""
from fastapi import APIRouter, HTTPException, Depends

from core.dependencies import get_current_active_user
from core.security import User
from schemas.response import APIResponse
from schemas.git import (
    GitBranch,
    GitStatus,
    GitCommitRequest,
    GitCommitResponse,
    GitPullRequest,
    GitPullResponse,
    GitPRRequest,
    GitPRResponse,
)
from services.git_service import git_service


router = APIRouter(prefix="/git", tags=["git"])


@router.get("/status", response_model=APIResponse[GitStatus])
async def get_git_status(
    current_user: User = Depends(get_current_active_user),
):
    """
    Get current Git repository status.
    Shows current branch, modified files, staged files, etc.
    """
    try:
        status = git_service.get_status()
        return APIResponse(
            success=True,
            data=status,
            message="Git status retrieved successfully",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/branches", response_model=APIResponse[list[GitBranch]])
async def get_branches(
    current_user: User = Depends(get_current_active_user),
):
    """
    Get list of all Git branches.
    """
    try:
        branches = git_service.get_branches()
        return APIResponse(
            success=True,
            data=branches,
            message=f"Retrieved {len(branches)} branches",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/commit", response_model=APIResponse[GitCommitResponse])
async def commit_changes(
    request: GitCommitRequest,
    current_user: User = Depends(get_current_active_user),
):
    """
    Commit changes to Git repository.
    Optionally auto-push after commit.
    """
    try:
        # Commit changes
        commit_response = git_service.commit(
            message=request.message,
            files=request.files,
            author_name=current_user.full_name or current_user.username,
            author_email=current_user.email or f"{current_user.username}@sparkle.studio",
        )

        # Push if requested
        if request.push:
            try:
                git_service.push()
                commit_response.pushed = True
            except Exception as e:
                # Commit succeeded but push failed
                return APIResponse(
                    success=True,
                    data=commit_response,
                    message=f"Committed successfully, but push failed: {str(e)}",
                )

        return APIResponse(
            success=True,
            data=commit_response,
            message="Changes committed successfully",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/pull", response_model=APIResponse[GitPullResponse])
async def pull_changes(
    request: GitPullRequest,
    current_user: User = Depends(get_current_active_user),
):
    """
    Pull changes from remote repository.
    """
    try:
        pull_response = git_service.pull(
            branch=request.branch,
            rebase=request.rebase,
        )

        return APIResponse(
            success=pull_response.success,
            data=pull_response,
            message=pull_response.message,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/create-pr", response_model=APIResponse[GitPRResponse])
async def create_pull_request(
    request: GitPRRequest,
    current_user: User = Depends(get_current_active_user),
):
    """
    Create a pull request on GitHub/GitLab.
    Requires Git token and remote URL to be configured.
    """
    try:
        pr_response = git_service.create_pull_request(
            title=request.title,
            description=request.description,
            source_branch=request.source_branch,
            target_branch=request.target_branch,
            draft=request.draft,
        )

        return APIResponse(
            success=True,
            data=pr_response,
            message=f"Pull request created: {pr_response.pr_url}",
        )
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
