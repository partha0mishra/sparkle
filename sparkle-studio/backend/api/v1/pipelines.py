"""
API endpoints for pipeline management.
"""
from fastapi import APIRouter, HTTPException, Depends
from typing import Any

from core.dependencies import get_current_active_user
from core.security import User
from schemas.response import APIResponse
from schemas.pipeline import (
    Pipeline,
    PipelineListItem,
    PipelineSaveRequest,
    PipelineExportResponse,
)
from services.pipeline_service import pipeline_service


router = APIRouter(prefix="/pipelines", tags=["pipelines"])


@router.get("", response_model=APIResponse[list[PipelineListItem]])
async def list_pipelines(
    current_user: User = Depends(get_current_active_user),
):
    """
    Get list of all pipelines from Git repository.
    Returns pipeline summaries with metadata.
    """
    try:
        pipelines = pipeline_service.list_pipelines()
        return APIResponse(
            success=True,
            data=pipelines,
            message=f"Retrieved {len(pipelines)} pipelines",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{pipeline_name}", response_model=APIResponse[Pipeline])
async def get_pipeline(
    pipeline_name: str,
    current_user: User = Depends(get_current_active_user),
):
    """
    Get full pipeline definition including nodes, edges, and configuration.
    """
    try:
        pipeline = pipeline_service.get_pipeline(pipeline_name)
        if pipeline is None:
            raise HTTPException(
                status_code=404,
                detail=f"Pipeline not found: {pipeline_name}",
            )

        return APIResponse(
            success=True,
            data=pipeline,
            message="Pipeline retrieved successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{pipeline_name}", response_model=APIResponse[Pipeline])
async def save_pipeline(
    pipeline_name: str,
    request: PipelineSaveRequest,
    current_user: User = Depends(get_current_active_user),
):
    """
    Save or update a pipeline.
    Creates commit to Git if auto-commit is enabled.
    """
    try:
        pipeline = pipeline_service.save_pipeline(
            pipeline_name=pipeline_name,
            pipeline=request.pipeline,
            commit_message=request.commit_message,
            auto_commit=True,
        )

        return APIResponse(
            success=True,
            data=pipeline,
            message=f"Pipeline '{pipeline_name}' saved successfully",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{pipeline_name}", response_model=APIResponse[dict])
async def delete_pipeline(
    pipeline_name: str,
    current_user: User = Depends(get_current_active_user),
):
    """
    Delete a pipeline from the repository.
    """
    try:
        success = pipeline_service.delete_pipeline(pipeline_name, auto_commit=True)
        if not success:
            raise HTTPException(
                status_code=404,
                detail=f"Pipeline not found: {pipeline_name}",
            )

        return APIResponse(
            success=True,
            data={"pipeline_name": pipeline_name},
            message=f"Pipeline '{pipeline_name}' deleted successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{pipeline_name}/export", response_model=APIResponse[PipelineExportResponse])
async def export_pipeline(
    pipeline_name: str,
    commit_message: str | None = None,
    current_user: User = Depends(get_current_active_user),
):
    """
    Force export pipeline to Git.
    Commits and pushes to remote repository.
    """
    try:
        export_result = pipeline_service.export_pipeline(
            pipeline_name, commit_message
        )

        return APIResponse(
            success=True,
            data=export_result,
            message=f"Pipeline '{pipeline_name}' exported successfully",
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("", response_model=APIResponse[Pipeline])
async def create_pipeline(
    pipeline_name: str,
    template: str | None = None,
    current_user: User = Depends(get_current_active_user),
):
    """
    Create a new empty pipeline.
    Optionally use a template.
    """
    try:
        # Check if pipeline already exists
        existing = pipeline_service.get_pipeline(pipeline_name)
        if existing:
            raise HTTPException(
                status_code=400,
                detail=f"Pipeline already exists: {pipeline_name}",
            )

        pipeline = pipeline_service.create_pipeline(pipeline_name, template)

        return APIResponse(
            success=True,
            data=pipeline,
            message=f"Pipeline '{pipeline_name}' created successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
