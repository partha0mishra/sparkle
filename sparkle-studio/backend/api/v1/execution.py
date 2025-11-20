"""
API endpoints for pipeline execution and monitoring.
"""
from fastapi import APIRouter, HTTPException, Depends

from core.dependencies import get_current_active_user
from core.security import User
from schemas.response import APIResponse
from schemas.execution import (
    DryRunRequest,
    DryRunResponse,
    ExecutionRequest,
    ExecutionResponse,
    BackfillRequest,
    ExecutionDetail,
)
from services.pipeline_service import pipeline_service
from services.spark_service import spark_service


router = APIRouter(prefix="/execute", tags=["execution"])


@router.post("/dry-run", response_model=APIResponse[DryRunResponse])
async def dry_run_pipeline(
    request: DryRunRequest,
    current_user: User = Depends(get_current_active_user),
):
    """
    Execute pipeline in dry-run mode with sample data.
    Used by Live Preview feature (Phase 5).
    Returns sample output for each node.
    """
    try:
        # Load pipeline
        pipeline = pipeline_service.get_pipeline(request.pipeline_name)
        if pipeline is None:
            raise HTTPException(
                status_code=404,
                detail=f"Pipeline not found: {request.pipeline_name}",
            )

        # Execute dry-run
        result = spark_service.dry_run_pipeline(
            pipeline=pipeline,
            sample_size=request.sample_size,
            node_id=request.node_id,
        )

        return APIResponse(
            success=result.success,
            data=result,
            message="Dry-run completed" if result.success else "Dry-run failed",
            error=result.error,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run", response_model=APIResponse[ExecutionResponse])
async def run_pipeline(
    request: ExecutionRequest,
    current_user: User = Depends(get_current_active_user),
):
    """
    Submit pipeline for full execution.
    Returns immediately with run_id for tracking.
    """
    try:
        # Load pipeline
        pipeline = pipeline_service.get_pipeline(request.pipeline_name)
        if pipeline is None:
            raise HTTPException(
                status_code=404,
                detail=f"Pipeline not found: {request.pipeline_name}",
            )

        # Submit execution
        result = spark_service.submit_pipeline(
            pipeline=pipeline,
            parameters=request.parameters,
            dry_run=request.dry_run,
        )

        return APIResponse(
            success=True,
            data=result,
            message="Pipeline submitted successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/backfill", response_model=APIResponse[ExecutionResponse])
async def backfill_pipeline(
    request: BackfillRequest,
    current_user: User = Depends(get_current_active_user),
):
    """
    Submit pipeline for backfill execution with date range.
    """
    try:
        # Load pipeline
        pipeline = pipeline_service.get_pipeline(request.pipeline_name)
        if pipeline is None:
            raise HTTPException(
                status_code=404,
                detail=f"Pipeline not found: {request.pipeline_name}",
            )

        # Add date range to parameters
        parameters = request.parameters.copy()
        parameters["start_date"] = request.start_date
        parameters["end_date"] = request.end_date

        # Submit execution
        result = spark_service.submit_pipeline(
            pipeline=pipeline,
            parameters=parameters,
            dry_run=False,
        )

        return APIResponse(
            success=True,
            data=result,
            message="Backfill submitted successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{run_id}", response_model=APIResponse[ExecutionDetail])
async def get_execution_status(
    run_id: str,
    current_user: User = Depends(get_current_active_user),
):
    """
    Get execution status and details.
    Includes Spark UI URL, logs, metrics, etc.
    """
    try:
        execution = spark_service.get_execution(run_id)
        if execution is None:
            raise HTTPException(
                status_code=404,
                detail=f"Execution not found: {run_id}",
            )

        return APIResponse(
            success=True,
            data=execution,
            message="Execution status retrieved successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
