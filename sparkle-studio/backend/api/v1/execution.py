"""
API endpoints for pipeline execution and monitoring (Phase 4: Enhanced).
"""
from fastapi import APIRouter, HTTPException, Depends, WebSocket, WebSocketDisconnect, Query
from typing import Optional
import asyncio
import json
from datetime import datetime

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
    ExecutionListResponse,
    ExecutionListItem,
    ExecutionProgress,
    ExecutionStopRequest,
    ExecutionStatus,
    WebSocketMessage,
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


# Phase 4: Enhanced execution monitoring endpoints


@router.get("/list", response_model=APIResponse[ExecutionListResponse])
async def list_executions(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    status: Optional[ExecutionStatus] = Query(None, description="Filter by status"),
    pipeline_name: Optional[str] = Query(None, description="Filter by pipeline name"),
    current_user: User = Depends(get_current_active_user),
):
    """
    Get paginated list of pipeline executions (history).
    Supports filtering by status and pipeline name.
    """
    try:
        executions = spark_service.list_executions(
            page=page,
            page_size=page_size,
            status=status,
            pipeline_name=pipeline_name,
        )

        return APIResponse(
            success=True,
            data=executions,
            message=f"Retrieved {len(executions.executions)} executions",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{run_id}/progress", response_model=APIResponse[ExecutionProgress])
async def get_execution_progress(
    run_id: str,
    current_user: User = Depends(get_current_active_user),
):
    """
    Get real-time execution progress with node-level status.
    Returns current node being executed and progress percentage.
    """
    try:
        progress = spark_service.get_execution_progress(run_id)
        if progress is None:
            raise HTTPException(
                status_code=404,
                detail=f"Execution not found: {run_id}",
            )

        return APIResponse(
            success=True,
            data=progress,
            message="Progress retrieved successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{run_id}/stop", response_model=APIResponse[ExecutionDetail])
async def stop_execution(
    run_id: str,
    request: ExecutionStopRequest,
    current_user: User = Depends(get_current_active_user),
):
    """
    Stop/cancel a running execution.
    Use force=true for SIGKILL (immediate termination).
    """
    try:
        execution = spark_service.stop_execution(run_id, force=request.force)
        if execution is None:
            raise HTTPException(
                status_code=404,
                detail=f"Execution not found: {run_id}",
            )

        return APIResponse(
            success=True,
            data=execution,
            message="Execution stopped successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.websocket("/{run_id}/logs")
async def websocket_execution_logs(websocket: WebSocket, run_id: str):
    """
    WebSocket endpoint for real-time execution logs.
    Streams log entries, progress updates, and status changes.

    Message types:
    - "log": Log entry from execution
    - "progress": Progress update (node status, percentage)
    - "status": Execution status change
    - "error": Error occurred
    - "complete": Execution completed
    """
    await websocket.accept()

    try:
        # Verify execution exists
        execution = spark_service.get_execution(run_id)
        if execution is None:
            await websocket.send_json({
                "type": "error",
                "message": f"Execution not found: {run_id}",
                "timestamp": datetime.now().isoformat(),
            })
            await websocket.close()
            return

        # Stream logs and updates
        last_status = execution.status

        while True:
            # Get latest execution state
            execution = spark_service.get_execution(run_id)
            if execution is None:
                break

            # Send status change if changed
            if execution.status != last_status:
                await websocket.send_json({
                    "type": "status",
                    "run_id": run_id,
                    "timestamp": datetime.now().isoformat(),
                    "data": {
                        "status": execution.status.value,
                        "message": f"Status changed to {execution.status.value}",
                    },
                })
                last_status = execution.status

            # Send progress update
            progress = spark_service.get_execution_progress(run_id)
            if progress:
                await websocket.send_json({
                    "type": "progress",
                    "run_id": run_id,
                    "timestamp": datetime.now().isoformat(),
                    "data": {
                        "progress_percent": progress.progress_percent,
                        "current_node": progress.current_node,
                        "nodes": [
                            {
                                "node_id": node.node_id,
                                "node_name": node.node_name,
                                "status": node.status.value,
                                "progress_percent": node.progress_percent,
                            }
                            for node in progress.nodes
                        ],
                    },
                })

            # Check if execution completed
            if execution.status in [ExecutionStatus.SUCCESS, ExecutionStatus.FAILED, ExecutionStatus.CANCELLED]:
                await websocket.send_json({
                    "type": "complete",
                    "run_id": run_id,
                    "timestamp": datetime.now().isoformat(),
                    "data": {
                        "status": execution.status.value,
                        "duration_seconds": execution.duration_seconds,
                        "error_message": execution.error_message,
                    },
                })
                break

            # Wait before next update
            await asyncio.sleep(1)

    except WebSocketDisconnect:
        print(f"WebSocket disconnected for execution: {run_id}")
    except Exception as e:
        print(f"WebSocket error: {e}")
        try:
            await websocket.send_json({
                "type": "error",
                "message": str(e),
                "timestamp": datetime.now().isoformat(),
            })
        except:
            pass
    finally:
        try:
            await websocket.close()
        except:
            pass
