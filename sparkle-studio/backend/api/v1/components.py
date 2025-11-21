"""
API endpoints for component management (Phase 2 - Enhanced).
Automatic component discovery with JSON Schema generation.
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Optional

from core.dependencies import get_current_active_user
from core.security import User
from schemas.response import APIResponse
from schemas.component import (
    ComponentListResponse,
    ComponentDetailResponse,
    ComponentValidationRequest,
    ComponentValidationResponse,
    ComponentSearchResponse,
    ComponentMetadata,
    ComponentSampleDataResponse,
)
from services.component_service import component_service


router = APIRouter(prefix="/components", tags=["components"])


@router.get("", response_model=APIResponse[ComponentListResponse])
async def list_all_components(
    current_user: User = Depends(get_current_active_user),
):
    """
    Get all available Sparkle components grouped by category.
    Includes full config schemas for each component.
    """
    try:
        components = component_service.get_all_components()
        return APIResponse(
            success=True,
            data=components,
            message=f"Retrieved {components.total_count} components",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/category/{category}", response_model=APIResponse[list[ComponentMetadata]])
async def list_components_by_category(
    category: str,
    current_user: User = Depends(get_current_active_user),
):
    """
    Get components filtered by category.
    Categories: connection, ingestor, transformer, ml, sink
    """
    try:
        components = component_service.get_components_by_category(category)
        return APIResponse(
            success=True,
            data=components,
            message=f"Retrieved {len(components)} components in category '{category}'",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{category}/{name}", response_model=APIResponse[ComponentDetailResponse])
async def get_component_detail(
    category: str,
    name: str,
    current_user: User = Depends(get_current_active_user),
):
    """
    Get detailed information about a specific component.
    Includes full JSON Schema for configuration and sample config.
    """
    try:
        component = component_service.get_component(category, name)
        if component is None:
            raise HTTPException(
                status_code=404,
                detail=f"Component not found: {category}/{name}",
            )

        return APIResponse(
            success=True,
            data=component,
            message="Component retrieved successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/{category}/{name}/validate",
    response_model=APIResponse[ComponentValidationResponse],
)
async def validate_component_config(
    category: str,
    name: str,
    request: ComponentValidationRequest,
    current_user: User = Depends(get_current_active_user),
):
    """
    Validate component configuration against its JSON Schema.
    Returns detailed field-level errors with paths.
    """
    try:
        validation = component_service.validate_config(category, name, request.config)

        return APIResponse(
            success=validation.valid,
            data=validation,
            message="Validation completed" if validation.valid else "Validation failed",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search", response_model=APIResponse[ComponentSearchResponse])
async def search_components(
    q: str = Query(..., description="Search query"),
    current_user: User = Depends(get_current_active_user),
):
    """
    Search components by name, description, or tags.
    Returns ranked results with relevance scores.

    Example: /api/v1/components/search?q=salesforce
    """
    try:
        results = component_service.search_components(q)
        return APIResponse(
            success=True,
            data=results,
            message=f"Found {results.total_results} matching components",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/{category}/{name}/sample-data",
    response_model=APIResponse[ComponentSampleDataResponse],
)
async def get_component_sample_data(
    category: str,
    name: str,
    sample_size: int = Query(10, ge=1, le=100, description="Number of sample rows"),
    current_user: User = Depends(get_current_active_user),
):
    """
    Execute component with sample config and return sample data.
    Used for Live Preview in Phase 5.

    Returns:
    - Sample data rows
    - Schema information
    - Execution time
    """
    try:
        result = component_service.get_sample_data(category, name, sample_size)
        return APIResponse(
            success=result.success,
            data=result,
            message="Sample data generated" if result.success else "Failed to generate sample data",
            error=result.error,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
