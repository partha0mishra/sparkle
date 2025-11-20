"""
API endpoints for component management.
"""
from fastapi import APIRouter, HTTPException, Depends
from typing import Any

from core.dependencies import get_current_active_user
from core.security import User
from schemas.response import APIResponse
from schemas.component import (
    ComponentListResponse,
    ComponentDetail,
    ComponentCategoriesResponse,
    ComponentValidationRequest,
    ComponentValidationResponse,
)
from services.component_service import component_service


router = APIRouter(prefix="/components", tags=["components"])


@router.get("", response_model=APIResponse[ComponentListResponse])
async def list_components(
    current_user: User = Depends(get_current_active_user),
):
    """
    Get list of all available Sparkle components.
    Components are grouped by type: ingestors, transformers, ml, connections.
    """
    try:
        components = component_service.get_all_components()
        return APIResponse(
            success=True,
            data=components,
            message="Components retrieved successfully",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/categories", response_model=APIResponse[ComponentCategoriesResponse])
async def get_component_categories(
    current_user: User = Depends(get_current_active_user),
):
    """
    Get components grouped by categories for UI sidebar.
    """
    try:
        categories = component_service.get_categories()
        return APIResponse(
            success=True,
            data=categories,
            message="Categories retrieved successfully",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{component_type}/{component_name}", response_model=APIResponse[ComponentDetail])
async def get_component(
    component_type: str,
    component_name: str,
    current_user: User = Depends(get_current_active_user),
):
    """
    Get detailed information about a specific component.
    Includes metadata, JSON Schema for config, and example config.
    """
    try:
        component = component_service.get_component(component_type, component_name)
        if component is None:
            raise HTTPException(
                status_code=404,
                detail=f"Component not found: {component_type}/{component_name}",
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
    "/{component_type}/{component_name}/validate",
    response_model=APIResponse[ComponentValidationResponse],
)
async def validate_component_config(
    component_type: str,
    component_name: str,
    request: ComponentValidationRequest,
    current_user: User = Depends(get_current_active_user),
):
    """
    Validate component configuration against its JSON Schema.
    Returns validation errors and warnings.
    """
    try:
        validation = component_service.validate_config(
            component_type, component_name, request.config
        )

        return APIResponse(
            success=validation.valid,
            data=validation,
            message="Validation completed" if validation.valid else "Validation failed",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
