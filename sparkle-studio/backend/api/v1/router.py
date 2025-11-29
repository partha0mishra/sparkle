"""
Main API v1 router that includes all sub-routers.
"""
from fastapi import APIRouter

from . import components, pipelines, execution, git, connections


api_router = APIRouter()

# Include all sub-routers
api_router.include_router(components.router)
api_router.include_router(pipelines.router)
api_router.include_router(execution.router)
api_router.include_router(git.router)
api_router.include_router(connections.router)
