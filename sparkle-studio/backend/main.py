"""
Main FastAPI application for Sparkle Studio Backend.
"""
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from core.config import settings
from core.dependencies import get_spark
from schemas.response import APIResponse, HealthResponse
from api.v1.router import api_router
from component_registry import get_registry
from services.git_service import git_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events.
    """
    # Startup
    print("🚀 Starting Sparkle Studio Backend...")

    # Initialize component registry (Phase 2)
    print("📦 Initializing component registry...")
    try:
        registry = get_registry()
        stats = registry.get_stats()
        print(f"✅ Sparkle Studio registered:")
        print(f"   • {stats.get('connection', 0)} connections")
        print(f"   • {stats.get('ingestor', 0)} ingestors")
        print(f"   • {stats.get('transformer', 0)} transformers")
        print(f"   • {stats.get('ml', 0)} ml modules")
        print(f"   • Total: {stats.get('total', 0)} components")
    except Exception as e:
        print(f"⚠️  Warning: Could not initialize components: {e}")

    # Check Spark availability
    print("⚡ Checking Spark availability...")
    try:
        spark = get_spark()
        print(f"✅ Spark session available: {spark.version}")
    except Exception as e:
        print(f"⚠️  Warning: Spark not available: {e}")

    # Check Git repository
    print("📁 Checking Git repository...")
    try:
        status = git_service.get_status()
        print(f"✅ Git repository ready (branch: {status.current_branch})")
    except Exception as e:
        print(f"⚠️  Warning: Git repository not available: {e}")

    print("✨ Sparkle Studio Backend started successfully!")

    yield

    # Shutdown
    print("👋 Shutting down Sparkle Studio Backend...")


# Create FastAPI application
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="Backend API for Sparkle Studio - Low-code/Pro-code hybrid UI for Project Sparkle",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.BACKEND_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add GZip compression
app.add_middleware(GZipMiddleware, minimum_size=1000)


# Exception handlers
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler."""
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "data": None,
            "message": "Internal server error",
            "error": str(exc) if settings.DEBUG else "An error occurred",
        },
    )


# Health check endpoint
@app.get("/health", response_model=APIResponse[HealthResponse])
async def health_check():
    """
    Health check endpoint.
    Returns service status and availability of dependencies.
    """
    # Check Spark
    spark_available = False
    try:
        spark = get_spark()
        spark_available = True
    except Exception:
        pass

    # Check Git
    git_repo_path = str(Path(settings.GIT_REPO_PATH))
    git_repo_exists = Path(settings.GIT_REPO_PATH).exists()

    health = HealthResponse(
        status="healthy",
        version=settings.VERSION,
        environment=settings.SPARKLE_ENV,
        spark_available=spark_available,
        git_repo_path=git_repo_path,
        git_repo_exists=git_repo_exists,
    )

    return APIResponse(
        success=True,
        data=health,
        message="Service is healthy",
    )


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": settings.PROJECT_NAME,
        "version": settings.VERSION,
        "status": "running",
        "docs": "/docs",
        "health": "/health",
    }


# Include API v1 router
app.include_router(api_router, prefix=settings.API_V1_STR)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level="info",
    )
