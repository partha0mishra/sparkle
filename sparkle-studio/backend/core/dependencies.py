"""
FastAPI dependencies for dependency injection.
"""
from typing import Optional, Generator
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pyspark.sql import SparkSession

from .config import settings
from .security import DEFAULT_DEV_USER, User, decode_access_token


security = HTTPBearer(auto_error=False)


# Spark session singleton
_spark_session: Optional[SparkSession] = None


def get_spark() -> SparkSession:
    """
    Get or create Spark session.
    Returns local Spark session or configured remote session.
    """
    global _spark_session

    if _spark_session is None:
        if settings.SPARK_REMOTE:
            # Remote Spark cluster (Databricks/EMR)
            _spark_session = (
                SparkSession.builder
                .appName(settings.SPARK_APP_NAME)
                .master(settings.SPARK_REMOTE)
                .getOrCreate()
            )
        else:
            # Local Spark session with macOS compatibility
            builder = (
                SparkSession.builder
                .appName(settings.SPARK_APP_NAME)
                .master(settings.SPARK_MASTER)
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
            )

            # Try to add Delta Lake support (optional)
            try:
                builder = (
                    builder
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                )
            except Exception:
                pass  # Delta Lake not available, continue without it

            _spark_session = builder.getOrCreate()

    return _spark_session


def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> User:
    """
    Get current authenticated user.
    Placeholder implementation - returns dev user for Phase 1-7.
    Will be replaced with real JWT validation in Phase 8.
    """
    # Phase 1-7: Return default dev user
    if settings.DEBUG or settings.SPARKLE_ENV == "local":
        return DEFAULT_DEV_USER

    # Phase 8: Real authentication
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token_data = decode_access_token(credentials.credentials)
    if token_data is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # In Phase 8, fetch user from database
    return DEFAULT_DEV_USER


def get_current_active_user(
    current_user: User = Depends(get_current_user)
) -> User:
    """Get current active user (not disabled)."""
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user
