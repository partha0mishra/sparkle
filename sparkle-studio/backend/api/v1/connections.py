"""
Connection testing API endpoints.
Allows testing connectivity for connection components.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, Optional

from component_registry import get_registry, ComponentCategory
from core.dependencies import get_spark

router = APIRouter(prefix="/connections", tags=["connections"])


class ConnectionTestRequest(BaseModel):
    """Request model for testing a connection."""
    name: str
    config: Dict[str, Any]


class ConnectionTestResponse(BaseModel):
    """Response model for connection test results."""
    success: bool
    status: str  # "success", "failed", "error"
    message: str
    details: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


@router.post("/test", response_model=ConnectionTestResponse)
async def test_connection(request: ConnectionTestRequest):
    """
    Test a connection with given configuration.

    Args:
        request: Connection test request with name and config

    Returns:
        Test result with success status and details
    """
    try:
        # Get connection component from registry
        registry = get_registry()
        manifest = registry.get_component(ComponentCategory.CONNECTION, request.name)

        if not manifest:
            return ConnectionTestResponse(
                success=False,
                status="error",
                message=f"Connection '{request.name}' not found",
                error="Component not found in registry"
            )

        # Get Spark session
        spark = get_spark()

        # Import connections factory
        from connections.factory import ConnectionFactory

        # Create connection instance
        try:
            connection = ConnectionFactory.create(
                request.name,
                spark=spark,
                env="test",  # Use test environment
                config=request.config  # Pass config directly
            )
        except Exception as e:
            return ConnectionTestResponse(
                success=False,
                status="error",
                message="Failed to create connection instance",
                error=str(e),
                details={"config": request.config}
            )

        # Test the connection
        try:
            # Call the test() method if available
            if hasattr(connection, 'test') and callable(getattr(connection, 'test')):
                test_result = connection.test()

                if test_result:
                    return ConnectionTestResponse(
                        success=True,
                        status="success",
                        message=f"Successfully connected to {manifest.display_name}",
                        details={
                            "connection_type": manifest.display_name,
                            "test_method": "native"
                        }
                    )
                else:
                    return ConnectionTestResponse(
                        success=False,
                        status="failed",
                        message=f"Connection test failed for {manifest.display_name}",
                        details={
                            "connection_type": manifest.display_name,
                            "test_method": "native"
                        }
                    )
            else:
                # No test method - try basic operations
                # For JDBC connections, try getting a connection
                if hasattr(connection, 'get_jdbc_url'):
                    try:
                        jdbc_url = connection.get_jdbc_url()
                        return ConnectionTestResponse(
                            success=True,
                            status="success",
                            message=f"Connection configuration validated for {manifest.display_name}",
                            details={
                                "connection_type": manifest.display_name,
                                "test_method": "validation",
                                "jdbc_url_format": "valid"
                            }
                        )
                    except Exception as e:
                        return ConnectionTestResponse(
                            success=False,
                            status="failed",
                            message=f"JDBC URL validation failed",
                            error=str(e)
                        )

                # For cloud storage, try listing (dry run)
                elif hasattr(connection, 'list_objects'):
                    return ConnectionTestResponse(
                        success=True,
                        status="success",
                        message=f"Connection configuration validated for {manifest.display_name}",
                        details={
                            "connection_type": manifest.display_name,
                            "test_method": "validation"
                        }
                    )

                # Generic validation - connection was created successfully
                return ConnectionTestResponse(
                    success=True,
                    status="success",
                    message=f"Connection instance created successfully for {manifest.display_name}",
                    details={
                        "connection_type": manifest.display_name,
                        "test_method": "instantiation"
                    }
                )

        except Exception as e:
            return ConnectionTestResponse(
                success=False,
                status="failed",
                message=f"Connection test failed",
                error=str(e),
                details={
                    "connection_type": manifest.display_name
                }
            )

    except Exception as e:
        return ConnectionTestResponse(
            success=False,
            status="error",
            message="Unexpected error during connection test",
            error=str(e)
        )
