"""
Pydantic schemas for Sparkle components (ingestors, transformers, ml, connections).
"""
from typing import Any, Optional
from pydantic import BaseModel, Field


class JSONSchema(BaseModel):
    """JSON Schema representation."""
    schema_: dict[str, Any] = Field(alias="$schema", default={})
    type: str = "object"
    properties: dict[str, Any] = {}
    required: list[str] = []
    title: Optional[str] = None
    description: Optional[str] = None
    additionalProperties: bool = False

    class Config:
        populate_by_name = True


class ComponentMetadata(BaseModel):
    """Metadata for a Sparkle component."""
    name: str
    type: str  # ingestor, transformer, ml, connection
    category: str  # e.g., "Database", "File", "API", "Transformation", "ML Model"
    description: Optional[str] = None
    version: Optional[str] = None
    author: Optional[str] = None
    tags: list[str] = []
    icon: Optional[str] = None  # Icon name for UI


class ComponentSchema(BaseModel):
    """Full component definition with metadata and config schema."""
    metadata: ComponentMetadata
    config_schema: dict[str, Any]  # JSON Schema for the component's configuration
    example_config: Optional[dict[str, Any]] = None


class ComponentDetail(BaseModel):
    """Detailed component information."""
    metadata: ComponentMetadata
    config_schema: dict[str, Any]
    example_config: Optional[dict[str, Any]] = None
    source_code: Optional[str] = None  # Python source code (optional)


class ComponentValidationRequest(BaseModel):
    """Request to validate component configuration."""
    config: dict[str, Any]


class ComponentValidationResponse(BaseModel):
    """Response from component validation."""
    valid: bool
    errors: list[str] = []
    warnings: list[str] = []


class ComponentCategory(BaseModel):
    """Component category for UI sidebar."""
    name: str
    display_name: str
    icon: Optional[str] = None
    components: list[ComponentMetadata] = []


class ComponentListResponse(BaseModel):
    """Response containing list of all components grouped by type."""
    ingestors: list[ComponentMetadata] = []
    transformers: list[ComponentMetadata] = []
    ml: list[ComponentMetadata] = []
    connections: list[ComponentMetadata] = []
    total_count: int = 0


class ComponentCategoriesResponse(BaseModel):
    """Response containing components grouped by categories."""
    categories: list[ComponentCategory] = []
