"""
Pydantic schemas for Sparkle components (Phase 2 - Enhanced).
"""
from typing import Any, Optional
from enum import Enum
from pydantic import BaseModel, Field


class ComponentCategoryEnum(str, Enum):
    """Component category enumeration."""
    CONNECTION = "connection"
    INGESTOR = "ingestor"
    TRANSFORMER = "transformer"
    ML = "ml"
    SINK = "sink"


class ComponentManifestSchema(BaseModel):
    """
    Complete manifest for a Sparkle component.
    Maps to component_registry.ComponentManifest for API responses.
    """
    # Identity
    name: str = Field(..., description="Component unique name (snake_case)")
    category: ComponentCategoryEnum = Field(..., description="Component category")
    display_name: str = Field(..., description="Human-readable display name")

    # Documentation
    description: Optional[str] = Field(None, description="Component description")
    icon: Optional[str] = Field(None, description="Icon name for UI")
    tags: list[str] = Field(default_factory=list, description="Tags for search and categorization")

    # Configuration
    config_schema: dict[str, Any] = Field(
        default_factory=dict,
        description="JSON Schema for component configuration"
    )
    sample_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Example configuration"
    )

    # Capabilities
    has_code_editor: bool = Field(
        default=False,
        description="Whether component supports custom code editing"
    )
    is_streaming: bool = Field(
        default=False,
        description="Whether component supports streaming mode"
    )
    supports_incremental: bool = Field(
        default=False,
        description="Whether component supports incremental loading"
    )

    # Module information
    module_path: Optional[str] = Field(None, description="Python module path")
    class_name: Optional[str] = Field(None, description="Class or function name")

    class Config:
        use_enum_values = True


class ComponentMetadata(BaseModel):
    """Simplified component metadata (for lists)."""
    name: str
    category: ComponentCategoryEnum
    display_name: str
    description: Optional[str] = None
    icon: Optional[str] = None
    tags: list[str] = []
    is_streaming: bool = False
    supports_incremental: bool = False

    class Config:
        use_enum_values = True


class ComponentDetail(BaseModel):
    """Detailed component information with full config schema."""
    manifest: ComponentManifestSchema
    source_code: Optional[str] = None  # Optional source code for inspection


class ComponentValidationRequest(BaseModel):
    """Request to validate component configuration."""
    config: dict[str, Any]


class FieldError(BaseModel):
    """Validation error for a specific field."""
    field: str = Field(..., description="Field path (e.g., 'database.host')")
    message: str = Field(..., description="Error message")
    error_type: str = Field(..., description="Error type (e.g., 'required', 'type', 'constraint')")


class ComponentValidationResponse(BaseModel):
    """Response from component validation."""
    valid: bool
    errors: list[FieldError] = []
    warnings: list[str] = []


class ComponentGroup(BaseModel):
    """Group of components for UI organization."""
    category: ComponentCategoryEnum
    display_name: str
    icon: Optional[str] = None
    count: int
    components: list[ComponentMetadata] = []

    class Config:
        use_enum_values = True


class ComponentListResponse(BaseModel):
    """Response containing all components grouped by category."""
    groups: list[ComponentGroup] = []
    total_count: int = 0
    stats: dict[str, int] = {}


class ComponentDetailResponse(BaseModel):
    """Response for single component detail."""
    component: ComponentManifestSchema


class ComponentSearchResult(BaseModel):
    """Single search result."""
    component: ComponentMetadata
    relevance_score: float = Field(
        default=1.0,
        description="Search relevance score (0-1)"
    )
    match_reason: str = Field(
        default="name",
        description="Why this matched (name, description, tag)"
    )


class ComponentSearchResponse(BaseModel):
    """Response from component search."""
    query: str
    results: list[ComponentSearchResult] = []
    total_results: int = 0


class ComponentSampleDataResponse(BaseModel):
    """Response from sample data execution."""
    component_name: str
    sample_data: list[dict[str, Any]] = []
    row_count: int = 0
    schema_: Optional[dict[str, Any]] = Field(default=None, alias="schema")
    execution_time_ms: float = 0
    success: bool = True
    error: Optional[str] = None

    class Config:
        populate_by_name = True
