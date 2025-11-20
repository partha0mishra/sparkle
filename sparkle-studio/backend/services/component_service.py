"""
Service for managing Sparkle components (Phase 2 - Real Implementation).
Uses ComponentRegistry for automatic component discovery and JSON Schema generation.
"""
import time
from typing import Optional
from jsonschema import validate, ValidationError, Draft7Validator

from component_registry import get_registry, ComponentCategory, ComponentManifest
from core.dependencies import get_spark
from schemas.component import (
    ComponentManifestSchema,
    ComponentMetadata,
    ComponentListResponse,
    ComponentGroup,
    ComponentDetailResponse,
    ComponentValidationResponse,
    FieldError,
    ComponentSearchResponse,
    ComponentSearchResult,
    ComponentSampleDataResponse,
)


class ComponentService:
    """Service for managing Sparkle components with registry integration."""

    def __init__(self):
        self._registry = get_registry()

    def get_all_components(self) -> ComponentListResponse:
        """
        Get all components grouped by category.
        Returns full manifests with config schemas.
        """
        all_components = self._registry.get_all()
        groups = []
        total_count = 0

        # Build groups for each category
        for category, components_dict in all_components.items():
            if not components_dict:
                continue

            # Convert manifests to metadata
            component_metadata_list = [
                self._manifest_to_metadata(manifest)
                for manifest in components_dict.values()
            ]

            group = ComponentGroup(
                category=category.value,
                display_name=self._category_display_name(category),
                icon=self._category_icon(category),
                count=len(component_metadata_list),
                components=component_metadata_list,
            )
            groups.append(group)
            total_count += len(component_metadata_list)

        # Get stats
        stats = self._registry.get_stats()

        return ComponentListResponse(
            groups=groups,
            total_count=total_count,
            stats=stats,
        )

    def get_component(
        self, category: str, name: str
    ) -> Optional[ComponentDetailResponse]:
        """Get detailed component information with full config schema."""
        try:
            cat_enum = ComponentCategory(category)
        except ValueError:
            return None

        manifest = self._registry.get_component(cat_enum, name)
        if not manifest:
            return None

        # Convert manifest to schema
        manifest_schema = self._manifest_to_schema(manifest)

        return ComponentDetailResponse(component=manifest_schema)

    def get_components_by_category(self, category: str) -> list[ComponentMetadata]:
        """Get all components in a specific category."""
        try:
            cat_enum = ComponentCategory(category)
        except ValueError:
            return []

        components_dict = self._registry.get_by_category(cat_enum)
        return [
            self._manifest_to_metadata(manifest)
            for manifest in components_dict.values()
        ]

    def validate_config(
        self, category: str, name: str, config: dict
    ) -> ComponentValidationResponse:
        """
        Validate component configuration against its JSON Schema.
        Returns detailed field-level errors.
        """
        try:
            cat_enum = ComponentCategory(category)
        except ValueError:
            return ComponentValidationResponse(
                valid=False,
                errors=[
                    FieldError(
                        field="category",
                        message=f"Invalid category: {category}",
                        error_type="invalid",
                    )
                ],
            )

        manifest = self._registry.get_component(cat_enum, name)
        if not manifest:
            return ComponentValidationResponse(
                valid=False,
                errors=[
                    FieldError(
                        field="component",
                        message=f"Component not found: {category}/{name}",
                        error_type="not_found",
                    )
                ],
            )

        # Validate against JSON Schema
        schema = manifest.config_schema
        if not schema:
            # No schema defined - allow anything
            return ComponentValidationResponse(valid=True)

        errors = []
        warnings = []

        try:
            # Create validator
            validator = Draft7Validator(schema)

            # Validate
            validation_errors = sorted(
                validator.iter_errors(config), key=lambda e: e.path
            )

            for error in validation_errors:
                # Build field path
                field_path = ".".join(str(p) for p in error.path) if error.path else "root"

                # Determine error type
                error_type = error.validator

                errors.append(
                    FieldError(
                        field=field_path,
                        message=error.message,
                        error_type=error_type,
                    )
                )

        except Exception as e:
            errors.append(
                FieldError(
                    field="validation",
                    message=f"Schema validation error: {str(e)}",
                    error_type="validation_error",
                )
            )

        return ComponentValidationResponse(
            valid=len(errors) == 0, errors=errors, warnings=warnings
        )

    def search_components(self, query: str) -> ComponentSearchResponse:
        """
        Search components by name, description, or tags.
        Returns ranked results with relevance scores.
        """
        manifests = self._registry.search(query)

        # Build search results with relevance scoring
        results = []
        query_lower = query.lower()

        for manifest in manifests:
            score = 0.0
            match_reason = "name"

            # Name match (highest score)
            if query_lower in manifest.name.lower():
                score = 1.0
                match_reason = "name"
                # Exact match gets bonus
                if query_lower == manifest.name.lower():
                    score = 1.5

            # Display name match
            elif query_lower in manifest.display_name.lower():
                score = 0.9
                match_reason = "display_name"

            # Description match
            elif manifest.description and query_lower in manifest.description.lower():
                score = 0.7
                match_reason = "description"

            # Tag match
            elif any(query_lower in tag.lower() for tag in manifest.tags):
                score = 0.8
                match_reason = "tag"

            results.append(
                ComponentSearchResult(
                    component=self._manifest_to_metadata(manifest),
                    relevance_score=min(score, 1.0),  # Cap at 1.0
                    match_reason=match_reason,
                )
            )

        # Sort by relevance score
        results.sort(key=lambda r: r.relevance_score, reverse=True)

        return ComponentSearchResponse(
            query=query, results=results, total_results=len(results)
        )

    def get_sample_data(
        self, category: str, name: str, sample_size: int = 10
    ) -> ComponentSampleDataResponse:
        """
        Execute component with sample config and return sample data.
        Used for Live Preview (Phase 5).
        """
        try:
            cat_enum = ComponentCategory(category)
        except ValueError:
            return ComponentSampleDataResponse(
                component_name=f"{category}/{name}",
                success=False,
                error=f"Invalid category: {category}",
            )

        manifest = self._registry.get_component(cat_enum, name)
        if not manifest:
            return ComponentSampleDataResponse(
                component_name=f"{category}/{name}",
                success=False,
                error=f"Component not found: {category}/{name}",
            )

        start_time = time.time()

        try:
            # Get sample config
            config = manifest.sample_config
            if not config:
                return ComponentSampleDataResponse(
                    component_name=name,
                    success=False,
                    error="No sample config available for this component",
                )

            # Execute component (placeholder for Phase 5)
            # In Phase 5, this will actually run the component
            spark = get_spark()

            # For now, return mock data
            sample_data = [
                {
                    "id": i,
                    "name": f"Sample {i}",
                    "value": f"Value {i}",
                }
                for i in range(sample_size)
            ]

            schema = {
                "fields": [
                    {"name": "id", "type": "integer"},
                    {"name": "name", "type": "string"},
                    {"name": "value", "type": "string"},
                ]
            }

            execution_time = (time.time() - start_time) * 1000

            return ComponentSampleDataResponse(
                component_name=name,
                sample_data=sample_data,
                row_count=len(sample_data),
                schema_=schema,
                execution_time_ms=execution_time,
                success=True,
            )

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            return ComponentSampleDataResponse(
                component_name=name,
                execution_time_ms=execution_time,
                success=False,
                error=str(e),
            )

    def _manifest_to_schema(self, manifest: ComponentManifest) -> ComponentManifestSchema:
        """Convert ComponentManifest to ComponentManifestSchema."""
        return ComponentManifestSchema(
            name=manifest.name,
            category=manifest.category.value,
            display_name=manifest.display_name,
            description=manifest.description,
            icon=manifest.icon,
            tags=manifest.tags or [],
            config_schema=manifest.config_schema or {},
            sample_config=manifest.sample_config or {},
            has_code_editor=manifest.has_code_editor,
            is_streaming=manifest.is_streaming,
            supports_incremental=manifest.supports_incremental,
            module_path=manifest.module_path,
            class_name=manifest.class_name,
        )

    def _manifest_to_metadata(self, manifest: ComponentManifest) -> ComponentMetadata:
        """Convert ComponentManifest to ComponentMetadata."""
        return ComponentMetadata(
            name=manifest.name,
            category=manifest.category.value,
            display_name=manifest.display_name,
            description=manifest.description,
            icon=manifest.icon,
            tags=manifest.tags or [],
            is_streaming=manifest.is_streaming,
            supports_incremental=manifest.supports_incremental,
        )

    def _category_display_name(self, category: ComponentCategory) -> str:
        """Get display name for category."""
        display_names = {
            ComponentCategory.CONNECTION: "Connections",
            ComponentCategory.INGESTOR: "Ingestors",
            ComponentCategory.TRANSFORMER: "Transformers",
            ComponentCategory.ML: "Machine Learning",
            ComponentCategory.SINK: "Sinks",
        }
        return display_names.get(category, category.value.title())

    def _category_icon(self, category: ComponentCategory) -> str:
        """Get icon for category."""
        icons = {
            ComponentCategory.CONNECTION: "database",
            ComponentCategory.INGESTOR: "download",
            ComponentCategory.TRANSFORMER: "transform",
            ComponentCategory.ML: "brain",
            ComponentCategory.SINK: "upload",
        }
        return icons.get(category, "component")


# Global component service instance
component_service = ComponentService()
