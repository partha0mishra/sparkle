"""
Service for discovering and managing Sparkle components.
Scans sparkle/ packages to build component registry with JSON Schema.
"""
import importlib
import inspect
import pkgutil
import sys
from pathlib import Path
from typing import Any, Optional
from functools import lru_cache

from core.config import settings
from schemas.component import (
    ComponentMetadata,
    ComponentSchema,
    ComponentDetail,
    ComponentListResponse,
    ComponentCategory,
    ComponentCategoriesResponse,
    ComponentValidationResponse,
)
from utils.schema_generator import generate_schema_from_class, extract_config_class


class ComponentService:
    """Service for managing Sparkle components."""

    def __init__(self):
        self.sparkle_path = Path(settings.SPARKLE_ENGINE_PATH)
        self._registry: dict[str, dict[str, ComponentSchema]] = {
            "ingestors": {},
            "transformers": {},
            "ml": {},
            "connections": {},
        }
        self._initialized = False

    def initialize(self):
        """Initialize component registry by scanning sparkle packages."""
        if self._initialized:
            return

        # Add sparkle path to sys.path if not already there
        if str(self.sparkle_path) not in sys.path:
            sys.path.insert(0, str(self.sparkle_path))

        # Scan each component type
        self._scan_components("ingestors", "sparkle.ingestors")
        self._scan_components("transformers", "sparkle.transformers")
        self._scan_components("ml", "sparkle.ml")
        self._scan_components("connections", "sparkle.connections")

        self._initialized = True

    def _scan_components(self, component_type: str, package_name: str):
        """Scan a package for components."""
        try:
            package = importlib.import_module(package_name)
        except (ImportError, ModuleNotFoundError):
            # Package doesn't exist, skip
            return

        # Iterate through all modules in package
        if hasattr(package, "__path__"):
            for importer, modname, ispkg in pkgutil.iter_modules(package.__path__):
                if modname.startswith("_"):
                    continue

                try:
                    module_name = f"{package_name}.{modname}"
                    module = importlib.import_module(module_name)

                    # Extract component metadata
                    component_schema = self._extract_component_schema(
                        module, component_type, modname
                    )
                    if component_schema:
                        self._registry[component_type][modname] = component_schema

                except Exception as e:
                    # Log error but continue
                    print(f"Error loading component {modname}: {e}")

    def _extract_component_schema(
        self, module: Any, component_type: str, module_name: str
    ) -> Optional[ComponentSchema]:
        """Extract component schema from module."""
        # Find the main class (usually named after the module or has run/execute method)
        main_class = None
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ == module.__name__:
                # Look for classes with run, execute, or transform methods
                if hasattr(obj, "run") or hasattr(obj, "execute") or hasattr(obj, "transform"):
                    main_class = obj
                    break

        if not main_class:
            return None

        # Extract metadata
        metadata = ComponentMetadata(
            name=module_name,
            type=component_type,
            category=self._get_category(component_type, module_name),
            description=self._extract_description(main_class),
            version=getattr(module, "__version__", "1.0.0"),
            tags=self._extract_tags(main_class),
        )

        # Extract config schema
        config_class = extract_config_class(module)
        if config_class:
            config_schema = generate_schema_from_class(config_class)
        else:
            # No config class found, generate empty schema
            config_schema = {"type": "object", "properties": {}}

        # Extract example config
        example_config = None
        if hasattr(module, "EXAMPLE_CONFIG"):
            example_config = module.EXAMPLE_CONFIG
        elif hasattr(main_class, "EXAMPLE_CONFIG"):
            example_config = main_class.EXAMPLE_CONFIG

        return ComponentSchema(
            metadata=metadata,
            config_schema=config_schema,
            example_config=example_config,
        )

    def _get_category(self, component_type: str, module_name: str) -> str:
        """Determine component category."""
        # Category mappings based on naming patterns
        categories = {
            "ingestors": {
                "postgres": "Database",
                "mysql": "Database",
                "mongodb": "Database",
                "s3": "Cloud Storage",
                "gcs": "Cloud Storage",
                "azure": "Cloud Storage",
                "csv": "File",
                "json": "File",
                "parquet": "File",
                "api": "API",
                "kafka": "Streaming",
            },
            "transformers": {
                "filter": "Data Quality",
                "deduplicate": "Data Quality",
                "validate": "Data Quality",
                "aggregate": "Aggregation",
                "join": "Join",
                "pivot": "Reshape",
                "unpivot": "Reshape",
            },
            "ml": {
                "regression": "Regression",
                "classification": "Classification",
                "clustering": "Clustering",
                "feature": "Feature Engineering",
            },
            "connections": {
                "postgres": "Database",
                "mysql": "Database",
                "s3": "Cloud Storage",
            },
        }

        type_categories = categories.get(component_type, {})
        for keyword, category in type_categories.items():
            if keyword in module_name.lower():
                return category

        return component_type.title()

    def _extract_description(self, cls: type) -> Optional[str]:
        """Extract description from class docstring."""
        if cls.__doc__:
            return cls.__doc__.strip().split("\n")[0]
        return None

    def _extract_tags(self, cls: type) -> list[str]:
        """Extract tags from class."""
        tags = []
        if hasattr(cls, "TAGS"):
            tags = cls.TAGS
        return tags

    def get_all_components(self) -> ComponentListResponse:
        """Get all components grouped by type."""
        self.initialize()

        response = ComponentListResponse(
            ingestors=[c.metadata for c in self._registry["ingestors"].values()],
            transformers=[c.metadata for c in self._registry["transformers"].values()],
            ml=[c.metadata for c in self._registry["ml"].values()],
            connections=[c.metadata for c in self._registry["connections"].values()],
        )
        response.total_count = (
            len(response.ingestors)
            + len(response.transformers)
            + len(response.ml)
            + len(response.connections)
        )
        return response

    def get_component(self, component_type: str, component_name: str) -> Optional[ComponentDetail]:
        """Get detailed component information."""
        self.initialize()

        if component_type not in self._registry:
            return None

        component_schema = self._registry[component_type].get(component_name)
        if not component_schema:
            return None

        return ComponentDetail(
            metadata=component_schema.metadata,
            config_schema=component_schema.config_schema,
            example_config=component_schema.example_config,
        )

    def get_categories(self) -> ComponentCategoriesResponse:
        """Get components grouped by categories."""
        self.initialize()

        categories_map: dict[str, ComponentCategory] = {}

        for component_type, components in self._registry.items():
            for component_schema in components.values():
                category_name = component_schema.metadata.category
                if category_name not in categories_map:
                    categories_map[category_name] = ComponentCategory(
                        name=category_name.lower().replace(" ", "_"),
                        display_name=category_name,
                        components=[],
                    )
                categories_map[category_name].components.append(component_schema.metadata)

        return ComponentCategoriesResponse(
            categories=list(categories_map.values())
        )

    def validate_config(
        self, component_type: str, component_name: str, config: dict[str, Any]
    ) -> ComponentValidationResponse:
        """Validate component configuration against schema."""
        self.initialize()

        if component_type not in self._registry:
            return ComponentValidationResponse(
                valid=False, errors=[f"Invalid component type: {component_type}"]
            )

        component_schema = self._registry[component_type].get(component_name)
        if not component_schema:
            return ComponentValidationResponse(
                valid=False, errors=[f"Component not found: {component_name}"]
            )

        # Basic JSON Schema validation
        errors = []
        warnings = []

        schema = component_schema.config_schema
        required_fields = schema.get("required", [])
        properties = schema.get("properties", {})

        # Check required fields
        for field in required_fields:
            if field not in config:
                errors.append(f"Missing required field: {field}")

        # Check field types (basic validation)
        for field, value in config.items():
            if field in properties:
                prop_schema = properties[field]
                expected_type = prop_schema.get("type")
                if expected_type:
                    if not self._validate_type(value, expected_type):
                        errors.append(
                            f"Field '{field}' has invalid type. Expected: {expected_type}"
                        )

        return ComponentValidationResponse(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

    def _validate_type(self, value: Any, expected_type: str | list) -> bool:
        """Basic type validation."""
        if isinstance(expected_type, list):
            return any(self._validate_type(value, t) for t in expected_type)

        type_map = {
            "string": str,
            "integer": int,
            "number": (int, float),
            "boolean": bool,
            "array": list,
            "object": dict,
            "null": type(None),
        }

        expected_py_type = type_map.get(expected_type)
        if expected_py_type:
            return isinstance(value, expected_py_type)
        return True


# Global component service instance
component_service = ComponentService()
