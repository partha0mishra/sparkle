"""
Component Registry for Sparkle Studio.
Automatically discovers and registers all Sparkle components with their config schemas.
"""
import importlib
import inspect
import pkgutil
import sys
from pathlib import Path
from typing import Any, Optional, Dict, List, Callable
from dataclasses import dataclass
from enum import Enum

from core.config import settings


class ComponentCategory(str, Enum):
    """Component categories."""
    CONNECTION = "connection"
    INGESTOR = "ingestor"
    TRANSFORMER = "transformer"
    ML = "ml"
    SINK = "sink"


@dataclass
class ComponentManifest:
    """Complete manifest for a Sparkle component."""
    # Identity
    name: str
    category: ComponentCategory
    display_name: str

    # Documentation
    description: Optional[str] = None
    icon: Optional[str] = None
    tags: List[str] = None
    sub_group: Optional[str] = None  # Sub-group for organizing components (e.g., "Cloud Object Storage")

    # Configuration
    config_schema: Dict[str, Any] = None
    sample_config: Dict[str, Any] = None

    # Capabilities
    has_code_editor: bool = False
    is_streaming: bool = False
    supports_incremental: bool = False

    # Module information
    module_path: Optional[str] = None
    class_name: Optional[str] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = []
        if self.config_schema is None:
            self.config_schema = {}
        if self.sample_config is None:
            self.sample_config = {}


class ComponentRegistry:
    """
    Registry for all Sparkle components.
    Scans sparkle package and builds in-memory registry.
    """

    def __init__(self):
        self.sparkle_path = Path(settings.SPARKLE_ENGINE_PATH)
        self._registry: Dict[ComponentCategory, Dict[str, ComponentManifest]] = {
            ComponentCategory.CONNECTION: {},
            ComponentCategory.INGESTOR: {},
            ComponentCategory.TRANSFORMER: {},
            ComponentCategory.ML: {},
            ComponentCategory.SINK: {},
        }
        self._initialized = False

    def build(self) -> "ComponentRegistry":
        """Build the component registry by scanning sparkle package."""
        if self._initialized:
            return self

        # Add sparkle path to sys.path
        if str(self.sparkle_path) not in sys.path:
            sys.path.insert(0, str(self.sparkle_path))

        # Scan each category
        self._scan_category(ComponentCategory.CONNECTION, "sparkle.connections")
        self._scan_category(ComponentCategory.INGESTOR, "sparkle.ingestors")
        self._scan_category(ComponentCategory.TRANSFORMER, "sparkle.transformers")
        self._scan_category(ComponentCategory.ML, "sparkle.ml")

        self._initialized = True
        return self

    def _scan_category(self, category: ComponentCategory, package_name: str):
        """Scan a package for components."""
        try:
            package = importlib.import_module(package_name)
        except (ImportError, ModuleNotFoundError) as e:
            print(f"Warning: Could not import {package_name}: {e}")
            return

        if not hasattr(package, "__path__"):
            return

        # Iterate through all modules in package
        for importer, modname, ispkg in pkgutil.iter_modules(package.__path__):
            if modname.startswith("_") or modname == "config_schema":
                continue

            try:
                module_name = f"{package_name}.{modname}"
                module = importlib.import_module(module_name)

                # Find components in this module
                components = self._extract_components_from_module(
                    module, category, modname
                )

                for component in components:
                    self._registry[category][component.name] = component

            except Exception as e:
                print(f"Warning: Error loading {modname} from {package_name}: {e}")

    def _extract_components_from_module(
        self, module: Any, category: ComponentCategory, module_name: str
    ) -> List[ComponentManifest]:
        """Extract component manifests from a module."""
        components = []

        # Look for classes and functions in the module
        for name, obj in inspect.getmembers(module):
            # Skip imported items
            if not hasattr(obj, "__module__") or obj.__module__ != module.__name__:
                continue

            # Try to extract manifest
            manifest = None

            if inspect.isclass(obj):
                manifest = self._extract_manifest_from_class(
                    obj, category, module_name, module.__name__
                )
            elif inspect.isfunction(obj):
                manifest = self._extract_manifest_from_function(
                    obj, category, module_name, module.__name__
                )

            if manifest:
                components.append(manifest)

        return components

    def _extract_manifest_from_class(
        self, cls: type, category: ComponentCategory, module_name: str, module_path: str
    ) -> Optional[ComponentManifest]:
        """Extract manifest from a class."""
        # Check if class has config_schema method
        if not (hasattr(cls, "config_schema") and callable(getattr(cls, "config_schema"))):
            return None

        # Extract metadata
        name = self._snake_case(cls.__name__)
        display_name = self._humanize_name(cls.__name__)
        description = self._extract_description(cls)
        icon = self._extract_icon(cls, category)
        tags = self._extract_tags(cls)
        sub_group = self._extract_sub_group(cls)

        # Extract config schema
        try:
            config_schema = cls.config_schema()
        except Exception as e:
            print(f"Warning: Could not extract config schema from {cls.__name__}: {e}")
            config_schema = {}

        # Extract sample config
        sample_config = {}
        if hasattr(cls, "sample_config") and callable(getattr(cls, "sample_config")):
            try:
                sample_config = cls.sample_config()
            except Exception:
                pass

        # Detect capabilities
        has_code_editor = self._has_custom_code_support(cls)
        is_streaming = self._is_streaming_component(cls)
        supports_incremental = self._supports_incremental(cls)

        return ComponentManifest(
            name=name,
            category=category,
            display_name=display_name,
            description=description,
            icon=icon,
            tags=tags,
            sub_group=sub_group,
            config_schema=config_schema,
            sample_config=sample_config,
            has_code_editor=has_code_editor,
            is_streaming=is_streaming,
            supports_incremental=supports_incremental,
            module_path=module_path,
            class_name=cls.__name__,
        )

    def _extract_manifest_from_function(
        self, func: Callable, category: ComponentCategory, module_name: str, module_path: str
    ) -> Optional[ComponentManifest]:
        """Extract manifest from a function."""
        # Check if function has config_schema
        if not (hasattr(func, "config_schema") and callable(getattr(func, "config_schema"))):
            return None

        # Extract metadata
        name = func.__name__
        display_name = self._humanize_name(func.__name__)
        description = self._extract_description(func)
        icon = self._extract_icon(func, category)
        tags = self._extract_tags(func)
        sub_group = self._extract_sub_group(func)

        # Extract config schema
        try:
            config_schema = func.config_schema()
        except Exception as e:
            print(f"Warning: Could not extract config schema from {func.__name__}: {e}")
            config_schema = {}

        # Extract sample config
        sample_config = {}
        if hasattr(func, "sample_config") and callable(getattr(func, "sample_config")):
            try:
                sample_config = func.sample_config()
            except Exception:
                pass

        return ComponentManifest(
            name=name,
            category=category,
            display_name=display_name,
            description=description,
            icon=icon,
            tags=tags,
            sub_group=sub_group,
            config_schema=config_schema,
            sample_config=sample_config,
            has_code_editor=False,
            is_streaming=False,
            supports_incremental="incremental" in name.lower(),
            module_path=module_path,
            class_name=func.__name__,
        )

    def _extract_description(self, obj: Any) -> Optional[str]:
        """Extract description from docstring."""
        if obj.__doc__:
            # Get first line of docstring
            lines = obj.__doc__.strip().split("\n")
            for line in lines:
                line = line.strip()
                if line and not line.startswith("Tags:") and not line.startswith("Category:"):
                    return line
        return None

    def _extract_icon(self, obj: Any, category: ComponentCategory) -> str:
        """Extract icon name from docstring or default."""
        if obj.__doc__:
            for line in obj.__doc__.split("\n"):
                if line.strip().startswith("Icon:"):
                    return line.split("Icon:")[1].strip()

        # Default icons by category
        default_icons = {
            ComponentCategory.CONNECTION: "database",
            ComponentCategory.INGESTOR: "download",
            ComponentCategory.TRANSFORMER: "transform",
            ComponentCategory.ML: "brain",
            ComponentCategory.SINK: "upload",
        }
        return default_icons.get(category, "component")

    def _extract_tags(self, obj: Any) -> List[str]:
        """Extract tags from docstring."""
        tags = []
        if obj.__doc__:
            for line in obj.__doc__.split("\n"):
                if line.strip().startswith("Tags:"):
                    tags_str = line.split("Tags:")[1].strip()
                    tags = [t.strip() for t in tags_str.split(",")]
                    break
        return tags

    def _extract_sub_group(self, obj: Any) -> Optional[str]:
        """Extract sub-group from docstring."""
        if obj.__doc__:
            for line in obj.__doc__.split("\n"):
                if line.strip().startswith("Sub-Group:"):
                    return line.split("Sub-Group:")[1].strip()
        return None

    def _has_custom_code_support(self, cls: type) -> bool:
        """Check if component supports custom code."""
        # Check for custom_code parameter in config schema
        if hasattr(cls, "config_schema"):
            try:
                schema = cls.config_schema()
                if "properties" in schema:
                    return "custom_code" in schema["properties"]
            except Exception:
                pass
        return False

    def _is_streaming_component(self, cls: type) -> bool:
        """Check if component is for streaming."""
        # Check for streaming methods or keywords
        streaming_indicators = ["streaming", "stream", "kafka", "kinesis"]

        # Check class name
        if any(ind in cls.__name__.lower() for ind in streaming_indicators):
            return True

        # Check if has run_streaming method
        if hasattr(cls, "run_streaming"):
            return True

        return False

    def _supports_incremental(self, cls: type) -> bool:
        """Check if component supports incremental loading."""
        incremental_indicators = ["incremental", "watermark", "checkpoint"]

        # Check class name
        if any(ind in cls.__name__.lower() for ind in incremental_indicators):
            return True

        # Check config schema for watermark-related fields
        if hasattr(cls, "config_schema"):
            try:
                schema = cls.config_schema()
                if "properties" in schema:
                    props = schema["properties"]
                    if any(key in props for key in ["watermark_column", "watermark", "checkpoint"]):
                        return True
            except Exception:
                pass

        return False

    def _snake_case(self, name: str) -> str:
        """Convert CamelCase to snake_case."""
        import re
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    def _humanize_name(self, name: str) -> str:
        """Convert snake_case or CamelCase to Human Readable."""
        # Convert CamelCase to spaces
        import re
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1 \2', name)
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1 \2', s1)
        # Convert snake_case to spaces
        s3 = s2.replace('_', ' ')
        # Title case
        return s3.title()

    def get_all(self) -> Dict[ComponentCategory, Dict[str, ComponentManifest]]:
        """Get all registered components."""
        return self._registry

    def get_by_category(self, category: ComponentCategory) -> Dict[str, ComponentManifest]:
        """Get components by category."""
        return self._registry.get(category, {})

    def get_component(self, category: ComponentCategory, name: str) -> Optional[ComponentManifest]:
        """Get specific component manifest."""
        return self._registry.get(category, {}).get(name)

    def search(self, query: str) -> List[ComponentManifest]:
        """Search components by name, description, or tags."""
        query_lower = query.lower()
        results = []

        for category_components in self._registry.values():
            for manifest in category_components.values():
                # Search in name
                if query_lower in manifest.name.lower():
                    results.append(manifest)
                    continue

                # Search in display name
                if query_lower in manifest.display_name.lower():
                    results.append(manifest)
                    continue

                # Search in description
                if manifest.description and query_lower in manifest.description.lower():
                    results.append(manifest)
                    continue

                # Search in tags
                if any(query_lower in tag.lower() for tag in manifest.tags):
                    results.append(manifest)
                    continue

        return results

    def get_stats(self) -> Dict[str, int]:
        """Get registry statistics."""
        stats = {}
        total = 0
        for category, components in self._registry.items():
            count = len(components)
            stats[category.value] = count
            total += count
        stats["total"] = total
        return stats


# Global registry instance
_global_registry: Optional[ComponentRegistry] = None


def get_registry() -> ComponentRegistry:
    """Get the global component registry."""
    global _global_registry
    if _global_registry is None:
        _global_registry = ComponentRegistry().build()
    return _global_registry
