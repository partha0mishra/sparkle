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
    ORCHESTRATOR = "orchestrator"
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
            ComponentCategory.ORCHESTRATOR: {},
            ComponentCategory.SINK: {},
        }
        self._initialized = False

    def build(self) -> "ComponentRegistry":
        """Build the component registry by reading from factory registries."""
        if self._initialized:
            return self

        # Add sparkle path to sys.path
        if str(self.sparkle_path) not in sys.path:
            sys.path.insert(0, str(self.sparkle_path))

        # Load components from factory registries
        self._load_from_connection_factory()
        self._load_from_ingestor_factory()
        self._load_from_ml_factory()
        self._load_transformers()
        self._load_from_pipeline_factory()

        self._initialized = True
        return self

    def _load_from_connection_factory(self):
        """Load connections from ConnectionFactory registry."""
        try:
            from connections.factory import ConnectionFactory
            
            for name, connection_class in ConnectionFactory._registry.items():
                manifest = self._create_manifest_from_class(
                    connection_class,
                    ComponentCategory.CONNECTION,
                    name
                )
                if manifest:
                    self._registry[ComponentCategory.CONNECTION][name] = manifest
                    
        except Exception as e:
            print(f"Warning: Could not load connections: {e}")

    def _load_from_ingestor_factory(self):
        """Load ingestors from IngestorFactory registry."""
        try:
            from ingestors.factory import IngestorFactory
            
            for name, ingestor_class in IngestorFactory._registry.items():
                manifest = self._create_manifest_from_class(
                    ingestor_class,
                    ComponentCategory.INGESTOR,
                    name
                )
                if manifest:
                    self._registry[ComponentCategory.INGESTOR][name] = manifest
                    
        except Exception as e:
            print(f"Warning: Could not load ingestors: {e}")

    def _load_from_ml_factory(self):
        """Load ML components from MLComponentFactory registry."""
        try:
            from ml.factory import MLComponentFactory
            
            for name, ml_class in MLComponentFactory._registry.items():
                manifest = self._create_manifest_from_class(
                    ml_class,
                    ComponentCategory.ML,
                    name
                )
                if manifest:
                    self._registry[ComponentCategory.ML][name] = manifest
                    
        except Exception as e:
            print(f"Warning: Could not load ML components: {e}")

    def _load_transformers(self):
        """Load transformer functions."""
        try:
            import transformers
            
            # Transformers are function-based, get them from the module
            transformer_categories = {
                'cleaning': transformers.cleaning,
                'enrichment': transformers.enrichment,
                'validation': transformers.validation,
                'scd': transformers.scd,
                'aggregation': transformers.aggregation,
                'parsing': transformers.parsing,
                'pii': transformers.pii,
                'datetime': transformers.datetime,
                'cdc': transformers.cdc,
                'advanced': transformers.advanced,
            }
            
            for category_name, module in transformer_categories.items():
                for name in dir(module):
                    if name.startswith('_'):
                        continue

                    func = getattr(module, name)
                    # Only process functions decorated with @transformer
                    if callable(func) and hasattr(func, '_is_transformer') and hasattr(func, '__doc__'):
                        manifest = self._create_manifest_from_function(
                            func,
                            ComponentCategory.TRANSFORMER,
                            name,
                            category_name
                        )
                        if manifest:
                            self._registry[ComponentCategory.TRANSFORMER][f"{category_name}.{name}"] = manifest
                            
        except Exception as e:
            print(f"Warning: Could not load transformers: {e}")

    def _load_from_pipeline_factory(self):
        """Load orchestration components from orchestrators package."""
        try:
            # A. Core Pipeline Templates (24)
            from orchestrators.factory import PipelineFactory
            from orchestrators.base import PipelineConfig
            
            for pipeline_type, pipeline_class in PipelineFactory._registry.items():
                try:
                    dummy_config = PipelineConfig(
                        pipeline_name=f"example_{pipeline_type}",
                        pipeline_type=pipeline_type,
                        env="dev"
                    )
                    
                    pipeline = pipeline_class(dummy_config, spark=None)
                    pipeline.build()
                    graph = self._extract_pipeline_graph(pipeline)
                    
                    manifest = self._create_orchestrator_manifest(
                        pipeline_class,
                        pipeline_type,
                        "Core Pipeline Templates",
                        graph
                    )
                    
                    if manifest:
                        self._registry[ComponentCategory.ORCHESTRATOR][f"pipeline_{pipeline_type}"] = manifest
                        
                except Exception as e:
                    print(f"Warning: Could not build pipeline {pipeline_type}: {e}")
            
            # B. Multi-Orchestrator Adapters (25)
            self._load_orchestrator_adapters()
            
            # C. Task Building Blocks (22)
            self._load_orchestrator_tasks()
            
            # D. Scheduling & Trigger Patterns (12)
            self._load_scheduling_patterns()
                    
        except Exception as e:
            print(f"Warning: Could not load orchestrators: {e}")

    def _load_orchestrator_adapters(self):
        """Load Multi-Orchestrator Adapters."""
        try:
            import orchestrators.adapters as adapters_module
            import inspect
            
            # Get all adapter classes from the adapters package
            adapter_modules = [
                'databricks', 'airflow', 'dagster', 'prefect', 'mage', 'dbt_cloud'
            ]
            
            for module_name in adapter_modules:
                try:
                    module = getattr(adapters_module, module_name, None)
                    if not module:
                        continue
                    
                    for name, obj in inspect.getmembers(module, inspect.isclass):
                        if name.endswith('Adapter') and obj.__module__.startswith('orchestrators.adapters'):
                            manifest = self._create_orchestrator_manifest(
                                obj,
                                name,
                                "Multi-Orchestrator Adapters",
                                None  # Adapters don't have graphs
                            )
                            
                            if manifest:
                                self._registry[ComponentCategory.ORCHESTRATOR][f"adapter_{name}"] = manifest
                                
                except Exception as e:
                    print(f"Warning: Could not load adapters from {module_name}: {e}")
                    
        except Exception as e:
            print(f"Warning: Could not load orchestrator adapters: {e}")

    def _load_orchestrator_tasks(self):
        """Load Task Building Blocks."""
        try:
            import orchestrators.tasks as tasks_module
            import inspect
            
            task_modules = [
                'ingest', 'transform', 'write', 'ml', 'quality', 
                'governance', 'notification', 'orchestration', 'observability'
            ]
            
            for module_name in task_modules:
                try:
                    module = getattr(tasks_module, module_name, None)
                    if not module:
                        continue
                    
                    for name, obj in inspect.getmembers(module, inspect.isclass):
                        if name.endswith('Task') and obj.__module__.startswith('orchestrators.tasks'):
                            manifest = self._create_orchestrator_manifest(
                                obj,
                                name,
                                "Task Building Blocks",
                                None
                            )
                            
                            if manifest:
                                self._registry[ComponentCategory.ORCHESTRATOR][f"task_{name}"] = manifest
                                
                except Exception as e:
                    print(f"Warning: Could not load tasks from {module_name}: {e}")
                    
        except Exception as e:
            print(f"Warning: Could not load orchestrator tasks: {e}")

    def _load_scheduling_patterns(self):
        """Load Scheduling & Trigger Patterns."""
        try:
            import orchestrators.scheduling.patterns as patterns_module
            import inspect
            
            for name, obj in inspect.getmembers(patterns_module):
                # Look for classes or functions that are scheduling-related
                if (inspect.isclass(obj) or inspect.isfunction(obj)) and \
                   obj.__module__ == 'orchestrators.scheduling.patterns':
                    manifest = self._create_orchestrator_manifest(
                        obj,
                        name,
                        "Scheduling & Trigger Patterns",
                        None
                    )
                    
                    if manifest:
                        self._registry[ComponentCategory.ORCHESTRATOR][f"schedule_{name}"] = manifest
                        
        except Exception as e:
            print(f"Warning: Could not load scheduling patterns: {e}")

    def _extract_pipeline_graph(self, pipeline) -> Dict[str, Any]:
        """Extract graph structure from a built pipeline."""
        nodes = []
        edges = []
        
        for i, task in enumerate(pipeline.tasks):
            nodes.append({
                "id": f"task_{i}",
                "type": task.__class__.__name__,
                "label": task.task_name,
                "config": task.config
            })
            
            # Create edges based on task order (sequential for now)
            if i > 0:
                edges.append({
                    "source": f"task_{i-1}",
                    "target": f"task_{i}"
                })
        
        return {
            "nodes": nodes,
            "edges": edges
        }

    def _create_orchestrator_manifest(
        self,
        component_class: type,
        component_name: str,
        sub_group: str,
        graph: Optional[Dict[str, Any]]
    ) -> Optional[ComponentManifest]:
        """Create manifest for an orchestrator component."""
        try:
            display_name = self._humanize_name(component_class.__name__)
            description = self._extract_description(component_class)
            
            # Store graph in config_schema if provided
            config_schema = {}
            if graph:
                config_schema = {
                    "type": "pipeline",
                    "graph": graph
                }
            
            return ComponentManifest(
                name=component_name,
                category=ComponentCategory.ORCHESTRATOR,
                display_name=display_name,
                description=description,
                icon="workflow",
                tags=["orchestrator", sub_group.lower().replace(" ", "_")],
                sub_group=sub_group,
                config_schema=config_schema,
                sample_config={},
                has_code_editor=False,
                is_streaming=False,
                supports_incremental=False,
                module_path=component_class.__module__,
                class_name=component_class.__name__,
            )
        except Exception as e:
            print(f"Warning: Could not create orchestrator manifest for {component_name}: {e}")
            return None

    def _determine_pipeline_subgroup(self, pipeline_type: str) -> str:
        """Determine sub-group for a pipeline based on its type."""
        if "bronze" in pipeline_type.lower():
            return "Bronze Layer"
        elif "silver" in pipeline_type.lower():
            return "Silver Layer"
        elif "gold" in pipeline_type.lower():
            return "Gold Layer"
        elif "ml_" in pipeline_type.lower() or "model" in pipeline_type.lower():
            return "Machine Learning"
        elif "feature" in pipeline_type.lower():
            return "Feature Store"
        elif "quality" in pipeline_type.lower() or "contract" in pipeline_type.lower():
            return "Data Quality"
        elif "maintenance" in pipeline_type.lower() or "vacuum" in pipeline_type.lower() or "optimize" in pipeline_type.lower():
            return "Maintenance"
        elif "governance" in pipeline_type.lower():
            return "Governance"
        else:
            return "Other"


    def _create_manifest_from_class(
        self,
        cls: type,
        category: ComponentCategory,
        name: str
    ) -> Optional[ComponentManifest]:
        """Create manifest from a factory-registered class."""
        try:
            display_name = self._humanize_name(cls.__name__)
            description = self._extract_description(cls)
            icon = self._extract_icon(cls, category, name)  # Pass name for icon mapping
            tags = self._extract_tags(cls)
            sub_group = self._extract_sub_group(cls)
            config_schema = self._extract_config_schema(cls)

            return ComponentManifest(
                name=name,
                category=category,
                display_name=display_name,
                description=description,
                icon=icon,
                tags=tags,
                sub_group=sub_group,
                config_schema=config_schema,
                sample_config=self._extract_sample_config(cls),
                has_code_editor=False,
                is_streaming=self._is_streaming_component(cls),
                supports_incremental=self._supports_incremental(cls),
                module_path=cls.__module__,
                class_name=cls.__name__,
            )
        except Exception as e:
            print(f"Warning: Could not create manifest for {name}: {e}")
            return None

    def _extract_config_schema(self, obj: Any) -> Dict[str, Any]:
        """Extract config schema from obj.config_schema() or docstring.
        Supports parsing of JSON examples under 'Example config' in docstrings.
        """
        # 1. Try config_schema() method
        if hasattr(obj, "config_schema") and callable(getattr(obj, "config_schema")):
            try:
                return obj.config_schema()
            except Exception:
                pass

        # 2. Try to parse from docstring
        if obj.__doc__:
            import re, json

            # Helper function to find matching brace
            def extract_json_block(text, start_pos):
                """Extract JSON block starting from { at start_pos."""
                brace_count = 0
                in_string = False
                escape_next = False

                for i in range(start_pos, len(text)):
                    char = text[i]

                    if escape_next:
                        escape_next = False
                        continue

                    if char == '\\':
                        escape_next = True
                        continue

                    if char == '"':
                        in_string = not in_string
                        continue

                    if not in_string:
                        if char == '{':
                            brace_count += 1
                        elif char == '}':
                            brace_count -= 1
                            if brace_count == 0:
                                return text[start_pos:i+1]

                return None

            # Find 'Config example:', 'Example config:', or 'Example config (...):' patterns
            config_patterns = [
                r'Config example:',
                r'Example config\s*\([^)]*\):',  # Example config (path):
                r'Example config:'
            ]

            for config_pattern in config_patterns:
                match = re.search(config_pattern, obj.__doc__, re.IGNORECASE)
                if match:
                    # Find the next { after the pattern
                    start_search = match.end()
                    brace_pos = obj.__doc__.find('{', start_search)

                    if brace_pos != -1:
                        config_str = extract_json_block(obj.__doc__, brace_pos)

                        if config_str:
                            try:
                                # Clean the JSON string
                                # First, normalize line endings and remove leading whitespace per line
                                lines = config_str.split('\n')
                                cleaned_lines = []
                                for line in lines:
                                    # Remove leading spaces/tabs from each line
                                    line = line.lstrip(' \t')
                                    # Remove trailing whitespace
                                    line = line.rstrip()
                                    if line:
                                        cleaned_lines.append(line)
                                config_str = ' '.join(cleaned_lines)

                                # Remove trailing commas
                                config_str = re.sub(r',\s*}', '}', config_str)  # Trailing commas in objects
                                config_str = re.sub(r',\s*]', ']', config_str)  # Trailing commas in arrays

                                # Try to parse
                                config = json.loads(config_str)

                                # Build JSON Schema from example
                                properties = {}
                                required = []
                                for key, value in config.items():
                                    prop_type = "string"
                                    if isinstance(value, bool):
                                        prop_type = "boolean"
                                    elif isinstance(value, int):
                                        prop_type = "integer"
                                    elif isinstance(value, float):
                                        prop_type = "number"
                                    elif isinstance(value, list):
                                        prop_type = "array"
                                    elif isinstance(value, dict):
                                        prop_type = "object"

                                    properties[key] = {
                                        "type": prop_type,
                                        "title": key.replace("_", " ").title(),
                                        "default": value,
                                    }
                                    required.append(key)

                                return {"type": "object", "properties": properties, "required": required}
                            except Exception:
                                # Try next pattern
                                continue

        return {}

    def _extract_sample_config(self, obj: Any) -> Dict[str, Any]:
        """Extract sample config from obj.sample_config() or docstring."""
        # 1. Try sample_config() method
        if hasattr(obj, "sample_config") and callable(getattr(obj, "sample_config")):
            try:
                return obj.sample_config()
            except Exception:
                pass

        # 2. Try to parse from docstring (reuse same brace-matching logic)
        if obj.__doc__:
            import re, json

            def extract_json_block(text, start_pos):
                """Extract JSON block starting from { at start_pos."""
                brace_count = 0
                in_string = False
                escape_next = False

                for i in range(start_pos, len(text)):
                    char = text[i]
                    if escape_next:
                        escape_next = False
                        continue
                    if char == '\\':
                        escape_next = True
                        continue
                    if char == '"':
                        in_string = not in_string
                        continue
                    if not in_string:
                        if char == '{':
                            brace_count += 1
                        elif char == '}':
                            brace_count -= 1
                            if brace_count == 0:
                                return text[start_pos:i+1]
                return None

            config_patterns = [r'Config example:', r'Example config:']

            for config_pattern in config_patterns:
                match = re.search(config_pattern, obj.__doc__, re.IGNORECASE)
                if match:
                    brace_pos = obj.__doc__.find('{', match.end())
                    if brace_pos != -1:
                        config_str = extract_json_block(obj.__doc__, brace_pos)
                        if config_str:
                            try:
                                config_str = re.sub(r'#.*', '', config_str)
                                config_str = re.sub(r'//.*', '', config_str)
                                config_str = re.sub(r',\s*}', '}', config_str)
                                config_str = re.sub(r',\s*]', ']', config_str)
                                config_str = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f]', '', config_str)
                                return json.loads(config_str)
                            except Exception:
                                continue

        return {}

    def _create_manifest_from_function(
        self,
        func: callable,
        category: ComponentCategory,
        name: str,
        sub_group: str = None
    ) -> Optional[ComponentManifest]:
        """Create manifest from a transformer function."""
        try:
            display_name = self._humanize_name(name)
            description = self._extract_description(func)

            # Extract Sub-Group and Tags from docstring
            extracted_sub_group = self._extract_sub_group(func)
            extracted_tags = self._extract_tags(func)

            # Use extracted sub_group if available, otherwise use parameter
            final_sub_group = extracted_sub_group or sub_group

            return ComponentManifest(
                name=name,
                category=category,
                display_name=display_name,
                description=description,
                icon="transform",
                tags=extracted_tags,
                sub_group=final_sub_group,
                config_schema={},
                sample_config={},
                has_code_editor=False,
                is_streaming=False,
                supports_incremental=False,
                module_path=func.__module__,
                class_name=name,
            )
        except Exception as e:
            print(f"Warning: Could not create manifest for {name}: {e}")
            return None

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

    def _extract_icon(self, obj: Any, category: ComponentCategory, name: str = None) -> str:
        """Extract icon name from docstring, icon mapping, or default."""
        # 1. Check docstring for explicit Icon: declaration
        if obj.__doc__:
            for line in obj.__doc__.split("\n"):
                if line.strip().startswith("Icon:"):
                    return line.split("Icon:")[1].strip()

        # 2. Use icon mapping for connections (logos/brand icons)
        if category == ComponentCategory.CONNECTION and name:
            try:
                from icon_mapping import get_connection_icon
                return get_connection_icon(name)
            except Exception:
                pass

        # 3. Default icons by category
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
        # Special cases map (normalized lower case -> display name)
        special_cases = {
            "graphql": "GraphQL",
            "rest": "REST API",
            "restapi": "REST API",
            "rest_api": "REST API",
            "api": "API",
            "aws": "AWS",
            "gcp": "GCP",
            "azure": "Azure",
            "s3": "S3",
            "gcs": "GCS",
            "jdbc": "JDBC",
            "http": "HTTP",
            "grpc": "gRPC",
            "soap": "SOAP",
            "sql": "SQL",
            "mysql": "MySQL",
            "postgresql": "PostgreSQL",
            "postgres": "PostgreSQL",
            "mssql": "MSSQL",
            "mongodb": "MongoDB",
            "cassandra": "Cassandra",
            "kafka": "Kafka",
            "mqtt": "MQTT",
            "nats": "NATS",
            "amqp": "AMQP",
        }

        # check if the whole name is a special case
        lower_name = name.lower().replace("_", "").replace("connection", "").strip()
        if lower_name in special_cases:
             # If it's just the protocol name (e.g. "graphql"), append "Connection"
             return f"{special_cases[lower_name]} Connection"

        # Convert CamelCase to spaces
        import re
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1 \2', name)
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1 \2', s1)
        # Convert snake_case to spaces
        s3 = s2.replace('_', ' ')
        
        # Title case
        title_cased = s3.title()
        
        # Fix specific words
        words = title_cased.split()
        fixed_words = []
        for word in words:
            lower_word = word.lower()
            if lower_word in special_cases:
                fixed_words.append(special_cases[lower_word])
            else:
                fixed_words.append(word)
                
        return " ".join(fixed_words)

    def get_all(self) -> Dict[ComponentCategory, Dict[str, ComponentManifest]]:
        """Get all registered components."""
        return self._registry

    def get_by_category(self, category: ComponentCategory) -> Dict[str, ComponentManifest]:
        """Get components by category."""
        return self._registry.get(category, {})

    def get_component(self, category: ComponentCategory, name: str) -> Optional[ComponentManifest]:
        """Get specific component manifest."""
        category_components = self._registry.get(category, {})

        # Direct lookup first (works for connections, ingestors, ml, sinks, orchestrators)
        if name in category_components:
            return category_components[name]

        # For transformers, the registry key includes category prefix: "category.function_name"
        # But the API receives just the function name, so we need to search for it
        if category == ComponentCategory.TRANSFORMER:
            for key, manifest in category_components.items():
                # Check if this key ends with the requested name
                # e.g., key="cleaning.apply_conditional_logic", name="apply_conditional_logic"
                if key.endswith(f".{name}"):
                    return manifest

        return None

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
