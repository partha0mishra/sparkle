"""
Utility to generate JSON Schema from Python classes, Pydantic models, and dataclasses.
Dynamically generates JSON Schema from Sparkle component config classes.
"""
import inspect
import typing
from typing import Any, Type, get_type_hints, get_origin, get_args
from dataclasses import is_dataclass, fields as dataclass_fields
from enum import Enum

try:
    from pydantic import BaseModel
    PYDANTIC_AVAILABLE = True
except ImportError:
    BaseModel = None
    PYDANTIC_AVAILABLE = False


class JSONSchemaGenerator:
    """Generate JSON Schema from Python types."""

    def __init__(self):
        self.definitions = {}

    def generate(self, cls: Type) -> dict[str, Any]:
        """
        Generate JSON Schema from a class.
        Supports: Pydantic models, dataclasses, and typed classes.
        """
        if PYDANTIC_AVAILABLE and BaseModel is not None and issubclass(cls, BaseModel):
            return self._generate_from_pydantic(cls)
        elif is_dataclass(cls):
            return self._generate_from_dataclass(cls)
        else:
            return self._generate_from_class(cls)

    def _generate_from_pydantic(self, cls: Type[BaseModel]) -> dict[str, Any]:
        """Generate schema from Pydantic model."""
        schema = cls.model_json_schema()
        # Remove $defs if empty
        if "$defs" in schema and not schema["$defs"]:
            del schema["$defs"]
        return schema

    def _generate_from_dataclass(self, cls: Type) -> dict[str, Any]:
        """Generate schema from dataclass."""
        properties = {}
        required = []

        for field in dataclass_fields(cls):
            field_schema = self._type_to_schema(field.type)
            if field.metadata:
                # Support metadata like description
                if "description" in field.metadata:
                    field_schema["description"] = field.metadata["description"]

            properties[field.name] = field_schema

            # Check if field has no default
            if field.default is field.default_factory is inspect._empty:
                required.append(field.name)

        schema = {
            "type": "object",
            "properties": properties,
            "required": required,
            "title": cls.__name__,
        }

        if cls.__doc__:
            schema["description"] = cls.__doc__.strip()

        return schema

    def _generate_from_class(self, cls: Type) -> dict[str, Any]:
        """Generate schema from regular class with type hints."""
        properties = {}
        required = []

        try:
            type_hints = get_type_hints(cls)
        except Exception:
            type_hints = {}

        # Try to get __init__ signature
        try:
            sig = inspect.signature(cls.__init__)
            for param_name, param in sig.parameters.items():
                if param_name == "self":
                    continue

                param_type = type_hints.get(param_name, param.annotation)
                if param_type == inspect.Parameter.empty:
                    param_type = Any

                field_schema = self._type_to_schema(param_type)
                properties[param_name] = field_schema

                if param.default == inspect.Parameter.empty:
                    required.append(param_name)

        except Exception:
            pass

        schema = {
            "type": "object",
            "properties": properties,
            "title": cls.__name__,
        }

        if required:
            schema["required"] = required

        if cls.__doc__:
            schema["description"] = cls.__doc__.strip()

        return schema

    def _type_to_schema(self, type_hint: Any) -> dict[str, Any]:
        """Convert Python type hint to JSON Schema."""
        origin = get_origin(type_hint)
        args = get_args(type_hint)

        # Handle None/Optional
        if type_hint is type(None):
            return {"type": "null"}

        # Handle Union (including Optional)
        if origin is typing.Union:
            # Check if it's Optional (Union with None)
            non_none_args = [arg for arg in args if arg is not type(None)]
            if len(non_none_args) == 1 and type(None) in args:
                # It's Optional[T]
                schema = self._type_to_schema(non_none_args[0])
                # Make nullable
                if "type" in schema:
                    schema["type"] = [schema["type"], "null"]
                return schema
            else:
                # General Union - use anyOf
                return {"anyOf": [self._type_to_schema(arg) for arg in args]}

        # Handle List
        if origin is list:
            item_schema = self._type_to_schema(args[0]) if args else {}
            return {"type": "array", "items": item_schema}

        # Handle Dict
        if origin is dict:
            if args and len(args) == 2:
                return {
                    "type": "object",
                    "additionalProperties": self._type_to_schema(args[1])
                }
            return {"type": "object"}

        # Handle Enum
        if inspect.isclass(type_hint) and issubclass(type_hint, Enum):
            return {
                "type": "string",
                "enum": [e.value for e in type_hint]
            }

        # Handle basic types
        type_mapping = {
            str: {"type": "string"},
            int: {"type": "integer"},
            float: {"type": "number"},
            bool: {"type": "boolean"},
            dict: {"type": "object"},
            list: {"type": "array"},
            Any: {},
        }

        if type_hint in type_mapping:
            return type_mapping[type_hint]

        # Handle classes (nested objects)
        if inspect.isclass(type_hint):
            if PYDANTIC_AVAILABLE and BaseModel is not None and issubclass(type_hint, BaseModel):
                return self._generate_from_pydantic(type_hint)
            elif is_dataclass(type_hint):
                return self._generate_from_dataclass(type_hint)
            else:
                # Try to generate from class
                return self._generate_from_class(type_hint)

        # Default: any type
        return {}


def generate_schema_from_class(cls: Type) -> dict[str, Any]:
    """
    Generate JSON Schema from a Python class.
    Convenience function.
    """
    generator = JSONSchemaGenerator()
    return generator.generate(cls)


def extract_config_class(component_module: Any) -> Type | None:
    """
    Extract the Config class from a Sparkle component module.
    Looks for classes named Config, {ComponentName}Config, etc.
    """
    # Try common patterns
    if hasattr(component_module, "Config"):
        return component_module.Config

    # Try {ModuleName}Config
    module_name = component_module.__name__.split(".")[-1]
    config_class_name = f"{module_name.title().replace('_', '')}Config"
    if hasattr(component_module, config_class_name):
        return getattr(component_module, config_class_name)

    # Search for any class with "Config" in name
    for name, obj in inspect.getmembers(component_module, inspect.isclass):
        if "Config" in name and obj.__module__ == component_module.__name__:
            return obj

    return None
