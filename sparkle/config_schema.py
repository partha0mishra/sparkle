"""
Config Schema system for Sparkle components.
Provides decorator and base class for automatic JSON Schema generation.
"""
from typing import Any, Type, Optional, get_type_hints, get_origin, get_args
from dataclasses import dataclass, fields, MISSING
from enum import Enum
import inspect


class Field:
    """Field descriptor with metadata for JSON Schema generation."""

    def __init__(
        self,
        default: Any = ...,
        *,
        description: str = "",
        title: Optional[str] = None,
        ge: Optional[float] = None,  # greater than or equal
        le: Optional[float] = None,  # less than or equal
        gt: Optional[float] = None,  # greater than
        lt: Optional[float] = None,  # less than
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        pattern: Optional[str] = None,
        examples: Optional[list] = None,
        deprecated: bool = False,
        # UI hints
        widget: Optional[str] = None,  # password, textarea, dropdown, file, date, etc.
        group: Optional[str] = None,  # Group fields in UI
        order: Optional[int] = None,  # Display order in UI
        placeholder: Optional[str] = None,
        help_text: Optional[str] = None,
    ):
        self.default = default
        self.description = description
        self.title = title
        self.ge = ge
        self.le = le
        self.gt = gt
        self.lt = lt
        self.min_length = min_length
        self.max_length = max_length
        self.pattern = pattern
        self.examples = examples or []
        self.deprecated = deprecated
        # UI hints
        self.widget = widget
        self.group = group
        self.order = order
        self.placeholder = placeholder
        self.help_text = help_text

    def __set_name__(self, owner, name):
        self.name = name

    @property
    def is_required(self) -> bool:
        return self.default is ...


def config_schema(cls: Type) -> Type:
    """
    Decorator that marks a class as a config schema.
    Adds a classmethod config_schema() that returns JSON Schema.
    """
    # Store original class
    original_class = cls

    # Add config_schema classmethod
    @classmethod
    def get_config_schema(cls_self) -> dict[str, Any]:
        """Generate JSON Schema from this config class."""
        return _generate_schema_from_class(original_class)

    original_class.config_schema = get_config_schema
    original_class._is_config_schema = True

    return original_class


def _generate_schema_from_class(cls: Type) -> dict[str, Any]:
    """Generate JSON Schema from a config class."""
    schema = {
        "type": "object",
        "title": cls.__name__,
        "properties": {},
        "required": [],
    }

    if cls.__doc__:
        schema["description"] = cls.__doc__.strip()

    # Try to get type hints
    try:
        type_hints = get_type_hints(cls)
    except Exception:
        type_hints = {}

    # Process class annotations
    if hasattr(cls, "__annotations__"):
        for field_name, field_type in cls.__annotations__.items():
            # Get field descriptor if it exists
            field_descriptor = None
            if hasattr(cls, field_name):
                attr = getattr(cls, field_name)
                if isinstance(attr, Field):
                    field_descriptor = attr

            # Generate field schema
            field_schema = _type_to_json_schema(field_type)

            # Add metadata from Field descriptor
            if field_descriptor:
                if field_descriptor.description:
                    field_schema["description"] = field_descriptor.description
                if field_descriptor.title:
                    field_schema["title"] = field_descriptor.title
                if field_descriptor.examples:
                    field_schema["examples"] = field_descriptor.examples
                if field_descriptor.deprecated:
                    field_schema["deprecated"] = True

                # Add constraints
                if field_descriptor.ge is not None:
                    field_schema["minimum"] = field_descriptor.ge
                if field_descriptor.le is not None:
                    field_schema["maximum"] = field_descriptor.le
                if field_descriptor.gt is not None:
                    field_schema["exclusiveMinimum"] = field_descriptor.gt
                if field_descriptor.lt is not None:
                    field_schema["exclusiveMaximum"] = field_descriptor.lt
                if field_descriptor.min_length is not None:
                    field_schema["minLength"] = field_descriptor.min_length
                if field_descriptor.max_length is not None:
                    field_schema["maxLength"] = field_descriptor.max_length
                if field_descriptor.pattern is not None:
                    field_schema["pattern"] = field_descriptor.pattern

                # Add UI hints
                ui_hints = {}
                if field_descriptor.widget:
                    ui_hints["widget"] = field_descriptor.widget
                if field_descriptor.group:
                    ui_hints["group"] = field_descriptor.group
                if field_descriptor.order is not None:
                    ui_hints["order"] = field_descriptor.order
                if field_descriptor.placeholder:
                    ui_hints["placeholder"] = field_descriptor.placeholder
                if field_descriptor.help_text:
                    ui_hints["help"] = field_descriptor.help_text

                if ui_hints:
                    field_schema["ui"] = ui_hints

                # Add default if not required
                if not field_descriptor.is_required:
                    field_schema["default"] = field_descriptor.default
                else:
                    schema["required"].append(field_name)
            else:
                # Check if has default value
                if hasattr(cls, field_name):
                    default_value = getattr(cls, field_name)
                    if not callable(default_value) and not isinstance(default_value, type):
                        field_schema["default"] = default_value
                    else:
                        schema["required"].append(field_name)
                else:
                    schema["required"].append(field_name)

            schema["properties"][field_name] = field_schema

    return schema


def _type_to_json_schema(python_type: Any) -> dict[str, Any]:
    """Convert Python type hint to JSON Schema."""
    origin = get_origin(python_type)
    args = get_args(python_type)

    # Handle None type
    if python_type is type(None):
        return {"type": "null"}

    # Handle Optional (Union with None)
    if origin is type(None) or (hasattr(python_type, "__origin__") and python_type.__origin__ is type(None)):
        return {"type": "null"}

    # Handle Union types (including Optional)
    if origin is type(python_type) or str(origin) == "typing.Union":
        if args:
            non_none_args = [arg for arg in args if arg is not type(None)]
            if len(non_none_args) == 1 and type(None) in args:
                # It's Optional[T]
                schema = _type_to_json_schema(non_none_args[0])
                # Make nullable
                if "type" in schema:
                    schema["type"] = [schema["type"], "null"]
                return schema
            else:
                # General Union - use anyOf
                return {"anyOf": [_type_to_json_schema(arg) for arg in args]}

    # Handle List
    if origin is list:
        if args:
            return {"type": "array", "items": _type_to_json_schema(args[0])}
        return {"type": "array"}

    # Handle Dict
    if origin is dict:
        if args and len(args) == 2:
            return {
                "type": "object",
                "additionalProperties": _type_to_json_schema(args[1])
            }
        return {"type": "object"}

    # Handle Enum
    if inspect.isclass(python_type) and issubclass(python_type, Enum):
        return {
            "type": "string",
            "enum": [e.value for e in python_type]
        }

    # Handle basic types
    type_map = {
        str: {"type": "string"},
        int: {"type": "integer"},
        float: {"type": "number"},
        bool: {"type": "boolean"},
        dict: {"type": "object"},
        list: {"type": "array"},
    }

    if python_type in type_map:
        return type_map[python_type]

    # Handle nested config schemas
    if hasattr(python_type, "_is_config_schema"):
        return python_type.config_schema()

    # Default: any type
    return {}


class BaseConfigSchema:
    """
    Base class for config schemas.
    Provides automatic config_schema() method.
    """

    @classmethod
    def config_schema(cls) -> dict[str, Any]:
        """Generate JSON Schema from this config class."""
        return _generate_schema_from_class(cls)

    @classmethod
    def sample_config(cls) -> dict[str, Any]:
        """Generate sample configuration."""
        config = {}
        type_hints = get_type_hints(cls) if hasattr(cls, "__annotations__") else {}

        for field_name in type_hints.keys():
            if hasattr(cls, field_name):
                attr = getattr(cls, field_name)
                if isinstance(attr, Field) and not attr.is_required:
                    config[field_name] = attr.default
                elif not callable(attr) and not isinstance(attr, type):
                    config[field_name] = attr

        return config


# Convenience type for annotations
ConfigSchema = Any
