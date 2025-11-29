from typing import Callable, Any, Dict
import importlib
from pyspark.sql import DataFrame

def get_transformer(name: str) -> Callable[[DataFrame, ...], DataFrame]:
    """
    Get a transformer function by name.
    Searches in all transformer submodules.
    """
    modules = [
        "sparkle.transformers.cleaning",
        "sparkle.transformers.enrichment",
        "sparkle.transformers.validation",
        "sparkle.transformers.scd",
        "sparkle.transformers.aggregation",
        "sparkle.transformers.parsing",
        "sparkle.transformers.pii",
        "sparkle.transformers.datetime",
        "sparkle.transformers.cdc",
        "sparkle.transformers.advanced"
    ]
    
    # Try to find the function in any of the modules
    for module_name in modules:
        try:
            # Handle import without 'sparkle.' prefix if running from inside
            if module_name.startswith("sparkle."):
                short_name = module_name.split(".", 1)[1]
                try:
                    module = importlib.import_module(module_name)
                except ImportError:
                    try:
                        module = importlib.import_module(short_name)
                    except ImportError:
                         # Try relative import if possible, or skip
                         continue
            else:
                module = importlib.import_module(module_name)
                
            if hasattr(module, name):
                return getattr(module, name)
        except ImportError:
            continue
            
    raise ValueError(f"Transformer '{name}' not found in any module")
