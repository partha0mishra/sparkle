"""
Sparkle Transformers - Pure, Stateless DataFrame Transformations

A comprehensive library of composable transformation functions organized into categories:
- Cleaning: Deduplication, null handling, string cleaning
- Enrichment: Derived columns, lookups, metadata addition
- Validation: Data quality checks and validation rules
- SCD: Slowly Changing Dimensions (Type 1, 2, 3)
- Aggregation: Grouping, pivoting, window functions
- Parsing: JSON/XML/nested structure handling
- PII: Masking, tokenization, anonymization

All transformers follow the pattern: (DataFrame, **kwargs) -> DataFrame
and can be chained using DataFrame.transform().

Usage:
    from sparkle.transformers import cleaning, enrichment, scd

    result = (df
        .transform(cleaning.drop_exact_duplicates)
        .transform(cleaning.standardize_nulls)
        .transform(cleaning.trim_and_clean_strings, columns=["name", "email"])
        .transform(enrichment.add_audit_columns, user="etl_process")
        .transform(scd.apply_scd_type2, key_columns=["customer_id"], compare_columns=["name", "email"])
    )

Or use the pipeline:
    from sparkle.transformers import TransformationPipeline
    from sparkle.transformers.cleaning import *

    pipeline = TransformationPipeline([
        (drop_exact_duplicates, {}),
        (standardize_nulls, {}),
        (trim_and_clean_strings, {"columns": ["name", "email"]})
    ])

    result = pipeline.apply(df)
"""

# Core utilities
from .base import (
    transformer,
    multi_output_transformer,
    TransformationPipeline,
    apply_transformers,
    get_transformation_metadata,
    validate_required_columns,
    safe_transform,
    ConditionalTransformer
)

# Import all transformer modules
from . import cleaning
from . import enrichment
from . import validation
from . import scd
from . import aggregation
from . import parsing
from . import pii

# Import commonly used transformers for direct access
from .cleaning import (
    drop_exact_duplicates,
    drop_duplicates_by_key,
    standardize_nulls,
    trim_and_clean_strings,
    normalize_case,
    remove_special_characters,
    replace_values,
    fill_nulls,
    drop_null_rows,
    coalesce_columns,
    filter_by_condition,
    remove_outliers_iqr,
    deduplicate_array_column,
    remove_empty_arrays,
    clean_email_addresses,
    clean_phone_numbers
)

from .enrichment import (
    add_audit_columns,
    add_ingestion_metadata,
    add_surrogate_key,
    add_hash_key,
    add_date_components,
    add_row_number,
    add_rank,
    perform_lookup,
    add_age_from_birthdate,
    add_tenure_months,
    add_business_key
)

from .validation import (
    add_validation_flags,
    mark_invalid_rows,
    split_valid_invalid,
    enforce_not_null,
    validate_length,
    validate_pattern,
    validate_range
)

from .scd import (
    apply_scd_type1,
    apply_scd_type2,
    apply_scd_type3,
    close_scd_records
)

from .aggregation import (
    aggregate_by_key,
    pivot_table,
    add_running_total,
    add_moving_average,
    add_lag_lead_columns,
    add_percentile_rank,
    add_ntile,
    rollup_aggregation,
    cube_aggregation
)

from .parsing import (
    parse_json_column,
    extract_json_field,
    explode_array_column,
    split_delimited_column,
    flatten_struct_column
)

from .pii import (
    mask_column,
    tokenize_column,
    redact_email,
    anonymize_with_lookup,
    remove_pii_columns
)


__all__ = [
    # Core utilities
    "transformer",
    "multi_output_transformer",
    "TransformationPipeline",
    "apply_transformers",
    "get_transformation_metadata",
    "validate_required_columns",
    "safe_transform",
    "ConditionalTransformer",

    # Modules
    "cleaning",
    "enrichment",
    "validation",
    "scd",
    "aggregation",
    "parsing",
    "pii",

    # Cleaning transformers
    "drop_exact_duplicates",
    "drop_duplicates_by_key",
    "standardize_nulls",
    "trim_and_clean_strings",
    "normalize_case",
    "remove_special_characters",
    "replace_values",
    "fill_nulls",
    "drop_null_rows",
    "coalesce_columns",
    "filter_by_condition",
    "remove_outliers_iqr",
    "deduplicate_array_column",
    "remove_empty_arrays",
    "clean_email_addresses",
    "clean_phone_numbers",

    # Enrichment transformers
    "add_audit_columns",
    "add_ingestion_metadata",
    "add_surrogate_key",
    "add_hash_key",
    "add_date_components",
    "add_row_number",
    "add_rank",
    "perform_lookup",
    "add_age_from_birthdate",
    "add_tenure_months",
    "add_business_key",

    # Validation transformers
    "add_validation_flags",
    "mark_invalid_rows",
    "split_valid_invalid",
    "enforce_not_null",
    "validate_length",
    "validate_pattern",
    "validate_range",

    # SCD transformers
    "apply_scd_type1",
    "apply_scd_type2",
    "apply_scd_type3",
    "close_scd_records",

    # Aggregation transformers
    "aggregate_by_key",
    "pivot_table",
    "add_running_total",
    "add_moving_average",
    "add_lag_lead_columns",
    "add_percentile_rank",
    "add_ntile",
    "rollup_aggregation",
    "cube_aggregation",

    # Parsing transformers
    "parse_json_column",
    "extract_json_field",
    "explode_array_column",
    "split_delimited_column",
    "flatten_struct_column",

    # PII transformers
    "mask_column",
    "tokenize_column",
    "redact_email",
    "anonymize_with_lookup",
    "remove_pii_columns",
]


# Version
__version__ = "1.0.0"


def list_all_transformers():
    """
    List all available transformer functions.

    Returns:
        Dict of {category: [transformer_names]}

    Example:
        >>> from sparkle.transformers import list_all_transformers
        >>> transformers = list_all_transformers()
        >>> print(transformers["cleaning"])
    """
    return {
        "cleaning": [name for name in dir(cleaning) if not name.startswith("_")],
        "enrichment": [name for name in dir(enrichment) if not name.startswith("_")],
        "validation": [name for name in dir(validation) if not name.startswith("_")],
        "scd": [name for name in dir(scd) if not name.startswith("_")],
        "aggregation": [name for name in dir(aggregation) if not name.startswith("_")],
        "parsing": [name for name in dir(parsing) if not name.startswith("_")],
        "pii": [name for name in dir(pii) if not name.startswith("_")]
    }


def get_transformer_help(transformer_func):
    """
    Get help for a specific transformer function.

    Args:
        transformer_func: Transformer function

    Returns:
        Docstring of the transformer

    Example:
        >>> from sparkle.transformers import get_transformer_help, drop_exact_duplicates
        >>> print(get_transformer_help(drop_exact_duplicates))
    """
    return transformer_func.__doc__ or "No documentation available"
