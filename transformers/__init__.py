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
- DateTime: Date/time operations and conversions
- CDC: Change Data Capture operations
- Advanced: Complex patterns and advanced operations

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
from . import datetime
from . import cdc
from . import advanced

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
    clean_phone_numbers,
    normalize_whitespace,
    cap_numeric_values
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
    add_business_key,
    add_derived_flag,
    add_string_metrics,
    add_domain_from_email,
    add_name_components
)

from .validation import (
    add_validation_flags,
    mark_invalid_rows,
    split_valid_invalid,
    enforce_not_null,
    validate_length,
    validate_pattern,
    validate_range,
    validate_uniqueness,
    validate_referential_integrity,
    validate_enum_values,
    check_data_quality,
    validate_completeness
)

from .scd import (
    apply_scd_type1,
    apply_scd_type2,
    apply_scd_type3,
    close_scd_records,
    detect_scd_changes,
    merge_scd_type2,
    get_scd_current_records,
    get_scd_as_of_date
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
    cube_aggregation,
    add_cumulative_distribution
)

from .parsing import (
    parse_json_column,
    extract_json_field,
    explode_array_column,
    split_delimited_column,
    flatten_struct_column,
    parse_xml_column,
    extract_map_keys_values,
    explode_map_column,
    parse_url_parameters,
    flatten_nested_arrays,
    parse_csv_column,
    extract_regex_groups,
    json_to_struct_array,
    struct_to_json
)

from .pii import (
    mask_column,
    tokenize_column,
    redact_email,
    anonymize_with_lookup,
    remove_pii_columns
)

from .datetime import (
    parse_date_string,
    parse_timestamp_string,
    convert_unix_timestamp,
    format_datetime,
    extract_date_parts,
    add_days_to_date,
    add_months_to_date,
    calculate_date_diff,
    get_next_day_of_week,
    get_last_day_of_month,
    truncate_to_period,
    add_age_category
)

from .cdc import (
    detect_changes,
    apply_cdc_merge,
    add_cdc_metadata,
    apply_soft_delete,
    merge_incremental_updates,
    capture_before_after,
    apply_delta_updates,
    track_row_versions,
    identify_latest_records
)

from .advanced import (
    sample_data,
    unpivot_columns,
    create_sessionization,
    apply_conditional_logic,
    create_composite_key,
    broadcast_join_small_table,
    calculate_running_statistics,
    create_bins,
    create_array_column,
    create_map_column,
    rank_within_groups,
    calculate_percentiles,
    apply_lookup_with_fallback,
    calculate_percent_change
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
    "datetime",
    "cdc",
    "advanced",

    # Cleaning transformers (18)
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
    "normalize_whitespace",
    "cap_numeric_values",

    # Enrichment transformers (15)
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
    "add_derived_flag",
    "add_string_metrics",
    "add_domain_from_email",
    "add_name_components",

    # Validation transformers (12)
    "add_validation_flags",
    "mark_invalid_rows",
    "split_valid_invalid",
    "enforce_not_null",
    "validate_length",
    "validate_pattern",
    "validate_range",
    "validate_uniqueness",
    "validate_referential_integrity",
    "validate_enum_values",
    "check_data_quality",
    "validate_completeness",

    # SCD transformers (8)
    "apply_scd_type1",
    "apply_scd_type2",
    "apply_scd_type3",
    "close_scd_records",
    "detect_scd_changes",
    "merge_scd_type2",
    "get_scd_current_records",
    "get_scd_as_of_date",

    # Aggregation transformers (10)
    "aggregate_by_key",
    "pivot_table",
    "add_running_total",
    "add_moving_average",
    "add_lag_lead_columns",
    "add_percentile_rank",
    "add_ntile",
    "rollup_aggregation",
    "cube_aggregation",
    "add_cumulative_distribution",

    # Parsing transformers (14)
    "parse_json_column",
    "extract_json_field",
    "explode_array_column",
    "split_delimited_column",
    "flatten_struct_column",
    "parse_xml_column",
    "extract_map_keys_values",
    "explode_map_column",
    "parse_url_parameters",
    "flatten_nested_arrays",
    "parse_csv_column",
    "extract_regex_groups",
    "json_to_struct_array",
    "struct_to_json",

    # PII transformers (5)
    "mask_column",
    "tokenize_column",
    "redact_email",
    "anonymize_with_lookup",
    "remove_pii_columns",

    # DateTime transformers (12)
    "parse_date_string",
    "parse_timestamp_string",
    "convert_unix_timestamp",
    "format_datetime",
    "extract_date_parts",
    "add_days_to_date",
    "add_months_to_date",
    "calculate_date_diff",
    "get_next_day_of_week",
    "get_last_day_of_month",
    "truncate_to_period",
    "add_age_category",

    # CDC transformers (9)
    "detect_changes",
    "apply_cdc_merge",
    "add_cdc_metadata",
    "apply_soft_delete",
    "merge_incremental_updates",
    "capture_before_after",
    "apply_delta_updates",
    "track_row_versions",
    "identify_latest_records",

    # Advanced transformers (14)
    "sample_data",
    "unpivot_columns",
    "create_sessionization",
    "apply_conditional_logic",
    "create_composite_key",
    "broadcast_join_small_table",
    "calculate_running_statistics",
    "create_bins",
    "create_array_column",
    "create_map_column",
    "rank_within_groups",
    "calculate_percentiles",
    "apply_lookup_with_fallback",
    "calculate_percent_change",
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
        "cleaning": [name for name in dir(cleaning) if not name.startswith("_") and callable(getattr(cleaning, name))],
        "enrichment": [name for name in dir(enrichment) if not name.startswith("_") and callable(getattr(enrichment, name))],
        "validation": [name for name in dir(validation) if not name.startswith("_") and callable(getattr(validation, name))],
        "scd": [name for name in dir(scd) if not name.startswith("_") and callable(getattr(scd, name))],
        "aggregation": [name for name in dir(aggregation) if not name.startswith("_") and callable(getattr(aggregation, name))],
        "parsing": [name for name in dir(parsing) if not name.startswith("_") and callable(getattr(parsing, name))],
        "pii": [name for name in dir(pii) if not name.startswith("_") and callable(getattr(pii, name))],
        "datetime": [name for name in dir(datetime) if not name.startswith("_") and callable(getattr(datetime, name))],
        "cdc": [name for name in dir(cdc) if not name.startswith("_") and callable(getattr(cdc, name))],
        "advanced": [name for name in dir(advanced) if not name.startswith("_") and callable(getattr(advanced, name))]
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
