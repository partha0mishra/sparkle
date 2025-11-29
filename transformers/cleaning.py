"""
Data cleaning transformers.

Pure functions for data cleansing, deduplication, null handling, etc.
"""

from typing import List, Optional, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, upper, lower, regexp_replace, when, coalesce,
    to_date, to_timestamp, lit, array_distinct, size, filter as array_filter
)
from .base import transformer


@transformer
def drop_exact_duplicates(df: DataFrame) -> DataFrame:
    """
    Drop exact duplicate rows.

    Sub-Group: Core Cleaning & Standardization
    Tags: deduplication, cleaning, duplicates

    Usage:
        df = df.transform(drop_exact_duplicates)
    """
    return df.dropDuplicates()


@transformer
def drop_duplicates_by_key(df: DataFrame, key_columns: List[str], keep: str = "first") -> DataFrame:
    """
    Drop duplicates based on key columns.

    Sub-Group: Core Cleaning & Standardization
    Tags: deduplication, cleaning, keys

    Args:
        df: Input DataFrame
        key_columns: Columns to use for deduplication
        keep: 'first' or 'last' (based on original row order)

    Usage:
        df = df.transform(drop_duplicates_by_key, key_columns=["id"], keep="last")
    """
    if keep == "first":
        return df.dropDuplicates(key_columns)
    elif keep == "last":
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number, desc, monotonically_increasing_id

        window = Window.partitionBy(*key_columns).orderBy(desc(monotonically_increasing_id()))
        return (
            df.withColumn("_rn", row_number().over(window))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )
    else:
        raise ValueError(f"keep must be 'first' or 'last', got '{keep}'")


@transformer
def standardize_nulls(df: DataFrame, null_values: Optional[List[str]] = None) -> DataFrame:
    """
    Replace various null representations with actual NULL.

    Sub-Group: Core Cleaning & Standardization
    Tags: nulls, cleaning, standardization

    Args:
        df: Input DataFrame
        null_values: List of values to treat as NULL (default: common variants)

    Usage:
        df = df.transform(standardize_nulls)
        df = df.transform(standardize_nulls, null_values=["N/A", "NONE"])
    """
    if null_values is None:
        null_values = ["", "NULL", "null", "Null", "N/A", "n/a", "NA", "None", "none", "#N/A"]

    for column in df.columns:
        condition = col(column).isin(null_values)
        df = df.withColumn(column, when(condition, lit(None)).otherwise(col(column)))

    return df


@transformer
def trim_and_clean_strings(df: DataFrame, columns: Optional[List[str]] = None) -> DataFrame:
    """
    Trim whitespace and clean string columns.

    Sub-Group: Core Cleaning & Standardization
    Tags: strings, cleaning, whitespace

    Args:
        df: Input DataFrame
        columns: Specific columns to clean (default: all string columns)

    Usage:
        df = df.transform(trim_and_clean_strings, columns=["name", "email"])
    """
    if columns is None:
        columns = [field.name for field in df.schema.fields if str(field.dataType) == "StringType"]

    for column in columns:
        if column in df.columns:
            df = df.withColumn(
                column,
                trim(regexp_replace(col(column), r'\s+', ' '))
            )

    return df


@transformer
def normalize_case(df: DataFrame, columns: List[str], case: str = "upper") -> DataFrame:
    """
    Normalize string case to upper/lower.

    Sub-Group: Core Cleaning & Standardization
    Tags: strings, case, normalization

    Args:
        df: Input DataFrame
        columns: Columns to normalize
        case: 'upper' or 'lower'

    Usage:
        df = df.transform(normalize_case, columns=["state"], case="upper")
    """
    case_func = upper if case == "upper" else lower

    for column in columns:
        if column in df.columns:
            df = df.withColumn(column, case_func(col(column)))

    return df


@transformer
def remove_special_characters(df: DataFrame, columns: List[str], replacement: str = "") -> DataFrame:
    """
    Remove special characters from string columns.

    Sub-Group: Core Cleaning & Standardization
    Tags: strings, cleaning, special-characters

    Args:
        df: Input DataFrame
        columns: Columns to clean
        replacement: What to replace special chars with (default: empty string)

    Usage:
        df = df.transform(remove_special_characters, columns=["phone"], replacement="")
    """
    pattern = r'[^a-zA-Z0-9\s]'

    for column in columns:
        if column in df.columns:
            df = df.withColumn(column, regexp_replace(col(column), pattern, replacement))

    return df


@transformer
def replace_values(df: DataFrame, replacements: Dict[str, Dict]) -> DataFrame:
    """
    Replace specific values in columns.

    Sub-Group: Core Cleaning & Standardization
    Tags: mapping, replacement, standardization

    Args:
        df: Input DataFrame
        replacements: Dict of {column: {old_value: new_value}}

    Usage:
        df = df.transform(replace_values, replacements={
            "status": {"Active": "A", "Inactive": "I"},
            "gender": {"M": "Male", "F": "Female"}
        })
    """
    for column, mapping in replacements.items():
        if column in df.columns:
            mapping_expr = None
            for old_val, new_val in mapping.items():
                if mapping_expr is None:
                    mapping_expr = when(col(column) == old_val, lit(new_val))
                else:
                    mapping_expr = mapping_expr.when(col(column) == old_val, lit(new_val))

            df = df.withColumn(column, mapping_expr.otherwise(col(column)))

    return df


@transformer
def fill_nulls(df: DataFrame, fill_values: Dict[str, any]) -> DataFrame:
    """
    Fill NULL values with specified defaults.

    Sub-Group: Core Cleaning & Standardization
    Tags: nulls, filling, defaults

    Args:
        df: Input DataFrame
        fill_values: Dict of {column: fill_value}

    Usage:
        df = df.transform(fill_nulls, fill_values={
            "quantity": 0,
            "status": "UNKNOWN"
        })
    """
    return df.fillna(fill_values)


@transformer
def drop_null_rows(df: DataFrame, columns: Optional[List[str]] = None, how: str = "any") -> DataFrame:
    """
    Drop rows with NULL values.

    Sub-Group: Core Cleaning & Standardization
    Tags: nulls, filtering, cleaning

    Args:
        df: Input DataFrame
        columns: Columns to check for NULLs (default: all)
        how: 'any' or 'all' - drop if any/all are NULL

    Usage:
        df = df.transform(drop_null_rows, columns=["id", "email"], how="any")
    """
    return df.dropna(how=how, subset=columns)


@transformer
def coalesce_columns(df: DataFrame, target_column: str, source_columns: List[str], drop_sources: bool = False) -> DataFrame:
    """
    Coalesce multiple columns into one (first non-null value).

    Sub-Group: Core Cleaning & Standardization
    Tags: nulls, coalesce, merging

    Args:
        df: Input DataFrame
        target_column: Name of output column
        source_columns: Columns to coalesce (in priority order)
        drop_sources: Whether to drop source columns after coalescing

    Usage:
        df = df.transform(coalesce_columns,
            target_column="email",
            source_columns=["primary_email", "secondary_email", "backup_email"],
            drop_sources=True
        )
    """
    df = df.withColumn(target_column, coalesce(*[col(c) for c in source_columns]))

    if drop_sources:
        df = df.drop(*source_columns)

    return df


@transformer
def filter_by_condition(df: DataFrame, condition: str) -> DataFrame:
    """
    Filter DataFrame by SQL-like condition.

    Sub-Group: Core Cleaning & Standardization
    Tags: filtering, condition, sql

    Args:
        df: Input DataFrame
        condition: SQL WHERE clause condition

    Usage:
        df = df.transform(filter_by_condition, condition="status = 'Active' AND age >= 18")
    """
    return df.filter(condition)


@transformer
def remove_outliers_iqr(df: DataFrame, column: str, multiplier: float = 1.5) -> DataFrame:
    """
    Remove outliers using IQR (Interquartile Range) method.

    Sub-Group: Core Cleaning & Standardization
    Tags: outliers, statistics, cleaning

    Args:
        df: Input DataFrame
        column: Numeric column to check for outliers
        multiplier: IQR multiplier (default 1.5 for standard outliers)

    Usage:
        df = df.transform(remove_outliers_iqr, column="price", multiplier=1.5)
    """
    quantiles = df.approxQuantile(column, [0.25, 0.75], 0.05)
    q1, q3 = quantiles[0], quantiles[1]
    iqr = q3 - q1

    lower_bound = q1 - (multiplier * iqr)
    upper_bound = q3 + (multiplier * iqr)

    return df.filter((col(column) >= lower_bound) & (col(column) <= upper_bound))


@transformer
def deduplicate_array_column(df: DataFrame, column: str) -> DataFrame:
    """
    Remove duplicates from array column.

    Sub-Group: Core Cleaning & Standardization
    Tags: arrays, deduplication, complex-types

    Args:
        df: Input DataFrame
        column: Array column to deduplicate

    Usage:
        df = df.transform(deduplicate_array_column, column="tags")
    """
    return df.withColumn(column, array_distinct(col(column)))


@transformer
def remove_empty_arrays(df: DataFrame, column: str) -> DataFrame:
    """
    Replace empty arrays with NULL.

    Sub-Group: Core Cleaning & Standardization
    Tags: arrays, nulls, complex-types

    Args:
        df: Input DataFrame
        column: Array column

    Usage:
        df = df.transform(remove_empty_arrays, column="tags")
    """
    return df.withColumn(
        column,
        when(size(col(column)) == 0, lit(None)).otherwise(col(column))
    )


@transformer
def clean_email_addresses(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Clean and validate email addresses.

    Sub-Group: Core Cleaning & Standardization
    Tags: email, validation, cleaning

    Args:
        df: Input DataFrame
        columns: Email columns to clean

    Usage:
        df = df.transform(clean_email_addresses, columns=["email", "backup_email"])
    """
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

    for column in columns:
        if column in df.columns:
            df = df.withColumn(
                column,
                when(col(column).rlike(email_pattern), lower(trim(col(column)))).otherwise(lit(None))
            )

    return df


@transformer
def clean_phone_numbers(df: DataFrame, columns: List[str], country_code: Optional[str] = None) -> DataFrame:
    """
    Clean phone numbers to standard format.

    Sub-Group: Core Cleaning & Standardization
    Tags: phone, validation, cleaning

    Args:
        df: Input DataFrame
        columns: Phone columns to clean
        country_code: Optional country code to prepend (e.g., "+1")

    Usage:
        df = df.transform(clean_phone_numbers, columns=["phone"], country_code="+1")
    """
    from pyspark.sql.functions import concat

    for column in columns:
        if column in df.columns:
            # Remove all non-digit characters
            df = df.withColumn(column, regexp_replace(col(column), r'\D', ''))

            # Add country code if specified
            if country_code:
                df = df.withColumn(
                    column,
                    when(col(column).isNotNull() & (col(column) != ''),
                         concat(lit(country_code), col(column))
                    ).otherwise(col(column))
                )

    return df


@transformer
def normalize_whitespace(df: DataFrame, columns: Optional[List[str]] = None) -> DataFrame:
    """
    Normalize whitespace in string columns (multiple spaces to single space).

    Sub-Group: Core Cleaning & Standardization
    Tags: strings, whitespace, normalization

    Args:
        df: Input DataFrame
        columns: Columns to normalize (default: all string columns)

    Usage:
        df = df.transform(normalize_whitespace, columns=["description", "notes"])
    """
    if columns is None:
        columns = [field.name for field in df.schema.fields if str(field.dataType) == "StringType"]

    for column in columns:
        if column in df.columns:
            df = df.withColumn(column, regexp_replace(trim(col(column)), r'\s+', ' '))

    return df


@transformer
def cap_numeric_values(df: DataFrame, column: str, min_value: Optional[float] = None,
                      max_value: Optional[float] = None) -> DataFrame:
    """
    Cap numeric values to min/max thresholds.

    Sub-Group: Core Cleaning & Standardization
    Tags: numeric, capping, outliers

    Args:
        df: Input DataFrame
        column: Numeric column to cap
        min_value: Minimum allowed value
        max_value: Maximum allowed value

    Usage:
        df = df.transform(cap_numeric_values, column="age", min_value=0, max_value=120)
        df = df.transform(cap_numeric_values, column="price", max_value=9999.99)
    """
    from pyspark.sql.functions import greatest, least

    if min_value is not None and max_value is not None:
        df = df.withColumn(column, least(greatest(col(column), lit(min_value)), lit(max_value)))
    elif min_value is not None:
        df = df.withColumn(column, greatest(col(column), lit(min_value)))
    elif max_value is not None:
        df = df.withColumn(column, least(col(column), lit(max_value)))

    return df
