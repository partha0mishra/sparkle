"""
Data validation transformers.

Functions for data quality checks and validation rules.
"""

from typing import List, Optional, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, length, regexp_extract, count, expr, sum as spark_sum
from .base import transformer, multi_output_transformer


@transformer
def add_validation_flags(df: DataFrame, rules: Dict[str, str], flag_column: str = "is_valid") -> DataFrame:
    """
    Add validation flag column based on rules.

    Sub-Group: Quality & Validation
    Tags: validation, quality, rules

    Args:
        df: Input DataFrame
        rules: Dict of {rule_name: SQL condition}
        flag_column: Name of validation flag column

    Usage:
        df = df.transform(add_validation_flags, rules={
            "valid_email": "email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'",
            "valid_age": "age BETWEEN 0 AND 120"
        })
    """
    validation_expr = lit(True)

    for rule_name, condition in rules.items():
        validation_expr = validation_expr & expr(condition)

    return df.withColumn(flag_column, validation_expr)


@transformer
def mark_invalid_rows(df: DataFrame, condition: str, invalid_column: str = "_is_invalid") -> DataFrame:
    """
    Mark rows that don't meet validation criteria.

    Sub-Group: Quality & Validation
    Tags: validation, flagging, quality

    Usage:
        df = df.transform(mark_invalid_rows, condition="email IS NULL OR age < 0")
    """
    return df.withColumn(invalid_column, expr(condition))


@multi_output_transformer
def split_valid_invalid(df: DataFrame, validation_column: str = "is_valid") -> Dict[str, DataFrame]:
    """
    Split DataFrame into valid and invalid records.

    Sub-Group: Quality & Validation
    Tags: validation, splitting, quality

    Usage:
        result = split_valid_invalid(df, validation_column="is_valid")
        valid_df = result["valid"]
        invalid_df = result["invalid"]
    """
    return {
        "valid": df.filter(col(validation_column) == True),
        "invalid": df.filter(col(validation_column) == False)
    }


@transformer
def enforce_not_null(df: DataFrame, columns: List[str], default_value: Optional[any] = "UNKNOWN") -> DataFrame:
    """
    Enforce NOT NULL constraint by filling with default.

    Sub-Group: Quality & Validation
    Tags: nulls, constraints, enforcement

    Usage:
        df = df.transform(enforce_not_null, columns=["customer_id", "product_id"], default_value="MISSING")
    """
    for column in columns:
        if column in df.columns:
            df = df.withColumn(column, when(col(column).isNull(), lit(default_value)).otherwise(col(column)))

    return df


@transformer
def validate_length(df: DataFrame, column: str, min_length: Optional[int] = None, max_length: Optional[int] = None, flag_column: Optional[str] = None) -> DataFrame:
    """
    Validate string length.

    Sub-Group: Quality & Validation
    Tags: validation, length, strings

    Usage:
        df = df.transform(validate_length, column="phone", min_length=10, max_length=15, flag_column="valid_phone")
    """
    condition = lit(True)

    if min_length is not None:
        condition = condition & (length(col(column)) >= min_length)

    if max_length is not None:
        condition = condition & (length(col(column)) <= max_length)

    if flag_column:
        return df.withColumn(flag_column, condition)
    else:
        return df.filter(condition)


@transformer
def validate_pattern(df: DataFrame, column: str, pattern: str, flag_column: Optional[str] = None) -> DataFrame:
    """
    Validate column matches regex pattern.

    Sub-Group: Quality & Validation
    Tags: validation, regex, patterns

    Usage:
        df = df.transform(validate_pattern, column="email", pattern=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', flag_column="valid_email")
    """
    condition = col(column).rlike(pattern)

    if flag_column:
        return df.withColumn(flag_column, condition)
    else:
        return df.filter(condition)


@transformer
def validate_range(df: DataFrame, column: str, min_value: Optional[float] = None, max_value: Optional[float] = None, flag_column: Optional[str] = None) -> DataFrame:
    """
    Validate numeric column is within range.

    Sub-Group: Quality & Validation
    Tags: validation, range, numeric

    Usage:
        df = df.transform(validate_range, column="age", min_value=0, max_value=120, flag_column="valid_age")
    """
    condition = lit(True)

    if min_value is not None:
        condition = condition & (col(column) >= min_value)

    if max_value is not None:
        condition = condition & (col(column) <= max_value)

    if flag_column:
        return df.withColumn(flag_column, condition)
    else:
        return df.filter(condition)


@transformer
def validate_uniqueness(df: DataFrame, columns: List[str], flag_column: str = "is_unique") -> DataFrame:
    """
    Flag rows that are unique based on specified columns.

    Sub-Group: Quality & Validation
    Tags: validation, uniqueness, duplicates

    Args:
        df: Input DataFrame
        columns: Columns to check for uniqueness
        flag_column: Name for uniqueness flag column

    Usage:
        df = df.transform(validate_uniqueness, columns=["customer_id", "order_id"], flag_column="is_unique_order")
    """
    from pyspark.sql.window import Window

    window = Window.partitionBy(*columns)

    return df.withColumn(
        flag_column,
        when(count(lit(1)).over(window) == 1, lit(True)).otherwise(lit(False))
    )


@transformer
def validate_referential_integrity(df: DataFrame, reference_df: DataFrame, key_columns: List[str],
                                   flag_column: str = "has_reference") -> DataFrame:
    """
    Validate referential integrity against reference table.

    Sub-Group: Quality & Validation
    Tags: validation, referential-integrity, foreign-keys

    Args:
        df: Input DataFrame
        reference_df: Reference DataFrame
        key_columns: Key columns to check
        flag_column: Name for flag column

    Usage:
        df = df.transform(validate_referential_integrity,
            reference_df=customer_master,
            key_columns=["customer_id"],
            flag_column="valid_customer"
        )
    """
    # Get distinct keys from reference
    ref_keys = reference_df.select(*key_columns).distinct()

    # Left join and check if match exists
    joined = df.join(ref_keys, on=key_columns, how="left_semi")

    # Mark rows with valid references
    valid_keys = joined.select(*key_columns).distinct()

    result = df.join(
        valid_keys.withColumn(flag_column, lit(True)),
        on=key_columns,
        how="left"
    )

    result = result.withColumn(
        flag_column,
        when(col(flag_column).isNull(), lit(False)).otherwise(lit(True))
    )

    return result


@transformer
def validate_enum_values(df: DataFrame, column: str, allowed_values: List[any],
                        flag_column: Optional[str] = None) -> DataFrame:
    """
    Validate column contains only allowed enum values.

    Sub-Group: Quality & Validation
    Tags: validation, enum, allowed-values

    Args:
        df: Input DataFrame
        column: Column to validate
        allowed_values: List of allowed values
        flag_column: Name for flag column (if None, filters invalid rows)

    Usage:
        df = df.transform(validate_enum_values,
            column="status",
            allowed_values=["Active", "Inactive", "Pending"],
            flag_column="valid_status"
        )
    """
    condition = col(column).isin(allowed_values)

    if flag_column:
        return df.withColumn(flag_column, condition)
    else:
        return df.filter(condition)


@transformer
def check_data_quality(df: DataFrame, rules: Dict[str, str], summary_column: str = "quality_score") -> DataFrame:
    """
    Calculate data quality score based on multiple rules.

    Sub-Group: Quality & Validation
    Tags: quality, scoring, metrics

    Args:
        df: Input DataFrame
        rules: Dict of {rule_name: SQL condition}
        summary_column: Name for quality score column (0.0 to 1.0)

    Usage:
        df = df.transform(check_data_quality, rules={
            "has_email": "email IS NOT NULL",
            "has_phone": "phone IS NOT NULL",
            "valid_age": "age BETWEEN 0 AND 120"
        })
    """
    total_rules = len(rules)

    if total_rules == 0:
        return df.withColumn(summary_column, lit(1.0))

    # Add individual rule columns
    for rule_name, condition in rules.items():
        df = df.withColumn(f"_rule_{rule_name}", when(expr(condition), lit(1)).otherwise(lit(0)))

    # Calculate score
    rule_columns = [f"_rule_{rule_name}" for rule_name in rules.keys()]
    score_expr = (spark_sum([col(c) for c in rule_columns]) / total_rules).cast("double")

    df = df.withColumn(summary_column, score_expr)

    # Drop temporary rule columns
    df = df.drop(*rule_columns)

    return df


@transformer
def validate_completeness(df: DataFrame, required_columns: List[str], threshold: float = 1.0,
                         flag_column: str = "is_complete") -> DataFrame:
    """
    Validate row completeness (percentage of non-null required columns).

    Sub-Group: Quality & Validation
    Tags: validation, completeness, nulls

    Args:
        df: Input DataFrame
        required_columns: Columns that should be non-null
        threshold: Minimum completeness threshold (0.0 to 1.0)
        flag_column: Name for completeness flag column

    Usage:
        df = df.transform(validate_completeness,
            required_columns=["name", "email", "phone", "address"],
            threshold=0.75,
            flag_column="is_sufficient"
        )
    """
    total_cols = len(required_columns)

    if total_cols == 0:
        return df.withColumn(flag_column, lit(True))

    # Count non-null columns
    non_null_count = spark_sum([
        when(col(c).isNotNull(), lit(1)).otherwise(lit(0))
        for c in required_columns
    ])

    completeness = (non_null_count / total_cols).cast("double")

    return df.withColumn(
        flag_column,
        when(completeness >= threshold, lit(True)).otherwise(lit(False))
    )
