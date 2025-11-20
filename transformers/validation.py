"""
Data validation transformers.

Functions for data quality checks and validation rules.
"""

from typing import List, Optional, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, length, regexp_extract, count
from .base import transformer, multi_output_transformer


@transformer
def add_validation_flags(df: DataFrame, rules: Dict[str, str], flag_column: str = "is_valid") -> DataFrame:
    """
    Add validation flag column based on rules.

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
        validation_expr = validation_expr & df.selectExpr(condition).first()[0]

    return df.withColumn(flag_column, validation_expr)


@transformer
def mark_invalid_rows(df: DataFrame, condition: str, invalid_column: str = "_is_invalid") -> DataFrame:
    """
    Mark rows that don't meet validation criteria.

    Usage:
        df = df.transform(mark_invalid_rows, condition="email IS NULL OR age < 0")
    """
    return df.withColumn(invalid_column, when(df.selectExpr(condition).columns[0], lit(True)).otherwise(lit(False)))


@multi_output_transformer
def split_valid_invalid(df: DataFrame, validation_column: str = "is_valid") -> Dict[str, DataFrame]:
    """
    Split DataFrame into valid and invalid records.

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
