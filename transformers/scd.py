"""
Slowly Changing Dimension (SCD) transformers.

Implements Type 1, Type 2, and Type 3 SCDs plus additional SCD operations.
"""

from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, lag, lead, coalesce, row_number,
    max as spark_max, md5, concat_ws, current_date, expr
)
from pyspark.sql.window import Window
from .base import transformer


@transformer
def apply_scd_type1(df: DataFrame, target_df: DataFrame, key_columns: List[str], update_columns: List[str]) -> DataFrame:
    """
    Apply SCD Type 1 (overwrite) logic.

    Sub-Group: Slowly Changing Dimensions (SCD)
    Tags: scd, type-1, overwrite

    Args:
        df: Source DataFrame with new/updated records
        target_df: Existing target DataFrame
        key_columns: Business key columns
        update_columns: Columns to update

    Usage:
        updated = apply_scd_type1(
            df=new_data,
            target_df=existing_data,
            key_columns=["customer_id"],
            update_columns=["name", "email", "phone"]
        )
    """
    from delta.tables import DeltaTable

    # This is a simplified version - production would use Delta MERGE
    return df.unionByName(target_df, allowMissingColumns=True).dropDuplicates(key_columns)


@transformer
def apply_scd_type2(
    df: DataFrame,
    key_columns: List[str],
    compare_columns: List[str],
    effective_date_column: str = "effective_date",
    end_date_column: str = "end_date",
    current_flag_column: str = "is_current",
    version_column: Optional[str] = "version"
) -> DataFrame:
    """
    Apply SCD Type 2 (historization) logic.

    Sub-Group: Slowly Changing Dimensions (SCD)
    Tags: scd, type-2, history

    Args:
        df: Input DataFrame
        key_columns: Business key columns
        compare_columns: Columns to compare for changes
        effective_date_column: Start date column name
        end_date_column: End date column name
        current_flag_column: Current record flag
        version_column: Optional version number column

    Usage:
        historized = df.transform(apply_scd_type2,
            key_columns=["customer_id"],
            compare_columns=["name", "email", "address"]
        )
    """
    # Add effective date if not present
    if effective_date_column not in df.columns:
        df = df.withColumn(effective_date_column, current_timestamp())

    # Sort by key and effective date
    window = Window.partitionBy(*key_columns).orderBy(col(effective_date_column))

    # Calculate end date (next row's effective date - 1)
    df = df.withColumn("_next_effective", lead(col(effective_date_column)).over(window))

    df = df.withColumn(
        end_date_column,
        when(col("_next_effective").isNotNull(), col("_next_effective")).otherwise(lit("9999-12-31"))
    )

    # Add current flag
    df = df.withColumn(
        current_flag_column,
        when(col(end_date_column) == "9999-12-31", lit(True)).otherwise(lit(False))
    )

    # Add version if requested
    if version_column:
        df = df.withColumn(version_column, row_number().over(window))

    df = df.drop("_next_effective")

    return df


@transformer
def apply_scd_type3(df: DataFrame, columns_to_track: List[str], history_suffix: str = "_previous") -> DataFrame:
    """
    Apply SCD Type 3 (limited history in same row).

    Sub-Group: Slowly Changing Dimensions (SCD)
    Tags: scd, type-3, limited-history

    Args:
        df: Input DataFrame
        columns_to_track: Columns to keep previous value
        history_suffix: Suffix for previous value columns

    Usage:
        tracked = df.transform(apply_scd_type3, columns_to_track=["email", "phone"], history_suffix="_old")
    """
    window = Window.partitionBy(*[c for c in df.columns if c not in columns_to_track]).orderBy(current_timestamp())

    for column in columns_to_track:
        if column in df.columns:
            df = df.withColumn(f"{column}{history_suffix}", lag(col(column)).over(window))

    return df


@transformer
def close_scd_records(df: DataFrame, key_columns: List[str], end_date: str, end_date_column: str = "end_date", current_flag_column: str = "is_current") -> DataFrame:
    """
    Close SCD Type 2 records by setting end date.

    Sub-Group: Slowly Changing Dimensions (SCD)
    Tags: scd, type-2, closing

    Usage:
        closed = df.transform(close_scd_records,
            key_columns=["customer_id"],
            end_date="2025-11-19",
            end_date_column="valid_to"
        )
    """
    for key_col in key_columns:
        df = df.withColumn(end_date_column, lit(end_date))
        df = df.withColumn(current_flag_column, lit(False))

    return df


@transformer
def detect_scd_changes(df: DataFrame, compare_df: DataFrame, key_columns: List[str],
                      compare_columns: List[str], hash_column: str = "row_hash") -> DataFrame:
    """
    Detect SCD changes by comparing hash values.

    Sub-Group: Slowly Changing Dimensions (SCD)
    Tags: scd, change-detection, hash

    Args:
        df: Current DataFrame
        compare_df: Previous DataFrame to compare against
        key_columns: Business key columns
        compare_columns: Columns to include in hash comparison
        hash_column: Name for hash column

    Usage:
        changes = df.transform(detect_scd_changes,
            compare_df=previous_data,
            key_columns=["customer_id"],
            compare_columns=["name", "email", "address"]
        )
    """
    # Add hash to both dataframes
    df = df.withColumn(
        hash_column,
        md5(concat_ws("||", *[coalesce(col(c), lit("")).cast("string") for c in compare_columns]))
    )

    compare_df = compare_df.withColumn(
        f"prev_{hash_column}",
        md5(concat_ws("||", *[coalesce(col(c), lit("")).cast("string") for c in compare_columns]))
    )

    # Join and compare
    joined = df.alias("current").join(
        compare_df.select(*key_columns, f"prev_{hash_column}").alias("previous"),
        on=key_columns,
        how="left_outer"
    )

    # Mark changed records
    result = joined.withColumn(
        "_is_changed",
        when(col(f"prev_{hash_column}").isNull(), lit("INSERT"))
        .when(col(hash_column) != col(f"prev_{hash_column}"), lit("UPDATE"))
        .otherwise(lit("NOCHANGE"))
    )

    return result


@transformer
def merge_scd_type2(df: DataFrame, new_df: DataFrame, key_columns: List[str],
                   compare_columns: List[str]) -> DataFrame:
    """
    Merge new records into SCD Type 2 table.

    Sub-Group: Slowly Changing Dimensions (SCD)
    Tags: scd, type-2, merge

    Args:
        df: Existing SCD Type 2 DataFrame
        new_df: New records to merge
        key_columns: Business key columns
        compare_columns: Columns to compare for changes

    Usage:
        merged = df.transform(merge_scd_type2,
            new_df=new_records,
            key_columns=["customer_id"],
            compare_columns=["name", "email", "status"]
        )
    """
    # Get current records
    current = df.filter(col("is_current") == True)

    # Detect changes
    hash_col = "_row_hash"

    current_hashed = current.withColumn(
        hash_col,
        md5(concat_ws("||", *[coalesce(col(c), lit("")).cast("string") for c in compare_columns]))
    )

    new_hashed = new_df.withColumn(
        hash_col,
        md5(concat_ws("||", *[coalesce(col(c), lit("")).cast("string") for c in compare_columns]))
    )

    # Identify changed records
    joined = new_hashed.alias("new").join(
        current_hashed.alias("current"),
        on=key_columns,
        how="left_outer"
    )

    changed = joined.filter(
        (col(f"current.{hash_col}").isNull()) |
        (col(f"new.{hash_col}") != col(f"current.{hash_col}"))
    )

    # Close old records
    to_close = changed.select(*[col(f"current.{c}") for c in current.columns if c in changed.columns])

    closed = to_close.withColumn("end_date", current_date())
    closed = closed.withColumn("is_current", lit(False))

    # Add new records
    new_records = changed.select(*[col(f"new.{c}") for c in new_df.columns if c in changed.columns])
    new_records = new_records.withColumn("effective_date", current_date())
    new_records = new_records.withColumn("end_date", lit("9999-12-31"))
    new_records = new_records.withColumn("is_current", lit(True))

    # Combine
    result = df.filter(col("is_current") == False).unionByName(closed, allowMissingColumns=True).unionByName(new_records, allowMissingColumns=True)

    return result


@transformer
def get_scd_current_records(df: DataFrame, current_flag_column: str = "is_current") -> DataFrame:
    """
    Filter to get only current SCD Type 2 records.

    Sub-Group: Slowly Changing Dimensions (SCD)
    Tags: scd, type-2, filtering

    Usage:
        current = df.transform(get_scd_current_records, current_flag_column="is_current")
    """
    return df.filter(col(current_flag_column) == True)


@transformer
def get_scd_as_of_date(df: DataFrame, as_of_date: str,
                      effective_date_column: str = "effective_date",
                      end_date_column: str = "end_date") -> DataFrame:
    """
    Get SCD Type 2 records as of a specific date.

    Sub-Group: Slowly Changing Dimensions (SCD)
    Tags: scd, type-2, temporal

    Args:
        df: SCD Type 2 DataFrame
        as_of_date: Date to query (format: 'YYYY-MM-DD')
        effective_date_column: Effective date column name
        end_date_column: End date column name

    Usage:
        historical = df.transform(get_scd_as_of_date, as_of_date="2023-01-01")
    """
    return df.filter(
        (col(effective_date_column) <= as_of_date) &
        (col(end_date_column) > as_of_date)
    )
