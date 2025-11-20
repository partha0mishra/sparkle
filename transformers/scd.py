"""
Slowly Changing Dimension (SCD) transformers.

Implements Type 1, Type 2, and Type 3 SCDs.
"""

from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, when, lag, lead, coalesce
from pyspark.sql.window import Window
from .base import transformer


@transformer
def apply_scd_type1(df: DataFrame, target_df: DataFrame, key_columns: List[str], update_columns: List[str]) -> DataFrame:
    """
    Apply SCD Type 1 (overwrite) logic.

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
    from pyspark.sql.functions import row_number, max as spark_max

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
