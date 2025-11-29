"""
Change Data Capture (CDC) transformers.

Functions for applying CDC operations, merge logic, and change tracking.
"""

from typing import List, Optional, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, md5, sha2, concat_ws,
    row_number, max as spark_max, coalesce
)
from pyspark.sql.window import Window
from .base import transformer


@transformer
def detect_changes(df: DataFrame, source_df: DataFrame, key_columns: List[str],
                  compare_columns: List[str], change_type_column: str = "change_type") -> DataFrame:
    """
    Detect changes between source and target datasets.

    Sub-Group: Change Data Capture Application
    Tags: cdc, change-detection, comparison

    Args:
        df: Target DataFrame (current state)
        source_df: Source DataFrame (new data)
        key_columns: Business key columns
        compare_columns: Columns to compare for changes
        change_type_column: Name for change type column (INSERT, UPDATE, DELETE, NOCHANGE)

    Usage:
        changes = df.transform(detect_changes,
            source_df=new_data,
            key_columns=["customer_id"],
            compare_columns=["name", "email", "status"]
        )
    """
    from pyspark.sql.functions import concat_ws, md5

    # Add hash columns for comparison
    df_with_hash = df.withColumn(
        "_target_hash",
        md5(concat_ws("||", *[coalesce(col(c), lit("")).cast("string") for c in compare_columns]))
    )

    source_with_hash = source_df.withColumn(
        "_source_hash",
        md5(concat_ws("||", *[coalesce(col(c), lit("")).cast("string") for c in compare_columns]))
    )

    # Full outer join on keys
    joined = source_with_hash.alias("source").join(
        df_with_hash.alias("target"),
        on=key_columns,
        how="full_outer"
    )

    # Determine change type
    result = joined.withColumn(
        change_type_column,
        when(col("target._target_hash").isNull(), lit("INSERT"))
        .when(col("source._source_hash").isNull(), lit("DELETE"))
        .when(col("source._source_hash") != col("target._target_hash"), lit("UPDATE"))
        .otherwise(lit("NOCHANGE"))
    )

    # Select source columns and change type
    select_cols = [col(f"source.{c}").alias(c) for c in source_df.columns] + [col(change_type_column)]
    result = result.select(*select_cols)

    return result


@transformer
def apply_cdc_merge(df: DataFrame, source_df: DataFrame, key_columns: List[str],
                   update_columns: Optional[List[str]] = None, delete_flag: bool = False) -> DataFrame:
    """
    Apply CDC merge logic (upsert + optional delete).

    Sub-Group: Change Data Capture Application
    Tags: cdc, merge, upsert

    Args:
        df: Target DataFrame
        source_df: Source DataFrame with changes
        key_columns: Business key columns
        update_columns: Columns to update (default: all non-key columns)
        delete_flag: If True, expects '_is_deleted' column in source

    Usage:
        merged = df.transform(apply_cdc_merge,
            source_df=cdc_data,
            key_columns=["customer_id"],
            update_columns=["name", "email", "status"]
        )
    """
    if update_columns is None:
        update_columns = [c for c in source_df.columns if c not in key_columns]

    # Add metadata
    source_df = source_df.withColumn("_cdc_timestamp", current_timestamp())

    if delete_flag:
        # Filter out deletes
        source_df = source_df.filter(col("_is_deleted") == False)

    # Use unionByName and dropDuplicates (simplified merge)
    # In production, use Delta Lake MERGE
    merged = source_df.unionByName(df, allowMissingColumns=True)

    # Keep latest record based on CDC timestamp
    window = Window.partitionBy(*key_columns).orderBy(col("_cdc_timestamp").desc())

    result = (
        merged
        .withColumn("_row_num", row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    return result


@transformer
def add_cdc_metadata(df: DataFrame, operation: str = "INSERT",
                    source_system: Optional[str] = None) -> DataFrame:
    """
    Add CDC metadata columns to track changes.

    Sub-Group: Change Data Capture Application
    Tags: cdc, metadata, tracking

    Args:
        df: Input DataFrame
        operation: CDC operation type (INSERT, UPDATE, DELETE)
        source_system: Source system name

    Usage:
        df = df.transform(add_cdc_metadata, operation="INSERT", source_system="salesforce")
    """
    result = df.withColumn("_cdc_operation", lit(operation))
    result = result.withColumn("_cdc_timestamp", current_timestamp())

    if source_system:
        result = result.withColumn("_cdc_source", lit(source_system))

    return result


@transformer
def apply_soft_delete(df: DataFrame, delete_keys: List[Dict], key_columns: List[str],
                     deleted_flag_column: str = "is_deleted",
                     deleted_at_column: str = "deleted_at") -> DataFrame:
    """
    Apply soft delete by marking records as deleted.

    Sub-Group: Change Data Capture Application
    Tags: cdc, soft-delete, tombstone

    Args:
        df: Input DataFrame
        delete_keys: List of dicts with key values to delete
        key_columns: Business key columns
        deleted_flag_column: Name of deleted flag column
        deleted_at_column: Name of deleted timestamp column

    Usage:
        df = df.transform(apply_soft_delete,
            delete_keys=[{"customer_id": "C001"}, {"customer_id": "C002"}],
            key_columns=["customer_id"]
        )
    """
    # Build condition for keys to delete
    delete_condition = None

    for key_dict in delete_keys:
        key_condition = None
        for key_col, key_val in key_dict.items():
            if key_col in key_columns:
                condition = (col(key_col) == key_val)
                key_condition = condition if key_condition is None else key_condition & condition

        if delete_condition is None:
            delete_condition = key_condition
        else:
            delete_condition = delete_condition | key_condition

    # Add deleted flag if not exists
    if deleted_flag_column not in df.columns:
        df = df.withColumn(deleted_flag_column, lit(False))

    if deleted_at_column not in df.columns:
        df = df.withColumn(deleted_at_column, lit(None).cast("timestamp"))

    # Mark records as deleted
    result = df.withColumn(
        deleted_flag_column,
        when(delete_condition, lit(True)).otherwise(col(deleted_flag_column))
    )

    result = result.withColumn(
        deleted_at_column,
        when(delete_condition, current_timestamp()).otherwise(col(deleted_at_column))
    )

    return result


@transformer
def merge_incremental_updates(df: DataFrame, incremental_df: DataFrame,
                              key_columns: List[str], timestamp_column: str) -> DataFrame:
    """
    Merge incremental updates keeping latest by timestamp.

    Sub-Group: Change Data Capture Application
    Tags: cdc, incremental, merge

    Args:
        df: Base DataFrame
        incremental_df: Incremental updates
        key_columns: Business key columns
        timestamp_column: Timestamp column to determine latest

    Usage:
        merged = df.transform(merge_incremental_updates,
            incremental_df=updates,
            key_columns=["order_id"],
            timestamp_column="updated_at"
        )
    """
    # Union and keep latest
    combined = df.unionByName(incremental_df, allowMissingColumns=True)

    window = Window.partitionBy(*key_columns).orderBy(col(timestamp_column).desc())

    result = (
        combined
        .withColumn("_row_num", row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    return result


@transformer
def capture_before_after(df: DataFrame, updated_df: DataFrame, key_columns: List[str]) -> DataFrame:
    """
    Capture before/after state of updated records.

    Sub-Group: Change Data Capture Application
    Tags: cdc, audit, before-after

    Args:
        df: Before state (current data)
        updated_df: After state (updated data)
        key_columns: Business key columns

    Returns:
        DataFrame with before_* and after_* columns

    Usage:
        audit = df.transform(capture_before_after,
            updated_df=new_data,
            key_columns=["customer_id"]
        )
    """
    # Rename columns
    before_df = df.select(
        *key_columns,
        *[col(c).alias(f"before_{c}") for c in df.columns if c not in key_columns]
    )

    after_df = updated_df.select(
        *key_columns,
        *[col(c).alias(f"after_{c}") for c in updated_df.columns if c not in key_columns]
    )

    # Join on keys
    result = before_df.join(after_df, on=key_columns, how="inner")

    return result


@transformer
def apply_delta_updates(df: DataFrame, delta_df: DataFrame, key_columns: List[str],
                       delta_columns: List[str]) -> DataFrame:
    """
    Apply delta/differential updates to numeric columns.

    Sub-Group: Change Data Capture Application
    Tags: cdc, delta, incremental

    Args:
        df: Base DataFrame
        delta_df: DataFrame with delta values (changes, not absolute values)
        key_columns: Business key columns
        delta_columns: Numeric columns containing deltas to apply

    Usage:
        updated = df.transform(apply_delta_updates,
            delta_df=deltas,
            key_columns=["product_id"],
            delta_columns=["quantity", "amount"]
        )
    """
    # Rename delta columns
    delta_renamed = delta_df.select(
        *key_columns,
        *[col(c).alias(f"_delta_{c}") for c in delta_columns]
    )

    # Join
    joined = df.join(delta_renamed, on=key_columns, how="left")

    # Apply deltas
    for delta_col in delta_columns:
        joined = joined.withColumn(
            delta_col,
            when(col(f"_delta_{delta_col}").isNotNull(),
                 col(delta_col) + col(f"_delta_{delta_col}")
            ).otherwise(col(delta_col))
        ).drop(f"_delta_{delta_col}")

    return joined


@transformer
def track_row_versions(df: DataFrame, key_columns: List[str],
                      version_column: str = "row_version") -> DataFrame:
    """
    Add row version number for change tracking.

    Sub-Group: Change Data Capture Application
    Tags: cdc, versioning, tracking

    Args:
        df: Input DataFrame
        key_columns: Business key columns
        version_column: Name for version column

    Usage:
        versioned = df.transform(track_row_versions, key_columns=["customer_id"])
    """
    window = Window.partitionBy(*key_columns).orderBy(current_timestamp())

    result = df.withColumn(version_column, row_number().over(window))

    return result


@transformer
def identify_latest_records(df: DataFrame, key_columns: List[str],
                           timestamp_column: str,
                           latest_flag_column: str = "is_latest") -> DataFrame:
    """
    Flag latest records in CDC stream.

    Sub-Group: Change Data Capture Application
    Tags: cdc, latest, filtering

    Args:
        df: Input DataFrame
        key_columns: Business key columns
        timestamp_column: Timestamp column
        latest_flag_column: Name for latest flag column

    Usage:
        df = df.transform(identify_latest_records,
            key_columns=["order_id"],
            timestamp_column="updated_at"
        )
    """
    window = Window.partitionBy(*key_columns).orderBy(col(timestamp_column).desc())

    result = df.withColumn("_row_num", row_number().over(window))

    result = result.withColumn(
        latest_flag_column,
        when(col("_row_num") == 1, lit(True)).otherwise(lit(False))
    ).drop("_row_num")

    return result
