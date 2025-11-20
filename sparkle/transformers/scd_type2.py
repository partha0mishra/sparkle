"""
SCD Type 2 (Slowly Changing Dimension) incremental merge transformer.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sparkle.config_schema import config_schema, Field, BaseConfigSchema


@config_schema
class SCDType2Config(BaseConfigSchema):
    """Configuration for SCD Type 2 incremental merge."""

    # Key columns
    business_keys: list[str] = Field(
        ...,
        description="Business key columns (natural keys)",
        min_length=1,
        examples=[["customer_id"], ["account_id", "region"]],
        group="Keys",
        order=1,
        help_text="Columns that uniquely identify a business entity",
    )

    # Tracking columns
    effective_date_column: str = Field(
        "effective_date",
        description="Column name for effective date (will be created if doesn't exist)",
        group="Tracking",
        order=10,
    )
    end_date_column: str = Field(
        "end_date",
        description="Column name for end date",
        group="Tracking",
        order=11,
    )
    is_current_column: str = Field(
        "is_current",
        description="Column name for current record flag",
        group="Tracking",
        order=12,
    )
    source_timestamp_column: str = Field(
        "updated_at",
        description="Column in source data indicating when record was updated",
        group="Tracking",
        order=13,
    )

    # Comparison columns
    compare_columns: list[str] = Field(
        None,
        description="Columns to compare for changes (null = all except keys and tracking)",
        group="Change Detection",
        order=20,
    )
    ignore_columns: list[str] = Field(
        [],
        description="Columns to ignore in change detection",
        group="Change Detection",
        order=21,
    )

    # Behavior
    end_date_value: str = Field(
        "9999-12-31",
        description="Value for end_date on current records",
        group="Behavior",
        order=30,
    )
    handle_deletes: bool = Field(
        False,
        description="Mark missing records as expired (soft delete)",
        group="Behavior",
        order=31,
    )


def scd_type_2_incremental_merge(
    target_df: DataFrame,
    source_df: DataFrame,
    config: dict,
) -> DataFrame:
    """
    Perform SCD Type 2 incremental merge.

    Tracks historical changes by:
    - Expiring old versions (setting end_date and is_current=false)
    - Inserting new versions for changed records
    - Keeping unchanged records as-is

    Tags: scd, type-2, history, incremental, merge
    Category: Data Warehouse
    Icon: history
    """
    cfg = SCDType2Config(**config) if isinstance(config, dict) else config

    # Add tracking columns to source if they don't exist
    if cfg.effective_date_column not in source_df.columns:
        source_df = source_df.withColumn(
            cfg.effective_date_column,
            F.col(cfg.source_timestamp_column) if cfg.source_timestamp_column in source_df.columns
            else F.current_date()
        )

    if cfg.is_current_column not in source_df.columns:
        source_df = source_df.withColumn(cfg.is_current_column, F.lit(True))

    if cfg.end_date_column not in source_df.columns:
        source_df = source_df.withColumn(cfg.end_date_column, F.lit(cfg.end_date_value))

    # Determine columns to compare
    if cfg.compare_columns:
        compare_cols = cfg.compare_columns
    else:
        # All columns except keys, tracking, and ignored
        excluded = set(cfg.business_keys + [
            cfg.effective_date_column,
            cfg.end_date_column,
            cfg.is_current_column,
        ] + cfg.ignore_columns)
        compare_cols = [c for c in source_df.columns if c not in excluded]

    # Join target and source on business keys
    join_condition = [F.col(f"target.{k}") == F.col(f"source.{k}") for k in cfg.business_keys]

    matched = (
        target_df.alias("target")
        .join(
            source_df.alias("source"),
            join_condition,
            "full_outer"
        )
    )

    # Detect changes
    change_condition = F.lit(False)
    for col in compare_cols:
        if col in target_df.columns and col in source_df.columns:
            change_condition = change_condition | (
                F.col(f"target.{col}") != F.col(f"source.{col}")
            )

    # Build result DataFrame
    # 1. Expired records (changed or deleted)
    expired_records = (
        matched
        .where(
            (F.col("source." + cfg.business_keys[0]).isNotNull()) &
            (F.col("target." + cfg.business_keys[0]).isNotNull()) &
            change_condition &
            F.col(f"target.{cfg.is_current_column}")
        )
        .select([F.col(f"target.{c}").alias(c) for c in target_df.columns])
        .withColumn(cfg.is_current_column, F.lit(False))
        .withColumn(cfg.end_date_column, F.current_date())
    )

    # 2. New/updated records from source
    new_records = (
        matched
        .where(
            (F.col("source." + cfg.business_keys[0]).isNotNull()) &
            (
                F.col("target." + cfg.business_keys[0]).isNull() |  # New record
                change_condition  # Changed record
            )
        )
        .select([F.col(f"source.{c}").alias(c) for c in source_df.columns])
    )

    # 3. Unchanged current records
    unchanged_records = (
        matched
        .where(
            (F.col("source." + cfg.business_keys[0]).isNotNull()) &
            (F.col("target." + cfg.business_keys[0]).isNotNull()) &
            ~change_condition
        )
        .select([F.col(f"target.{c}").alias(c) for c in target_df.columns])
    )

    # 4. Historical records (already expired)
    historical_records = target_df.where(~F.col(cfg.is_current_column))

    # Union all
    result = expired_records.unionByName(new_records).unionByName(unchanged_records).unionByName(historical_records)

    return result


scd_type_2_incremental_merge.config_schema = lambda: SCDType2Config.config_schema()
scd_type_2_incremental_merge.sample_config = lambda: {
    "business_keys": ["customer_id"],
    "effective_date_column": "effective_date",
    "end_date_column": "end_date",
    "is_current_column": "is_current",
    "source_timestamp_column": "updated_at",
    "compare_columns": None,
    "ignore_columns": [],
    "end_date_value": "9999-12-31",
    "handle_deletes": False,
}
