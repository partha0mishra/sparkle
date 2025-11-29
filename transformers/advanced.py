"""
Advanced pattern transformers.

Complex transformations including sampling, pivoting, sessionization, and statistical operations.
"""

from typing import List, Optional, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, rand, expr, sum as spark_sum, count, avg,
    lag, lead, unix_timestamp, row_number, dense_rank, collect_list,
    explode, array, struct, map_from_arrays, map_keys, map_values,
    regexp_extract, concat_ws, md5, monotonically_increasing_id,
    broadcast, coalesce, greatest, least
)
from pyspark.sql.window import Window
from .base import transformer


@transformer
def sample_data(df: DataFrame, fraction: float, seed: Optional[int] = None,
               stratify_column: Optional[str] = None) -> DataFrame:
    """
    Sample data with optional stratification.

    Sub-Group: Advanced Patterns
    Tags: sampling, testing, stratification

    Args:
        df: Input DataFrame
        fraction: Fraction of data to sample (0.0 to 1.0)
        seed: Random seed for reproducibility
        stratify_column: Column to stratify sampling on

    Usage:
        df = df.transform(sample_data, fraction=0.1, seed=42)
        df = df.transform(sample_data, fraction=0.2, stratify_column="category")
    """
    if stratify_column:
        return df.sampleBy(stratify_column, fractions={}, seed=seed)
    else:
        return df.sample(withReplacement=False, fraction=fraction, seed=seed)


@transformer
def unpivot_columns(df: DataFrame, id_columns: List[str], value_columns: List[str],
                   variable_column: str = "variable", value_column: str = "value") -> DataFrame:
    """
    Unpivot (melt) wide format to long format.

    Sub-Group: Advanced Patterns
    Tags: unpivot, melt, reshape

    Args:
        df: Input DataFrame
        id_columns: Columns to keep as identifiers
        value_columns: Columns to unpivot
        variable_column: Name for variable column
        value_column: Name for value column

    Usage:
        df = df.transform(unpivot_columns,
            id_columns=["customer_id", "date"],
            value_columns=["q1_sales", "q2_sales", "q3_sales", "q4_sales"],
            variable_column="quarter",
            value_column="sales"
        )
    """
    # Create array of structs
    unpivot_expr = [
        struct(lit(c).alias(variable_column), col(c).alias(value_column))
        for c in value_columns
    ]

    # Stack values
    result = df.select(*id_columns, explode(array(*unpivot_expr)).alias("_unpivoted"))

    result = result.select(
        *id_columns,
        col(f"_unpivoted.{variable_column}").alias(variable_column),
        col(f"_unpivoted.{value_column}").alias(value_column)
    )

    return result


@transformer
def create_sessionization(df: DataFrame, partition_by: List[str], timestamp_column: str,
                         timeout_seconds: int, session_id_column: str = "session_id") -> DataFrame:
    """
    Create session IDs based on time gaps.

    Sub-Group: Advanced Patterns
    Tags: sessionization, time-series, grouping

    Args:
        df: Input DataFrame
        partition_by: Columns to partition by (e.g., user_id)
        timestamp_column: Timestamp column
        timeout_seconds: Session timeout in seconds
        session_id_column: Name for session ID column

    Usage:
        df = df.transform(create_sessionization,
            partition_by=["user_id"],
            timestamp_column="event_time",
            timeout_seconds=1800
        )
    """
    window = Window.partitionBy(*partition_by).orderBy(timestamp_column)

    # Calculate time difference from previous event
    df = df.withColumn(
        "_prev_timestamp",
        lag(unix_timestamp(col(timestamp_column))).over(window)
    )

    df = df.withColumn(
        "_time_diff",
        unix_timestamp(col(timestamp_column)) - coalesce(col("_prev_timestamp"), lit(0))
    )

    # Mark session boundaries (gap > timeout)
    df = df.withColumn(
        "_new_session",
        when((col("_time_diff") > timeout_seconds) | col("_prev_timestamp").isNull(), lit(1))
        .otherwise(lit(0))
    )

    # Cumulative sum to create session IDs
    window_cumsum = Window.partitionBy(*partition_by).orderBy(timestamp_column).rowsBetween(Window.unboundedPreceding, 0)

    df = df.withColumn(session_id_column, spark_sum(col("_new_session")).over(window_cumsum))

    # Clean up temporary columns
    df = df.drop("_prev_timestamp", "_time_diff", "_new_session")

    return df


@transformer
def apply_conditional_logic(df: DataFrame, conditions: List[Dict[str, Any]],
                           target_column: str, default_value: Any = None) -> DataFrame:
    """
    Apply complex conditional logic with multiple when-then rules.

    Sub-Group: Advanced Patterns
    Tags: conditional, business-logic, rules

    Args:
        df: Input DataFrame
        conditions: List of dicts with 'condition' (SQL) and 'value' keys
        target_column: Name for result column
        default_value: Default value if no conditions match

    Usage:
        df = df.transform(apply_conditional_logic,
            conditions=[
                {"condition": "age < 18", "value": "Minor"},
                {"condition": "age >= 18 AND age < 65", "value": "Adult"},
                {"condition": "age >= 65", "value": "Senior"}
            ],
            target_column="age_group",
            default_value="Unknown"
        )
    """
    # Build when-otherwise chain
    result_expr = None

    for cond_dict in conditions:
        condition_sql = cond_dict["condition"]
        value = cond_dict["value"]

        if result_expr is None:
            result_expr = when(expr(condition_sql), lit(value))
        else:
            result_expr = result_expr.when(expr(condition_sql), lit(value))

    if default_value is not None:
        result_expr = result_expr.otherwise(lit(default_value))

    return df.withColumn(target_column, result_expr)


@transformer
def create_composite_key(df: DataFrame, key_columns: List[str], target_column: str = "composite_key",
                        hash_key: bool = False, separator: str = "|") -> DataFrame:
    """
    Create composite key from multiple columns.

    Sub-Group: Advanced Patterns
    Tags: keys, hashing, indexing

    Args:
        df: Input DataFrame
        key_columns: Columns to combine
        target_column: Name for composite key column
        hash_key: If True, return MD5 hash instead of concatenated string
        separator: Separator for concatenation (if not hashing)

    Usage:
        df = df.transform(create_composite_key,
            key_columns=["customer_id", "product_id", "order_date"],
            target_column="order_key",
            hash_key=True
        )
    """
    concatenated = concat_ws(separator, *[coalesce(col(c), lit("")).cast("string") for c in key_columns])

    if hash_key:
        return df.withColumn(target_column, md5(concatenated))
    else:
        return df.withColumn(target_column, concatenated)


@transformer
def broadcast_join_small_table(df: DataFrame, small_df: DataFrame, join_columns: List[str],
                              how: str = "left") -> DataFrame:
    """
    Perform broadcast join for performance with small lookup tables.

    Sub-Group: Advanced Patterns
    Tags: performance, join, broadcast

    Args:
        df: Large DataFrame
        small_df: Small DataFrame to broadcast
        join_columns: Columns to join on
        how: Join type

    Usage:
        df = df.transform(broadcast_join_small_table,
            small_df=dimension_table,
            join_columns=["product_id"],
            how="left"
        )
    """
    return df.join(broadcast(small_df), on=join_columns, how=how)


@transformer
def calculate_running_statistics(df: DataFrame, partition_by: List[str], order_by: List[str],
                                 value_column: str, statistics: List[str]) -> DataFrame:
    """
    Calculate running statistics (sum, avg, min, max, count).

    Sub-Group: Advanced Patterns
    Tags: statistics, window-functions, aggregation

    Args:
        df: Input DataFrame
        partition_by: Partition columns
        order_by: Order columns
        value_column: Column to calculate statistics on
        statistics: List of statistics ('sum', 'avg', 'min', 'max', 'count')

    Usage:
        df = df.transform(calculate_running_statistics,
            partition_by=["customer_id"],
            order_by=["order_date"],
            value_column="amount",
            statistics=["sum", "avg", "count"]
        )
    """
    from pyspark.sql.functions import min as spark_min, max as spark_max

    window = (
        Window
        .partitionBy(*partition_by)
        .orderBy(*order_by)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    stat_functions = {
        "sum": spark_sum,
        "avg": avg,
        "min": spark_min,
        "max": spark_max,
        "count": count
    }

    for stat in statistics:
        if stat in stat_functions:
            df = df.withColumn(
                f"{value_column}_running_{stat}",
                stat_functions[stat](col(value_column)).over(window)
            )

    return df


@transformer
def create_bins(df: DataFrame, column: str, bins: List[float], labels: Optional[List[str]] = None,
               target_column: Optional[str] = None) -> DataFrame:
    """
    Bin numeric column into categories.

    Sub-Group: Advanced Patterns
    Tags: binning, categorization, discretization

    Args:
        df: Input DataFrame
        column: Numeric column to bin
        bins: List of bin edges (must be sorted)
        labels: Optional labels for bins (length = len(bins) - 1)
        target_column: Name for binned column (default: column_binned)

    Usage:
        df = df.transform(create_bins,
            column="age",
            bins=[0, 18, 35, 50, 65, 100],
            labels=["Child", "Young Adult", "Middle Age", "Senior", "Elderly"],
            target_column="age_group"
        )
    """
    target = target_column or f"{column}_binned"

    if labels and len(labels) != len(bins) - 1:
        raise ValueError(f"Number of labels ({len(labels)}) must equal number of bins - 1 ({len(bins) - 1})")

    # Build when-otherwise chain
    bin_expr = None

    for i in range(len(bins) - 1):
        lower = bins[i]
        upper = bins[i + 1]
        label = labels[i] if labels else f"{lower}-{upper}"

        condition = (col(column) >= lower) & (col(column) < upper)

        if i == len(bins) - 2:  # Last bin includes upper bound
            condition = (col(column) >= lower) & (col(column) <= upper)

        if bin_expr is None:
            bin_expr = when(condition, lit(label))
        else:
            bin_expr = bin_expr.when(condition, lit(label))

    bin_expr = bin_expr.otherwise(lit(None))

    return df.withColumn(target, bin_expr)


@transformer
def create_array_column(df: DataFrame, columns: List[str], target_column: str = "array_col") -> DataFrame:
    """
    Combine multiple columns into array column.

    Sub-Group: Advanced Patterns
    Tags: arrays, restructuring, complex-types

    Args:
        df: Input DataFrame
        columns: Columns to combine into array
        target_column: Name for array column

    Usage:
        df = df.transform(create_array_column,
            columns=["q1_sales", "q2_sales", "q3_sales", "q4_sales"],
            target_column="quarterly_sales"
        )
    """
    return df.withColumn(target_column, array(*[col(c) for c in columns]))


@transformer
def create_map_column(df: DataFrame, key_columns: List[str], value_columns: List[str],
                     target_column: str = "map_col") -> DataFrame:
    """
    Create map column from key and value columns.

    Sub-Group: Advanced Patterns
    Tags: maps, complex-types, restructuring

    Args:
        df: Input DataFrame
        key_columns: Columns to use as map keys
        value_columns: Columns to use as map values
        target_column: Name for map column

    Usage:
        df = df.transform(create_map_column,
            key_columns=["metric1", "metric2", "metric3"],
            value_columns=["value1", "value2", "value3"],
            target_column="metrics_map"
        )
    """
    if len(key_columns) != len(value_columns):
        raise ValueError("Number of key columns must equal number of value columns")

    keys_array = array(*[lit(k) for k in key_columns])
    values_array = array(*[col(v) for v in value_columns])

    return df.withColumn(target_column, map_from_arrays(keys_array, values_array))


@transformer
def rank_within_groups(df: DataFrame, partition_by: List[str], order_by: List[str],
                      rank_column: str = "rank", dense: bool = True,
                      top_n: Optional[int] = None) -> DataFrame:
    """
    Rank records within groups and optionally filter top N.

    Sub-Group: Advanced Patterns
    Tags: ranking, window-functions, top-n

    Args:
        df: Input DataFrame
        partition_by: Partition columns
        order_by: Order columns (use desc for descending)
        rank_column: Name for rank column
        dense: Use dense_rank (no gaps) vs rank (with gaps)
        top_n: If specified, filter to top N per partition

    Usage:
        df = df.transform(rank_within_groups,
            partition_by=["category"],
            order_by=["sales"],
            rank_column="sales_rank",
            top_n=10
        )
    """
    from pyspark.sql.functions import desc

    window = Window.partitionBy(*partition_by).orderBy(*[col(c) for c in order_by])

    rank_func = dense_rank if dense else row_number

    df = df.withColumn(rank_column, rank_func().over(window))

    if top_n:
        df = df.filter(col(rank_column) <= top_n)

    return df


@transformer
def calculate_percentiles(df: DataFrame, column: str, percentiles: List[float],
                         target_prefix: str = "percentile") -> DataFrame:
    """
    Calculate percentile values for a column.

    Sub-Group: Advanced Patterns
    Tags: statistics, percentiles, distribution

    Args:
        df: Input DataFrame
        column: Numeric column
        percentiles: List of percentiles (0.0 to 1.0)
        target_prefix: Prefix for percentile columns

    Usage:
        df = df.transform(calculate_percentiles,
            column="amount",
            percentiles=[0.25, 0.5, 0.75, 0.95],
            target_prefix="amount_p"
        )
    """
    percentile_values = df.approxQuantile(column, percentiles, 0.01)

    for i, (p, value) in enumerate(zip(percentiles, percentile_values)):
        p_int = int(p * 100)
        df = df.withColumn(f"{target_prefix}_{p_int}", lit(value))

    return df


@transformer
def apply_lookup_with_fallback(df: DataFrame, lookup_column: str,
                               lookup_map: Dict[str, str],
                               fallback_column: Optional[str] = None,
                               target_column: Optional[str] = None) -> DataFrame:
    """
    Apply lookup with fallback to original or another column.

    Sub-Group: Advanced Patterns
    Tags: lookup, mapping, fallback

    Args:
        df: Input DataFrame
        lookup_column: Column to look up
        lookup_map: Mapping dictionary
        fallback_column: Column to use if lookup fails (default: original column)
        target_column: Name for result column (default: lookup_column)

    Usage:
        df = df.transform(apply_lookup_with_fallback,
            lookup_column="country_code",
            lookup_map={"US": "United States", "UK": "United Kingdom"},
            target_column="country_name"
        )
    """
    target = target_column or lookup_column
    fallback = fallback_column or lookup_column

    # Build when-otherwise chain
    lookup_expr = None

    for key, value in lookup_map.items():
        if lookup_expr is None:
            lookup_expr = when(col(lookup_column) == key, lit(value))
        else:
            lookup_expr = lookup_expr.when(col(lookup_column) == key, lit(value))

    lookup_expr = lookup_expr.otherwise(col(fallback))

    return df.withColumn(target, lookup_expr)


@transformer
def calculate_percent_change(df: DataFrame, partition_by: List[str], order_by: List[str],
                            value_column: str, periods: int = 1,
                            target_column: Optional[str] = None) -> DataFrame:
    """
    Calculate percent change over periods.

    Sub-Group: Advanced Patterns
    Tags: time-series, change, percentage

    Args:
        df: Input DataFrame
        partition_by: Partition columns
        order_by: Order columns
        value_column: Column to calculate change on
        periods: Number of periods to look back
        target_column: Name for percent change column

    Usage:
        df = df.transform(calculate_percent_change,
            partition_by=["product_id"],
            order_by=["month"],
            value_column="sales",
            periods=1,
            target_column="mom_change_pct"
        )
    """
    target = target_column or f"{value_column}_pct_change"

    window = Window.partitionBy(*partition_by).orderBy(*order_by)

    df = df.withColumn("_prev_value", lag(col(value_column), periods).over(window))

    df = df.withColumn(
        target,
        when(col("_prev_value").isNotNull() & (col("_prev_value") != 0),
             ((col(value_column) - col("_prev_value")) / col("_prev_value") * 100)
        ).otherwise(lit(None))
    ).drop("_prev_value")

    return df
