"""
Aggregation and window function transformers.

Functions for grouping, pivoting, windowing, and statistical aggregations.
"""

from typing import List, Optional, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, min as spark_min, max as spark_max,
    stddev, variance, collect_list, collect_set, first, last,
    lag, lead, row_number, rank, dense_rank, ntile, percent_rank,
    cume_dist, when, lit
)
from pyspark.sql.window import Window
from .base import transformer


@transformer
def aggregate_by_key(df: DataFrame, group_by: List[str], aggregations: Dict[str, List[str]]) -> DataFrame:
    """
    Aggregate DataFrame by key columns.

    Sub-Group: Aggregation & Windowing
    Tags: aggregation, groupby, metrics

    Args:
        df: Input DataFrame
        group_by: Columns to group by
        aggregations: Dict of {column: [agg_functions]}

    Usage:
        df = df.transform(aggregate_by_key,
            group_by=["customer_id", "order_date"],
            aggregations={
                "amount": ["sum", "avg", "count"],
                "quantity": ["sum", "max"]
            }
        )
    """
    agg_exprs = []

    for column, functions in aggregations.items():
        for func in functions:
            if func == "sum":
                agg_exprs.append(spark_sum(col(column)).alias(f"{column}_{func}"))
            elif func == "avg":
                agg_exprs.append(avg(col(column)).alias(f"{column}_{func}"))
            elif func == "count":
                agg_exprs.append(count(col(column)).alias(f"{column}_{func}"))
            elif func == "min":
                agg_exprs.append(spark_min(col(column)).alias(f"{column}_{func}"))
            elif func == "max":
                agg_exprs.append(spark_max(col(column)).alias(f"{column}_{func}"))
            elif func == "stddev":
                agg_exprs.append(stddev(col(column)).alias(f"{column}_{func}"))
            elif func == "variance":
                agg_exprs.append(variance(col(column)).alias(f"{column}_{func}"))

    return df.groupBy(*group_by).agg(*agg_exprs)


@transformer
def pivot_table(df: DataFrame, index: List[str], columns: str, values: str, agg_func: str = "sum") -> DataFrame:
    """
    Create pivot table.

    Sub-Group: Aggregation & Windowing
    Tags: pivot, reshape, aggregation

    Args:
        df: Input DataFrame
        index: Columns for rows
        columns: Column to pivot on
        values: Column to aggregate
        agg_func: Aggregation function

    Usage:
        df = df.transform(pivot_table,
            index=["customer_id"],
            columns="product_category",
            values="amount",
            agg_func="sum"
        )
    """
    agg_map = {
        "sum": spark_sum,
        "avg": avg,
        "count": count,
        "min": spark_min,
        "max": spark_max
    }

    agg_function = agg_map.get(agg_func, spark_sum)

    return df.groupBy(*index).pivot(columns).agg(agg_function(col(values)))


@transformer
def add_running_total(df: DataFrame, partition_by: List[str], order_by: List[str], value_column: str, target_column: str = "running_total") -> DataFrame:
    """
    Add running total within partitions.

    Sub-Group: Aggregation & Windowing
    Tags: window-functions, running-total, cumulative

    Usage:
        df = df.transform(add_running_total,
            partition_by=["customer_id"],
            order_by=["order_date"],
            value_column="amount",
            target_column="cumulative_spend"
        )
    """
    window = (
        Window
        .partitionBy(*[col(c) for c in partition_by])
        .orderBy(*[col(c) for c in order_by])
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return df.withColumn(target_column, spark_sum(col(value_column)).over(window))


@transformer
def add_moving_average(df: DataFrame, partition_by: List[str], order_by: List[str], value_column: str, window_size: int, target_column: str = "moving_avg") -> DataFrame:
    """
    Add moving average.

    Sub-Group: Aggregation & Windowing
    Tags: window-functions, moving-average, time-series

    Args:
        df: Input DataFrame
        partition_by: Partition columns
        order_by: Order columns
        value_column: Column to average
        window_size: Number of rows for moving average
        target_column: Output column name

    Usage:
        df = df.transform(add_moving_average,
            partition_by=["customer_id"],
            order_by=["order_date"],
            value_column="amount",
            window_size=7,
            target_column="avg_7day"
        )
    """
    window = (
        Window
        .partitionBy(*[col(c) for c in partition_by])
        .orderBy(*[col(c) for c in order_by])
        .rowsBetween(-(window_size - 1), 0)
    )

    return df.withColumn(target_column, avg(col(value_column)).over(window))


@transformer
def add_lag_lead_columns(df: DataFrame, partition_by: List[str], order_by: List[str], columns: List[str], periods: int = 1) -> DataFrame:
    """
    Add lag (previous) and lead (next) columns.

    Sub-Group: Aggregation & Windowing
    Tags: window-functions, lag, lead

    Usage:
        df = df.transform(add_lag_lead_columns,
            partition_by=["customer_id"],
            order_by=["order_date"],
            columns=["amount"],
            periods=1
        )
    """
    window = Window.partitionBy(*[col(c) for c in partition_by]).orderBy(*[col(c) for c in order_by])

    for column in columns:
        if column in df.columns:
            df = df.withColumn(f"{column}_lag_{periods}", lag(col(column), periods).over(window))
            df = df.withColumn(f"{column}_lead_{periods}", lead(col(column), periods).over(window))

    return df


@transformer
def add_percentile_rank(df: DataFrame, partition_by: List[str], order_by: List[str], target_column: str = "percentile") -> DataFrame:
    """
    Add percentile rank.

    Sub-Group: Aggregation & Windowing
    Tags: window-functions, ranking, percentile

    Usage:
        df = df.transform(add_percentile_rank,
            partition_by=["category"],
            order_by=["sales"],
            target_column="sales_percentile"
        )
    """
    window = Window.partitionBy(*[col(c) for c in partition_by]).orderBy(*[col(c) for c in order_by])

    return df.withColumn(target_column, percent_rank().over(window))


@transformer
def add_ntile(df: DataFrame, partition_by: List[str], order_by: List[str], n: int, target_column: str = "ntile") -> DataFrame:
    """
    Add ntile (divide into N buckets).

    Sub-Group: Aggregation & Windowing
    Tags: window-functions, bucketing, ntile

    Usage:
        df = df.transform(add_ntile,
            partition_by=["region"],
            order_by=["revenue"],
            n=4,
            target_column="revenue_quartile"
        )
    """
    window = Window.partitionBy(*[col(c) for c in partition_by]).orderBy(*[col(c) for c in order_by])

    return df.withColumn(target_column, ntile(n).over(window))


@transformer
def rollup_aggregation(df: DataFrame, group_by: List[str], agg_column: str, agg_func: str = "sum") -> DataFrame:
    """
    Perform rollup aggregation (creates subtotals).

    Sub-Group: Aggregation & Windowing
    Tags: rollup, hierarchical, subtotals

    Usage:
        df = df.transform(rollup_aggregation,
            group_by=["region", "category", "product"],
            agg_column="sales",
            agg_func="sum"
        )
    """
    agg_map = {
        "sum": spark_sum,
        "avg": avg,
        "count": count
    }

    agg_function = agg_map.get(agg_func, spark_sum)

    return df.rollup(*group_by).agg(agg_function(col(agg_column)).alias(f"{agg_column}_{agg_func}"))


@transformer
def cube_aggregation(df: DataFrame, group_by: List[str], agg_column: str, agg_func: str = "sum") -> DataFrame:
    """
    Perform cube aggregation (all possible combinations).

    Sub-Group: Aggregation & Windowing
    Tags: cube, olap, cross-tabulation

    Usage:
        df = df.transform(cube_aggregation,
            group_by=["region", "category"],
            agg_column="sales",
            agg_func="sum"
        )
    """
    agg_map = {
        "sum": spark_sum,
        "avg": avg,
        "count": count
    }

    agg_function = agg_map.get(agg_func, spark_sum)

    return df.cube(*group_by).agg(agg_function(col(agg_column)).alias(f"{agg_column}_{agg_func}"))


@transformer
def add_cumulative_distribution(df: DataFrame, partition_by: List[str], order_by: List[str],
                               target_column: str = "cumulative_dist") -> DataFrame:
    """
    Add cumulative distribution function.

    Sub-Group: Aggregation & Windowing
    Tags: window-functions, distribution, statistics

    Usage:
        df = df.transform(add_cumulative_distribution,
            partition_by=["category"],
            order_by=["amount"],
            target_column="cume_dist"
        )
    """
    window = Window.partitionBy(*[col(c) for c in partition_by]).orderBy(*[col(c) for c in order_by])

    return df.withColumn(target_column, cume_dist().over(window))
