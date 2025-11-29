"""
Data enrichment transformers.

Functions that add derived columns, perform lookups, calculate metrics, etc.
"""

from typing import List, Optional, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, current_date, year, month, dayofmonth,
    hour, minute, datediff, months_between, concat, concat_ws, md5, sha2,
    monotonically_increasing_id, row_number, rank, dense_rank, regexp_extract,
    length, upper, lower, trim, expr, explode, posexplode
)
from pyspark.sql.window import Window
from .base import transformer


@transformer
def add_audit_columns(df: DataFrame, user: Optional[str] = "system") -> DataFrame:
    """
    Add standard audit columns.

    Sub-Group: Enrichment & Lookups
    Tags: audit, metadata, timestamps

    Usage:
        df = df.transform(add_audit_columns, user="etl_process")
    """
    return (df
        .withColumn("created_at", current_timestamp())
        .withColumn("created_by", lit(user))
        .withColumn("updated_at", current_timestamp())
        .withColumn("updated_by", lit(user))
    )


@transformer
def add_ingestion_metadata(df: DataFrame, source: str, batch_id: Optional[str] = None) -> DataFrame:
    """
    Add ingestion metadata columns.

    Sub-Group: Enrichment & Lookups
    Tags: metadata, ingestion, tracking

    Usage:
        df = df.transform(add_ingestion_metadata, source="salesforce", batch_id="20251119_01")
    """
    return (df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_ingestion_date", current_date())
        .withColumn("_source_system", lit(source))
        .withColumn("_batch_id", lit(batch_id))
    )


@transformer
def add_surrogate_key(df: DataFrame, column_name: str = "surrogate_key") -> DataFrame:
    """
    Add monotonically increasing surrogate key.

    Sub-Group: Enrichment & Lookups
    Tags: keys, surrogate, indexing

    Usage:
        df = df.transform(add_surrogate_key, column_name="sk")
    """
    return df.withColumn(column_name, monotonically_increasing_id())


@transformer
def add_hash_key(df: DataFrame, source_columns: List[str], target_column: str = "hash_key", algorithm: str = "md5") -> DataFrame:
    """
    Add hash-based key from source columns.

    Sub-Group: Enrichment & Lookups
    Tags: keys, hashing, composite

    Args:
        df: Input DataFrame
        source_columns: Columns to hash
        target_column: Name of hash column
        algorithm: 'md5', 'sha256', 'sha512'

    Usage:
        df = df.transform(add_hash_key, source_columns=["id", "name"], target_column="business_key", algorithm="sha256")
    """
    concatenated = concat_ws("||", *[col(c) for c in source_columns])

    if algorithm == "md5":
        hash_expr = md5(concatenated)
    elif algorithm in ["sha256", "sha512"]:
        bits = int(algorithm.replace("sha", ""))
        hash_expr = sha2(concatenated, bits)
    else:
        raise ValueError(f"Unsupported algorithm: {algorithm}")

    return df.withColumn(target_column, hash_expr)


@transformer
def add_date_components(df: DataFrame, date_column: str, prefix: str = "") -> DataFrame:
    """
    Add year, month, day components from date column.

    Sub-Group: Enrichment & Lookups
    Tags: datetime, extraction, components

    Usage:
        df = df.transform(add_date_components, date_column="order_date", prefix="order_")
    """
    return (df
        .withColumn(f"{prefix}year", year(col(date_column)))
        .withColumn(f"{prefix}month", month(col(date_column)))
        .withColumn(f"{prefix}day", dayofmonth(col(date_column)))
    )


@transformer
def add_row_number(df: DataFrame, partition_by: Optional[List[str]] = None, order_by: Optional[List[str]] = None, column_name: str = "row_num") -> DataFrame:
    """
    Add row number within partitions.

    Sub-Group: Enrichment & Lookups
    Tags: window-functions, numbering, ranking

    Usage:
        df = df.transform(add_row_number, partition_by=["customer_id"], order_by=["order_date"], column_name="order_sequence")
    """
    if partition_by is None:
        window = Window.orderBy(*[col(c) for c in order_by]) if order_by else Window.orderBy(monotonically_increasing_id())
    else:
        window_spec = Window.partitionBy(*[col(c) for c in partition_by])
        if order_by:
            window_spec = window_spec.orderBy(*[col(c) for c in order_by])
        window = window_spec

    return df.withColumn(column_name, row_number().over(window))


@transformer
def add_rank(df: DataFrame, partition_by: List[str], order_by: List[str], column_name: str = "rank", dense: bool = False) -> DataFrame:
    """
    Add rank within partitions.

    Sub-Group: Enrichment & Lookups
    Tags: window-functions, ranking, ordering

    Usage:
        df = df.transform(add_rank, partition_by=["category"], order_by=["sales"], column_name="sales_rank", dense=True)
    """
    window = Window.partitionBy(*[col(c) for c in partition_by]).orderBy(*[col(c) for c in order_by])

    rank_func = dense_rank if dense else rank

    return df.withColumn(column_name, rank_func().over(window))


@transformer
def perform_lookup(df: DataFrame, lookup_df: DataFrame, join_columns: List[str], select_columns: Optional[List[str]] = None, how: str = "left") -> DataFrame:
    """
    Perform lookup join with another DataFrame.

    Sub-Group: Enrichment & Lookups
    Tags: join, lookup, dimension

    Args:
        df: Main DataFrame
        lookup_df: Lookup DataFrame
        join_columns: Columns to join on
        select_columns: Columns to select from lookup (default: all)
        how: Join type

    Usage:
        df = df.transform(perform_lookup,
            lookup_df=customer_master,
            join_columns=["customer_id"],
            select_columns=["customer_name", "customer_segment"]
        )
    """
    if select_columns:
        lookup_df = lookup_df.select(join_columns + select_columns)

    return df.join(lookup_df, on=join_columns, how=how)


@transformer
def add_age_from_birthdate(df: DataFrame, birthdate_column: str, target_column: str = "age", reference_date: Optional[str] = None) -> DataFrame:
    """
    Calculate age from birthdate.

    Sub-Group: Enrichment & Lookups
    Tags: datetime, calculation, age

    Usage:
        df = df.transform(add_age_from_birthdate, birthdate_column="date_of_birth", target_column="age")
    """
    ref_date = current_date() if reference_date is None else lit(reference_date)

    return df.withColumn(
        target_column,
        (datediff(ref_date, col(birthdate_column)) / 365.25).cast("int")
    )


@transformer
def add_tenure_months(df: DataFrame, start_date_column: str, end_date_column: Optional[str] = None, target_column: str = "tenure_months") -> DataFrame:
    """
    Calculate tenure in months between two dates.

    Sub-Group: Enrichment & Lookups
    Tags: datetime, calculation, tenure

    Usage:
        df = df.transform(add_tenure_months, start_date_column="hire_date", target_column="employment_months")
    """
    end_date = current_date() if end_date_column is None else col(end_date_column)

    return df.withColumn(
        target_column,
        months_between(end_date, col(start_date_column)).cast("int")
    )


@transformer
def add_business_key(df: DataFrame, key_columns: List[str], target_column: str = "business_key", separator: str = "|") -> DataFrame:
    """
    Create business key from multiple columns.

    Sub-Group: Enrichment & Lookups
    Tags: keys, composite, business-logic

    Usage:
        df = df.transform(add_business_key, key_columns=["customer_id", "product_id"], target_column="composite_key")
    """
    return df.withColumn(target_column, concat_ws(separator, *[col(c) for c in key_columns]))


@transformer
def add_derived_flag(df: DataFrame, condition: str, target_column: str, true_value: any = True, false_value: any = False) -> DataFrame:
    """
    Add derived boolean/flag column based on condition.

    Sub-Group: Enrichment & Lookups
    Tags: flags, conditional, business-logic

    Args:
        df: Input DataFrame
        condition: SQL condition string
        target_column: Name for flag column
        true_value: Value when condition is true
        false_value: Value when condition is false

    Usage:
        df = df.transform(add_derived_flag,
            condition="amount > 1000",
            target_column="is_high_value",
            true_value=True,
            false_value=False
        )
    """
    return df.withColumn(
        target_column,
        when(expr(condition), lit(true_value)).otherwise(lit(false_value))
    )


@transformer
def add_string_metrics(df: DataFrame, columns: List[str], metrics: Optional[List[str]] = None) -> DataFrame:
    """
    Add string metrics (length, word count, etc.).

    Sub-Group: Enrichment & Lookups
    Tags: strings, metrics, analysis

    Args:
        df: Input DataFrame
        columns: String columns to analyze
        metrics: List of metrics ('length', 'word_count', 'upper_count', 'lower_count')

    Usage:
        df = df.transform(add_string_metrics,
            columns=["description", "comments"],
            metrics=["length", "word_count"]
        )
    """
    if metrics is None:
        metrics = ["length"]

    for column in columns:
        if column in df.columns:
            if "length" in metrics:
                df = df.withColumn(f"{column}_length", length(col(column)))

            if "word_count" in metrics:
                df = df.withColumn(
                    f"{column}_word_count",
                    expr(f"size(split(trim({column}), '\\\\s+'))")
                )

            if "upper_count" in metrics:
                df = df.withColumn(
                    f"{column}_upper_count",
                    length(col(column)) - length(regexp_extract(col(column), "[^A-Z]", 0))
                )

            if "lower_count" in metrics:
                df = df.withColumn(
                    f"{column}_lower_count",
                    length(col(column)) - length(regexp_extract(col(column), "[^a-z]", 0))
                )

    return df


@transformer
def add_domain_from_email(df: DataFrame, email_column: str, target_column: str = "email_domain") -> DataFrame:
    """
    Extract domain from email address.

    Sub-Group: Enrichment & Lookups
    Tags: email, extraction, parsing

    Usage:
        df = df.transform(add_domain_from_email, email_column="email", target_column="domain")
    """
    return df.withColumn(
        target_column,
        regexp_extract(col(email_column), r'@(.+)$', 1)
    )


@transformer
def add_name_components(df: DataFrame, name_column: str, prefix: str = "") -> DataFrame:
    """
    Split full name into first and last name components.

    Sub-Group: Enrichment & Lookups
    Tags: names, parsing, extraction

    Args:
        df: Input DataFrame
        name_column: Column containing full name
        prefix: Prefix for output columns

    Usage:
        df = df.transform(add_name_components, name_column="full_name", prefix="customer_")
    """
    from pyspark.sql.functions import split

    name_parts = split(trim(col(name_column)), r'\s+')

    return (df
        .withColumn(f"{prefix}first_name", name_parts.getItem(0))
        .withColumn(f"{prefix}last_name",
            when(expr(f"size(split(trim({name_column}), '\\\\s+')) > 1"),
                 name_parts.getItem(expr(f"size(split(trim({name_column}), '\\\\s+')) - 1"))
            ).otherwise(lit(None))
        )
    )
