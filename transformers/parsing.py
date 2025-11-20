"""
Complex type parsing transformers.

Functions for parsing JSON, XML, arrays, maps, and nested structures.
"""

from typing import List, Optional, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, from_json, get_json_object, explode, explode_outer,
    posexplode, split, concat_ws
)
from pyspark.sql.types import StructType, MapType
from .base import transformer


@transformer
def parse_json_column(df: DataFrame, json_column: str, schema: Optional[StructType] = None, drop_source: bool = False) -> DataFrame:
    """
    Parse JSON string column into struct.

    Args:
        df: Input DataFrame
        json_column: Column containing JSON string
        schema: Optional schema (otherwise inferred)
        drop_source: Whether to drop original JSON column

    Usage:
        df = df.transform(parse_json_column, json_column="json_data", drop_source=True)
    """
    if schema:
        df = df.withColumn(f"{json_column}_parsed", from_json(col(json_column), schema))
    else:
        # Infer schema from sample
        sample_json = df.select(json_column).first()[0]
        if sample_json:
            schema = spark.read.json(spark.sparkContext.parallelize([sample_json])).schema
            df = df.withColumn(f"{json_column}_parsed", from_json(col(json_column), schema))

    # Flatten struct into columns
    if f"{json_column}_parsed" in df.columns:
        parsed_df = df.select("*", f"{json_column}_parsed.*")
        if drop_source:
            parsed_df = parsed_df.drop(json_column, f"{json_column}_parsed")
        return parsed_df

    return df


@transformer
def extract_json_field(df: DataFrame, json_column: str, json_path: str, target_column: str) -> DataFrame:
    """
    Extract specific field from JSON column.

    Args:
        df: Input DataFrame
        json_column: Column containing JSON
        json_path: JSON path (e.g., '$.user.email')
        target_column: Name for extracted field

    Usage:
        df = df.transform(extract_json_field, json_column="payload", json_path="$.user.id", target_column="user_id")
    """
    return df.withColumn(target_column, get_json_object(col(json_column), json_path))


@transformer
def explode_array_column(df: DataFrame, array_column: str, exploded_column_name: Optional[str] = None, keep_position: bool = False) -> DataFrame:
    """
    Explode array column into multiple rows.

    Usage:
        df = df.transform(explode_array_column, array_column="tags", exploded_column_name="tag")
    """
    exploded_name = exploded_column_name or f"{array_column}_item"

    if keep_position:
        df = df.withColumn("_exploded", posexplode(col(array_column)))
        df = df.select("*", col("_exploded.pos").alias(f"{exploded_name}_position"), col("_exploded.col").alias(exploded_name))
        df = df.drop("_exploded", array_column)
    else:
        df = df.withColumn(exploded_name, explode_outer(col(array_column)))
        df = df.drop(array_column)

    return df


@transformer
def split_delimited_column(df: DataFrame, column: str, delimiter: str, target_columns: Optional[List[str]] = None) -> DataFrame:
    """
    Split delimited string into multiple columns.

    Usage:
        df = df.transform(split_delimited_column, column="full_name", delimiter=" ", target_columns=["first_name", "last_name"])
    """
    split_col = split(col(column), delimiter)

    if target_columns:
        for i, target_col in enumerate(target_columns):
            df = df.withColumn(target_col, split_col.getItem(i))
    else:
        df = df.withColumn(f"{column}_array", split_col)

    return df


@transformer
def flatten_struct_column(df: DataFrame, struct_column: str, separator: str = "_", drop_source: bool = True) -> DataFrame:
    """
    Flatten nested struct column into individual columns.

    Usage:
        df = df.transform(flatten_struct_column, struct_column="address", separator="_")
    """
    struct_fields = df.schema[struct_column].dataType.fields

    for field in struct_fields:
        df = df.withColumn(f"{struct_column}{separator}{field.name}", col(f"{struct_column}.{field.name}"))

    if drop_source:
        df = df.drop(struct_column)

    return df
