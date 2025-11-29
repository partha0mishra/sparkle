"""
Complex type parsing transformers.

Functions for parsing JSON, XML, arrays, maps, and nested structures.
"""

from typing import List, Optional, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, from_json, get_json_object, explode, explode_outer,
    posexplode, split, concat_ws, to_json, schema_of_json,
    map_keys, map_values, map_entries, create_map, expr, lit, array, struct
)

# Optional imports for newer PySpark versions
try:
    from pyspark.sql.functions import from_xml, xpath, xpath_string, xpath_int, xpath_double
    HAS_XML_SUPPORT = True
except ImportError:
    HAS_XML_SUPPORT = False

from pyspark.sql.types import StructType, MapType, ArrayType
from .base import transformer


@transformer
def parse_json_column(df: DataFrame, json_column: str, schema: Optional[StructType] = None, drop_source: bool = False) -> DataFrame:
    """
    Parse JSON string column into struct.

    Sub-Group: Parsing & Extraction
    Tags: json, parsing, nested

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
        sample_json = df.select(json_column).filter(col(json_column).isNotNull()).first()
        if sample_json and sample_json[0]:
            inferred_schema = schema_of_json(lit(sample_json[0]))
            df = df.withColumn(f"{json_column}_parsed", from_json(col(json_column), inferred_schema))

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

    Sub-Group: Parsing & Extraction
    Tags: json, extraction, path

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

    Sub-Group: Parsing & Extraction
    Tags: arrays, explode, unnest

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

    Sub-Group: Parsing & Extraction
    Tags: strings, splitting, delimited

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

    Sub-Group: Parsing & Extraction
    Tags: struct, flattening, nested

    Usage:
        df = df.transform(flatten_struct_column, struct_column="address", separator="_")
    """
    struct_fields = df.schema[struct_column].dataType.fields

    for field in struct_fields:
        df = df.withColumn(f"{struct_column}{separator}{field.name}", col(f"{struct_column}.{field.name}"))

    if drop_source:
        df = df.drop(struct_column)

    return df


@transformer
def parse_xml_column(df: DataFrame, xml_column: str, xpath_expr: str, target_column: str, value_type: str = "string") -> DataFrame:
    """
    Parse XML column using XPath expression.

    Sub-Group: Parsing & Extraction
    Tags: xml, parsing, xpath

    Args:
        df: Input DataFrame
        xml_column: Column containing XML string
        xpath_expr: XPath expression
        target_column: Name for extracted value
        value_type: Type of value ('string', 'int', 'double')

    Usage:
        df = df.transform(parse_xml_column,
            xml_column="xml_data",
            xpath_expr="//user/email",
            target_column="email",
            value_type="string"
        )
    """
    if not HAS_XML_SUPPORT:
        raise ImportError("XML parsing requires PySpark 3.4+. Please upgrade PySpark.")

    if value_type == "int":
        df = df.withColumn(target_column, xpath_int(col(xml_column), xpath_expr))
    elif value_type == "double":
        df = df.withColumn(target_column, xpath_double(col(xml_column), xpath_expr))
    else:
        df = df.withColumn(target_column, xpath_string(col(xml_column), xpath_expr))

    return df


@transformer
def extract_map_keys_values(df: DataFrame, map_column: str, extract_keys: bool = True, extract_values: bool = True) -> DataFrame:
    """
    Extract keys and/or values from map column into arrays.

    Sub-Group: Parsing & Extraction
    Tags: maps, extraction, complex-types

    Args:
        df: Input DataFrame
        map_column: Map column
        extract_keys: Extract keys to array
        extract_values: Extract values to array

    Usage:
        df = df.transform(extract_map_keys_values, map_column="metadata", extract_keys=True, extract_values=True)
    """
    if extract_keys:
        df = df.withColumn(f"{map_column}_keys", map_keys(col(map_column)))

    if extract_values:
        df = df.withColumn(f"{map_column}_values", map_values(col(map_column)))

    return df


@transformer
def explode_map_column(df: DataFrame, map_column: str, key_column: str = "key", value_column: str = "value") -> DataFrame:
    """
    Explode map column into key-value rows.

    Sub-Group: Parsing & Extraction
    Tags: maps, explode, unnest

    Usage:
        df = df.transform(explode_map_column, map_column="attributes", key_column="attr_name", value_column="attr_value")
    """
    df = df.withColumn("_exploded", explode(map_entries(col(map_column))))

    df = df.select(
        "*",
        col("_exploded.key").alias(key_column),
        col("_exploded.value").alias(value_column)
    ).drop("_exploded", map_column)

    return df


@transformer
def parse_url_parameters(df: DataFrame, url_column: str, target_column: str = "url_params") -> DataFrame:
    """
    Parse URL query parameters into map.

    Sub-Group: Parsing & Extraction
    Tags: url, parsing, parameters

    Args:
        df: Input DataFrame
        url_column: Column containing URL
        target_column: Name for parsed params map

    Usage:
        df = df.transform(parse_url_parameters, url_column="request_url", target_column="params")
    """
    from pyspark.sql.functions import regexp_extract, split as spark_split

    # Extract query string
    df = df.withColumn("_query_string", regexp_extract(col(url_column), r'\?(.+)$', 1))

    # Parse into key-value pairs (simplified - would need more complex logic for real URLs)
    df = df.withColumn(target_column, expr(
        "map_from_arrays(" +
        "transform(split(_query_string, '&'), x -> split(x, '=')[0]), " +
        "transform(split(_query_string, '&'), x -> split(x, '=')[1])" +
        ")"
    ))

    df = df.drop("_query_string")

    return df


@transformer
def flatten_nested_arrays(df: DataFrame, array_column: str, target_column: Optional[str] = None) -> DataFrame:
    """
    Flatten nested arrays (array of arrays) into single array.

    Sub-Group: Parsing & Extraction
    Tags: arrays, flattening, nested

    Args:
        df: Input DataFrame
        array_column: Nested array column
        target_column: Name for flattened array (default: same column)

    Usage:
        df = df.transform(flatten_nested_arrays, array_column="nested_tags", target_column="flat_tags")
    """
    target = target_column or array_column

    df = df.withColumn(target, expr(f"flatten({array_column})"))

    if target != array_column:
        df = df.drop(array_column)

    return df


@transformer
def parse_csv_column(df: DataFrame, csv_column: str, schema: StructType, delimiter: str = ",") -> DataFrame:
    """
    Parse CSV string column into struct.

    Sub-Group: Parsing & Extraction
    Tags: csv, parsing, delimited

    Args:
        df: Input DataFrame
        csv_column: Column containing CSV string
        schema: Schema for CSV data
        delimiter: CSV delimiter

    Usage:
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        schema = StructType([
            StructField("name", StringType()),
            StructField("age", IntegerType())
        ])
        df = df.transform(parse_csv_column, csv_column="csv_data", schema=schema)
    """
    from pyspark.sql.functions import from_csv

    df = df.withColumn(
        f"{csv_column}_parsed",
        from_csv(col(csv_column), schema, {"delimiter": delimiter})
    )

    # Flatten struct
    df = df.select("*", f"{csv_column}_parsed.*").drop(f"{csv_column}_parsed")

    return df


@transformer
def extract_regex_groups(df: DataFrame, column: str, pattern: str, group_names: List[str]) -> DataFrame:
    """
    Extract regex capture groups into separate columns.

    Sub-Group: Parsing & Extraction
    Tags: regex, extraction, parsing

    Args:
        df: Input DataFrame
        column: Column to extract from
        pattern: Regex pattern with capture groups
        group_names: Names for extracted groups

    Usage:
        df = df.transform(extract_regex_groups,
            column="log_line",
            pattern=r"(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] \"(\w+)",
            group_names=["ip_address", "timestamp", "method"]
        )
    """
    from pyspark.sql.functions import regexp_extract

    for i, group_name in enumerate(group_names, start=1):
        df = df.withColumn(group_name, regexp_extract(col(column), pattern, i))

    return df


@transformer
def json_to_struct_array(df: DataFrame, json_array_column: str, element_schema: Optional[StructType] = None) -> DataFrame:
    """
    Parse JSON array string into array of structs.

    Sub-Group: Parsing & Extraction
    Tags: json, arrays, nested

    Args:
        df: Input DataFrame
        json_array_column: Column containing JSON array string
        element_schema: Schema for array elements

    Usage:
        df = df.transform(json_to_struct_array, json_array_column="items_json")
    """
    if element_schema:
        array_schema = ArrayType(element_schema)
        df = df.withColumn(f"{json_array_column}_parsed", from_json(col(json_array_column), array_schema))
    else:
        # Infer schema
        sample = df.select(json_array_column).filter(col(json_array_column).isNotNull()).first()
        if sample and sample[0]:
            inferred_schema = schema_of_json(lit(sample[0]))
            df = df.withColumn(f"{json_array_column}_parsed", from_json(col(json_array_column), inferred_schema))

    return df


@transformer
def struct_to_json(df: DataFrame, struct_column: str, target_column: Optional[str] = None, drop_source: bool = False) -> DataFrame:
    """
    Convert struct/map/array column to JSON string.

    Sub-Group: Parsing & Extraction
    Tags: json, serialization, conversion

    Args:
        df: Input DataFrame
        struct_column: Struct/map/array column to convert
        target_column: Name for JSON column (default: struct_column_json)
        drop_source: Whether to drop source column

    Usage:
        df = df.transform(struct_to_json, struct_column="address", target_column="address_json", drop_source=True)
    """
    target = target_column or f"{struct_column}_json"

    df = df.withColumn(target, to_json(col(struct_column)))

    if drop_source:
        df = df.drop(struct_column)

    return df
