"""
Data Quality transformers for standardization and cleaning.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sparkle.config_schema import config_schema, Field, BaseConfigSchema


@config_schema
class StandardizeNullsConfig(BaseConfigSchema):
    """Configuration for standardizing null values."""

    null_representations: list[str] = Field(
        ["null", "NULL", "None", "NONE", "N/A", "n/a", "", " "],
        description="String values to convert to null",
        min_length=1,
        group="Null Handling",
        order=1,
    )
    columns: list[str] = Field(
        None,
        description="Columns to process (null = all string columns)",
        group="Null Handling",
        order=2,
    )
    trim_whitespace: bool = Field(
        True,
        description="Trim whitespace before checking for null representations",
        group="Null Handling",
        order=3,
    )


@config_schema
class TrimAndCleanConfig(BaseConfigSchema):
    """Configuration for trimming and cleaning strings."""

    columns: list[str] = Field(
        None,
        description="Columns to clean (null = all string columns)",
        group="Cleaning",
        order=1,
    )
    trim_whitespace: bool = Field(
        True,
        description="Remove leading/trailing whitespace",
        group="Cleaning",
        order=2,
    )
    remove_extra_spaces: bool = Field(
        True,
        description="Replace multiple spaces with single space",
        group="Cleaning",
        order=3,
    )
    lowercase: bool = Field(
        False,
        description="Convert to lowercase",
        group="Cleaning",
        order=4,
    )
    uppercase: bool = Field(
        False,
        description="Convert to uppercase",
        group="Cleaning",
        order=5,
    )
    remove_special_chars: bool = Field(
        False,
        description="Remove special characters (keep only alphanumeric and spaces)",
        group="Cleaning",
        order=6,
    )


def standardize_nulls(df: DataFrame, config: dict) -> DataFrame:
    """
    Standardize null value representations.

    Tags: data-quality, cleaning, nulls
    Category: Data Quality
    Icon: clean
    """
    cfg = StandardizeNullsConfig(**config) if isinstance(config, dict) else config

    # Get columns to process
    if cfg.columns:
        cols_to_process = cfg.columns
    else:
        # Process all string columns
        cols_to_process = [field.name for field in df.schema.fields if str(field.dataType) == "StringType"]

    # Apply standardization
    for col in cols_to_process:
        if col in df.columns:
            # Build condition for null representations
            if cfg.trim_whitespace:
                condition = F.trim(F.col(col)).isin(cfg.null_representations)
            else:
                condition = F.col(col).isin(cfg.null_representations)

            # Replace with null
            df = df.withColumn(col, F.when(condition, None).otherwise(F.col(col)))

    return df


standardize_nulls.config_schema = lambda: StandardizeNullsConfig.config_schema()
standardize_nulls.sample_config = lambda: {
    "null_representations": ["null", "NULL", "N/A", ""],
    "columns": None,
    "trim_whitespace": True,
}


def trim_and_clean_strings(df: DataFrame, config: dict) -> DataFrame:
    """
    Trim and clean string columns.

    Tags: data-quality, cleaning, strings, trimming
    Category: Data Quality
    Icon: clean
    """
    cfg = TrimAndCleanConfig(**config) if isinstance(config, dict) else config

    # Get columns to process
    if cfg.columns:
        cols_to_process = cfg.columns
    else:
        # Process all string columns
        cols_to_process = [field.name for field in df.schema.fields if str(field.dataType) == "StringType"]

    # Apply cleaning
    for col in cols_to_process:
        if col in df.columns:
            expr = F.col(col)

            if cfg.trim_whitespace:
                expr = F.trim(expr)

            if cfg.remove_extra_spaces:
                expr = F.regexp_replace(expr, r'\s+', ' ')

            if cfg.lowercase:
                expr = F.lower(expr)
            elif cfg.uppercase:
                expr = F.upper(expr)

            if cfg.remove_special_chars:
                expr = F.regexp_replace(expr, r'[^a-zA-Z0-9\s]', '')

            df = df.withColumn(col, expr)

    return df


trim_and_clean_strings.config_schema = lambda: TrimAndCleanConfig.config_schema()
trim_and_clean_strings.sample_config = lambda: {
    "columns": None,
    "trim_whitespace": True,
    "remove_extra_spaces": True,
    "lowercase": False,
    "uppercase": False,
    "remove_special_chars": False,
}
