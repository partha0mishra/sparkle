"""
Sparkle Transformers - Pure, Stateless DataFrame Transformations

Core utilities and base classes for composable transformations.
"""

from typing import Callable, List, Optional, Any, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import functools
import logging


logger = logging.getLogger("sparkle.transformers")


def transformer(func: Callable) -> Callable:
    """
    Decorator that marks a function as a transformer.

    Transformers must have signature: (df: DataFrame, **kwargs) -> DataFrame

    Usage:
        @transformer
        def my_transform(df: DataFrame, column: str) -> DataFrame:
            return df.withColumn(column, col(column).cast("string"))
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        df = args[0] if args else kwargs.get('df')
        if not isinstance(df, DataFrame):
            raise ValueError(f"First argument to transformer must be DataFrame, got {type(df)}")

        result = func(*args, **kwargs)

        if not isinstance(result, DataFrame):
            raise ValueError(f"Transformer must return DataFrame, got {type(result)}")

        return result

    wrapper._is_transformer = True
    return wrapper


def multi_output_transformer(func: Callable) -> Callable:
    """
    Decorator for transformers that return multiple DataFrames.

    Usage:
        @multi_output_transformer
        def split_by_type(df: DataFrame) -> Dict[str, DataFrame]:
            return {
                "type_a": df.filter(col("type") == "A"),
                "type_b": df.filter(col("type") == "B")
            }
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        df = args[0] if args else kwargs.get('df')
        if not isinstance(df, DataFrame):
            raise ValueError(f"First argument must be DataFrame, got {type(df)}")

        result = func(*args, **kwargs)

        if isinstance(result, dict):
            for key, value in result.items():
                if not isinstance(value, DataFrame):
                    raise ValueError(f"All outputs must be DataFrames, got {type(value)} for key '{key}'")
        elif isinstance(result, (list, tuple)):
            for i, value in enumerate(result):
                if not isinstance(value, DataFrame):
                    raise ValueError(f"All outputs must be DataFrames, got {type(value)} at index {i}")

        return result

    wrapper._is_multi_output_transformer = True
    return wrapper


class TransformationPipeline:
    """
    Chain multiple transformers into a pipeline.

    Usage:
        pipeline = TransformationPipeline([
            (drop_exact_duplicates, {}),
            (standardize_nulls, {}),
            (trim_and_clean_strings, {"columns": ["name", "email"]})
        ])

        result = pipeline.apply(df)
    """

    def __init__(self, transformers: List[tuple]):
        """
        Initialize pipeline.

        Args:
            transformers: List of (transformer_func, kwargs) tuples
        """
        self.transformers = transformers

    def apply(self, df: DataFrame) -> DataFrame:
        """Apply all transformers in sequence."""
        result = df

        for i, (transformer_func, kwargs) in enumerate(self.transformers):
            logger.info(f"Applying transformer {i+1}/{len(self.transformers)}: {transformer_func.__name__}")
            result = transformer_func(result, **kwargs)

        return result

    def add(self, transformer_func: Callable, **kwargs):
        """Add a transformer to the pipeline."""
        self.transformers.append((transformer_func, kwargs))
        return self


def apply_transformers(df: DataFrame, *transformers) -> DataFrame:
    """
    Apply a sequence of transformers using DataFrame.transform().

    Usage:
        result = apply_transformers(
            df,
            drop_exact_duplicates,
            standardize_nulls,
            lambda df: trim_and_clean_strings(df, columns=["name"])
        )
    """
    result = df
    for transformer in transformers:
        if callable(transformer):
            result = result.transform(transformer)
        else:
            raise ValueError(f"Transformer must be callable, got {type(transformer)}")

    return result


def get_transformation_metadata(df: DataFrame) -> Dict[str, Any]:
    """
    Get metadata about a DataFrame for transformation tracking.

    Returns:
        Dictionary with schema, row count, column count, etc.
    """
    return {
        "schema": df.schema.simpleString(),
        "columns": df.columns,
        "column_count": len(df.columns),
        "estimated_row_count": df.count() if df.isStreaming == False else None,
        "is_streaming": df.isStreaming
    }


def validate_required_columns(df: DataFrame, required_columns: List[str]):
    """
    Validate that all required columns exist in DataFrame.

    Raises:
        ValueError: If any required column is missing
    """
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def safe_transform(df: DataFrame, transformer_func: Callable, **kwargs) -> DataFrame:
    """
    Apply a transformer with error handling.

    Args:
        df: Input DataFrame
        transformer_func: Transformer function
        **kwargs: Arguments to transformer

    Returns:
        Transformed DataFrame, or original on error
    """
    try:
        return transformer_func(df, **kwargs)
    except Exception as e:
        logger.error(f"Transformer {transformer_func.__name__} failed: {e}")
        logger.warning("Returning original DataFrame")
        return df


class ConditionalTransformer:
    """
    Apply transformer only if condition is met.

    Usage:
        conditional = ConditionalTransformer(
            condition=lambda df: df.count() > 1000,
            transformer=sample_data,
            kwargs={"fraction": 0.1}
        )

        result = conditional.apply(df)
    """

    def __init__(self, condition: Callable[[DataFrame], bool], transformer: Callable, **kwargs):
        """
        Initialize conditional transformer.

        Args:
            condition: Function that takes DataFrame and returns bool
            transformer: Transformer to apply if condition is True
            **kwargs: Arguments to pass to transformer
        """
        self.condition = condition
        self.transformer = transformer
        self.kwargs = kwargs

    def apply(self, df: DataFrame) -> DataFrame:
        """Apply transformer if condition is met."""
        if self.condition(df):
            logger.info(f"Condition met, applying {self.transformer.__name__}")
            return self.transformer(df, **self.kwargs)
        else:
            logger.info(f"Condition not met, skipping {self.transformer.__name__}")
            return df
