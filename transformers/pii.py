"""
PII (Personally Identifiable Information) transformers.

Functions for masking, tokenization, and anonymization of sensitive data.
"""

from typing import List, Optional, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, regexp_replace, md5, sha2, substring, concat, length
from .base import transformer


@transformer
def mask_column(df: DataFrame, columns: List[str], mask_char: str = "*", visible_chars: int = 0, visible_position: str = "start") -> DataFrame:
    """
    Mask sensitive columns.

    Args:
        df: Input DataFrame
        columns: Columns to mask
        mask_char: Character to use for masking
        visible_chars: Number of characters to leave visible
        visible_position: 'start' or 'end' - where to show visible chars

    Usage:
        df = df.transform(mask_column, columns=["ssn"], mask_char="*", visible_chars=4, visible_position="end")
    """
    for column in columns:
        if column in df.columns:
            col_length = length(col(column))

            if visible_chars > 0:
                if visible_position == "end":
                    # Show last N chars
                    masked = concat(
                        lit(mask_char).repeat(col_length - visible_chars),
                        substring(col(column), -visible_chars, visible_chars)
                    )
                else:
                    # Show first N chars
                    masked = concat(
                        substring(col(column), 1, visible_chars),
                        lit(mask_char).repeat(col_length - visible_chars)
                    )
            else:
                # Mask everything
                masked = lit(mask_char).repeat(col_length)

            df = df.withColumn(column, masked)

    return df


@transformer
def tokenize_column(df: DataFrame, columns: List[str], algorithm: str = "sha256", salt: Optional[str] = None) -> DataFrame:
    """
    Create irreversible tokens from sensitive columns.

    Args:
        df: Input DataFrame
        columns: Columns to tokenize
        algorithm: 'md5', 'sha256', 'sha512'
        salt: Optional salt value

    Usage:
        df = df.transform(tokenize_column, columns=["email", "phone"], algorithm="sha256", salt="my_secret_salt")
    """
    for column in columns:
        if column in df.columns:
            value = col(column)

            if salt:
                value = concat(value, lit(salt))

            if algorithm == "md5":
                df = df.withColumn(column, md5(value))
            elif algorithm in ["sha256", "sha512"]:
                bits = int(algorithm.replace("sha", ""))
                df = df.withColumn(column, sha2(value, bits))

    return df


@transformer
def redact_email(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Redact email addresses while preserving domain.

    Usage:
        df = df.transform(redact_email, columns=["email"])
        # john.doe@example.com -> ****@example.com
    """
    for column in columns:
        if column in df.columns:
            df = df.withColumn(
                column,
                concat(lit("****@"), regexp_extract(col(column), r'@(.+)', 1))
            )

    return df


@transformer
def anonymize_with_lookup(df: DataFrame, column: str, lookup_dict: Dict[str, str]) -> DataFrame:
    """
    Anonymize using lookup dictionary.

    Usage:
        df = df.transform(anonymize_with_lookup, column="customer_id", lookup_dict={"C001": "ANON_001", "C002": "ANON_002"})
    """
    mapping_expr = None

    for original, anonymized in lookup_dict.items():
        if mapping_expr is None:
            mapping_expr = when(col(column) == original, lit(anonymized))
        else:
            mapping_expr = mapping_expr.when(col(column) == original, lit(anonymized))

    mapping_expr = mapping_expr.otherwise(col(column))

    return df.withColumn(column, mapping_expr)


@transformer
def remove_pii_columns(df: DataFrame, pii_columns: List[str]) -> DataFrame:
    """
    Drop PII columns entirely.

    Usage:
        df = df.transform(remove_pii_columns, pii_columns=["ssn", "credit_card", "password"])
    """
    return df.drop(*[c for c in pii_columns if c in df.columns])
