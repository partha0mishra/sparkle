"""
Date and time transformation functions.

Comprehensive date/time operations for parsing, conversion, arithmetic, and extraction.
"""

from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, to_date, to_timestamp, date_format, year, month, dayofmonth,
    dayofweek, dayofyear, weekofyear, quarter, hour, minute, second,
    unix_timestamp, from_unixtime, date_add, date_sub, datediff,
    months_between, add_months, next_day, last_day, trunc, lit,
    when, current_date, current_timestamp
)
from .base import transformer


@transformer
def parse_date_string(df: DataFrame, column: str, format: str, target_column: Optional[str] = None) -> DataFrame:
    """
    Parse string column to date using specified format.

    Sub-Group: Date & Time Mastery
    Tags: datetime, parsing, conversion

    Args:
        df: Input DataFrame
        column: String column containing dates
        format: Date format pattern (e.g., 'yyyy-MM-dd', 'MM/dd/yyyy')
        target_column: Name for parsed date column (default: column_date)

    Usage:
        df = df.transform(parse_date_string, column="date_str", format="yyyy-MM-dd")
        df = df.transform(parse_date_string, column="order_date", format="MM/dd/yyyy", target_column="parsed_date")
    """
    target = target_column or f"{column}_date"
    return df.withColumn(target, to_date(col(column), format))


@transformer
def parse_timestamp_string(df: DataFrame, column: str, format: str, target_column: Optional[str] = None) -> DataFrame:
    """
    Parse string column to timestamp using specified format.

    Sub-Group: Date & Time Mastery
    Tags: datetime, parsing, timestamp

    Args:
        df: Input DataFrame
        column: String column containing timestamps
        format: Timestamp format pattern (e.g., 'yyyy-MM-dd HH:mm:ss')
        target_column: Name for parsed timestamp column (default: column_timestamp)

    Usage:
        df = df.transform(parse_timestamp_string, column="created_at", format="yyyy-MM-dd HH:mm:ss")
    """
    target = target_column or f"{column}_timestamp"
    return df.withColumn(target, to_timestamp(col(column), format))


@transformer
def convert_unix_timestamp(df: DataFrame, column: str, target_column: Optional[str] = None, to_unix: bool = False) -> DataFrame:
    """
    Convert between Unix timestamp and datetime.

    Sub-Group: Date & Time Mastery
    Tags: datetime, unix, conversion

    Args:
        df: Input DataFrame
        column: Source column
        target_column: Target column name
        to_unix: If True, convert datetime to unix; if False, convert unix to datetime

    Usage:
        df = df.transform(convert_unix_timestamp, column="timestamp", to_unix=False)
        df = df.transform(convert_unix_timestamp, column="event_time", target_column="unix_time", to_unix=True)
    """
    target = target_column or (f"{column}_unix" if to_unix else f"{column}_datetime")

    if to_unix:
        return df.withColumn(target, unix_timestamp(col(column)))
    else:
        return df.withColumn(target, from_unixtime(col(column)))


@transformer
def format_datetime(df: DataFrame, column: str, format: str, target_column: Optional[str] = None) -> DataFrame:
    """
    Format datetime column to string with specified pattern.

    Sub-Group: Date & Time Mastery
    Tags: datetime, formatting, string

    Args:
        df: Input DataFrame
        column: Datetime column
        format: Output format pattern (e.g., 'yyyy-MM-dd', 'MMM dd, yyyy')
        target_column: Name for formatted column (default: column_formatted)

    Usage:
        df = df.transform(format_datetime, column="order_date", format="yyyy-MM-dd")
        df = df.transform(format_datetime, column="created_at", format="MMM dd, yyyy HH:mm", target_column="display_date")
    """
    target = target_column or f"{column}_formatted"
    return df.withColumn(target, date_format(col(column), format))


@transformer
def extract_date_parts(df: DataFrame, column: str, parts: Optional[List[str]] = None, prefix: str = "") -> DataFrame:
    """
    Extract date/time components (year, month, day, hour, etc.).

    Sub-Group: Date & Time Mastery
    Tags: datetime, extraction, components

    Args:
        df: Input DataFrame
        column: Datetime column
        parts: List of parts to extract (year, month, day, hour, minute, second, dayofweek, quarter)
        prefix: Prefix for new columns

    Usage:
        df = df.transform(extract_date_parts, column="order_date", parts=["year", "month", "day"])
        df = df.transform(extract_date_parts, column="timestamp", parts=["hour", "minute"], prefix="event_")
    """
    if parts is None:
        parts = ["year", "month", "day"]

    part_functions = {
        "year": year,
        "month": month,
        "day": dayofmonth,
        "dayofweek": dayofweek,
        "dayofyear": dayofyear,
        "weekofyear": weekofyear,
        "quarter": quarter,
        "hour": hour,
        "minute": minute,
        "second": second
    }

    for part in parts:
        if part in part_functions:
            df = df.withColumn(f"{prefix}{part}", part_functions[part](col(column)))

    return df


@transformer
def add_days_to_date(df: DataFrame, column: str, days: int, target_column: Optional[str] = None) -> DataFrame:
    """
    Add or subtract days from a date column.

    Sub-Group: Date & Time Mastery
    Tags: datetime, arithmetic, date-math

    Args:
        df: Input DataFrame
        column: Date column
        days: Number of days to add (negative to subtract)
        target_column: Name for new date column (default: column_adjusted)

    Usage:
        df = df.transform(add_days_to_date, column="order_date", days=30, target_column="expected_delivery")
        df = df.transform(add_days_to_date, column="start_date", days=-7, target_column="prior_week")
    """
    target = target_column or f"{column}_adjusted"

    if days >= 0:
        return df.withColumn(target, date_add(col(column), days))
    else:
        return df.withColumn(target, date_sub(col(column), abs(days)))


@transformer
def add_months_to_date(df: DataFrame, column: str, months: int, target_column: Optional[str] = None) -> DataFrame:
    """
    Add or subtract months from a date column.

    Sub-Group: Date & Time Mastery
    Tags: datetime, arithmetic, date-math

    Args:
        df: Input DataFrame
        column: Date column
        months: Number of months to add (negative to subtract)
        target_column: Name for new date column (default: column_adjusted)

    Usage:
        df = df.transform(add_months_to_date, column="subscription_start", months=12, target_column="renewal_date")
        df = df.transform(add_months_to_date, column="report_date", months=-1, target_column="prior_month")
    """
    target = target_column or f"{column}_adjusted"
    return df.withColumn(target, add_months(col(column), months))


@transformer
def calculate_date_diff(df: DataFrame, start_column: str, end_column: str, target_column: str = "days_diff", unit: str = "days") -> DataFrame:
    """
    Calculate difference between two dates.

    Sub-Group: Date & Time Mastery
    Tags: datetime, arithmetic, difference

    Args:
        df: Input DataFrame
        start_column: Start date column
        end_column: End date column
        target_column: Name for difference column
        unit: Unit of difference ('days' or 'months')

    Usage:
        df = df.transform(calculate_date_diff, start_column="start_date", end_column="end_date", target_column="duration_days")
        df = df.transform(calculate_date_diff, start_column="hire_date", end_column="term_date", target_column="tenure_months", unit="months")
    """
    if unit == "days":
        return df.withColumn(target_column, datediff(col(end_column), col(start_column)))
    elif unit == "months":
        return df.withColumn(target_column, months_between(col(end_column), col(start_column)))
    else:
        raise ValueError(f"Unsupported unit: {unit}. Use 'days' or 'months'")


@transformer
def get_next_day_of_week(df: DataFrame, column: str, day_of_week: str, target_column: Optional[str] = None) -> DataFrame:
    """
    Get next occurrence of specified day of week.

    Sub-Group: Date & Time Mastery
    Tags: datetime, calendar, business-logic

    Args:
        df: Input DataFrame
        column: Date column
        day_of_week: Day name (Mon, Tue, Wed, Thu, Fri, Sat, Sun)
        target_column: Name for new date column (default: next_{day})

    Usage:
        df = df.transform(get_next_day_of_week, column="order_date", day_of_week="Mon", target_column="next_monday")
        df = df.transform(get_next_day_of_week, column="current_date", day_of_week="Fri")
    """
    target = target_column or f"next_{day_of_week.lower()}"
    return df.withColumn(target, next_day(col(column), day_of_week))


@transformer
def get_last_day_of_month(df: DataFrame, column: str, target_column: Optional[str] = None) -> DataFrame:
    """
    Get last day of the month for given date.

    Sub-Group: Date & Time Mastery
    Tags: datetime, calendar, month-end

    Args:
        df: Input DataFrame
        column: Date column
        target_column: Name for new date column (default: column_month_end)

    Usage:
        df = df.transform(get_last_day_of_month, column="order_date", target_column="month_end")
    """
    target = target_column or f"{column}_month_end"
    return df.withColumn(target, last_day(col(column)))


@transformer
def truncate_to_period(df: DataFrame, column: str, period: str, target_column: Optional[str] = None) -> DataFrame:
    """
    Truncate datetime to start of period (year, month, week, day, hour).

    Sub-Group: Date & Time Mastery
    Tags: datetime, truncation, rounding

    Args:
        df: Input DataFrame
        column: Datetime column
        period: Period to truncate to ('year', 'month', 'week', 'day', 'hour')
        target_column: Name for truncated column (default: column_trunc)

    Usage:
        df = df.transform(truncate_to_period, column="order_timestamp", period="month", target_column="order_month")
        df = df.transform(truncate_to_period, column="event_time", period="hour", target_column="event_hour")
    """
    target = target_column or f"{column}_trunc"

    valid_periods = ['year', 'yyyy', 'yy', 'month', 'mm', 'mon', 'week', 'day', 'dd', 'hour', 'hh']
    if period.lower() not in valid_periods:
        raise ValueError(f"Invalid period: {period}. Must be one of {valid_periods}")

    return df.withColumn(target, trunc(col(column), period))


@transformer
def add_age_category(df: DataFrame, date_column: str, reference_date: Optional[str] = None,
                    target_column: str = "age_category", categories: Optional[List[tuple]] = None) -> DataFrame:
    """
    Categorize age/tenure based on date difference.

    Sub-Group: Date & Time Mastery
    Tags: datetime, categorization, business-logic

    Args:
        df: Input DataFrame
        date_column: Date column to calculate age from
        reference_date: Reference date (default: current_date)
        target_column: Name for category column
        categories: List of (days_threshold, label) tuples

    Usage:
        df = df.transform(add_age_category,
            date_column="created_at",
            target_column="record_age",
            categories=[(7, "New"), (30, "Recent"), (90, "Old"), (float('inf'), "Very Old")]
        )
    """
    if categories is None:
        categories = [
            (30, "0-30 days"),
            (90, "31-90 days"),
            (180, "91-180 days"),
            (365, "181-365 days"),
            (float('inf'), "Over 1 year")
        ]

    ref_date = current_date() if reference_date is None else lit(reference_date)
    days_diff = datediff(ref_date, col(date_column))

    # Build when-otherwise chain
    category_expr = None
    for threshold, label in sorted(categories, key=lambda x: x[0]):
        if category_expr is None:
            category_expr = when(days_diff <= threshold, lit(label))
        else:
            category_expr = category_expr.when(days_diff <= threshold, lit(label))

    category_expr = category_expr.otherwise(lit("Unknown"))

    return df.withColumn(target_column, category_expr)
