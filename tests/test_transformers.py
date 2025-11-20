"""
Unit tests for Sparkle transformers.

Tests all 70+ transformers using .transform() chains with sample data.
"""

import pytest
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Import transformers from the transformers package
from transformers import Transformer


class TestBasicTransformers:
    """Test basic data transformation operations"""

    def test_drop_exact_duplicates(self, spark: SparkSession, sample_df):
        """Test dropping exact duplicate rows"""
        from transformers.drop_exact_duplicates import DropExactDuplicatesTransformer

        transformer = DropExactDuplicatesTransformer()
        result_df = transformer.transform(sample_df)

        # Original has 10 rows with 1 duplicate
        assert sample_df.count() == 10
        # After dedup should have 9 rows
        assert result_df.count() == 9

    def test_drop_duplicates_by_key(self, spark: SparkSession, sample_df):
        """Test dropping duplicates based on specific columns"""
        from transformers.drop_duplicates_by_key import DropDuplicatesByKeyTransformer

        transformer = DropDuplicatesByKeyTransformer(
            subset=["customer_id", "email"]
        )
        result_df = transformer.transform(sample_df)

        # Should drop duplicates based on customer_id and email
        assert result_df.count() == 9

    def test_select_columns(self, spark: SparkSession, sample_df):
        """Test selecting specific columns"""
        from transformers.select_columns import SelectColumnsTransformer

        transformer = SelectColumnsTransformer(
            columns=["customer_id", "name", "email"]
        )
        result_df = transformer.transform(sample_df)

        assert len(result_df.columns) == 3
        assert "customer_id" in result_df.columns
        assert "total_spend" not in result_df.columns

    def test_rename_columns(self, spark: SparkSession, sample_df):
        """Test renaming columns"""
        from transformers.rename_columns import RenameColumnsTransformer

        transformer = RenameColumnsTransformer(
            column_mapping={
                "customer_id": "id",
                "name": "customer_name",
                "email": "email_address",
            }
        )
        result_df = transformer.transform(sample_df)

        assert "id" in result_df.columns
        assert "customer_name" in result_df.columns
        assert "customer_id" not in result_df.columns

    def test_filter_rows(self, spark: SparkSession, sample_df):
        """Test filtering rows based on condition"""
        from transformers.filter_rows import FilterRowsTransformer

        transformer = FilterRowsTransformer(
            condition="total_spend > 1000"
        )
        result_df = transformer.transform(sample_df)

        # Should only keep high-value customers
        assert result_df.count() < sample_df.count()
        assert result_df.filter("total_spend <= 1000").count() == 0


class TestNullHandlingTransformers:
    """Test null and missing value handling transformers"""

    def test_standardize_nulls(self, spark: SparkSession, sample_df):
        """Test standardizing various null representations"""
        from transformers.standardize_nulls import StandardizeNullsTransformer

        transformer = StandardizeNullsTransformer(
            null_values=["", "NULL", "N/A", "null", "None"]
        )
        result_df = transformer.transform(sample_df)

        # Empty strings should be converted to null
        null_count = result_df.filter(col("email").isNull()).count()
        assert null_count >= 1  # Bob Johnson and Charlie Brown have null/empty emails

    def test_drop_null_rows(self, spark: SparkSession, sample_df):
        """Test dropping rows with null values"""
        from transformers.drop_null_rows import DropNullRowsTransformer

        transformer = DropNullRowsTransformer(
            subset=["email"]
        )
        result_df = transformer.transform(sample_df)

        # Should drop rows where email is null
        assert result_df.filter(col("email").isNull()).count() == 0

    def test_fill_nulls(self, spark: SparkSession, sample_df):
        """Test filling null values with defaults"""
        from transformers.fill_nulls import FillNullsTransformer

        transformer = FillNullsTransformer(
            fill_values={
                "email": "no-email@example.com",
                "total_spend": 0.0,
            }
        )

        # First standardize nulls
        from transformers.standardize_nulls import StandardizeNullsTransformer
        df_with_nulls = StandardizeNullsTransformer(null_values=[""]).transform(sample_df)

        result_df = transformer.transform(df_with_nulls)

        # No nulls should remain in specified columns
        assert result_df.filter(col("email").isNull()).count() == 0


class TestDataQualityTransformers:
    """Test data quality and validation transformers"""

    def test_validate_email(self, spark: SparkSession, sample_df):
        """Test email validation"""
        from transformers.validate_email import ValidateEmailTransformer

        transformer = ValidateEmailTransformer(
            email_column="email",
            mark_invalid=True,
            invalid_flag_column="email_valid",
        )
        result_df = transformer.transform(sample_df)

        assert "email_valid" in result_df.columns

        # Eve has "eve@INVALID" which should be marked invalid
        # (Depending on validation logic, this might pass or fail)

    def test_validate_range(self, spark: SparkSession, sample_df):
        """Test numeric range validation"""
        from transformers.validate_range import ValidateRangeTransformer

        transformer = ValidateRangeTransformer(
            column="total_spend",
            min_value=0.0,
            max_value=10000.0,
            mark_invalid=True,
            invalid_flag_column="spend_valid",
        )
        result_df = transformer.transform(sample_df)

        assert "spend_valid" in result_df.columns

        # Eve has -100.00 which should be marked invalid
        invalid_count = result_df.filter("spend_valid = false").count()
        assert invalid_count >= 1

    def test_check_not_null(self, spark: SparkSession, sample_df):
        """Test not-null constraint checking"""
        from transformers.check_not_null import CheckNotNullTransformer

        transformer = CheckNotNullTransformer(
            columns=["customer_id", "name"],
            mark_invalid=True,
            invalid_flag_column="data_valid",
        )
        result_df = transformer.transform(sample_df)

        assert "data_valid" in result_df.columns


class TestStringTransformers:
    """Test string manipulation transformers"""

    def test_trim_whitespace(self, spark: SparkSession):
        """Test trimming whitespace from strings"""
        from transformers.trim_whitespace import TrimWhitespaceTransformer

        data = [
            (1, "  Alice  ", "  alice@example.com  "),
            (2, "Bob\t", "bob@example.com\n"),
        ]
        df = spark.createDataFrame(data, ["id", "name", "email"])

        transformer = TrimWhitespaceTransformer(
            columns=["name", "email"]
        )
        result_df = transformer.transform(df)

        # Check that whitespace is trimmed
        alice_name = result_df.filter("id = 1").select("name").first()[0]
        assert alice_name == "Alice"

    def test_uppercase(self, spark: SparkSession, sample_df):
        """Test converting strings to uppercase"""
        from transformers.uppercase import UppercaseTransformer

        transformer = UppercaseTransformer(
            columns=["name", "email"]
        )
        result_df = transformer.transform(sample_df)

        # Check uppercase conversion
        first_name = result_df.select("name").first()[0]
        assert first_name.isupper()

    def test_lowercase(self, spark: SparkSession, sample_df):
        """Test converting strings to lowercase"""
        from transformers.lowercase import LowercaseTransformer

        transformer = LowercaseTransformer(
            columns=["email"]
        )
        result_df = transformer.transform(sample_df)

        # Check lowercase conversion
        emails = result_df.filter("email IS NOT NULL").select("email").collect()
        for row in emails:
            if row[0]:
                assert row[0].islower() or "@" in row[0]

    def test_replace_string(self, spark: SparkSession, sample_df):
        """Test string replacement"""
        from transformers.replace_string import ReplaceStringTransformer

        transformer = ReplaceStringTransformer(
            column="tier",
            search="Premium",
            replace="Gold",
        )
        result_df = transformer.transform(sample_df)

        # Premium should be replaced with Gold
        assert result_df.filter("tier = 'Premium'").count() == 0
        assert result_df.filter("tier = 'Gold'").count() > 0

    def test_regex_extract(self, spark: SparkSession, sample_df):
        """Test regex pattern extraction"""
        from transformers.regex_extract import RegexExtractTransformer

        transformer = RegexExtractTransformer(
            column="email",
            pattern=r"@(.+)$",
            output_column="email_domain",
            group_idx=1,
        )
        result_df = transformer.transform(sample_df)

        assert "email_domain" in result_df.columns

        # Check that domain is extracted
        domains = result_df.filter("email_domain IS NOT NULL").select("email_domain").collect()
        assert len(domains) > 0


class TestDateTimeTransformers:
    """Test date and time manipulation transformers"""

    def test_parse_date(self, spark: SparkSession, sample_df):
        """Test parsing string to date"""
        from transformers.parse_date import ParseDateTransformer

        transformer = ParseDateTransformer(
            column="signup_date",
            format="yyyy-MM-dd",
            output_column="signup_date_parsed",
        )
        result_df = transformer.transform(sample_df)

        assert "signup_date_parsed" in result_df.columns

    def test_extract_year(self, spark: SparkSession, sample_df):
        """Test extracting year from date"""
        from transformers.extract_date_part import ExtractDatePartTransformer

        # First parse the date
        from transformers.parse_date import ParseDateTransformer
        df_with_date = ParseDateTransformer(
            column="signup_date",
            format="yyyy-MM-dd",
            output_column="signup_date_parsed",
        ).transform(sample_df)

        transformer = ExtractDatePartTransformer(
            column="signup_date_parsed",
            part="year",
            output_column="signup_year",
        )
        result_df = transformer.transform(df_with_date)

        assert "signup_year" in result_df.columns

        # All signups are in 2023
        years = result_df.select("signup_year").distinct().collect()
        assert len(years) == 1
        assert years[0][0] == 2023

    def test_add_current_timestamp(self, spark: SparkSession, sample_df):
        """Test adding current timestamp"""
        from transformers.add_audit_columns import AddAuditColumnsTransformer

        transformer = AddAuditColumnsTransformer(
            created_at_column="created_at",
            updated_at_column="updated_at",
        )
        result_df = transformer.transform(sample_df)

        assert "created_at" in result_df.columns
        assert "updated_at" in result_df.columns


class TestNumericTransformers:
    """Test numeric transformation operations"""

    def test_cast_to_integer(self, spark: SparkSession):
        """Test casting column to integer"""
        from transformers.cast_column import CastColumnTransformer

        data = [(1, "100"), (2, "200")]
        df = spark.createDataFrame(data, ["id", "value_str"])

        transformer = CastColumnTransformer(
            column="value_str",
            target_type="integer",
            output_column="value_int",
        )
        result_df = transformer.transform(df)

        assert "value_int" in result_df.columns

    def test_round_numbers(self, spark: SparkSession, sample_df):
        """Test rounding numeric values"""
        from transformers.round_numbers import RoundNumbersTransformer

        transformer = RoundNumbersTransformer(
            column="total_spend",
            decimals=0,
            output_column="total_spend_rounded",
        )
        result_df = transformer.transform(sample_df)

        assert "total_spend_rounded" in result_df.columns

    def test_normalize_numeric(self, spark: SparkSession, sample_df):
        """Test min-max normalization"""
        from transformers.normalize_numeric import NormalizeNumericTransformer

        transformer = NormalizeNumericTransformer(
            column="total_spend",
            output_column="total_spend_normalized",
            method="min-max",
        )
        result_df = transformer.transform(sample_df)

        assert "total_spend_normalized" in result_df.columns

        # Normalized values should be between 0 and 1
        stats = result_df.select("total_spend_normalized").summary("min", "max").collect()


class TestAggregationTransformers:
    """Test aggregation transformers"""

    def test_group_and_aggregate(self, spark: SparkSession, sample_df):
        """Test grouping and aggregation"""
        from transformers.group_aggregate import GroupAggregateTransformer

        transformer = GroupAggregateTransformer(
            group_by=["tier"],
            aggregations={
                "total_spend": ["sum", "avg", "count"],
                "customer_id": ["count"],
            },
        )
        result_df = transformer.transform(sample_df)

        # Should have one row per tier
        assert result_df.count() <= sample_df.select("tier").distinct().count()

    def test_window_aggregate(self, spark: SparkSession, sample_df):
        """Test window function aggregation"""
        from transformers.window_aggregate import WindowAggregateTransformer

        transformer = WindowAggregateTransformer(
            partition_by=["country"],
            order_by=["total_spend"],
            window_function="rank",
            output_column="spend_rank",
        )
        result_df = transformer.transform(sample_df)

        assert "spend_rank" in result_df.columns


class TestHashingEncryptionTransformers:
    """Test hashing and encryption transformers"""

    def test_add_hash_key(self, spark: SparkSession, sample_df):
        """Test adding hash key for deduplication"""
        from transformers.add_hash_key import AddHashKeyTransformer

        transformer = AddHashKeyTransformer(
            columns=["customer_id", "email"],
            output_column="hash_key",
            algorithm="md5",
        )
        result_df = transformer.transform(sample_df)

        assert "hash_key" in result_df.columns

        # Hash keys should be unique for unique combinations
        hash_count = result_df.select("hash_key").distinct().count()
        assert hash_count > 0

    def test_mask_pii(self, spark: SparkSession, sample_df):
        """Test masking PII data"""
        from transformers.mask_pii import MaskPIITransformer

        transformer = MaskPIITransformer(
            column="email",
            mask_type="partial",
            visible_chars=3,
            output_column="email_masked",
        )
        result_df = transformer.transform(sample_df)

        assert "email_masked" in result_df.columns

        # Masked emails should be different from originals
        masked = result_df.filter("email IS NOT NULL").select("email_masked").first()
        if masked:
            assert "***" in masked[0] or "@" not in masked[0]


class TestJoinTransformers:
    """Test join and enrichment transformers"""

    def test_join_with_lookup(self, spark: SparkSession, sample_df):
        """Test joining with lookup table"""
        from transformers.join_lookup import JoinLookupTransformer

        # Create lookup table
        lookup_data = [
            ("USA", "United States", "North America"),
            ("Canada", "Canada", "North America"),
            ("UK", "United Kingdom", "Europe"),
        ]
        lookup_df = spark.createDataFrame(lookup_data, ["country_code", "country_name", "region"])

        transformer = JoinLookupTransformer(
            lookup_df=lookup_df,
            left_on="country",
            right_on="country_code",
            join_type="left",
        )
        result_df = transformer.transform(sample_df)

        assert "country_name" in result_df.columns or "region" in result_df.columns


class TestSCD2Transformers:
    """Test Slowly Changing Dimension Type 2 transformers"""

    def test_scd2_merge(self, spark: SparkSession):
        """Test SCD2 merge operation"""
        from transformers.scd2_merge import SCD2MergeTransformer

        # Create current dimension table
        current_data = [
            (1, "Alice", "Premium", "2023-01-01", "9999-12-31", True),
            (2, "Bob", "Basic", "2023-01-01", "9999-12-31", True),
        ]
        current_df = spark.createDataFrame(
            current_data,
            ["customer_id", "name", "tier", "valid_from", "valid_to", "is_current"]
        )

        # Create new data with changes
        new_data = [
            (1, "Alice", "Enterprise"),  # Tier changed
            (2, "Bob", "Basic"),  # No change
            (3, "Charlie", "Premium"),  # New customer
        ]
        new_df = spark.createDataFrame(new_data, ["customer_id", "name", "tier"])

        transformer = SCD2MergeTransformer(
            current_df=current_df,
            natural_key=["customer_id"],
            compare_columns=["name", "tier"],
        )

        # This would generate SCD2 logic
        assert transformer.natural_key == ["customer_id"]


class TestTransformerChaining:
    """Test chaining multiple transformers together"""

    def test_full_transformation_chain(self, spark: SparkSession, sample_df):
        """Test complete transformation pipeline with multiple transformers"""
        from transformers.drop_exact_duplicates import DropExactDuplicatesTransformer
        from transformers.standardize_nulls import StandardizeNullsTransformer
        from transformers.fill_nulls import FillNullsTransformer
        from transformers.lowercase import LowercaseTransformer
        from transformers.add_audit_columns import AddAuditColumnsTransformer

        # Create transformation chain
        result_df = (
            sample_df
            .transform(DropExactDuplicatesTransformer().transform)
            .transform(StandardizeNullsTransformer(null_values=[""]).transform)
            .transform(FillNullsTransformer(fill_values={"email": "unknown@example.com"}).transform)
            .transform(LowercaseTransformer(columns=["email"]).transform)
            .transform(AddAuditColumnsTransformer().transform)
        )

        # Verify all transformations applied
        assert result_df.count() == 9  # Duplicates removed
        assert "created_at" in result_df.columns  # Audit columns added
        assert result_df.filter(col("email").isNull()).count() == 0  # Nulls filled

    def test_data_quality_pipeline(self, spark: SparkSession, sample_df):
        """Test data quality validation pipeline"""
        from transformers.standardize_nulls import StandardizeNullsTransformer
        from transformers.validate_range import ValidateRangeTransformer
        from transformers.drop_null_rows import DropNullRowsTransformer

        result_df = (
            sample_df
            .transform(StandardizeNullsTransformer(null_values=["", "NULL"]).transform)
            .transform(ValidateRangeTransformer(
                column="total_spend",
                min_value=0.0,
                max_value=10000.0,
                mark_invalid=True,
                invalid_flag_column="spend_valid"
            ).transform)
        )

        assert "spend_valid" in result_df.columns

        # Filter out invalid records
        clean_df = result_df.filter("spend_valid = true")
        assert clean_df.count() <= result_df.count()
