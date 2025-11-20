"""
Integration tests for Sparkle framework.

Tests end-to-end workflows using pytest-spark and local Delta tables.
Tests complete pipelines: ingest → transform → write → ML → scoring
"""

import pytest
import tempfile
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum


class TestBronzeIngestionIntegration:
    """Test bronze layer ingestion end-to-end"""

    def test_csv_to_bronze_delta_table(self, spark: SparkSession, tmp_path: Path):
        """Test CSV ingestion to Bronze Delta table"""
        # Create source CSV
        csv_file = tmp_path / "source.csv"
        csv_content = """customer_id,name,email,signup_date
1,John Doe,john@example.com,2023-01-15
2,Jane Smith,jane@example.com,2023-02-20
3,Bob Johnson,bob@example.com,2023-03-10
"""
        csv_file.write_text(csv_content)

        # Ingest to Bronze
        from ingestors.csv_ingestor import CSVIngestor

        ingestor = CSVIngestor(
            path=str(csv_file),
            header=True,
            infer_schema=True,
        )

        bronze_df = ingestor.read(spark)

        # Write to Bronze Delta table
        bronze_path = tmp_path / "bronze" / "customers"
        bronze_df.write.format("delta").mode("overwrite").save(str(bronze_path))

        # Verify Bronze table
        bronze_read_df = spark.read.format("delta").load(str(bronze_path))

        assert bronze_read_df.count() == 3
        assert "customer_id" in bronze_read_df.columns

    def test_json_to_bronze_with_audit_columns(self, spark: SparkSession, tmp_path: Path):
        """Test JSON ingestion with audit column addition"""
        # Create source JSON
        json_file = tmp_path / "source.json"
        json_content = """{"id": 1, "name": "Alice", "value": 100}
{"id": 2, "name": "Bob", "value": 200}
{"id": 3, "name": "Charlie", "value": 300}
"""
        json_file.write_text(json_content)

        # Ingest
        from ingestors.json_ingestor import JSONIngestor

        ingestor = JSONIngestor(path=str(json_file))
        raw_df = ingestor.read(spark)

        # Add audit columns
        from transformers.add_audit_columns import AddAuditColumnsTransformer

        transformer = AddAuditColumnsTransformer()
        bronze_df = transformer.transform(raw_df)

        # Write to Bronze
        bronze_path = tmp_path / "bronze" / "data"
        bronze_df.write.format("delta").mode("overwrite").save(str(bronze_path))

        # Verify
        bronze_read_df = spark.read.format("delta").load(str(bronze_path))

        assert bronze_read_df.count() == 3
        assert "created_at" in bronze_read_df.columns
        assert "updated_at" in bronze_read_df.columns


class TestSilverTransformationIntegration:
    """Test silver layer transformation end-to-end"""

    def test_bronze_to_silver_transformation_chain(self, spark: SparkSession, tmp_path: Path):
        """Test Bronze → Silver with transformation chain"""
        # Create Bronze data
        bronze_data = [
            (1, "  Alice  ", "alice@example.com", 100.0, "2023-01-15"),
            (2, "Bob", "", 200.0, "2023-02-20"),
            (3, "Charlie", "charlie@EXAMPLE.COM", 300.0, "2023-03-10"),
            (1, "  Alice  ", "alice@example.com", 100.0, "2023-01-15"),  # Duplicate
            (4, "Diana", "diana@example.com", -50.0, "2023-04-05"),  # Invalid value
        ]

        bronze_df = spark.createDataFrame(
            bronze_data,
            ["customer_id", "name", "email", "total_spend", "signup_date"]
        )

        # Save to Bronze Delta
        bronze_path = tmp_path / "bronze" / "customers"
        bronze_df.write.format("delta").mode("overwrite").save(str(bronze_path))

        # Read Bronze
        bronze_read_df = spark.read.format("delta").load(str(bronze_path))

        # Apply Silver transformations
        from transformers.drop_exact_duplicates import DropExactDuplicatesTransformer
        from transformers.standardize_nulls import StandardizeNullsTransformer
        from transformers.lowercase import LowercaseTransformer
        from transformers.trim_whitespace import TrimWhitespaceTransformer
        from transformers.validate_range import ValidateRangeTransformer

        silver_df = (
            bronze_read_df
            .transform(DropExactDuplicatesTransformer().transform)
            .transform(TrimWhitespaceTransformer(columns=["name"]).transform)
            .transform(StandardizeNullsTransformer(null_values=[""]).transform)
            .transform(LowercaseTransformer(columns=["email"]).transform)
            .transform(ValidateRangeTransformer(
                column="total_spend",
                min_value=0.0,
                max_value=100000.0,
                mark_invalid=True,
                invalid_flag_column="spend_valid"
            ).transform)
        )

        # Write to Silver Delta
        silver_path = tmp_path / "silver" / "customers"
        silver_df.write.format("delta").mode("overwrite").save(str(silver_path))

        # Verify Silver table
        silver_read_df = spark.read.format("delta").load(str(silver_path))

        assert silver_read_df.count() == 4  # Duplicate removed
        assert "spend_valid" in silver_read_df.columns

        # Verify transformations applied
        alice_row = silver_read_df.filter("customer_id = 1").first()
        assert alice_row["name"] == "Alice"  # Trimmed
        assert alice_row["email"] == "alice@example.com"  # Lowercased

    def test_silver_incremental_load(self, spark: SparkSession, tmp_path: Path):
        """Test incremental Silver load from Bronze"""
        bronze_path = tmp_path / "bronze" / "incremental_test"
        silver_path = tmp_path / "silver" / "incremental_test"

        # Initial load
        initial_data = [
            (1, "Alice", "2023-01-01T10:00:00"),
            (2, "Bob", "2023-01-01T11:00:00"),
        ]

        df_initial = spark.createDataFrame(initial_data, ["id", "name", "updated_at"])
        df_initial.write.format("delta").mode("overwrite").save(str(bronze_path))

        # Process to Silver
        bronze_df = spark.read.format("delta").load(str(bronze_path))
        bronze_df.write.format("delta").mode("overwrite").save(str(silver_path))

        # New incremental data
        incremental_data = [
            (3, "Charlie", "2023-01-02T10:00:00"),
            (4, "Diana", "2023-01-02T11:00:00"),
        ]

        df_incremental = spark.createDataFrame(incremental_data, ["id", "name", "updated_at"])
        df_incremental.write.format("delta").mode("append").save(str(bronze_path))

        # Process incremental to Silver
        bronze_df_full = spark.read.format("delta").load(str(bronze_path))

        # Get max timestamp from Silver
        silver_df = spark.read.format("delta").load(str(silver_path))
        max_timestamp = silver_df.selectExpr("max(updated_at) as max_ts").first()["max_ts"]

        # Filter new records
        new_records = bronze_df_full.filter(col("updated_at") > max_timestamp)

        # Append to Silver
        new_records.write.format("delta").mode("append").save(str(silver_path))

        # Verify
        silver_final = spark.read.format("delta").load(str(silver_path))
        assert silver_final.count() == 4


class TestGoldAggregationIntegration:
    """Test gold layer aggregation end-to-end"""

    def test_silver_to_gold_aggregation(self, spark: SparkSession, tmp_path: Path):
        """Test Silver → Gold aggregation"""
        # Create Silver data
        silver_data = [
            (1, "Alice", "Premium", "USA", 1000.0, "2023-01-15"),
            (2, "Bob", "Basic", "USA", 200.0, "2023-01-20"),
            (3, "Charlie", "Premium", "UK", 1500.0, "2023-02-10"),
            (4, "Diana", "Enterprise", "USA", 5000.0, "2023-02-15"),
            (5, "Eve", "Basic", "Canada", 300.0, "2023-03-01"),
        ]

        silver_df = spark.createDataFrame(
            silver_data,
            ["customer_id", "name", "tier", "country", "total_spend", "signup_date"]
        )

        # Save to Silver
        silver_path = tmp_path / "silver" / "customers"
        silver_df.write.format("delta").mode("overwrite").save(str(silver_path))

        # Aggregate to Gold
        from transformers.group_aggregate import GroupAggregateTransformer

        silver_read_df = spark.read.format("delta").load(str(silver_path))

        # Aggregate by tier and country
        gold_df = silver_read_df.groupBy("tier", "country").agg(
            count("customer_id").alias("customer_count"),
            spark_sum("total_spend").alias("total_revenue"),
        )

        # Write to Gold
        gold_path = tmp_path / "gold" / "customer_metrics"
        gold_df.write.format("delta").mode("overwrite").save(str(gold_path))

        # Verify Gold table
        gold_read_df = spark.read.format("delta").load(str(gold_path))

        assert gold_read_df.count() > 0

        # Verify aggregation
        usa_premium = gold_read_df.filter("tier = 'Premium' AND country = 'USA'").first()
        assert usa_premium["customer_count"] == 1
        assert usa_premium["total_revenue"] == 1000.0


class TestFeatureStoreIntegration:
    """Test feature store materialization end-to-end"""

    def test_feature_engineering_pipeline(self, spark: SparkSession, tmp_path: Path):
        """Test feature engineering for ML"""
        # Create Gold data
        gold_data = [
            (1, 5, 1500.0, 10, "Premium"),
            (2, 2, 300.0, 30, "Basic"),
            (3, 10, 5000.0, 5, "Enterprise"),
            (4, 1, 150.0, 60, "Basic"),
        ]

        gold_df = spark.createDataFrame(
            gold_data,
            ["customer_id", "order_count", "total_spend", "days_since_last_order", "tier"]
        )

        # Feature engineering
        from ml.feature_engineering.vector_assembler import VectorAssemblerFeature

        # Create features
        gold_df = gold_df.withColumn(
            "avg_order_value",
            col("total_spend") / col("order_count")
        )

        # Assemble features
        assembler = VectorAssemblerFeature(
            input_cols=["order_count", "total_spend", "days_since_last_order", "avg_order_value"],
            output_col="features",
        )

        features_df = assembler.transform(gold_df)

        # Write to Feature Store
        feature_store_path = tmp_path / "feature_store" / "customer_features"
        features_df.write.format("delta").mode("overwrite").save(str(feature_store_path))

        # Verify Feature Store
        fs_read_df = spark.read.format("delta").load(str(feature_store_path))

        assert fs_read_df.count() == 4
        assert "features" in fs_read_df.columns


class TestMLPipelineIntegration:
    """Test end-to-end ML pipeline integration"""

    def test_full_ml_pipeline_bronze_to_predictions(self, spark: SparkSession, tmp_path: Path):
        """Test complete pipeline: Bronze → Silver → Gold → Features → Train → Predict"""

        # Step 1: Create Bronze data (raw customer data)
        bronze_data = []
        import random
        random.seed(42)

        for i in range(200):
            order_count = random.randint(1, 20)
            total_spend = random.uniform(100, 10000)
            days_since_last_order = random.randint(1, 90)

            # Label: churn if days_since_last_order > 60
            churn = 1 if days_since_last_order > 60 else 0

            bronze_data.append((
                i,
                order_count,
                total_spend,
                days_since_last_order,
                churn
            ))

        bronze_df = spark.createDataFrame(
            bronze_data,
            ["customer_id", "order_count", "total_spend", "days_since_last_order", "churned"]
        )

        bronze_path = tmp_path / "bronze" / "customers"
        bronze_df.write.format("delta").mode("overwrite").save(str(bronze_path))

        # Step 2: Silver transformation
        from transformers.drop_exact_duplicates import DropExactDuplicatesTransformer

        silver_df = (
            spark.read.format("delta").load(str(bronze_path))
            .transform(DropExactDuplicatesTransformer().transform)
        )

        silver_path = tmp_path / "silver" / "customers"
        silver_df.write.format("delta").mode("overwrite").save(str(silver_path))

        # Step 3: Feature engineering
        from ml.feature_engineering.vector_assembler import VectorAssemblerFeature

        features_df = silver_df.withColumn(
            "avg_order_value",
            col("total_spend") / col("order_count")
        )

        assembler = VectorAssemblerFeature(
            input_cols=["order_count", "total_spend", "days_since_last_order", "avg_order_value"],
            output_col="features",
        )

        ml_df = assembler.transform(features_df)

        # Step 4: Train/Test split
        train_df, test_df = ml_df.randomSplit([0.8, 0.2], seed=42)

        # Step 5: Train model
        from ml.trainers.random_forest_trainer import RandomForestTrainer

        trainer = RandomForestTrainer(
            features_col="features",
            label_col="churned",
            num_trees=10,
        )

        model = trainer.train(train_df)

        # Step 6: Score
        predictions_df = model.transform(test_df)

        # Step 7: Evaluate
        from ml.metrics.classification_metrics import ClassificationMetrics

        metrics_evaluator = ClassificationMetrics(
            prediction_col="prediction",
            label_col="churned",
        )

        metrics = metrics_evaluator.evaluate(predictions_df)

        # Step 8: Write predictions to Gold
        gold_predictions_path = tmp_path / "gold" / "churn_predictions"
        predictions_df.write.format("delta").mode("overwrite").save(str(gold_predictions_path))

        # Verify end-to-end
        predictions_read_df = spark.read.format("delta").load(str(gold_predictions_path))

        assert predictions_read_df.count() > 0
        assert "prediction" in predictions_read_df.columns
        assert metrics["accuracy"] > 0.5

        print(f"End-to-End ML Pipeline Metrics: {metrics}")


class TestSCD2Integration:
    """Test SCD Type 2 integration"""

    def test_scd2_dimension_update(self, spark: SparkSession, tmp_path: Path):
        """Test SCD2 dimension updates"""
        dim_path = tmp_path / "dimensions" / "customers"

        # Initial dimension load
        initial_data = [
            (1, "Alice", "Premium", "2023-01-01", "9999-12-31", True),
            (2, "Bob", "Basic", "2023-01-01", "9999-12-31", True),
        ]

        dim_df = spark.createDataFrame(
            initial_data,
            ["customer_id", "name", "tier", "valid_from", "valid_to", "is_current"]
        )

        dim_df.write.format("delta").mode("overwrite").save(str(dim_path))

        # New data with changes
        new_data = [
            (1, "Alice", "Enterprise"),  # Tier changed
            (2, "Bob", "Basic"),  # No change
            (3, "Charlie", "Premium"),  # New customer
        ]

        new_df = spark.createDataFrame(new_data, ["customer_id", "name", "tier"])

        # Read current dimension
        current_dim_df = spark.read.format("delta").load(str(dim_path))

        # Identify changes (simplified SCD2 logic)
        from pyspark.sql.functions import lit, current_date

        # For this test, we'll manually create the SCD2 updates
        # Close old record for Alice
        updated_old = current_dim_df.filter("customer_id = 1").withColumn(
            "valid_to", lit("2023-06-01")
        ).withColumn(
            "is_current", lit(False)
        )

        # Create new record for Alice
        new_alice = spark.createDataFrame(
            [(1, "Alice", "Enterprise", "2023-06-01", "9999-12-31", True)],
            ["customer_id", "name", "tier", "valid_from", "valid_to", "is_current"]
        )

        # Keep Bob unchanged
        bob = current_dim_df.filter("customer_id = 2")

        # Add new customer Charlie
        new_charlie = spark.createDataFrame(
            [(3, "Charlie", "Premium", "2023-06-01", "9999-12-31", True)],
            ["customer_id", "name", "tier", "valid_from", "valid_to", "is_current"]
        )

        # Combine all
        final_dim_df = updated_old.union(new_alice).union(bob).union(new_charlie)

        # Write back
        final_dim_df.write.format("delta").mode("overwrite").save(str(dim_path))

        # Verify
        result_df = spark.read.format("delta").load(str(dim_path))

        assert result_df.count() == 4  # 2 old + 1 new Alice + Bob + Charlie
        assert result_df.filter("is_current = true").count() == 3  # Alice (new), Bob, Charlie


class TestRealTimeStreamingIntegration:
    """Test real-time streaming integration (simulated)"""

    def test_streaming_aggregation(self, spark: SparkSession, tmp_path: Path):
        """Test streaming aggregation using structured streaming"""
        source_path = tmp_path / "streaming_source"
        checkpoint_path = tmp_path / "checkpoint"
        output_path = tmp_path / "streaming_output"

        source_path.mkdir()

        # Create initial batch of files
        batch1_data = [
            ("event_1", "page_view", "user_1", "2023-01-01T10:00:00"),
            ("event_2", "click", "user_1", "2023-01-01T10:01:00"),
            ("event_3", "page_view", "user_2", "2023-01-01T10:02:00"),
        ]

        df_batch1 = spark.createDataFrame(
            batch1_data,
            ["event_id", "event_type", "user_id", "timestamp"]
        )

        df_batch1.write.mode("overwrite").json(str(source_path / "batch1"))

        # Read as stream
        streaming_df = (
            spark.readStream
            .schema(df_batch1.schema)
            .json(str(source_path))
        )

        # Aggregate
        aggregated_df = streaming_df.groupBy("event_type").count()

        # Write stream (using memory sink for testing)
        query = (
            aggregated_df.writeStream
            .format("memory")
            .queryName("event_counts")
            .outputMode("complete")
            .start()
        )

        # Wait for processing
        query.processAllAvailable()

        # Read from memory sink
        result_df = spark.sql("SELECT * FROM event_counts")

        assert result_df.count() > 0

        # Stop query
        query.stop()
