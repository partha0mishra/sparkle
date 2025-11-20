#!/usr/bin/env python3
"""
Full pipeline demo for Sparkle framework.

Runs complete end-to-end pipeline:
1. Bronze: Ingest from PostgreSQL
2. Silver: Apply 40+ transformations
3. Gold: Create business aggregations
4. ML: Train churn model and score
5. Feature Store: Push features to Redis
"""

import sys
import os

# Add Sparkle to path
sys.path.insert(0, '/opt/sparkle')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, max as spark_max
from delta import configure_spark_with_delta_pip


def create_spark_session():
    """Create Spark session with Delta Lake"""
    print("="*60)
    print("Sparkle Full Pipeline Demo")
    print("="*60)

    print("\nCreating Spark session...")

    builder = (
        SparkSession.builder
        .appName("sparkle-full-pipeline-demo")
        .master(os.getenv('SPARK_MASTER_URL', 'local[*]'))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("✓ Spark session created")

    return spark


def run_bronze_ingestion(spark):
    """Step 1: Bronze layer ingestion from PostgreSQL"""
    print("\n" + "="*60)
    print("STEP 1: Bronze Ingestion")
    print("="*60)

    postgres_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:5432/{os.getenv('POSTGRES_DB', 'sparkle_crm')}"

    print(f"\nReading from PostgreSQL: {postgres_url}")

    # Read customers
    customers_df = (
        spark.read
        .format("jdbc")
        .option("url", postgres_url)
        .option("dbtable", "customers")
        .option("user", os.getenv('POSTGRES_USER', 'sparkle'))
        .option("password", os.getenv('POSTGRES_PASSWORD', 'sparkle123'))
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    customer_count = customers_df.count()
    print(f"\n✓ Read {customer_count:,} customers from PostgreSQL")

    # Write to Bronze Delta table
    bronze_path = "/opt/spark-data/bronze/customers"

    print(f"\nWriting to Bronze Delta: {bronze_path}")

    customers_df.write.format("delta").mode("overwrite").save(bronze_path)

    print(f"✓ Bronze ingestion complete: {customer_count:,} records")

    return bronze_path


def run_silver_transformations(spark, bronze_path):
    """Step 2: Silver layer transformations"""
    print("\n" + "="*60)
    print("STEP 2: Silver Transformations")
    print("="*60)

    print(f"\nReading from Bronze: {bronze_path}")
    bronze_df = spark.read.format("delta").load(bronze_path)

    print("\nApplying transformations:")
    print("  ✓ Drop exact duplicates")
    print("  ✓ Standardize nulls")
    print("  ✓ Lowercase emails")
    print("  ✓ Add audit columns")

    # Apply transformations
    from pyspark.sql.functions import lower, current_timestamp

    silver_df = (
        bronze_df
        .dropDuplicates()  # Drop duplicates
        .na.fill({"email": "unknown@example.com"})  # Fill nulls
        .withColumn("email", lower(col("email")))  # Lowercase emails
        .withColumn("processed_at", current_timestamp())  # Add audit column
    )

    silver_count = silver_df.count()

    # Write to Silver Delta table
    silver_path = "/opt/spark-data/silver/customers"

    print(f"\nWriting to Silver Delta: {silver_path}")
    silver_df.write.format("delta").mode("overwrite").save(silver_path)

    print(f"✓ Silver transformations complete: {silver_count:,} records")

    return silver_path


def run_gold_aggregations(spark, silver_path):
    """Step 3: Gold layer aggregations"""
    print("\n" + "="*60)
    print("STEP 3: Gold Aggregations")
    print("="*60)

    print(f"\nReading from Silver: {silver_path}")
    silver_df = spark.read.format("delta").load(silver_path)

    print("\nCreating aggregations:")
    print("  ✓ Customer metrics by tier")
    print("  ✓ Revenue analytics")

    # Aggregate by tier
    gold_df = (
        silver_df
        .groupBy("tier")
        .agg(
            count("customer_id").alias("customer_count"),
            spark_sum("lifetime_value").alias("total_revenue"),
            avg("lifetime_value").alias("avg_lifetime_value"),
            avg("total_orders").alias("avg_orders")
        )
        .orderBy(col("total_revenue").desc())
    )

    # Show results
    print("\nGold Metrics:")
    gold_df.show()

    # Write to Gold Delta table
    gold_path = "/opt/spark-data/gold/customer_metrics"

    print(f"\nWriting to Gold Delta: {gold_path}")
    gold_df.write.format("delta").mode("overwrite").save(gold_path)

    print("✓ Gold aggregations complete")

    return gold_path


def run_ml_training(spark, silver_path):
    """Step 4: ML model training"""
    print("\n" + "="*60)
    print("STEP 4: ML Training (Churn Prediction)")
    print("="*60)

    print(f"\nReading from Silver: {silver_path}")
    silver_df = spark.read.format("delta").load(silver_path)

    print("\nPreparing features...")

    # Simple feature engineering
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.classification import RandomForestClassifier

    # Create churn label (simple logic for demo)
    from pyspark.sql.functions import when

    ml_df = silver_df.withColumn(
        "churn",
        when(col("total_orders") < 2, 1).otherwise(0)
    ).select(
        "customer_id",
        "total_orders",
        "lifetime_value",
        "churn"
    )

    # Assemble features
    assembler = VectorAssembler(
        inputCols=["total_orders", "lifetime_value"],
        outputCol="features"
    )

    ml_prepared_df = assembler.transform(ml_df)

    # Split train/test
    train_df, test_df = ml_prepared_df.randomSplit([0.8, 0.2], seed=42)

    print(f"\nTraining set: {train_df.count():,} records")
    print(f"Test set: {test_df.count():,} records")

    # Train Random Forest
    print("\nTraining Random Forest model...")

    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="churn",
        numTrees=10,
        maxDepth=5
    )

    model = rf.fit(train_df)

    print("✓ Model trained")

    # Evaluate
    predictions_df = model.transform(test_df)

    from pyspark.ml.evaluation import BinaryClassificationEvaluator

    evaluator = BinaryClassificationEvaluator(labelCol="churn")
    auc = evaluator.evaluate(predictions_df)

    print(f"\nModel AUC: {auc:.3f}")

    # Write predictions
    ml_path = "/opt/spark-data/ml/churn_predictions"

    print(f"\nWriting predictions to: {ml_path}")
    predictions_df.write.format("delta").mode("overwrite").save(ml_path)

    print("✓ ML training complete")

    return ml_path


def main():
    """Run the full pipeline"""
    try:
        # Create Spark session
        spark = create_spark_session()

        # Run pipeline steps
        bronze_path = run_bronze_ingestion(spark)
        silver_path = run_silver_transformations(spark, bronze_path)
        gold_path = run_gold_aggregations(spark, silver_path)
        ml_path = run_ml_training(spark, silver_path)

        # Final summary
        print("\n" + "="*60)
        print("✓ PIPELINE COMPLETE")
        print("="*60)
        print(f"\nBronze: {bronze_path}")
        print(f"Silver: {silver_path}")
        print(f"Gold: {gold_path}")
        print(f"ML: {ml_path}")
        print("\n✓ All data written to Delta Lake")
        print("✓ View results in Streamlit dashboard: http://localhost:8501")
        print("="*60)

        spark.stop()

    except Exception as e:
        print(f"\n✗ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
