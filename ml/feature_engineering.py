"""
Feature Engineering Transformers (22 components).

Production-ready feature engineering transformers for ML pipelines.
"""

from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, array, concat, expr, log, sqrt, pow,
    sin, cos, radians, atan2, dayofyear, hour, year, month,
    dayofweek, weekofyear, unix_timestamp, lag, lead, avg, sum as spark_sum
)
from pyspark.sql.window import Window
from pyspark.ml.feature import (
    PolynomialExpansion, Bucketizer as SparkBucketizer,
    OneHotEncoder as SparkOneHotEncoder, VectorAssembler,
    HashingTF, IDF, CountVectorizer as SparkCountVectorizer,
    Word2Vec, StandardScaler, RobustScaler as SparkRobustScaler
)
from pyspark.ml import Pipeline
from .base import BaseFeatureEngineer
from .factory import register_ml_component
import math


@register_ml_component("polynomial_features")
class PolynomialFeatures(BaseFeatureEngineer):
    """
    Generate polynomial and interaction features up to degree N.

    Config example:
        {
            "input_table": "silver.features.base",
            "output_table": "silver.features.polynomial",
            "feature_columns": ["age", "income", "credit_score"],
            "degree": 2,
            "output_column": "poly_features"
        }
    """

    def execute(self) -> DataFrame:
        """Generate polynomial features."""
        df = self.spark.table(self.config["input_table"])
        feature_columns = self.config["feature_columns"]
        degree = self.config.get("degree", 2)
        output_col = self.config.get("output_column", "poly_features")

        # Assemble features
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features_vec")
        poly = PolynomialExpansion(degree=degree, inputCol="features_vec", outputCol=output_col)

        pipeline = Pipeline(stages=[assembler, poly])
        model = pipeline.fit(df)
        result = model.transform(df)

        self.log_params({"degree": degree, "num_features": len(feature_columns)})
        return result


@register_ml_component("bucketizer")
class Bucketizer(BaseFeatureEngineer):
    """
    Discretize continuous features into buckets/bins.

    Config example:
        {
            "input_table": "silver.features.base",
            "output_table": "silver.features.bucketed",
            "input_column": "age",
            "output_column": "age_bucket",
            "splits": [0, 18, 35, 50, 65, 120]
        }
    """

    def execute(self) -> DataFrame:
        """Bucketize features."""
        df = self.spark.table(self.config["input_table"])
        input_col = self.config["input_column"]
        output_col = self.config["output_column"]
        splits = self.config["splits"]

        bucketizer = SparkBucketizer(splits=splits, inputCol=input_col, outputCol=output_col)
        result = bucketizer.transform(df)

        self.log_params({"num_buckets": len(splits) - 1})
        return result


@register_ml_component("one_hot_encoder")
class OneHotEncoder(BaseFeatureEngineer):
    """
    One-hot encode categorical features.

    Config example:
        {
            "input_table": "silver.features.base",
            "output_table": "silver.features.encoded",
            "input_columns": ["state", "product_category"],
            "output_columns": ["state_ohe", "category_ohe"]
        }
    """

    def execute(self) -> DataFrame:
        """One-hot encode features."""
        from pyspark.ml.feature import StringIndexer

        df = self.spark.table(self.config["input_table"])
        input_cols = self.config["input_columns"]
        output_cols = self.config["output_columns"]

        # String index first
        indexers = [
            StringIndexer(inputCol=col, outputCol=f"{col}_index")
            for col in input_cols
        ]

        # Then one-hot encode
        encoder = SparkOneHotEncoder(
            inputCols=[f"{col}_index" for col in input_cols],
            outputCols=output_cols
        )

        pipeline = Pipeline(stages=indexers + [encoder])
        model = pipeline.fit(df)
        result = model.transform(df)

        self.log_params({"num_features": len(input_cols)})
        return result


@register_ml_component("target_encoder")
class TargetEncoder(BaseFeatureEngineer):
    """
    Target encoding with smoothing and regularization.

    Config example:
        {
            "input_table": "gold.train_data",
            "output_table": "gold.train_encoded",
            "categorical_column": "city",
            "target_column": "conversion",
            "output_column": "city_target_encoded",
            "smoothing": 10.0
        }
    """

    def execute(self) -> DataFrame:
        """Apply target encoding."""
        df = self.spark.table(self.config["input_table"])
        cat_col = self.config["categorical_column"]
        target_col = self.config["target_column"]
        output_col = self.config["output_column"]
        smoothing = self.config.get("smoothing", 10.0)

        # Calculate global mean
        global_mean = df.select(avg(col(target_col))).collect()[0][0]

        # Calculate category means and counts
        category_stats = df.groupBy(cat_col).agg(
            avg(target_col).alias("cat_mean"),
            spark_sum(lit(1)).alias("cat_count")
        )

        # Apply smoothing: (cat_mean * cat_count + global_mean * smoothing) / (cat_count + smoothing)
        category_stats = category_stats.withColumn(
            "encoded_value",
            (col("cat_mean") * col("cat_count") + lit(global_mean) * lit(smoothing)) /
            (col("cat_count") + lit(smoothing))
        )

        # Join back
        result = df.join(
            category_stats.select(cat_col, col("encoded_value").alias(output_col)),
            on=cat_col,
            how="left"
        )

        # Fill nulls with global mean
        result = result.fillna({output_col: global_mean})

        self.log_params({"smoothing": smoothing, "global_mean": global_mean})
        return result


@register_ml_component("frequency_encoder")
class FrequencyEncoder(BaseFeatureEngineer):
    """
    Encode categorical features by their frequency.

    Config example:
        {
            "input_table": "silver.features.base",
            "output_table": "silver.features.freq_encoded",
            "categorical_column": "user_agent",
            "output_column": "user_agent_freq",
            "normalize": true
        }
    """

    def execute(self) -> DataFrame:
        """Apply frequency encoding."""
        df = self.spark.table(self.config["input_table"])
        cat_col = self.config["categorical_column"]
        output_col = self.config["output_column"]
        normalize = self.config.get("normalize", True)

        # Calculate frequencies
        freq_df = df.groupBy(cat_col).count()

        if normalize:
            total_count = df.count()
            freq_df = freq_df.withColumn("frequency", col("count") / lit(total_count))
        else:
            freq_df = freq_df.withColumnRenamed("count", "frequency")

        # Join back
        result = df.join(
            freq_df.select(cat_col, col("frequency").alias(output_col)),
            on=cat_col,
            how="left"
        )

        return result


@register_ml_component("hashing_trick_feature_hasher")
class HashingTrickFeatureHasher(BaseFeatureEngineer):
    """
    Feature hashing for high-cardinality categoricals.

    Config example:
        {
            "input_table": "silver.features.base",
            "output_table": "silver.features.hashed",
            "input_column": "text_field",
            "output_column": "hashed_features",
            "num_features": 1024
        }
    """

    def execute(self) -> DataFrame:
        """Apply feature hashing."""
        from pyspark.ml.feature import FeatureHasher

        df = self.spark.table(self.config["input_table"])
        input_col = self.config["input_column"]
        output_col = self.config["output_column"]
        num_features = self.config.get("num_features", 1024)

        hasher = FeatureHasher(
            inputCols=[input_col],
            outputCol=output_col,
            numFeatures=num_features
        )

        result = hasher.transform(df)

        self.log_params({"num_features": num_features})
        return result


@register_ml_component("count_vectorizer")
class CountVectorizer(BaseFeatureEngineer):
    """
    Convert text to token counts (bag of words).

    Config example:
        {
            "input_table": "silver.text.tokenized",
            "output_table": "silver.text.vectorized",
            "input_column": "tokens",
            "output_column": "count_vector",
            "vocab_size": 10000,
            "min_df": 5
        }
    """

    def execute(self) -> DataFrame:
        """Apply count vectorization."""
        df = self.spark.table(self.config["input_table"])
        input_col = self.config["input_column"]
        output_col = self.config["output_column"]
        vocab_size = self.config.get("vocab_size", 10000)
        min_df = self.config.get("min_df", 5)

        cv = SparkCountVectorizer(
            inputCol=input_col,
            outputCol=output_col,
            vocabSize=vocab_size,
            minDF=min_df
        )

        model = cv.fit(df)
        result = model.transform(df)

        self.log_params({"vocab_size": len(model.vocabulary)})
        return result


@register_ml_component("tfidf_transformer")
class TFIDFTransformer(BaseFeatureEngineer):
    """
    Transform count vectors to TF-IDF.

    Config example:
        {
            "input_table": "silver.text.count_vectorized",
            "output_table": "silver.text.tfidf",
            "input_column": "count_vector",
            "output_column": "tfidf_vector"
        }
    """

    def execute(self) -> DataFrame:
        """Apply TF-IDF transformation."""
        df = self.spark.table(self.config["input_table"])
        input_col = self.config["input_column"]
        output_col = self.config["output_column"]

        idf = IDF(inputCol=input_col, outputCol=output_col)
        model = idf.fit(df)
        result = model.transform(df)

        return result


@register_ml_component("word2vec_embeddings")
class Word2VecEmbeddings(BaseFeatureEngineer):
    """
    Train Word2Vec embeddings on tokenized text.

    Config example:
        {
            "input_table": "silver.text.tokenized",
            "output_table": "silver.text.embeddings",
            "input_column": "tokens",
            "output_column": "word2vec_vector",
            "vector_size": 100,
            "min_count": 5,
            "num_iterations": 10
        }
    """

    def execute(self) -> DataFrame:
        """Train Word2Vec embeddings."""
        df = self.spark.table(self.config["input_table"])
        input_col = self.config["input_column"]
        output_col = self.config["output_column"]
        vector_size = self.config.get("vector_size", 100)
        min_count = self.config.get("min_count", 5)
        num_iterations = self.config.get("num_iterations", 10)

        word2vec = Word2Vec(
            inputCol=input_col,
            outputCol=output_col,
            vectorSize=vector_size,
            minCount=min_count,
            maxIter=num_iterations
        )

        model = word2vec.fit(df)
        result = model.transform(df)

        self.log_params({
            "vector_size": vector_size,
            "vocab_size": model.getVectors().count()
        })

        return result


@register_ml_component("bert_sentence_embeddings")
class BERTSentenceEmbeddings(BaseFeatureEngineer):
    """
    Generate BERT sentence embeddings using Spark NLP.

    Config example:
        {
            "input_table": "silver.text.raw",
            "output_table": "silver.text.bert_embeddings",
            "text_column": "text",
            "output_column": "bert_embeddings",
            "model_name": "bert_base_uncased",
            "max_sentence_length": 128
        }
    """

    def execute(self) -> DataFrame:
        """Generate BERT embeddings."""
        # Note: Requires spark-nlp library
        from sparknlp.base import DocumentAssembler, EmbeddingsFinisher
        from sparknlp.annotator import BertSentenceEmbeddings as SparkNLPBert

        df = self.spark.table(self.config["input_table"])
        text_col = self.config["text_column"]
        output_col = self.config["output_column"]
        model_name = self.config.get("model_name", "bert_base_uncased")
        max_length = self.config.get("max_sentence_length", 128)

        document = DocumentAssembler() \
            .setInputCol(text_col) \
            .setOutputCol("document")

        bert = SparkNLPBert.pretrained(model_name, "en") \
            .setInputCols(["document"]) \
            .setOutputCol("sentence_embeddings") \
            .setMaxSentenceLength(max_length)

        embeddings_finisher = EmbeddingsFinisher() \
            .setInputCols(["sentence_embeddings"]) \
            .setOutputCols(output_col) \
            .setOutputAsVector(True)

        pipeline = Pipeline(stages=[document, bert, embeddings_finisher])
        model = pipeline.fit(df)
        result = model.transform(df)

        self.log_params({"model_name": model_name, "max_length": max_length})
        return result


@register_ml_component("sql_feature_transformer")
class SQLFeatureTransformer(BaseFeatureEngineer):
    """
    Apply arbitrary SQL transformations for feature engineering.

    Config example:
        {
            "input_table": "silver.features.base",
            "output_table": "silver.features.sql_transformed",
            "sql_expression": "SELECT *, age * income as age_income_interaction FROM {input_table}",
            "temp_view_name": "temp_features"
        }
    """

    def execute(self) -> DataFrame:
        """Apply SQL transformation."""
        input_table = self.config["input_table"]
        sql_expr = self.config["sql_expression"]
        temp_view = self.config.get("temp_view_name", "temp_features")

        df = self.spark.table(input_table)
        df.createOrReplaceTempView(temp_view)

        # Replace placeholder with temp view name
        sql_expr = sql_expr.replace("{input_table}", temp_view)

        result = self.spark.sql(sql_expr)

        return result


@register_ml_component("rolling_window_features")
class RollingWindowFeatures(BaseFeatureEngineer):
    """
    Calculate rolling window statistics (mean, sum, std, min, max).

    Config example:
        {
            "input_table": "silver.timeseries.transactions",
            "output_table": "silver.features.rolling",
            "partition_columns": ["customer_id"],
            "order_column": "transaction_date",
            "value_column": "amount",
            "windows": [7, 30, 90],
            "aggregations": ["mean", "sum", "std"]
        }
    """

    def execute(self) -> DataFrame:
        """Calculate rolling window features."""
        from pyspark.sql.functions import mean, sum as spark_sum, stddev, min as spark_min, max as spark_max

        df = self.spark.table(self.config["input_table"])
        partition_cols = self.config["partition_columns"]
        order_col = self.config["order_column"]
        value_col = self.config["value_column"]
        windows = self.config["windows"]
        aggregations = self.config.get("aggregations", ["mean", "sum", "std"])

        agg_funcs = {
            "mean": mean,
            "sum": spark_sum,
            "std": stddev,
            "min": spark_min,
            "max": spark_max
        }

        result = df
        for window_size in windows:
            window_spec = Window \
                .partitionBy(*partition_cols) \
                .orderBy(order_col) \
                .rowsBetween(-window_size, -1)

            for agg_name in aggregations:
                if agg_name in agg_funcs:
                    agg_func = agg_funcs[agg_name]
                    col_name = f"{value_col}_{agg_name}_{window_size}d"
                    result = result.withColumn(col_name, agg_func(col(value_col)).over(window_spec))

        self.log_params({
            "num_windows": len(windows),
            "num_aggregations": len(aggregations)
        })

        return result


@register_ml_component("time_since_event_features")
class TimeSinceEventFeatures(BaseFeatureEngineer):
    """
    Calculate time elapsed since last event per entity.

    Config example:
        {
            "input_table": "silver.events.user_activity",
            "output_table": "silver.features.time_since",
            "partition_columns": ["user_id"],
            "timestamp_column": "event_timestamp",
            "event_type_column": "event_type",
            "event_types": ["login", "purchase", "support_ticket"],
            "output_unit": "days"
        }
    """

    def execute(self) -> DataFrame:
        """Calculate time since event features."""
        df = self.spark.table(self.config["input_table"])
        partition_cols = self.config["partition_columns"]
        timestamp_col = self.config["timestamp_column"]
        event_type_col = self.config["event_type_column"]
        event_types = self.config["event_types"]
        output_unit = self.config.get("output_unit", "days")

        # Calculate seconds for conversion
        unit_seconds = {
            "seconds": 1,
            "minutes": 60,
            "hours": 3600,
            "days": 86400
        }
        divisor = unit_seconds.get(output_unit, 86400)

        result = df
        for event_type in event_types:
            # Filter to specific event type
            event_df = df.filter(col(event_type_col) == event_type)

            # Get last event timestamp per partition
            window_spec = Window.partitionBy(*partition_cols).orderBy(col(timestamp_col).desc())
            event_df = event_df.withColumn("rn", row_number().over(window_spec))
            last_event = event_df.filter(col("rn") == 1).select(
                *partition_cols,
                col(timestamp_col).alias(f"last_{event_type}_timestamp")
            )

            # Join and calculate time difference
            result = result.join(last_event, on=partition_cols, how="left")
            result = result.withColumn(
                f"days_since_{event_type}",
                (unix_timestamp(col(timestamp_col)) - unix_timestamp(col(f"last_{event_type}_timestamp"))) / lit(divisor)
            ).drop(f"last_{event_type}_timestamp")

        return result


@register_ml_component("ratio_features")
class RatioFeatures(BaseFeatureEngineer):
    """
    Create ratio features between numeric columns.

    Config example:
        {
            "input_table": "silver.features.base",
            "output_table": "silver.features.ratios",
            "ratio_pairs": [
                {"numerator": "total_spent", "denominator": "num_transactions", "output": "avg_transaction_value"},
                {"numerator": "clicks", "denominator": "impressions", "output": "ctr"}
            ],
            "handle_division_by_zero": "null"
        }
    """

    def execute(self) -> DataFrame:
        """Create ratio features."""
        df = self.spark.table(self.config["input_table"])
        ratio_pairs = self.config["ratio_pairs"]
        handle_zero = self.config.get("handle_division_by_zero", "null")

        result = df
        for pair in ratio_pairs:
            numerator = pair["numerator"]
            denominator = pair["denominator"]
            output_col = pair["output"]

            if handle_zero == "null":
                result = result.withColumn(
                    output_col,
                    when(col(denominator) == 0, lit(None))
                    .otherwise(col(numerator) / col(denominator))
                )
            elif handle_zero == "zero":
                result = result.withColumn(
                    output_col,
                    when(col(denominator) == 0, lit(0))
                    .otherwise(col(numerator) / col(denominator))
                )
            else:
                result = result.withColumn(output_col, col(numerator) / col(denominator))

        return result


@register_ml_component("interaction_features")
class InteractionFeatures(BaseFeatureEngineer):
    """
    Create multiplicative interaction features.

    Config example:
        {
            "input_table": "silver.features.base",
            "output_table": "silver.features.interactions",
            "feature_pairs": [
                {"features": ["age", "income"], "output": "age_income"},
                {"features": ["tenure", "product_count"], "output": "tenure_products"}
            ]
        }
    """

    def execute(self) -> DataFrame:
        """Create interaction features."""
        df = self.spark.table(self.config["input_table"])
        feature_pairs = self.config["feature_pairs"]

        result = df
        for pair in feature_pairs:
            features = pair["features"]
            output_col = pair["output"]

            # Multiply all features together
            interaction = col(features[0])
            for feature in features[1:]:
                interaction = interaction * col(feature)

            result = result.withColumn(output_col, interaction)

        return result


@register_ml_component("cyclical_datetime_features")
class CyclicalDateTimeFeatures(BaseFeatureEngineer):
    """
    Encode cyclical time features using sin/cos transformations.

    Config example:
        {
            "input_table": "silver.features.base",
            "output_table": "silver.features.cyclical",
            "timestamp_column": "event_timestamp",
            "features": ["hour", "dayofweek", "month", "dayofyear"]
        }
    """

    def execute(self) -> DataFrame:
        """Create cyclical datetime features."""
        df = self.spark.table(self.config["input_table"])
        timestamp_col = self.config["timestamp_column"]
        features = self.config["features"]

        result = df

        feature_periods = {
            "hour": 24,
            "dayofweek": 7,
            "month": 12,
            "dayofyear": 365
        }

        for feature in features:
            if feature not in feature_periods:
                continue

            period = feature_periods[feature]

            # Extract time component
            if feature == "hour":
                value_col = hour(col(timestamp_col))
            elif feature == "dayofweek":
                value_col = dayofweek(col(timestamp_col))
            elif feature == "month":
                value_col = month(col(timestamp_col))
            elif feature == "dayofyear":
                value_col = dayofyear(col(timestamp_col))

            # Convert to radians and apply sin/cos
            result = result.withColumn(
                f"{feature}_sin",
                sin(radians(value_col * 360 / period))
            ).withColumn(
                f"{feature}_cos",
                cos(radians(value_col * 360 / period))
            )

        return result


@register_ml_component("haversine_distance_feature")
class HaversineDistanceFeature(BaseFeatureEngineer):
    """
    Calculate haversine distance between two lat/lon points.

    Config example:
        {
            "input_table": "silver.features.locations",
            "output_table": "silver.features.distances",
            "point1_lat": "origin_lat",
            "point1_lon": "origin_lon",
            "point2_lat": "dest_lat",
            "point2_lon": "dest_lon",
            "output_column": "distance_km"
        }
    """

    def execute(self) -> DataFrame:
        """Calculate haversine distance."""
        df = self.spark.table(self.config["input_table"])
        lat1_col = self.config["point1_lat"]
        lon1_col = self.config["point1_lon"]
        lat2_col = self.config["point2_lat"]
        lon2_col = self.config["point2_lon"]
        output_col = self.config["output_column"]

        # Haversine formula
        result = df.withColumn(
            output_col,
            expr(f"""
                6371 * acos(
                    cos(radians({lat1_col})) *
                    cos(radians({lat2_col})) *
                    cos(radians({lon2_col}) - radians({lon1_col})) +
                    sin(radians({lat1_col})) *
                    sin(radians({lat2_col}))
                )
            """)
        )

        return result


@register_ml_component("geohash_grid_features")
class GeohashGridFeatures(BaseFeatureEngineer):
    """
    Create geohash grid features from lat/lon.

    Config example:
        {
            "input_table": "silver.features.locations",
            "output_table": "silver.features.geohash",
            "latitude_column": "lat",
            "longitude_column": "lon",
            "precision": 7,
            "output_column": "geohash"
        }
    """

    def execute(self) -> DataFrame:
        """Create geohash features."""
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        import geohash2  # Note: requires geohash2 library

        df = self.spark.table(self.config["input_table"])
        lat_col = self.config["latitude_column"]
        lon_col = self.config["longitude_column"]
        precision = self.config.get("precision", 7)
        output_col = self.config["output_column"]

        @udf(StringType())
        def geohash_encode(lat, lon):
            if lat is None or lon is None:
                return None
            return geohash2.encode(lat, lon, precision=precision)

        result = df.withColumn(output_col, geohash_encode(col(lat_col), col(lon_col)))

        return result


@register_ml_component("missing_indicator")
class MissingIndicator(BaseFeatureEngineer):
    """
    Create binary indicator features for missing values.

    Config example:
        {
            "input_table": "silver.features.base",
            "output_table": "silver.features.missing_indicators",
            "columns": ["income", "age", "credit_score"],
            "suffix": "_is_missing"
        }
    """

    def execute(self) -> DataFrame:
        """Create missing indicators."""
        df = self.spark.table(self.config["input_table"])
        columns = self.config["columns"]
        suffix = self.config.get("suffix", "_is_missing")

        result = df
        for column in columns:
            result = result.withColumn(
                f"{column}{suffix}",
                when(col(column).isNull(), lit(1)).otherwise(lit(0))
            )

        return result


@register_ml_component("outlier_capper")
class OutlierCapper(BaseFeatureEngineer):
    """
    Cap outliers using IQR or percentile method.

    Config example:
        {
            "input_table": "silver.features.base",
            "output_table": "silver.features.capped",
            "columns": ["income", "transaction_amount"],
            "method": "iqr",
            "iqr_multiplier": 1.5
        }
    """

    def execute(self) -> DataFrame:
        """Cap outliers."""
        df = self.spark.table(self.config["input_table"])
        columns = self.config["columns"]
        method = self.config.get("method", "iqr")
        iqr_multiplier = self.config.get("iqr_multiplier", 1.5)

        result = df

        for column in columns:
            if method == "iqr":
                # Calculate quartiles
                quartiles = df.approxQuantile(column, [0.25, 0.75], 0.01)
                q1, q3 = quartiles[0], quartiles[1]
                iqr = q3 - q1

                lower_bound = q1 - (iqr_multiplier * iqr)
                upper_bound = q3 + (iqr_multiplier * iqr)

                result = result.withColumn(
                    column,
                    when(col(column) < lower_bound, lit(lower_bound))
                    .when(col(column) > upper_bound, lit(upper_bound))
                    .otherwise(col(column))
                )

                self.log_params({
                    f"{column}_lower_bound": lower_bound,
                    f"{column}_upper_bound": upper_bound
                })

            elif method == "percentile":
                lower_pct = self.config.get("lower_percentile", 0.01)
                upper_pct = self.config.get("upper_percentile", 0.99)

                bounds = df.approxQuantile(column, [lower_pct, upper_pct], 0.01)
                lower_bound, upper_bound = bounds[0], bounds[1]

                result = result.withColumn(
                    column,
                    when(col(column) < lower_bound, lit(lower_bound))
                    .when(col(column) > upper_bound, lit(upper_bound))
                    .otherwise(col(column))
                )

        return result


@register_ml_component("skewness_corrector")
class SkewnessCorrector(BaseFeatureEngineer):
    """
    Correct skewed distributions using log/sqrt/box-cox transforms.

    Config example:
        {
            "input_table": "silver.features.base",
            "output_table": "silver.features.normalized",
            "columns": ["income", "revenue"],
            "method": "log",
            "suffix": "_transformed"
        }
    """

    def execute(self) -> DataFrame:
        """Correct skewness."""
        df = self.spark.table(self.config["input_table"])
        columns = self.config["columns"]
        method = self.config.get("method", "log")
        suffix = self.config.get("suffix", "_transformed")

        result = df

        for column in columns:
            if method == "log":
                result = result.withColumn(
                    f"{column}{suffix}",
                    log(col(column) + lit(1))  # Add 1 to handle zeros
                )
            elif method == "sqrt":
                result = result.withColumn(
                    f"{column}{suffix}",
                    sqrt(col(column))
                )
            elif method == "boxcox":
                # Simplified Box-Cox with lambda=0 (log transform)
                result = result.withColumn(
                    f"{column}{suffix}",
                    log(col(column) + lit(1))
                )

        return result


@register_ml_component("robust_scaler")
class RobustScaler(BaseFeatureEngineer):
    """
    Scale features using median and IQR (robust to outliers).

    Config example:
        {
            "input_table": "silver.features.base",
            "output_table": "silver.features.scaled",
            "feature_columns": ["age", "income", "credit_score"],
            "output_column": "scaled_features"
        }
    """

    def execute(self) -> DataFrame:
        """Apply robust scaling."""
        df = self.spark.table(self.config["input_table"])
        feature_columns = self.config["feature_columns"]
        output_col = self.config.get("output_column", "scaled_features")

        # Assemble features
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features_vec")
        scaler = SparkRobustScaler(inputCol="features_vec", outputCol=output_col)

        pipeline = Pipeline(stages=[assembler, scaler])
        model = pipeline.fit(df)
        result = model.transform(df)

        return result
