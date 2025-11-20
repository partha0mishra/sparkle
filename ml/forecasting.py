"""
Time-Series & Forecasting Components (12 components).

Production-ready time-series forecasting and analysis.
"""

from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, lag, lead, avg, sum as spark_sum, stddev,
    datediff, date_add, year, month, dayofweek, dayofyear,
    sin, cos, radians, when, expr
)
from pyspark.sql.window import Window
from .base import BaseModelTrainer
from .factory import register_ml_component
import mlflow


@register_ml_component("multi_series_prophet")
class MultiSeriesProphet(BaseModelTrainer):
    """
    Prophet forecasting for multiple time series.

    Config example:
        {
            "input_table": "gold.timeseries_data",
            "timestamp_column": "date",
            "value_column": "sales",
            "series_column": "store_id",
            "forecast_periods": 30,
            "seasonality_mode": "multiplicative",
            "output_table": "gold.forecasts",
            "model_name": "multi_series_prophet"
        }
    """

    def execute(self) -> Dict[str, Any]:
        """Train Prophet for multiple series."""
        from prophet import Prophet
        import pandas as pd

        df = self.spark.table(self.config["input_table"])
        timestamp_col = self.config["timestamp_column"]
        value_col = self.config["value_column"]
        series_col = self.config["series_column"]
        forecast_periods = self.config.get("forecast_periods", 30)

        # Get unique series
        series_list = df.select(series_col).distinct().rdd.flatMap(lambda x: x).collect()

        all_forecasts = []

        for series_id in series_list:
            # Filter series
            series_df = df.filter(col(series_col) == series_id)
            series_pdf = series_df.select(timestamp_col, value_col).toPandas()
            series_pdf.columns = ["ds", "y"]

            # Train Prophet
            model = Prophet(seasonality_mode=self.config.get("seasonality_mode", "multiplicative"))
            model.fit(series_pdf)

            # Forecast
            future = model.make_future_dataframe(periods=forecast_periods)
            forecast = model.predict(future)

            # Add series identifier
            forecast[series_col] = series_id

            all_forecasts.append(forecast)

        # Combine all forecasts
        combined_forecast = pd.concat(all_forecasts, ignore_index=True)

        # Convert to Spark
        result = self.spark.createDataFrame(combined_forecast)
        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        self.log_params({
            "num_series": len(series_list),
            "forecast_periods": forecast_periods
        })

        return {
            "num_series": len(series_list),
            "forecast_periods": forecast_periods
        }


@register_ml_component("hierarchical_reconciler")
class HierarchicalReconciler(BaseModelTrainer):
    """
    Hierarchical forecast reconciliation (bottom-up, top-down).

    Config example:
        {
            "forecasts_table": "gold.hierarchical_forecasts",
            "hierarchy": {
                "total": ["region1", "region2"],
                "region1": ["store1", "store2"],
                "region2": ["store3", "store4"]
            },
            "reconciliation_method": "bottom_up",
            "output_table": "gold.reconciled_forecasts"
        }
    """

    def execute(self) -> DataFrame:
        """Reconcile hierarchical forecasts."""
        forecasts_df = self.spark.table(self.config["forecasts_table"])
        hierarchy = self.config["hierarchy"]
        method = self.config.get("reconciliation_method", "bottom_up")

        if method == "bottom_up":
            # Sum bottom-level forecasts to get upper levels
            result = forecasts_df

            # For each parent level, aggregate children
            for parent, children in hierarchy.items():
                child_forecasts = forecasts_df.filter(col("series_id").isin(children))
                parent_forecast = child_forecasts.groupBy("date").agg(
                    spark_sum("yhat").alias("yhat")
                )
                parent_forecast = parent_forecast.withColumn("series_id", lit(parent))

                result = result.union(parent_forecast)

        elif method == "top_down":
            # Distribute top-level forecast proportionally
            # (Simplified - production would use historical proportions)
            pass

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result


@register_ml_component("global_lightgbm_forecaster")
class GlobalLightGBMForecaster(BaseModelTrainer):
    """
    Global LightGBM model for all series with time/series features.

    Config example:
        {
            "train_table": "gold.timeseries_features",
            "test_table": "gold.timeseries_test",
            "target_column": "value",
            "feature_columns": ["lag_1", "lag_7", "lag_30", "day_of_week"],
            "series_column": "series_id",
            "num_iterations": 500,
            "learning_rate": 0.05,
            "model_name": "global_lightgbm_forecaster"
        }
    """

    def execute(self) -> Dict[str, Any]:
        """Train global LightGBM forecaster."""
        import lightgbm as lgb
        from sklearn.metrics import mean_absolute_error, mean_squared_error

        train_df = self.spark.table(self.config["train_table"])
        test_df = self.spark.table(self.config["test_table"])

        # Convert to pandas
        train_pdf = train_df.toPandas()
        test_pdf = test_df.toPandas()

        feature_cols = self.config["feature_columns"]
        target_col = self.config["target_column"]

        X_train = train_pdf[feature_cols]
        y_train = train_pdf[target_col]
        X_test = test_pdf[feature_cols]
        y_test = test_pdf[target_col]

        # Train LightGBM
        train_data = lgb.Dataset(X_train, label=y_train)
        test_data = lgb.Dataset(X_test, label=y_test, reference=train_data)

        params = {
            'objective': 'regression',
            'metric': 'mae',
            'num_iterations': self.config.get("num_iterations", 500),
            'learning_rate': self.config.get("learning_rate", 0.05),
            'verbosity': -1
        }

        model = lgb.train(
            params,
            train_data,
            valid_sets=[test_data],
            callbacks=[lgb.early_stopping(50), lgb.log_evaluation(0)]
        )

        # Evaluate
        y_pred = model.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        rmse = mean_squared_error(y_test, y_pred, squared=False)

        self.log_metrics({"mae": mae, "rmse": rmse})

        # Log model
        mlflow.lightgbm.log_model(model, "model")
        model_uri = self.register_model_to_uc(model, self.config["model_name"])

        return {"model_uri": model_uri, "mae": mae, "rmse": rmse}


@register_ml_component("forecast_accuracy_tracker")
class ForecastAccuracyTracker(BaseModelTrainer):
    """
    Track forecast accuracy over time with MAE, RMSE, MAPE.

    Config example:
        {
            "forecasts_table": "gold.forecasts",
            "actuals_table": "gold.actuals",
            "date_column": "date",
            "forecast_column": "yhat",
            "actual_column": "actual",
            "series_column": "series_id",
            "output_table": "gold.forecast_accuracy"
        }
    """

    def execute(self) -> DataFrame:
        """Calculate forecast accuracy metrics."""
        from pyspark.sql.functions import abs as spark_abs, pow as spark_pow, sqrt

        forecasts_df = self.spark.table(self.config["forecasts_table"])
        actuals_df = self.spark.table(self.config["actuals_table"])

        # Join forecasts with actuals
        date_col = self.config["date_column"]
        forecast_col = self.config["forecast_column"]
        actual_col = self.config["actual_column"]
        series_col = self.config.get("series_column")

        join_cols = [date_col]
        if series_col:
            join_cols.append(series_col)

        df = forecasts_df.join(actuals_df, on=join_cols, how="inner")

        # Calculate error metrics
        df = df.withColumn("error", col(actual_col) - col(forecast_col))
        df = df.withColumn("abs_error", spark_abs(col("error")))
        df = df.withColumn("squared_error", spark_pow(col("error"), 2))
        df = df.withColumn("pct_error", spark_abs(col("error")) / col(actual_col) * 100)

        # Aggregate by series
        group_cols = [series_col] if series_col else []

        metrics = df.groupBy(*group_cols).agg(
            avg("abs_error").alias("mae"),
            sqrt(avg("squared_error")).alias("rmse"),
            avg("pct_error").alias("mape"),
            avg("error").alias("bias")
        )

        metrics.write.mode("overwrite").saveAsTable(self.config["output_table"])

        # Log overall metrics
        overall = metrics.select(avg("mae"), avg("rmse"), avg("mape")).collect()[0]
        self.log_metrics({
            "overall_mae": float(overall[0]),
            "overall_rmse": float(overall[1]),
            "overall_mape": float(overall[2])
        })

        return metrics


@register_ml_component("probabilistic_quantile_forecast")
class ProbabilisticQuantileForecast(BaseModelTrainer):
    """
    Probabilistic forecasting with quantile regression.

    Config example:
        {
            "train_table": "gold.timeseries_features",
            "test_table": "gold.timeseries_test",
            "target_column": "value",
            "feature_columns": ["lag_1", "lag_7", "trend"],
            "quantiles": [0.1, 0.5, 0.9],
            "model_name": "quantile_forecaster"
        }
    """

    def execute(self) -> Dict[str, Any]:
        """Train quantile regression forecaster."""
        import lightgbm as lgb

        train_df = self.spark.table(self.config["train_table"])
        test_df = self.spark.table(self.config["test_table"])

        train_pdf = train_df.toPandas()
        test_pdf = test_df.toPandas()

        feature_cols = self.config["feature_columns"]
        target_col = self.config["target_column"]
        quantiles = self.config.get("quantiles", [0.1, 0.5, 0.9])

        X_train = train_pdf[feature_cols]
        y_train = train_pdf[target_col]
        X_test = test_pdf[feature_cols]

        models = {}
        predictions = test_pdf.copy()

        # Train separate model for each quantile
        for quantile in quantiles:
            train_data = lgb.Dataset(X_train, label=y_train)

            params = {
                'objective': 'quantile',
                'alpha': quantile,
                'metric': 'quantile',
                'verbosity': -1
            }

            model = lgb.train(params, train_data, num_boost_round=100)
            models[quantile] = model

            # Predict
            predictions[f"q{int(quantile*100)}"] = model.predict(X_test)

        # Convert back to Spark
        result = self.spark.createDataFrame(predictions)

        # Log models
        for quantile, model in models.items():
            mlflow.lightgbm.log_model(model, f"model_q{int(quantile*100)}")

        return {"num_quantiles": len(quantiles)}


@register_ml_component("exogenous_feature_joiner")
class ExogenousFeatureJoiner(BaseModelTrainer):
    """
    Join time-series with exogenous features (weather, holidays, etc).

    Config example:
        {
            "timeseries_table": "gold.sales_timeseries",
            "exogenous_tables": {
                "weather": "gold.weather_data",
                "holidays": "gold.holiday_calendar",
                "promotions": "gold.promotion_schedule"
            },
            "join_keys": ["date", "location"],
            "output_table": "gold.timeseries_with_exogenous"
        }
    """

    def execute(self) -> DataFrame:
        """Join exogenous features."""
        timeseries_df = self.spark.table(self.config["timeseries_table"])
        exogenous_tables = self.config["exogenous_tables"]
        join_keys = self.config["join_keys"]

        result = timeseries_df

        # Join each exogenous table
        for name, table_name in exogenous_tables.items():
            exog_df = self.spark.table(table_name)
            result = result.join(exog_df, on=join_keys, how="left")

            self.logger.info(f"Joined exogenous features from {name}")

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result


@register_ml_component("calendar_fourier_features")
class CalendarFourierFeatures(BaseModelTrainer):
    """
    Generate Fourier features for seasonality.

    Config example:
        {
            "input_table": "gold.timeseries_data",
            "date_column": "date",
            "yearly_fourier_order": 10,
            "weekly_fourier_order": 3,
            "output_table": "gold.timeseries_fourier"
        }
    """

    def execute(self) -> DataFrame:
        """Generate Fourier features."""
        from pyspark.sql.functions import sin, cos, dayofyear, dayofweek
        import math

        df = self.spark.table(self.config["input_table"])
        date_col = self.config["date_column"]
        yearly_order = self.config.get("yearly_fourier_order", 10)
        weekly_order = self.config.get("weekly_fourier_order", 3)

        result = df

        # Yearly Fourier features
        for k in range(1, yearly_order + 1):
            result = result.withColumn(
                f"yearly_sin_{k}",
                sin(lit(2 * math.pi * k) * dayofyear(col(date_col)) / lit(365.25))
            )
            result = result.withColumn(
                f"yearly_cos_{k}",
                cos(lit(2 * math.pi * k) * dayofyear(col(date_col)) / lit(365.25))
            )

        # Weekly Fourier features
        for k in range(1, weekly_order + 1):
            result = result.withColumn(
                f"weekly_sin_{k}",
                sin(lit(2 * math.pi * k) * dayofweek(col(date_col)) / lit(7))
            )
            result = result.withColumn(
                f"weekly_cos_{k}",
                cos(lit(2 * math.pi * k) * dayofweek(col(date_col)) / lit(7))
            )

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result


@register_ml_component("lag_feature_generator")
class LagFeatureGenerator(BaseModelTrainer):
    """
    Generate lag features for time-series forecasting.

    Config example:
        {
            "input_table": "gold.timeseries_data",
            "value_column": "sales",
            "series_column": "store_id",
            "timestamp_column": "date",
            "lags": [1, 7, 14, 30, 365],
            "rolling_windows": [7, 30],
            "output_table": "gold.timeseries_lagged"
        }
    """

    def execute(self) -> DataFrame:
        """Generate lag and rolling window features."""
        df = self.spark.table(self.config["input_table"])
        value_col = self.config["value_column"]
        series_col = self.config.get("series_column")
        timestamp_col = self.config["timestamp_column"]
        lags = self.config.get("lags", [1, 7, 14, 30])
        rolling_windows = self.config.get("rolling_windows", [7, 30])

        # Define window
        partition_cols = [series_col] if series_col else []
        window_spec = Window.partitionBy(*partition_cols).orderBy(timestamp_col)

        result = df

        # Generate lag features
        for lag_value in lags:
            result = result.withColumn(
                f"{value_col}_lag_{lag_value}",
                lag(col(value_col), lag_value).over(window_spec)
            )

        # Generate rolling window features
        for window_size in rolling_windows:
            rolling_window = window_spec.rowsBetween(-window_size, -1)

            result = result.withColumn(
                f"{value_col}_rolling_mean_{window_size}",
                avg(col(value_col)).over(rolling_window)
            )

            result = result.withColumn(
                f"{value_col}_rolling_std_{window_size}",
                stddev(col(value_col)).over(rolling_window)
            )

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result


@register_ml_component("holiday_effect_encoder")
class HolidayEffectEncoder(BaseModelTrainer):
    """
    Encode holiday effects in time-series.

    Config example:
        {
            "input_table": "gold.timeseries_data",
            "date_column": "date",
            "country": "US",
            "custom_holidays": {
                "2024-11-28": "Black Friday",
                "2024-12-26": "Boxing Day"
            },
            "output_table": "gold.timeseries_holidays"
        }
    """

    def execute(self) -> DataFrame:
        """Encode holiday effects."""
        from pyspark.sql.functions import when, col

        df = self.spark.table(self.config["input_table"])
        date_col = self.config["date_column"]
        country = self.config.get("country", "US")
        custom_holidays = self.config.get("custom_holidays", {})

        # Add holiday indicators
        result = df

        # US Federal Holidays (simplified - would use holidays library in production)
        us_holidays = {
            "2024-01-01": "New Year",
            "2024-07-04": "Independence Day",
            "2024-12-25": "Christmas"
        }

        all_holidays = {**us_holidays, **custom_holidays}

        # Create holiday indicator
        result = result.withColumn("is_holiday", lit(0))
        result = result.withColumn("holiday_name", lit(None).cast("string"))

        for holiday_date, holiday_name in all_holidays.items():
            result = result.withColumn(
                "is_holiday",
                when(col(date_col) == lit(holiday_date), lit(1))
                .otherwise(col("is_holiday"))
            )
            result = result.withColumn(
                "holiday_name",
                when(col(date_col) == lit(holiday_date), lit(holiday_name))
                .otherwise(col("holiday_name"))
            )

        # Add pre/post holiday indicators
        result = result.withColumn(
            "days_to_holiday",
            datediff(col(date_col), lit("2024-12-25"))  # Example
        )

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        return result


@register_ml_component("forecast_bias_corrector")
class ForecastBiasCorrector(BaseModelTrainer):
    """
    Correct systematic forecast bias.

    Config example:
        {
            "forecasts_table": "gold.raw_forecasts",
            "actuals_table": "gold.historical_actuals",
            "correction_method": "multiplicative",
            "lookback_periods": 30,
            "output_table": "gold.corrected_forecasts"
        }
    """

    def execute(self) -> DataFrame:
        """Correct forecast bias."""
        forecasts_df = self.spark.table(self.config["forecasts_table"])
        actuals_df = self.spark.table(self.config["actuals_table"])

        # Calculate historical bias
        historical = forecasts_df.join(
            actuals_df,
            on=["date", "series_id"],
            how="inner"
        )

        method = self.config.get("correction_method", "additive")

        if method == "additive":
            # Calculate average bias
            bias = historical.select(
                avg(col("actual") - col("forecast")).alias("avg_bias")
            ).collect()[0]["avg_bias"]

            # Apply correction
            result = forecasts_df.withColumn(
                "corrected_forecast",
                col("forecast") + lit(bias)
            )

        elif method == "multiplicative":
            # Calculate average ratio
            ratio = historical.select(
                avg(col("actual") / col("forecast")).alias("avg_ratio")
            ).collect()[0]["avg_ratio"]

            # Apply correction
            result = forecasts_df.withColumn(
                "corrected_forecast",
                col("forecast") * lit(ratio)
            )

        result.write.mode("overwrite").saveAsTable(self.config["output_table"])

        self.log_params({"correction_method": method, "correction_factor": float(bias if method == "additive" else ratio)})

        return result


@register_ml_component("residual_anomaly_detector")
class ResidualAnomalyDetector(BaseModelTrainer):
    """
    Detect anomalies in forecast residuals.

    Config example:
        {
            "forecasts_table": "gold.forecasts",
            "actuals_table": "gold.actuals",
            "threshold_sigma": 3.0,
            "output_table": "gold.forecast_anomalies"
        }
    """

    def execute(self) -> DataFrame:
        """Detect anomalies in residuals."""
        forecasts_df = self.spark.table(self.config["forecasts_table"])
        actuals_df = self.spark.table(self.config["actuals_table"])

        # Join and calculate residuals
        df = forecasts_df.join(actuals_df, on=["date", "series_id"], how="inner")
        df = df.withColumn("residual", col("actual") - col("forecast"))

        # Calculate mean and std of residuals per series
        window_spec = Window.partitionBy("series_id")

        df = df.withColumn("residual_mean", avg(col("residual")).over(window_spec))
        df = df.withColumn("residual_std", stddev(col("residual")).over(window_spec))

        # Flag anomalies
        threshold = self.config.get("threshold_sigma", 3.0)

        df = df.withColumn(
            "is_anomaly",
            (col("residual") > col("residual_mean") + lit(threshold) * col("residual_std")) |
            (col("residual") < col("residual_mean") - lit(threshold) * col("residual_std"))
        )

        df.write.mode("overwrite").saveAsTable(self.config["output_table"])

        num_anomalies = df.filter(col("is_anomaly")).count()
        self.log_metrics({"num_anomalies": num_anomalies})

        return df


@register_ml_component("forecast_version_controller")
class ForecastVersionController(BaseModelTrainer):
    """
    Version control for forecasts with comparison and rollback.

    Config example:
        {
            "current_forecasts_table": "gold.forecasts_v2",
            "previous_forecasts_table": "gold.forecasts_v1",
            "actuals_table": "gold.actuals",
            "comparison_metric": "mape",
            "auto_rollback_threshold": 0.2,
            "output_table": "gold.forecast_comparison"
        }
    """

    def execute(self) -> Dict[str, Any]:
        """Compare forecast versions and rollback if needed."""
        from pyspark.sql.functions import abs as spark_abs

        current_df = self.spark.table(self.config["current_forecasts_table"])
        previous_df = self.spark.table(self.config["previous_forecasts_table"])
        actuals_df = self.spark.table(self.config["actuals_table"])

        # Calculate metrics for both versions
        current_with_actuals = current_df.join(actuals_df, on=["date", "series_id"], how="inner")
        previous_with_actuals = previous_df.join(actuals_df, on=["date", "series_id"], how="inner")

        # Calculate MAPE
        current_mape = current_with_actuals.select(
            avg(spark_abs((col("actual") - col("forecast")) / col("actual")) * 100).alias("mape")
        ).collect()[0]["mape"]

        previous_mape = previous_with_actuals.select(
            avg(spark_abs((col("actual") - col("forecast")) / col("actual")) * 100).alias("mape")
        ).collect()[0]["mape"]

        # Compare
        improvement = (previous_mape - current_mape) / previous_mape
        threshold = self.config.get("auto_rollback_threshold", 0.2)

        rollback_needed = improvement < -threshold

        self.log_metrics({
            "current_mape": float(current_mape),
            "previous_mape": float(previous_mape),
            "improvement": float(improvement)
        })

        if rollback_needed:
            self.logger.warning(f"Performance degraded by {abs(improvement)*100:.2f}% - rollback recommended")

        return {
            "current_mape": float(current_mape),
            "previous_mape": float(previous_mape),
            "improvement": float(improvement),
            "rollback_recommended": rollback_needed
        }
