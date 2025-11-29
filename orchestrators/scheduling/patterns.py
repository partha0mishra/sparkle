from typing import Dict, Any, List, Optional
from datetime import datetime, time
from pyspark.sql import SparkSession


class DailyAtMidnightSchedule:
    """
    Cron 0 0 * * * with catchup
    """
    @staticmethod
    def get_cron() -> str:
        return "0 0 * * *"

    @staticmethod
    def get_config() -> Dict[str, Any]:
        return {
            "schedule": "0 0 * * *",
            "catchup": True,
            "max_active_runs": 1
        }


class HourlySchedule:
    """
    Every hour on the hour
    """
    @staticmethod
    def get_cron() -> str:
        return "0 * * * *"

    @staticmethod
    def get_config() -> Dict[str, Any]:
        return {
            "schedule": "0 * * * *",
            "catchup": True,
            "max_active_runs": 3
        }


class Every15MinutesSchedule:
    """
    0,15,30,45
    """
    @staticmethod
    def get_cron() -> str:
        return "0,15,30,45 * * * *"

    @staticmethod
    def get_config() -> Dict[str, Any]:
        return {
            "schedule": "0,15,30,45 * * * *",
            "catchup": False,
            "max_active_runs": 5
        }


class EventDrivenFileArrivalTrigger:
    """
    S3/ADLS event or Unity Catalog table change feed
    """
    def __init__(self, config: Dict[str, Any]):
        """
        Args:
            config: {
                "source_type": "s3|adls|unity_catalog",
                "bucket": "bucket-name",
                "prefix": "path/to/watch",
                "table": "catalog.schema.table",
                "poll_interval_seconds": 60
            }
        """
        self.config = config
        self.source_type = config.get("source_type", "s3")

    def check_for_new_files(self, spark: SparkSession = None) -> bool:
        """Check if new files have arrived."""
        if self.source_type == "s3":
            # Check S3 for new files
            return self._check_s3()
        elif self.source_type == "adls":
            # Check ADLS for new files
            return self._check_adls()
        elif self.source_type == "unity_catalog":
            # Check Unity Catalog change data feed
            return self._check_unity_catalog(spark)
        return False

    def _check_s3(self) -> bool:
        # Placeholder - would use boto3 to check for new objects
        return False

    def _check_adls(self) -> bool:
        # Placeholder - would use Azure SDK to check for new files
        return False

    def _check_unity_catalog(self, spark: SparkSession) -> bool:
        # Placeholder - would query change data feed
        if spark:
            table = self.config.get("table")
            # df = spark.read.format("delta").option("readChangeFeed", "true").table(table)
        return False


class EventDrivenKafkaLagTrigger:
    """
    Triggers when consumer lag > threshold
    """
    def __init__(self, config: Dict[str, Any]):
        """
        Args:
            config: {
                "kafka_bootstrap_servers": "localhost:9092",
                "consumer_group": "my-consumer-group",
                "topic": "my-topic",
                "lag_threshold": 10000
            }
        """
        self.config = config
        self.lag_threshold = config.get("lag_threshold", 10000)

    def check_lag(self) -> bool:
        """Check if consumer lag exceeds threshold."""
        # Placeholder - would use kafka-python or AdminClient to check lag
        current_lag = self._get_current_lag()
        return current_lag > self.lag_threshold

    def _get_current_lag(self) -> int:
        # Placeholder - would connect to Kafka and get consumer group lag
        return 0


class EventDrivenTableUpdatedTrigger:
    """
    Reacts to new Delta commits via change data feed
    """
    def __init__(self, config: Dict[str, Any]):
        """
        Args:
            config: {
                "catalog": "sparkle_prod",
                "schema": "bronze",
                "table": "customers",
                "poll_interval_seconds": 60,
                "watermark_column": "_commit_timestamp"
            }
        """
        self.config = config
        self.poll_interval = config.get("poll_interval_seconds", 60)

    def check_for_updates(self, spark: SparkSession, last_check_timestamp: datetime) -> bool:
        """Check if table has been updated since last check."""
        table_path = f"{self.config['catalog']}.{self.config['schema']}.{self.config['table']}"

        try:
            # Read table metadata to check last commit time
            # In real implementation, would use Delta Lake's describe history
            # df = spark.sql(f"DESCRIBE HISTORY {table_path}")
            # latest_version = df.first()
            # return latest_version.timestamp > last_check_timestamp
            return False
        except Exception:
            return False


class CatchupBackfillSchedule:
    """
    Allows backfill=true without duplicating runs
    """
    def __init__(self, config: Dict[str, Any]):
        """
        Args:
            config: {
                "schedule": "0 0 * * *",
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
                "max_active_runs": 5,
                "depends_on_past": True
            }
        """
        self.config = config

    def get_schedule_config(self) -> Dict[str, Any]:
        """Get schedule configuration for orchestrator."""
        return {
            "schedule": self.config.get("schedule", "0 0 * * *"),
            "catchup": True,
            "start_date": self.config.get("start_date"),
            "end_date": self.config.get("end_date"),
            "max_active_runs": self.config.get("max_active_runs", 5),
            "depends_on_past": self.config.get("depends_on_past", True)
        }

    def should_run_for_date(self, execution_date: datetime, completed_dates: List[datetime]) -> bool:
        """Determine if pipeline should run for given date."""
        # Check if this date has already been successfully completed
        if execution_date in completed_dates:
            return False

        # Check if previous date completed (if depends_on_past)
        if self.config.get("depends_on_past", True):
            # Would check if previous execution completed successfully
            pass

        return True


class CronExpressionBuilder:
    """
    Helper to build complex cron with holidays excluded
    """
    @staticmethod
    def build(schedule_type: str, time: str, exclude_holidays: bool = False,
              country: str = "US", custom_cron: str = None) -> str:
        """
        Build cron expression.

        Args:
            schedule_type: daily, hourly, weekly, monthly
            time: HH:MM format for daily/weekly/monthly
            exclude_holidays: Whether to skip holidays
            country: Country code for holiday calendar
            custom_cron: Custom cron expression

        Returns:
            Cron expression string
        """
        if custom_cron:
            return custom_cron

        if schedule_type == "daily":
            hour, minute = time.split(":")
            return f"{minute} {hour} * * *"
        elif schedule_type == "hourly":
            return "0 * * * *"
        elif schedule_type == "weekly":
            hour, minute = time.split(":")
            return f"{minute} {hour} * * 0"  # Sunday
        elif schedule_type == "monthly":
            hour, minute = time.split(":")
            return f"{minute} {hour} 1 * *"  # First of month

        return "* * * * *"


class HolidayCalendarSkip:
    """
    Skips runs on country-specific holidays
    """
    def __init__(self, country: str = "US"):
        self.country = country
        self._load_holidays()

    def _load_holidays(self):
        """Load holiday calendar for country."""
        # Placeholder - would use holidays library
        # import holidays
        # self.holiday_calendar = holidays.CountryHoliday(self.country)
        self.holiday_calendar = []

    def should_skip(self, date: datetime) -> bool:
        """Check if date is a holiday and should be skipped."""
        # return date.date() in self.holiday_calendar
        return False


class DependencyChainBuilder:
    """
    Programmatically wires task dependencies
    """
    def __init__(self):
        self.dependencies = {}

    def add_dependency(self, task: str, depends_on: List[str]):
        """Add dependency for a task."""
        self.dependencies[task] = depends_on

    def build_dependencies(self, tasks: List[Any]) -> Dict[str, List[str]]:
        """Build dependency graph for tasks."""
        return self.dependencies

    def get_execution_order(self) -> List[List[str]]:
        """Get topologically sorted execution order."""
        # Placeholder - would implement topological sort
        return []


class SLAMonitor:
    """
    Measures end-to-end latency + row counts → alerts if breached
    """
    def __init__(self, config: Dict[str, Any]):
        """
        Args:
            config: {
                "max_duration_seconds": 3600,
                "min_row_count": 1000,
                "max_row_count": 1000000,
                "alert_channels": ["slack", "email"]
            }
        """
        self.config = config

    def check_sla(self, start_time: datetime, end_time: datetime,
                   row_count: int, sla_config: Dict = None) -> bool:
        """
        Check if SLA was met.

        Returns:
            True if SLA met, False if breached
        """
        config = sla_config or self.config

        # Check duration
        duration_seconds = (end_time - start_time).total_seconds()
        if duration_seconds > config.get("max_duration_seconds", float('inf')):
            return False

        # Check row count bounds
        min_rows = config.get("min_row_count", 0)
        max_rows = config.get("max_row_count", float('inf'))
        if not (min_rows <= row_count <= max_rows):
            return False

        return True

    def get_sla_metrics(self, start_time: datetime, end_time: datetime,
                        row_count: int) -> Dict[str, Any]:
        """Get SLA metrics for reporting."""
        duration_seconds = (end_time - start_time).total_seconds()

        return {
            "duration_seconds": duration_seconds,
            "row_count": row_count,
            "sla_met": self.check_sla(start_time, end_time, row_count),
            "timestamp": end_time.isoformat()
        }


class DataFreshnessMonitor:
    """
    Compares max(event_date) vs expectations
    """
    def __init__(self, config: Dict[str, Any]):
        """
        Args:
            config: {
                "table": "catalog.schema.table",
                "event_column": "event_timestamp",
                "max_age_hours": 24,
                "alert_channels": ["slack"]
            }
        """
        self.config = config

    def check_freshness(self, spark: SparkSession, max_event_date: datetime = None,
                       threshold_hours: int = None) -> bool:
        """
        Check if data is fresh enough.

        Returns:
            True if data is fresh, False if stale
        """
        threshold = threshold_hours or self.config.get("max_age_hours", 24)

        if max_event_date is None:
            # Query table to get max event date
            table = self.config.get("table")
            event_column = self.config.get("event_column", "event_timestamp")

            try:
                result = spark.sql(f"SELECT MAX({event_column}) as max_date FROM {table}")
                max_event_date = result.first()["max_date"]
            except Exception:
                return False

        if max_event_date is None:
            return False

        # Check if data is within threshold
        now = datetime.now()
        age_hours = (now - max_event_date).total_seconds() / 3600

        return age_hours <= threshold

    def get_freshness_metrics(self, spark: SparkSession) -> Dict[str, Any]:
        """Get freshness metrics for reporting."""
        table = self.config.get("table")
        event_column = self.config.get("event_column", "event_timestamp")

        try:
            result = spark.sql(f"""
                SELECT
                    MAX({event_column}) as max_date,
                    MIN({event_column}) as min_date,
                    COUNT(*) as row_count
                FROM {table}
            """)
            row = result.first()

            max_date = row["max_date"]
            age_hours = (datetime.now() - max_date).total_seconds() / 3600 if max_date else None

            return {
                "table": table,
                "max_event_date": max_date.isoformat() if max_date else None,
                "min_event_date": row["min_date"].isoformat() if row["min_date"] else None,
                "age_hours": age_hours,
                "row_count": row["row_count"],
                "is_fresh": self.check_freshness(spark, max_date)
            }
        except Exception as e:
            return {
                "table": table,
                "error": str(e),
                "is_fresh": False
            }


class PipelineLineageVisualizer:
    """
    Generates HTML/OpenLineage JSON visualization of full config-driven DAG
    """
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}

    def generate_visualization(self, pipeline_config: Dict) -> str:
        """Generate HTML visualization of pipeline DAG."""
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Pipeline Lineage: {pipeline_config.get('pipeline_name', 'Unknown')}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .node {{ border: 1px solid #ccc; padding: 10px; margin: 5px;
                        border-radius: 5px; background: #f9f9f9; }}
                .edge {{ margin-left: 20px; color: #666; }}
            </style>
        </head>
        <body>
            <h1>Pipeline: {pipeline_config.get('pipeline_name', 'Unknown')}</h1>
            <h2>Type: {pipeline_config.get('pipeline_type', 'Unknown')}</h2>
            <div class="lineage">
                <!-- Lineage visualization would be generated here -->
            </div>
        </body>
        </html>
        """
        return html

    def generate_openlineage_json(self, pipeline_config: Dict) -> Dict[str, Any]:
        """Generate OpenLineage-compliant JSON."""
        return {
            "eventType": "START",
            "eventTime": datetime.now().isoformat(),
            "run": {
                "runId": "run-id-placeholder",
                "facets": {}
            },
            "job": {
                "namespace": "sparkle",
                "name": pipeline_config.get("pipeline_name", "unknown"),
                "facets": {
                    "documentation": {
                        "description": pipeline_config.get("description", "")
                    }
                }
            },
            "inputs": [],
            "outputs": [],
            "producer": "sparkle-orchestrator",
            "schemaURL": "https://openlineage.io/spec/1-0-0/OpenLineage.json"
        }
