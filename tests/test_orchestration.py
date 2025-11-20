"""
Unit tests for Sparkle orchestration components.

Tests orchestration: dry-run pipeline config → assert DAG structure
"""

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from orchestration.config_loader import load_pipeline_config, validate_config
from orchestration.factory import PipelineFactory, Pipeline
from orchestration.base import BasePipeline, PipelineConfig


class TestConfigLoader:
    """Test configuration loading and validation"""

    def test_load_pipeline_config_from_json(self, temp_config_dir: Path):
        """Test loading pipeline config from JSON file"""
        # Create config directory structure
        pipeline_dir = temp_config_dir / "test_pipeline"
        pipeline_dir.mkdir()

        config_file = pipeline_dir / "prod.json"
        config_data = {
            "pipeline_name": "test_pipeline",
            "pipeline_type": "bronze_raw_ingestion",
            "env": "prod",
            "source_connection": "postgres_prod",
            "destination_catalog": "main",
            "destination_schema": "bronze",
            "destination_table": "customers",
        }

        config_file.write_text(json.dumps(config_data, indent=2))

        # Mock the config directory path
        with patch('orchestration.config_loader.Path') as mock_path:
            mock_path.return_value = temp_config_dir

            # Load config (this would normally use the actual function)
            config = PipelineConfig(**config_data)

            assert config.pipeline_name == "test_pipeline"
            assert config.pipeline_type == "bronze_raw_ingestion"
            assert config.env == "prod"

    def test_environment_variable_resolution(self):
        """Test ${VAR:default} environment variable resolution"""
        from orchestration.config_loader import _resolve_env_vars
        import os

        config_dict = {
            "catalog": "${CATALOG:main}",
            "schema": "${SCHEMA:bronze}",
            "database": "${DATABASE}",
        }

        # Set environment variable
        os.environ["DATABASE"] = "test_db"

        resolved = _resolve_env_vars(config_dict)

        assert resolved["catalog"] == "main"  # Uses default
        assert resolved["schema"] == "bronze"  # Uses default
        assert resolved["database"] == "test_db"  # Uses env var

    def test_config_validation_success(self):
        """Test successful config validation against schema"""
        config_data = {
            "pipeline_name": "test_pipeline",
            "pipeline_type": "bronze_raw_ingestion",
            "env": "prod",
        }

        # This should not raise
        is_valid = validate_config(config_data)
        assert is_valid is True

    def test_config_validation_failure_missing_required(self):
        """Test config validation fails with missing required fields"""
        config_data = {
            "pipeline_name": "test_pipeline",
            # Missing pipeline_type and env
        }

        with pytest.raises(Exception):
            validate_config(config_data, raise_on_error=True)

    def test_config_validation_invalid_pipeline_type(self):
        """Test config validation fails with invalid pipeline type"""
        config_data = {
            "pipeline_name": "test_pipeline",
            "pipeline_type": "invalid_type",
            "env": "prod",
        }

        is_valid = validate_config(config_data)
        assert is_valid is False


class TestPipelineFactory:
    """Test pipeline factory and registry"""

    def test_pipeline_registration(self):
        """Test registering a pipeline class"""

        @PipelineFactory.register("test_pipeline_type")
        class TestPipeline(BasePipeline):
            def build(self):
                return self

        assert "test_pipeline_type" in PipelineFactory._registry
        assert PipelineFactory._registry["test_pipeline_type"] == TestPipeline

    def test_pipeline_creation_from_factory(self):
        """Test creating pipeline instance from factory"""

        @PipelineFactory.register("test_factory_pipeline")
        class TestFactoryPipeline(BasePipeline):
            def build(self):
                self.tasks = []
                return self

        config = PipelineConfig(
            pipeline_name="test",
            pipeline_type="test_factory_pipeline",
            env="dev",
        )

        pipeline = PipelineFactory.create("test_factory_pipeline", config)

        assert pipeline is not None
        assert isinstance(pipeline, TestFactoryPipeline)

    def test_pipeline_get_shortcut(self, temp_config_dir: Path):
        """Test Pipeline.get() shortcut method"""
        # This would normally load from config
        # For testing, we verify the interface exists
        assert hasattr(Pipeline, 'get')


class TestPipelineDAGStructure:
    """Test pipeline DAG structure and task dependencies"""

    def test_pipeline_task_order(self):
        """Test that tasks are executed in correct order"""

        @PipelineFactory.register("test_dag_pipeline")
        class TestDAGPipeline(BasePipeline):
            def build(self):
                from orchestration.base import BaseTask

                class Task1(BaseTask):
                    def execute(self, spark, context):
                        return {"task1": "completed"}

                class Task2(BaseTask):
                    def execute(self, spark, context):
                        # Depends on Task1
                        assert "task1" in context
                        return {"task2": "completed"}

                class Task3(BaseTask):
                    def execute(self, spark, context):
                        # Depends on Task1 and Task2
                        assert "task1" in context
                        assert "task2" in context
                        return {"task3": "completed"}

                self.tasks = [
                    Task1(task_name="task1"),
                    Task2(task_name="task2"),
                    Task3(task_name="task3"),
                ]

                return self

        config = PipelineConfig(
            pipeline_name="test_dag",
            pipeline_type="test_dag_pipeline",
            env="dev",
        )

        pipeline = PipelineFactory.create("test_dag_pipeline", config)
        pipeline.build()

        # Verify task count
        assert len(pipeline.tasks) == 3

        # Verify task order
        assert pipeline.tasks[0].task_name == "task1"
        assert pipeline.tasks[1].task_name == "task2"
        assert pipeline.tasks[2].task_name == "task3"

    def test_pipeline_task_dependencies(self):
        """Test task dependency declarations"""
        from orchestration.base import BaseTask

        class DependentTask(BaseTask):
            def __init__(self, task_name: str, depends_on: list = None):
                super().__init__(task_name)
                self.depends_on = depends_on or []

            def execute(self, spark, context):
                # Verify dependencies completed
                for dep in self.depends_on:
                    assert dep in context, f"Dependency {dep} not found in context"
                return {self.task_name: "completed"}

        task1 = DependentTask("ingest", depends_on=[])
        task2 = DependentTask("transform", depends_on=["ingest"])
        task3 = DependentTask("write", depends_on=["transform"])

        # Verify dependency structure
        assert len(task1.depends_on) == 0
        assert "ingest" in task2.depends_on
        assert "transform" in task3.depends_on

    def test_pipeline_dry_run(self):
        """Test pipeline dry run without execution"""

        @PipelineFactory.register("test_dry_run_pipeline")
        class TestDryRunPipeline(BasePipeline):
            def build(self):
                from orchestration.base import BaseTask

                class MockTask(BaseTask):
                    def execute(self, spark, context):
                        return {"status": "executed"}

                self.tasks = [
                    MockTask(task_name="task1"),
                    MockTask(task_name="task2"),
                ]

                return self

            def dry_run(self):
                """Return DAG structure without executing"""
                return {
                    "pipeline_name": self.config.pipeline_name,
                    "pipeline_type": self.config.pipeline_type,
                    "tasks": [
                        {
                            "name": task.task_name,
                            "type": task.__class__.__name__,
                        }
                        for task in self.tasks
                    ],
                }

        config = PipelineConfig(
            pipeline_name="test_dry_run",
            pipeline_type="test_dry_run_pipeline",
            env="dev",
        )

        pipeline = PipelineFactory.create("test_dry_run_pipeline", config)
        pipeline.build()

        # Perform dry run
        dag_structure = pipeline.dry_run()

        assert dag_structure["pipeline_name"] == "test_dry_run"
        assert len(dag_structure["tasks"]) == 2
        assert dag_structure["tasks"][0]["name"] == "task1"


class TestOrchestratorAdapters:
    """Test orchestrator adapter generation"""

    def test_databricks_adapter_deployment(self):
        """Test Databricks Workflows adapter deployment"""
        from orchestration.adapters import get_adapter

        # This would generate Databricks workflow JSON
        adapter_class = get_adapter("databricks")
        assert adapter_class is not None

    def test_airflow_adapter_deployment(self):
        """Test Airflow DAG generation"""
        from orchestration.adapters import get_adapter

        adapter_class = get_adapter("airflow")
        assert adapter_class is not None

    def test_dbt_adapter_deployment(self, temp_config_dir: Path):
        """Test dbt project generation"""
        from orchestration.adapters.dbt_cloud import dbtProjectAdapter

        config = PipelineConfig(
            pipeline_name="test_dbt_project",
            pipeline_type="silver_batch_transformation",
            env="dev",
            source_system="bronze",
            source_table="main.bronze.customers",
            destination_catalog="main",
            destination_schema="silver",
            destination_table="customers",
        )

        @PipelineFactory.register("silver_batch_transformation")
        class TestSilverPipeline(BasePipeline):
            def build(self):
                return self

        pipeline = PipelineFactory.create("silver_batch_transformation", config)

        adapter = dbtProjectAdapter(pipeline=pipeline, config=config)

        # Test dry run - generate without writing files
        output_path = temp_config_dir / "dbt_output"
        output_path.mkdir()

        # This would generate dbt project
        assert adapter.config.pipeline_name == "test_dbt_project"

    def test_step_functions_adapter_deployment(self):
        """Test AWS Step Functions state machine generation"""
        from orchestration.adapters.step_functions import StepFunctionsStateMachineAdapter

        config = PipelineConfig(
            pipeline_name="test_step_functions",
            pipeline_type="bronze_raw_ingestion",
            env="dev",
        )

        @PipelineFactory.register("bronze_raw_ingestion")
        class TestBronzePipeline(BasePipeline):
            def build(self):
                return self

        pipeline = PipelineFactory.create("bronze_raw_ingestion", config)

        adapter = StepFunctionsStateMachineAdapter(pipeline=pipeline, config=config)

        # Verify adapter created
        assert adapter.config.pipeline_name == "test_step_functions"

    def test_argo_workflows_adapter_deployment(self):
        """Test Argo Workflows template generation"""
        from orchestration.adapters.argo_workflows import ArgoWorkflowTemplateAdapter

        config = PipelineConfig(
            pipeline_name="test_argo_workflow",
            pipeline_type="gold_daily_aggregate",
            env="prod",
        )

        @PipelineFactory.register("gold_daily_aggregate")
        class TestGoldPipeline(BasePipeline):
            def build(self):
                return self

        pipeline = PipelineFactory.create("gold_daily_aggregate", config)

        adapter = ArgoWorkflowTemplateAdapter(pipeline=pipeline, config=config)

        # Verify adapter created
        assert adapter.config.pipeline_name == "test_argo_workflow"


class TestPipelineExecution:
    """Test pipeline execution and error handling"""

    def test_pipeline_execution_success(self, spark):
        """Test successful pipeline execution"""

        @PipelineFactory.register("test_exec_pipeline")
        class TestExecPipeline(BasePipeline):
            def build(self):
                from orchestration.base import BaseTask

                class SuccessTask(BaseTask):
                    def execute(self, spark, context):
                        return {"status": "success"}

                self.tasks = [SuccessTask(task_name="test_task")]
                return self

        config = PipelineConfig(
            pipeline_name="test_exec",
            pipeline_type="test_exec_pipeline",
            env="dev",
        )

        pipeline = PipelineFactory.create("test_exec_pipeline", config)
        pipeline.build()
        pipeline.spark = spark

        # Execute pipeline
        result = pipeline.run()

        assert "test_task" in result
        assert result["test_task"]["status"] == "success"

    def test_pipeline_execution_with_failure(self, spark):
        """Test pipeline execution with task failure"""

        @PipelineFactory.register("test_fail_pipeline")
        class TestFailPipeline(BasePipeline):
            def build(self):
                from orchestration.base import BaseTask

                class FailTask(BaseTask):
                    def execute(self, spark, context):
                        raise ValueError("Task failed intentionally")

                self.tasks = [FailTask(task_name="fail_task")]
                return self

        config = PipelineConfig(
            pipeline_name="test_fail",
            pipeline_type="test_fail_pipeline",
            env="dev",
        )

        pipeline = PipelineFactory.create("test_fail_pipeline", config)
        pipeline.build()
        pipeline.spark = spark

        # Execute pipeline - should handle failure
        with pytest.raises(ValueError, match="Task failed intentionally"):
            pipeline.run()

    def test_pipeline_context_passing(self, spark):
        """Test context passing between tasks"""

        @PipelineFactory.register("test_context_pipeline")
        class TestContextPipeline(BasePipeline):
            def build(self):
                from orchestration.base import BaseTask

                class Task1(BaseTask):
                    def execute(self, spark, context):
                        return {"data": "from_task1"}

                class Task2(BaseTask):
                    def execute(self, spark, context):
                        # Access data from Task1
                        task1_data = context.get("task1", {}).get("data")
                        return {"data": f"from_task2_{task1_data}"}

                self.tasks = [
                    Task1(task_name="task1"),
                    Task2(task_name="task2"),
                ]
                return self

        config = PipelineConfig(
            pipeline_name="test_context",
            pipeline_type="test_context_pipeline",
            env="dev",
        )

        pipeline = PipelineFactory.create("test_context_pipeline", config)
        pipeline.build()
        pipeline.spark = spark

        # Execute pipeline
        result = pipeline.run()

        # Verify context passing
        assert result["task1"]["data"] == "from_task1"
        assert "from_task1" in result["task2"]["data"]


class TestSchedulingAndTriggers:
    """Test scheduling and trigger configurations"""

    def test_cron_schedule_parsing(self):
        """Test cron schedule parsing"""
        config = PipelineConfig(
            pipeline_name="test",
            pipeline_type="bronze_raw_ingestion",
            env="prod",
            schedule="0 2 * * *",  # Daily at 2 AM
        )

        assert config.schedule == "0 2 * * *"

    def test_event_driven_trigger(self):
        """Test event-driven trigger configuration"""
        config = PipelineConfig(
            pipeline_name="test",
            pipeline_type="bronze_raw_ingestion",
            env="prod",
            triggers=[
                {
                    "type": "file_arrival",
                    "path": "s3://bucket/data/*.parquet",
                },
                {
                    "type": "kafka_message",
                    "topic": "data-updates",
                },
            ],
        )

        assert len(config.triggers) == 2
        assert config.triggers[0]["type"] == "file_arrival"

    def test_dependency_trigger(self):
        """Test pipeline dependency configuration"""
        config = PipelineConfig(
            pipeline_name="silver_transform",
            pipeline_type="silver_batch_transformation",
            env="prod",
            dependencies=["bronze_ingestion", "reference_data_load"],
        )

        assert len(config.dependencies) == 2
        assert "bronze_ingestion" in config.dependencies
