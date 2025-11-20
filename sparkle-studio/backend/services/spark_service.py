"""
Service for managing Spark execution.
Handles local Spark sessions and remote cluster submissions (Databricks/EMR).
"""
import uuid
import time
from typing import Any, Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame

from core.config import settings
from core.dependencies import get_spark
from schemas.execution import (
    ExecutionStatus,
    ExecutionResponse,
    ExecutionDetail,
    DryRunResult,
    DryRunResponse,
)
from schemas.pipeline import Pipeline


class SparkService:
    """Service for Spark execution."""

    def __init__(self):
        self._executions: dict[str, ExecutionDetail] = {}

    def dry_run_pipeline(
        self,
        pipeline: Pipeline,
        sample_size: int = 1000,
        node_id: Optional[str] = None,
    ) -> DryRunResponse:
        """
        Execute pipeline with sample data (dry-run mode).
        Used by Live Preview in Phase 5.
        """
        run_id = str(uuid.uuid4())
        start_time = time.time()

        results: list[DryRunResult] = []
        success = True
        error_message = None

        try:
            spark = get_spark()

            # Execute nodes in order (respecting edges)
            execution_order = self._get_execution_order(pipeline)

            # Track dataframes for each node
            node_outputs: dict[str, DataFrame] = {}

            for current_node in execution_order:
                node_start = time.time()

                try:
                    # Get input dataframes from upstream nodes
                    input_dfs = []
                    for edge in pipeline.edges:
                        if edge.target == current_node.id:
                            if edge.source in node_outputs:
                                input_dfs.append(node_outputs[edge.source])

                    # Execute node (simulated - will be real in Phase 5)
                    output_df = self._execute_node(
                        spark, current_node, input_dfs, sample_size
                    )

                    if output_df is not None:
                        node_outputs[current_node.id] = output_df

                        # Collect sample data
                        sample_data = output_df.limit(100).toPandas().to_dict(orient="records")
                        schema = {
                            "fields": [
                                {"name": field.name, "type": str(field.dataType)}
                                for field in output_df.schema
                            ]
                        }

                        node_time = (time.time() - node_start) * 1000

                        results.append(
                            DryRunResult(
                                success=True,
                                node_id=current_node.id,
                                node_name=current_node.data.label,
                                sample_data=sample_data,
                                row_count=output_df.count(),
                                schema_=schema,
                                execution_time_ms=node_time,
                            )
                        )

                    # Stop if we've reached the target node
                    if node_id and current_node.id == node_id:
                        break

                except Exception as e:
                    results.append(
                        DryRunResult(
                            success=False,
                            node_id=current_node.id,
                            node_name=current_node.data.label,
                            error=str(e),
                            execution_time_ms=(time.time() - node_start) * 1000,
                        )
                    )
                    success = False
                    error_message = f"Node {current_node.data.label} failed: {str(e)}"
                    break

        except Exception as e:
            success = False
            error_message = str(e)

        total_time = (time.time() - start_time) * 1000

        return DryRunResponse(
            run_id=run_id,
            pipeline_name=pipeline.metadata.name,
            results=results,
            total_execution_time_ms=total_time,
            success=success,
            error=error_message,
        )

    def submit_pipeline(
        self,
        pipeline: Pipeline,
        parameters: dict[str, Any],
        dry_run: bool = False,
    ) -> ExecutionResponse:
        """
        Submit pipeline for execution.
        Returns immediately with run_id for tracking.
        """
        run_id = str(uuid.uuid4())

        # Create execution record
        execution = ExecutionDetail(
            run_id=run_id,
            pipeline_name=pipeline.metadata.name,
            status=ExecutionStatus.PENDING,
            submitted_at=datetime.now(),
            parameters=parameters,
        )

        self._executions[run_id] = execution

        # Submit to cluster (stub for Phase 1, will be real in Phase 6)
        if settings.CLUSTER_TYPE == "databricks":
            self._submit_to_databricks(pipeline, parameters, run_id)
        elif settings.CLUSTER_TYPE == "emr":
            self._submit_to_emr(pipeline, parameters, run_id)
        else:
            # Local execution (simulated)
            execution.status = ExecutionStatus.RUNNING
            execution.started_at = datetime.now()

        return ExecutionResponse(
            run_id=run_id,
            pipeline_name=pipeline.metadata.name,
            status=execution.status,
            submitted_at=execution.submitted_at,
            message="Pipeline submitted successfully",
        )

    def get_execution(self, run_id: str) -> Optional[ExecutionDetail]:
        """Get execution status and details."""
        return self._executions.get(run_id)

    def _get_execution_order(self, pipeline: Pipeline) -> list:
        """
        Determine execution order based on pipeline edges (topological sort).
        """
        # Build adjacency list
        graph: dict[str, list[str]] = {node.id: [] for node in pipeline.nodes}
        in_degree = {node.id: 0 for node in pipeline.nodes}

        for edge in pipeline.edges:
            graph[edge.source].append(edge.target)
            in_degree[edge.target] += 1

        # Find nodes with no incoming edges
        queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
        order = []

        while queue:
            node_id = queue.pop(0)
            order.append(node_id)

            for neighbor in graph[node_id]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # Map back to node objects
        node_map = {node.id: node for node in pipeline.nodes}
        return [node_map[node_id] for node_id in order if node_id in node_map]

    def _execute_node(
        self,
        spark: SparkSession,
        node: Any,
        input_dfs: list[DataFrame],
        sample_size: int,
    ) -> Optional[DataFrame]:
        """
        Execute a single node (stub for Phase 1).
        Real implementation will be in Phase 5-6.
        """
        # For Phase 1, create dummy dataframe
        if input_dfs:
            # Transform input
            return input_dfs[0].limit(sample_size)
        else:
            # Source node - create sample data
            data = [{"id": i, "value": f"sample_{i}"} for i in range(sample_size)]
            return spark.createDataFrame(data)

    def _submit_to_databricks(
        self, pipeline: Pipeline, parameters: dict, run_id: str
    ):
        """Submit job to Databricks (stub for Phase 6)."""
        # Will use Databricks Jobs API
        pass

    def _submit_to_emr(self, pipeline: Pipeline, parameters: dict, run_id: str):
        """Submit job to EMR (stub for Phase 6)."""
        # Will use EMR API
        pass


# Global spark service instance
spark_service = SparkService()
