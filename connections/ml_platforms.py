"""
Machine Learning platform connections.

Supports: MLflow, AWS SageMaker, Google Vertex AI, Azure ML,
Databricks Feature Store, Feast, Tecton.
"""

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession

from .base import SparkleConnection
from .factory import register_connection


@register_connection("mlflow")
class MLflowConnection(SparkleConnection):
    """
    MLflow tracking and model registry connection.

    Example config (config/connections/mlflow/prod.json):
        {
            "tracking_uri": "https://mlflow.example.com",
            "registry_uri": "https://mlflow.example.com",
            "experiment_name": "customer_churn",
            "auth_type": "basic",
            "username": "${MLFLOW_USER}",
            "password": "${MLFLOW_PASSWORD}"
        }

    Or with Databricks:
        {
            "tracking_uri": "databricks",
            "registry_uri": "databricks-uc",
            "workspace_url": "https://dbc-abc123.cloud.databricks.com",
            "access_token": "${DATABRICKS_TOKEN}"
        }

    Usage:
        >>> conn = get_connection("mlflow", spark, env="prod")
        >>> # MLflow tracking is automatically configured
        >>> import mlflow
        >>> mlflow.set_tracking_uri(conn.get_tracking_uri())
        >>> with mlflow.start_run():
        ...     mlflow.log_metric("accuracy", 0.95)
    """

    def test(self) -> bool:
        """Test MLflow connection."""
        try:
            import mlflow

            tracking_uri = self.config.get("tracking_uri", "http://localhost:5000")
            mlflow.set_tracking_uri(tracking_uri)

            # Try to list experiments
            experiments = mlflow.search_experiments()
            return True
        except Exception as e:
            self.logger.error(f"MLflow connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get MLflow configuration."""
        return {
            "tracking_uri": self.config.get("tracking_uri", ""),
            "registry_uri": self.config.get("registry_uri", ""),
            "experiment_name": self.config.get("experiment_name", "")
        }

    def get_tracking_uri(self) -> str:
        """Get MLflow tracking URI."""
        return self.config.get("tracking_uri", "http://localhost:5000")

    def get_registry_uri(self) -> str:
        """Get MLflow model registry URI."""
        return self.config.get("registry_uri", self.get_tracking_uri())

    def configure_mlflow(self) -> None:
        """Configure MLflow with connection settings."""
        import mlflow

        mlflow.set_tracking_uri(self.get_tracking_uri())
        mlflow.set_registry_uri(self.get_registry_uri())

        if "experiment_name" in self.config:
            mlflow.set_experiment(self.config["experiment_name"])

        # Set authentication if needed
        if self.config.get("auth_type") == "basic":
            import os
            os.environ["MLFLOW_TRACKING_USERNAME"] = self.config["username"]
            os.environ["MLFLOW_TRACKING_PASSWORD"] = self.config["password"]
        elif self.config.get("auth_type") == "token":
            import os
            os.environ["MLFLOW_TRACKING_TOKEN"] = self.config["token"]


@register_connection("sagemaker")
@register_connection("aws_sagemaker")
class SageMakerConnection(SparkleConnection):
    """
    AWS SageMaker connection.

    Example config:
        {
            "region": "us-east-1",
            "role_arn": "arn:aws:iam::123456789012:role/SageMakerRole",
            "s3_bucket": "my-sagemaker-bucket",
            "auth_type": "iam_role"
        }

    Or with explicit credentials:
        {
            "region": "us-east-1",
            "aws_access_key_id": "${AWS_ACCESS_KEY_ID}",
            "aws_secret_access_key": "${AWS_SECRET_ACCESS_KEY}",
            "role_arn": "arn:aws:iam::123456789012:role/SageMakerRole",
            "s3_bucket": "my-sagemaker-bucket"
        }
    """

    def test(self) -> bool:
        """Test SageMaker connection."""
        try:
            import boto3

            # Create SageMaker client
            session_kwargs = {"region_name": self.config["region"]}

            if "aws_access_key_id" in self.config:
                session_kwargs["aws_access_key_id"] = self.config["aws_access_key_id"]
                session_kwargs["aws_secret_access_key"] = self.config["aws_secret_access_key"]

            session = boto3.Session(**session_kwargs)
            sagemaker_client = session.client("sagemaker")

            # Test by listing notebook instances (should return empty list if none exist)
            sagemaker_client.list_notebook_instances(MaxResults=1)
            return True
        except Exception as e:
            self.logger.error(f"SageMaker connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get SageMaker configuration."""
        return {
            "region": self.config["region"],
            "role_arn": self.config.get("role_arn", ""),
            "s3_bucket": self.config.get("s3_bucket", "")
        }

    def get_sagemaker_session(self):
        """Get configured SageMaker session."""
        import boto3
        import sagemaker

        session_kwargs = {"region_name": self.config["region"]}

        if "aws_access_key_id" in self.config:
            session_kwargs["aws_access_key_id"] = self.config["aws_access_key_id"]
            session_kwargs["aws_secret_access_key"] = self.config["aws_secret_access_key"]

        boto_session = boto3.Session(**session_kwargs)

        return sagemaker.Session(
            boto_session=boto_session,
            default_bucket=self.config.get("s3_bucket")
        )


@register_connection("vertex_ai")
@register_connection("gcp_vertex_ai")
class VertexAIConnection(SparkleConnection):
    """
    Google Cloud Vertex AI connection.

    Example config:
        {
            "project_id": "my-gcp-project",
            "region": "us-central1",
            "staging_bucket": "gs://my-vertex-bucket",
            "service_account_key_file": "/path/to/keyfile.json"
        }
    """

    def test(self) -> bool:
        """Test Vertex AI connection."""
        try:
            from google.cloud import aiplatform

            aiplatform.init(
                project=self.config["project_id"],
                location=self.config.get("region", "us-central1"),
                staging_bucket=self.config.get("staging_bucket")
            )

            return True
        except Exception as e:
            self.logger.error(f"Vertex AI connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Vertex AI configuration."""
        return {
            "project_id": self.config["project_id"],
            "region": self.config.get("region", "us-central1"),
            "staging_bucket": self.config.get("staging_bucket", "")
        }

    def initialize_vertex_ai(self) -> None:
        """Initialize Vertex AI SDK."""
        from google.cloud import aiplatform

        init_kwargs = {
            "project": self.config["project_id"],
            "location": self.config.get("region", "us-central1")
        }

        if "staging_bucket" in self.config:
            init_kwargs["staging_bucket"] = self.config["staging_bucket"]

        if "service_account_key_file" in self.config:
            import os
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.config["service_account_key_file"]

        aiplatform.init(**init_kwargs)


@register_connection("azure_ml")
@register_connection("azureml")
class AzureMLConnection(SparkleConnection):
    """
    Azure Machine Learning connection.

    Example config:
        {
            "subscription_id": "abc123-def456-ghi789",
            "resource_group": "ml-resources",
            "workspace_name": "prod-ml-workspace",
            "tenant_id": "${AZURE_TENANT_ID}",
            "client_id": "${AZURE_CLIENT_ID}",
            "client_secret": "${AZURE_CLIENT_SECRET}"
        }
    """

    def test(self) -> bool:
        """Test Azure ML connection."""
        try:
            from azureml.core import Workspace
            from azureml.core.authentication import ServicePrincipalAuthentication

            auth = ServicePrincipalAuthentication(
                tenant_id=self.config["tenant_id"],
                service_principal_id=self.config["client_id"],
                service_principal_password=self.config["client_secret"]
            )

            ws = Workspace(
                subscription_id=self.config["subscription_id"],
                resource_group=self.config["resource_group"],
                workspace_name=self.config["workspace_name"],
                auth=auth
            )

            return ws is not None
        except Exception as e:
            self.logger.error(f"Azure ML connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Azure ML configuration."""
        return {
            "subscription_id": self.config["subscription_id"],
            "resource_group": self.config["resource_group"],
            "workspace_name": self.config["workspace_name"]
        }

    def get_workspace(self):
        """Get Azure ML Workspace."""
        from azureml.core import Workspace
        from azureml.core.authentication import ServicePrincipalAuthentication

        auth = ServicePrincipalAuthentication(
            tenant_id=self.config["tenant_id"],
            service_principal_id=self.config["client_id"],
            service_principal_password=self.config["client_secret"]
        )

        return Workspace(
            subscription_id=self.config["subscription_id"],
            resource_group=self.config["resource_group"],
            workspace_name=self.config["workspace_name"],
            auth=auth
        )


@register_connection("feature_store")
@register_connection("databricks_feature_store")
class FeatureStoreConnection(SparkleConnection):
    """
    Databricks Feature Store connection.

    Example config:
        {
            "workspace_url": "https://dbc-abc123.cloud.databricks.com",
            "access_token": "${DATABRICKS_TOKEN}",
            "catalog": "main",
            "schema": "feature_store"
        }
    """

    def test(self) -> bool:
        """Test Feature Store connection."""
        try:
            from databricks import feature_store

            fs = feature_store.FeatureStoreClient()
            return True
        except Exception as e:
            self.logger.error(f"Feature Store connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Feature Store configuration."""
        return {
            "catalog": self.config.get("catalog", "main"),
            "schema": self.config.get("schema", "feature_store")
        }

    def get_client(self):
        """Get Feature Store client."""
        from databricks import feature_store

        return feature_store.FeatureStoreClient()


@register_connection("feast")
class FeastConnection(SparkleConnection):
    """
    Feast feature store connection.

    Example config:
        {
            "repo_path": "/path/to/feature_repo",
            "registry": "s3://my-bucket/feast/registry.db",
            "online_store": {
                "type": "redis",
                "connection_string": "redis.example.com:6379"
            },
            "offline_store": {
                "type": "spark",
                "spark_conf": {
                    "spark.master": "local"
                }
            }
        }
    """

    def test(self) -> bool:
        """Test Feast connection."""
        try:
            from feast import FeatureStore

            store = FeatureStore(repo_path=self.config.get("repo_path", "."))
            return True
        except Exception as e:
            self.logger.error(f"Feast connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Feast configuration."""
        return {
            "repo_path": self.config.get("repo_path", "."),
            "registry": self.config.get("registry", "")
        }

    def get_feature_store(self):
        """Get Feast feature store."""
        from feast import FeatureStore

        return FeatureStore(repo_path=self.config.get("repo_path", "."))


@register_connection("tecton")
class TectonConnection(SparkleConnection):
    """
    Tecton feature platform connection.

    Example config:
        {
            "api_key": "${TECTON_API_KEY}",
            "workspace": "prod",
            "url": "https://app.tecton.ai"
        }
    """

    def test(self) -> bool:
        """Test Tecton connection."""
        try:
            import tecton

            tecton.conf.set("TECTON_API_KEY", self.config["api_key"])
            return True
        except Exception as e:
            self.logger.error(f"Tecton connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Tecton configuration."""
        return {
            "workspace": self.config.get("workspace", "prod"),
            "url": self.config.get("url", "https://app.tecton.ai")
        }
