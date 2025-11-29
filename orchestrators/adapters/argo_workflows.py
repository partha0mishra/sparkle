"""
Argo Workflows Adapter (5 components).

Generates Kubernetes-native workflows from Sparkle pipelines.
"""

import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional
from ..base import BasePipeline, BaseTask
import logging

logger = logging.getLogger(__name__)


class ArgoWorkflowTemplateAdapter:
    """
    Generates Argo Workflow templates from pipeline configuration.

    Creates WorkflowTemplate CRDs with all steps, artifacts, and dependencies.
    """

    def __init__(self, pipeline: BasePipeline):
        self.pipeline = pipeline
        self.config = pipeline.config

    def generate_deployment(self, output_path: Optional[str] = None) -> str:
        """
        Generate Argo Workflow template.

        Args:
            output_path: Output directory for workflow YAML

        Returns:
            Path to generated files
        """
        if output_path is None:
            output_path = Path.cwd() / "deployments" / "argo" / self.config.pipeline_name

        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Build pipeline to get tasks
        self.pipeline.build()

        # Generate WorkflowTemplate
        workflow_template = self._generate_workflow_template()

        # Write to file
        with open(output_dir / "workflow-template.yaml", "w") as f:
            yaml.dump(workflow_template, f, default_flow_style=False, sort_keys=False)

        # Generate Workflow (instantiation)
        workflow = self._generate_workflow()
        with open(output_dir / "workflow.yaml", "w") as f:
            yaml.dump(workflow, f, default_flow_style=False, sort_keys=False)

        # Generate CronWorkflow for scheduling
        if self.config.schedule:
            cron_workflow = self._generate_cron_workflow()
            with open(output_dir / "cron-workflow.yaml", "w") as f:
                yaml.dump(cron_workflow, f, default_flow_style=False, sort_keys=False)

        # Generate deployment script
        self._generate_deployment_script(output_dir)

        # Generate README
        self._generate_readme(output_dir)

        logger.info(f"Generated Argo Workflow at: {output_dir}")

        return str(output_dir)

    def _generate_workflow_template(self) -> Dict[str, Any]:
        """Generate WorkflowTemplate CRD."""
        templates = [self._generate_entrypoint_template()]

        # Add template for each task
        for task in self.pipeline.tasks:
            template = self._task_to_template(task)
            templates.append(template)

        workflow_template = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "WorkflowTemplate",
            "metadata": {
                "name": self.config.pipeline_name,
                "namespace": "argo",
                "labels": self._format_labels(),
                "annotations": {
                    "sparkle.io/pipeline-type": self.config.pipeline_type,
                    "sparkle.io/environment": self.config.env
                }
            },
            "spec": {
                "entrypoint": "main",
                "arguments": {
                    "parameters": self._generate_parameters()
                },
                "templates": templates,
                "volumeClaimTemplates": [
                    {
                        "metadata": {"name": "workdir"},
                        "spec": {
                            "accessModes": ["ReadWriteOnce"],
                            "resources": {
                                "requests": {"storage": "1Gi"}
                            }
                        }
                    }
                ],
                "serviceAccountName": "argo-workflow",
                "ttlStrategy": {
                    "secondsAfterCompletion": 86400,  # 24 hours
                    "secondsAfterSuccess": 86400,
                    "secondsAfterFailure": 604800  # 7 days
                }
            }
        }

        return workflow_template

    def _generate_entrypoint_template(self) -> Dict[str, Any]:
        """Generate main DAG template."""
        tasks = []

        for i, task in enumerate(self.pipeline.tasks):
            task_def = {
                "name": task.task_name.replace("_", "-"),
                "template": task.task_name.replace("_", "-"),
                "arguments": {
                    "parameters": [
                        {"name": "config", "value": "{{workflow.parameters.config}}"}
                    ]
                }
            }

            # Add dependencies
            if i > 0:
                task_def["dependencies"] = [self.pipeline.tasks[i-1].task_name.replace("_", "-")]

            tasks.append(task_def)

        return {
            "name": "main",
            "dag": {
                "tasks": tasks
            }
        }

    def _task_to_template(self, task: BaseTask) -> Dict[str, Any]:
        """
        Convert Sparkle task to Argo template.

        Args:
            task: Task to convert

        Returns:
            Template definition
        """
        template_name = task.task_name.replace("_", "-")

        # Determine container image based on task type
        container_image = self._get_container_image(task)

        return {
            "name": template_name,
            "inputs": {
                "parameters": [
                    {"name": "config"}
                ]
            },
            "container": {
                "image": container_image,
                "command": ["python", "/app/run_task.py"],
                "args": [
                    "--task-name", task.task_name,
                    "--config", "{{inputs.parameters.config}}"
                ],
                "env": [
                    {"name": "SPARK_HOME", "value": "/opt/spark"},
                    {"name": "PYTHONPATH", "value": "/app"},
                    {"name": "ENVIRONMENT", "value": self.config.env}
                ],
                "volumeMounts": [
                    {"name": "workdir", "mountPath": "/work"}
                ],
                "resources": {
                    "requests": {
                        "memory": "4Gi",
                        "cpu": "2"
                    },
                    "limits": {
                        "memory": "8Gi",
                        "cpu": "4"
                    }
                }
            },
            "retryStrategy": {
                "limit": "3",
                "retryPolicy": "OnFailure",
                "backoff": {
                    "duration": "60s",
                    "factor": 2,
                    "maxDuration": "10m"
                }
            }
        }

    def _generate_workflow(self) -> Dict[str, Any]:
        """Generate Workflow (instantiation of template)."""
        return {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "Workflow",
            "metadata": {
                "generateName": f"{self.config.pipeline_name}-",
                "namespace": "argo"
            },
            "spec": {
                "workflowTemplateRef": {
                    "name": self.config.pipeline_name
                },
                "arguments": {
                    "parameters": [
                        {
                            "name": "config",
                            "value": self._serialize_config()
                        }
                    ]
                }
            }
        }

    def _generate_cron_workflow(self) -> Dict[str, Any]:
        """Generate CronWorkflow for scheduled execution."""
        return {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "CronWorkflow",
            "metadata": {
                "name": f"{self.config.pipeline_name}-cron",
                "namespace": "argo"
            },
            "spec": {
                "schedule": self.config.schedule,
                "timezone": "UTC",
                "concurrencyPolicy": "Forbid",
                "startingDeadlineSeconds": 300,
                "workflowSpec": {
                    "workflowTemplateRef": {
                        "name": self.config.pipeline_name
                    },
                    "arguments": {
                        "parameters": [
                            {
                                "name": "config",
                                "value": self._serialize_config()
                            }
                        ]
                    }
                }
            }
        }

    def _generate_deployment_script(self, output_dir: Path):
        """Generate kubectl deployment script."""
        script = f'''#!/bin/bash
# Deployment script for Argo Workflow: {self.config.pipeline_name}

set -e

NAMESPACE="${{NAMESPACE:-argo}}"

echo "Deploying Argo Workflow: {self.config.pipeline_name}"

# Create namespace if it doesn't exist
kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

# Apply WorkflowTemplate
echo "Applying WorkflowTemplate..."
kubectl apply -f workflow-template.yaml -n $NAMESPACE

# Apply CronWorkflow (if exists)
if [ -f cron-workflow.yaml ]; then
    echo "Applying CronWorkflow..."
    kubectl apply -f cron-workflow.yaml -n $NAMESPACE
fi

echo "Deployment complete!"

# Submit a test workflow
echo "Submitting test workflow..."
kubectl create -f workflow.yaml -n $NAMESPACE

echo "Monitor workflow with:"
echo "  argo list -n $NAMESPACE"
echo "  argo get {self.config.pipeline_name}-xxxxx -n $NAMESPACE"
echo "  argo logs {self.config.pipeline_name}-xxxxx -n $NAMESPACE"
'''

        script_file = output_dir / "deploy.sh"
        with open(script_file, "w") as f:
            f.write(script.strip())

        script_file.chmod(0o755)

    def _generate_readme(self, output_dir: Path):
        """Generate README with usage instructions."""
        readme = f"""# Argo Workflow: {self.config.pipeline_name}

Kubernetes-native workflow generated from Sparkle pipeline configuration.

## Files

- `workflow-template.yaml` - Reusable workflow template
- `workflow.yaml` - Workflow instantiation (one-time run)
- `cron-workflow.yaml` - Scheduled workflow (if schedule is configured)
- `deploy.sh` - Deployment script

## Prerequisites

1. Kubernetes cluster with Argo Workflows installed
2. `kubectl` configured to access the cluster
3. `argo` CLI installed (optional, for easier workflow management)

## Installation

Install Argo Workflows if not already installed:

```bash
kubectl create namespace argo
kubectl apply -n argo -f https://github.com/argoproj/argo-workflows/releases/latest/download/install.yaml
```

## Deployment

Deploy the workflow:

```bash
./deploy.sh
```

Or manually:

```bash
# Apply template
kubectl apply -f workflow-template.yaml -n argo

# Submit workflow
kubectl create -f workflow.yaml -n argo

# Apply cron workflow (for scheduled execution)
kubectl apply -f cron-workflow.yaml -n argo
```

## Monitoring

List workflows:

```bash
argo list -n argo
# or
kubectl get workflows -n argo
```

Get workflow details:

```bash
argo get {self.config.pipeline_name}-xxxxx -n argo
```

View logs:

```bash
argo logs {self.config.pipeline_name}-xxxxx -n argo
```

## Configuration

Pipeline configuration:

- **Type**: {self.config.pipeline_type}
- **Environment**: {self.config.env}
- **Schedule**: {self.config.schedule or "Manual"}

## Cleanup

Delete the workflow template:

```bash
kubectl delete workflowtemplate {self.config.pipeline_name} -n argo
```

Delete cron workflow:

```bash
kubectl delete cronworkflow {self.config.pipeline_name}-cron -n argo
```
"""

        with open(output_dir / "README.md", "w") as f:
            f.write(readme)

    def _format_labels(self) -> Dict[str, str]:
        """Format labels for Kubernetes."""
        labels = {
            "sparkle.io/pipeline": self.config.pipeline_name,
            "sparkle.io/type": self.config.pipeline_type,
            "sparkle.io/env": self.config.env
        }

        if self.config.tags:
            for k, v in self.config.tags.items():
                # Kubernetes label keys must be valid DNS subdomains
                safe_key = k.replace("_", "-").lower()
                labels[f"sparkle.io/{safe_key}"] = v[:63]  # Max 63 chars

        return labels

    def _generate_parameters(self) -> List[Dict[str, Any]]:
        """Generate workflow parameters."""
        return [
            {
                "name": "config",
                "value": self._serialize_config()
            },
            {
                "name": "env",
                "value": self.config.env
            }
        ]

    def _serialize_config(self) -> str:
        """Serialize config to JSON string."""
        import json
        import dataclasses
        return json.dumps(dataclasses.asdict(self.config))

    def _get_container_image(self, task: BaseTask) -> str:
        """Get appropriate container image for task."""
        # Default to Spark image
        return "apache/spark-py:v3.5.0"


class ArgoDagAdapter:
    """
    Generates DAG-based workflows with complex dependencies.

    Enables parallel execution, fan-out/fan-in patterns.
    """

    @staticmethod
    def generate_dag_template(
        name: str,
        tasks: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Generate DAG template with dependencies.

        Args:
            name: Template name
            tasks: List of task definitions

        Returns:
            DAG template
        """
        return {
            "name": name,
            "dag": {
                "tasks": tasks
            }
        }

    @staticmethod
    def generate_parallel_tasks(
        base_task: str,
        items: List[Any]
    ) -> List[Dict[str, Any]]:
        """
        Generate parallel tasks for fan-out pattern.

        Args:
            base_task: Base task template
            items: Items to process in parallel

        Returns:
            List of parallel task definitions
        """
        tasks = []

        for i, item in enumerate(items):
            tasks.append({
                "name": f"{base_task}-{i}",
                "template": base_task,
                "arguments": {
                    "parameters": [
                        {"name": "item", "value": str(item)}
                    ]
                }
            })

        return tasks


class ArgoStepTemplateAdapter:
    """
    Generates reusable step templates for common operations.

    Creates a library of templates for tasks, scripts, containers.
    """

    @staticmethod
    def generate_container_step(
        name: str,
        image: str,
        command: List[str],
        args: List[str]
    ) -> Dict[str, Any]:
        """
        Generate container step template.

        Args:
            name: Step name
            image: Container image
            command: Command to run
            args: Command arguments

        Returns:
            Container template
        """
        return {
            "name": name,
            "container": {
                "image": image,
                "command": command,
                "args": args
            }
        }

    @staticmethod
    def generate_script_step(
        name: str,
        image: str,
        script: str
    ) -> Dict[str, Any]:
        """
        Generate script step template.

        Args:
            name: Step name
            image: Container image
            script: Script content

        Returns:
            Script template
        """
        return {
            "name": name,
            "script": {
                "image": image,
                "command": ["bash"],
                "source": script
            }
        }

    @staticmethod
    def generate_resource_step(
        name: str,
        action: str,
        manifest: str
    ) -> Dict[str, Any]:
        """
        Generate resource manipulation step.

        Args:
            name: Step name
            action: Action (create, apply, delete, patch)
            manifest: Kubernetes manifest

        Returns:
            Resource template
        """
        return {
            "name": name,
            "resource": {
                "action": action,
                "manifest": manifest
            }
        }


class ArgoEventSourceAdapter:
    """
    Generates Argo Events EventSource for event-driven workflows.

    Supports S3, Kafka, webhooks, calendars, etc.
    """

    def __init__(self, pipeline: BasePipeline):
        self.pipeline = pipeline
        self.config = pipeline.config

    def generate_event_source(self, output_path: str) -> str:
        """
        Generate EventSource CRD.

        Args:
            output_path: Output directory

        Returns:
            Path to generated file
        """
        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        event_source = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "EventSource",
            "metadata": {
                "name": f"{self.config.pipeline_name}-event-source",
                "namespace": "argo-events"
            },
            "spec": self._generate_event_spec()
        }

        with open(output_dir / "event-source.yaml", "w") as f:
            yaml.dump(event_source, f, default_flow_style=False, sort_keys=False)

        # Generate Sensor
        sensor = self._generate_sensor()
        with open(output_dir / "sensor.yaml", "w") as f:
            yaml.dump(sensor, f, default_flow_style=False, sort_keys=False)

        return str(output_dir)

    def _generate_event_spec(self) -> Dict[str, Any]:
        """Generate event source specification."""
        # Example: S3 event source
        if self.config.extra_config.get("trigger_type") == "s3":
            return {
                "minio": {
                    "example": {
                        "bucket": {
                            "name": self.config.extra_config.get("s3_bucket", "data-bucket")
                        },
                        "endpoint": "s3.amazonaws.com",
                        "events": ["s3:ObjectCreated:*"],
                        "filter": {
                            "prefix": self.config.extra_config.get("s3_prefix", "data/")
                        },
                        "accessKey": {
                            "name": "aws-secret",
                            "key": "accesskey"
                        },
                        "secretKey": {
                            "name": "aws-secret",
                            "key": "secretkey"
                        }
                    }
                }
            }

        # Example: Webhook event source
        elif self.config.extra_config.get("trigger_type") == "webhook":
            return {
                "webhook": {
                    "example": {
                        "endpoint": "/webhook",
                        "method": "POST",
                        "port": "12000"
                    }
                }
            }

        # Default: Calendar (scheduled)
        else:
            return {
                "calendar": {
                    "example": {
                        "schedule": self.config.schedule or "0 0 * * *",
                        "timezone": "UTC"
                    }
                }
            }

    def _generate_sensor(self) -> Dict[str, Any]:
        """Generate Sensor CRD that triggers workflow."""
        return {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "Sensor",
            "metadata": {
                "name": f"{self.config.pipeline_name}-sensor",
                "namespace": "argo-events"
            },
            "spec": {
                "template": {
                    "serviceAccountName": "argo-events-sa"
                },
                "dependencies": [
                    {
                        "name": "example-dep",
                        "eventSourceName": f"{self.config.pipeline_name}-event-source",
                        "eventName": "example"
                    }
                ],
                "triggers": [
                    {
                        "template": {
                            "name": "workflow-trigger",
                            "argoWorkflow": {
                                "operation": "submit",
                                "source": {
                                    "resource": {
                                        "apiVersion": "argoproj.io/v1alpha1",
                                        "kind": "Workflow",
                                        "metadata": {
                                            "generateName": f"{self.config.pipeline_name}-",
                                            "namespace": "argo"
                                        },
                                        "spec": {
                                            "workflowTemplateRef": {
                                                "name": self.config.pipeline_name
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        }


class ArgoArtifactAdapter:
    """
    Manages artifacts (inputs/outputs) between workflow steps.

    Supports S3, GCS, Azure, Artifactory, etc.
    """

    @staticmethod
    def generate_artifact_repository() -> Dict[str, Any]:
        """
        Generate artifact repository configuration.

        Returns:
            ConfigMap for artifact repository
        """
        return {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "artifact-repositories",
                "namespace": "argo",
                "annotations": {
                    "workflows.argoproj.io/default-artifact-repository": "default-v1"
                }
            },
            "data": {
                "default-v1": yaml.dump({
                    "s3": {
                        "bucket": "argo-artifacts",
                        "endpoint": "s3.amazonaws.com",
                        "insecure": False,
                        "accessKeySecret": {
                            "name": "aws-secret",
                            "key": "accesskey"
                        },
                        "secretKeySecret": {
                            "name": "aws-secret",
                            "key": "secretkey"
                        }
                    }
                })
            }
        }

    @staticmethod
    def generate_input_artifact(
        name: str,
        path: str,
        s3_key: str
    ) -> Dict[str, Any]:
        """
        Generate input artifact definition.

        Args:
            name: Artifact name
            path: Path in container
            s3_key: S3 object key

        Returns:
            Artifact definition
        """
        return {
            "name": name,
            "path": path,
            "s3": {
                "key": s3_key
            }
        }

    @staticmethod
    def generate_output_artifact(
        name: str,
        path: str,
        s3_key: str
    ) -> Dict[str, Any]:
        """
        Generate output artifact definition.

        Args:
            name: Artifact name
            path: Path in container
            s3_key: S3 object key

        Returns:
            Artifact definition
        """
        return {
            "name": name,
            "path": path,
            "s3": {
                "key": s3_key
            }
        }
