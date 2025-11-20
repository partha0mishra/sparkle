"""
AWS Step Functions Adapter (5 components).

Generates AWS Step Functions state machines from Sparkle pipelines.
"""

import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from ..base import BasePipeline, BaseTask
import logging

logger = logging.getLogger(__name__)


class StepFunctionsStateMachineAdapter:
    """
    Generates AWS Step Functions state machine definition (ASL).

    Creates state machine JSON with all states, transitions, and error handling.
    """

    def __init__(self, pipeline: BasePipeline):
        self.pipeline = pipeline
        self.config = pipeline.config

    def generate_deployment(self, output_path: Optional[str] = None) -> str:
        """
        Generate Step Functions state machine definition.

        Args:
            output_path: Output directory for state machine JSON

        Returns:
            Path to generated files
        """
        if output_path is None:
            output_path = Path.cwd() / "deployments" / "step_functions" / self.config.pipeline_name

        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate state machine definition
        state_machine = self._generate_state_machine()

        # Write state machine JSON
        sm_file = output_dir / "state_machine.json"
        with open(sm_file, "w") as f:
            json.dump(state_machine, f, indent=2)

        # Generate CloudFormation template
        self._generate_cloudformation(output_dir, state_machine)

        # Generate Terraform configuration
        self._generate_terraform(output_dir, state_machine)

        # Generate deployment script
        self._generate_deployment_script(output_dir)

        logger.info(f"Generated Step Functions state machine at: {output_dir}")

        return str(output_dir)

    def _generate_state_machine(self) -> Dict[str, Any]:
        """Generate state machine definition in ASL format."""
        states = {}
        first_state = None

        # Build pipeline to get tasks
        self.pipeline.build()

        # Convert each task to a state
        for i, task in enumerate(self.pipeline.tasks):
            state_name = task.task_name.replace("_", " ").title().replace(" ", "")

            if i == 0:
                first_state = state_name

            state_def = StepFunctionsTaskAdapter.task_to_state(task, i == len(self.pipeline.tasks) - 1)
            states[state_name] = state_def

        # Build state machine
        state_machine = {
            "Comment": f"State machine for {self.config.pipeline_name}",
            "StartAt": first_state or "StartState",
            "States": states if states else {
                "StartState": {
                    "Type": "Pass",
                    "End": True
                }
            },
            "TimeoutSeconds": 3600,
            "Version": "1.0"
        }

        return state_machine

    def _generate_cloudformation(self, output_dir: Path, state_machine: Dict[str, Any]):
        """Generate CloudFormation template."""
        template = {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Description": f"Step Functions state machine for {self.config.pipeline_name}",
            "Parameters": {
                "Environment": {
                    "Type": "String",
                    "Default": self.config.env,
                    "AllowedValues": ["dev", "qa", "prod"]
                }
            },
            "Resources": {
                "StateMachine": {
                    "Type": "AWS::StepFunctions::StateMachine",
                    "Properties": {
                        "StateMachineName": f"{self.config.pipeline_name}-{self.config.env}",
                        "DefinitionString": json.dumps(state_machine),
                        "RoleArn": {"Fn::GetAtt": ["StateMachineRole", "Arn"]},
                        "Tags": self._format_tags_for_cfn()
                    }
                },
                "StateMachineRole": {
                    "Type": "AWS::IAM::Role",
                    "Properties": {
                        "AssumeRolePolicyDocument": {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Principal": {
                                        "Service": "states.amazonaws.com"
                                    },
                                    "Action": "sts:AssumeRole"
                                }
                            ]
                        },
                        "ManagedPolicyArns": [
                            "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
                        ]
                    }
                }
            },
            "Outputs": {
                "StateMachineArn": {
                    "Value": {"Ref": "StateMachine"},
                    "Export": {"Name": {"Fn::Sub": "${AWS::StackName}-StateMachineArn"}}
                }
            }
        }

        with open(output_dir / "cloudformation.yaml", "w") as f:
            import yaml
            yaml.dump(template, f, default_flow_style=False, sort_keys=False)

    def _generate_terraform(self, output_dir: Path, state_machine: Dict[str, Any]):
        """Generate Terraform configuration."""
        terraform_content = f'''
resource "aws_sfn_state_machine" "{self.config.pipeline_name}" {{
  name     = "{self.config.pipeline_name}-{self.config.env}"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = <<EOF
{json.dumps(state_machine, indent=2)}
EOF

  tags = {{
{self._format_tags_for_terraform()}
  }}
}}

resource "aws_iam_role" "step_functions_role" {{
  name = "{self.config.pipeline_name}-step-functions-role"

  assume_role_policy = <<EOF
{{
  "Version": "2012-10-17",
  "Statement": [
    {{
      "Effect": "Allow",
      "Principal": {{
        "Service": "states.amazonaws.com"
      }},
      "Action": "sts:AssumeRole"
    }}
  ]
}}
EOF
}}

resource "aws_iam_role_policy_attachment" "step_functions_cloudwatch" {{
  role       = aws_iam_role.step_functions_role.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
}}

output "state_machine_arn" {{
  value = aws_sfn_state_machine.{self.config.pipeline_name}.arn
}}
'''

        with open(output_dir / "main.tf", "w") as f:
            f.write(terraform_content.strip())

    def _generate_deployment_script(self, output_dir: Path):
        """Generate deployment script."""
        script = f'''#!/bin/bash
# Deployment script for {self.config.pipeline_name}

set -e

ENVIRONMENT="${{1:-{self.config.env}}}"
STACK_NAME="{self.config.pipeline_name}-$ENVIRONMENT"

echo "Deploying Step Functions state machine: $STACK_NAME"

# Deploy using CloudFormation
aws cloudformation deploy \\
    --template-file cloudformation.yaml \\
    --stack-name $STACK_NAME \\
    --parameter-overrides Environment=$ENVIRONMENT \\
    --capabilities CAPABILITY_IAM \\
    --region us-east-1

echo "Deployment complete!"

# Get state machine ARN
STATE_MACHINE_ARN=$(aws cloudformation describe-stacks \\
    --stack-name $STACK_NAME \\
    --query "Stacks[0].Outputs[?OutputKey=='StateMachineArn'].OutputValue" \\
    --output text)

echo "State Machine ARN: $STATE_MACHINE_ARN"

# Start an execution (optional)
# aws stepfunctions start-execution \\
#     --state-machine-arn $STATE_MACHINE_ARN \\
#     --input '{{"pipeline":"test"}}'
'''

        script_file = output_dir / "deploy.sh"
        with open(script_file, "w") as f:
            f.write(script.strip())

        # Make executable
        script_file.chmod(0o755)

    def _format_tags_for_cfn(self) -> List[Dict[str, str]]:
        """Format tags for CloudFormation."""
        if not self.config.tags:
            return []

        return [{"Key": k, "Value": v} for k, v in self.config.tags.items()]

    def _format_tags_for_terraform(self) -> str:
        """Format tags for Terraform."""
        if not self.config.tags:
            return ""

        lines = [f'    {k} = "{v}"' for k, v in self.config.tags.items()]
        return "\n".join(lines)


class StepFunctionsTaskAdapter:
    """
    Converts individual tasks to Step Functions states.

    Maps task types to appropriate state types (Task, Pass, Choice, etc.).
    """

    @staticmethod
    def task_to_state(task: BaseTask, is_last: bool = False) -> Dict[str, Any]:
        """
        Convert a Sparkle task to a Step Functions state.

        Args:
            task: Task to convert
            is_last: Whether this is the last task

        Returns:
            State definition dictionary
        """
        state = {
            "Type": "Task",
            "Resource": "arn:aws:states:::databricks:runNow.sync",
            "Parameters": {
                "ClusterId.$": "$.cluster_id",
                "Notebook": {
                    "Path": f"/Repos/sparkle/notebooks/{task.task_name}",
                    "BaseParameters": task.config
                }
            },
            "Retry": [
                {
                    "ErrorEquals": ["States.Timeout", "States.TaskFailed"],
                    "IntervalSeconds": 60,
                    "MaxAttempts": 3,
                    "BackoffRate": 2.0
                }
            ],
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "ResultPath": "$.error",
                    "Next": "HandleFailure"
                }
            ]
        }

        if is_last:
            state["End"] = True
        else:
            state["Next"] = "NextState"  # Will be updated during state machine generation

        return state


class StepFunctionsChoiceAdapter:
    """
    Generates Choice states for conditional branching.

    Enables dynamic routing based on task results.
    """

    @staticmethod
    def generate_choice_state(
        condition_field: str,
        branches: Dict[str, str],
        default: str = "DefaultState"
    ) -> Dict[str, Any]:
        """
        Generate a Choice state definition.

        Args:
            condition_field: Field to evaluate
            branches: Map of values to next states
            default: Default state if no conditions match

        Returns:
            Choice state definition
        """
        choices = []

        for value, next_state in branches.items():
            choices.append({
                "Variable": f"$.{condition_field}",
                "StringEquals": value,
                "Next": next_state
            })

        return {
            "Type": "Choice",
            "Choices": choices,
            "Default": default
        }


class StepFunctionsParallelAdapter:
    """
    Generates Parallel states for concurrent execution.

    Enables parallel task execution for backfills and multi-table processing.
    """

    @staticmethod
    def generate_parallel_state(
        branches: List[Dict[str, Any]],
        next_state: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate a Parallel state definition.

        Args:
            branches: List of branch state machines
            next_state: Next state after parallel completion

        Returns:
            Parallel state definition
        """
        state = {
            "Type": "Parallel",
            "Branches": branches,
            "Retry": [
                {
                    "ErrorEquals": ["States.Timeout"],
                    "IntervalSeconds": 60,
                    "MaxAttempts": 2,
                    "BackoffRate": 2.0
                }
            ],
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "ResultPath": "$.error",
                    "Next": "HandleParallelFailure"
                }
            ]
        }

        if next_state:
            state["Next"] = next_state
        else:
            state["End"] = True

        return state

    @staticmethod
    def generate_map_state(
        iterator: Dict[str, Any],
        items_path: str = "$.items",
        max_concurrency: int = 10
    ) -> Dict[str, Any]:
        """
        Generate a Map state for dynamic parallelism.

        Args:
            iterator: State machine for each item
            items_path: JSONPath to items array
            max_concurrency: Maximum concurrent executions

        Returns:
            Map state definition
        """
        return {
            "Type": "Map",
            "ItemsPath": items_path,
            "MaxConcurrency": max_concurrency,
            "Iterator": iterator,
            "ResultPath": "$.results"
        }


class StepFunctionsEventBridgeAdapter:
    """
    Generates EventBridge rules for triggering state machines.

    Enables event-driven execution from S3, DynamoDB, Kafka, etc.
    """

    def __init__(self, pipeline: BasePipeline):
        self.pipeline = pipeline
        self.config = pipeline.config

    def generate_eventbridge_rule(self, output_path: str) -> str:
        """
        Generate EventBridge rule configuration.

        Args:
            output_path: Output directory

        Returns:
            Path to generated files
        """
        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate rule based on schedule or event pattern
        if self.config.schedule:
            rule = self._generate_scheduled_rule()
        else:
            rule = self._generate_event_pattern_rule()

        # Write CloudFormation template
        template = {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Description": f"EventBridge rule for {self.config.pipeline_name}",
            "Resources": {
                "EventRule": {
                    "Type": "AWS::Events::Rule",
                    "Properties": {
                        "Name": f"{self.config.pipeline_name}-trigger",
                        "Description": f"Trigger for {self.config.pipeline_name}",
                        **rule,
                        "State": "ENABLED",
                        "Targets": [
                            {
                                "Arn": {"Fn::Sub": "${StateMachineArn}"},
                                "RoleArn": {"Fn::GetAtt": ["EventBridgeRole", "Arn"]},
                                "Id": f"{self.config.pipeline_name}-target"
                            }
                        ]
                    }
                },
                "EventBridgeRole": {
                    "Type": "AWS::IAM::Role",
                    "Properties": {
                        "AssumeRolePolicyDocument": {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Principal": {
                                        "Service": "events.amazonaws.com"
                                    },
                                    "Action": "sts:AssumeRole"
                                }
                            ]
                        },
                        "Policies": [
                            {
                                "PolicyName": "StartStateMachine",
                                "PolicyDocument": {
                                    "Version": "2012-10-17",
                                    "Statement": [
                                        {
                                            "Effect": "Allow",
                                            "Action": "states:StartExecution",
                                            "Resource": {"Fn::Sub": "${StateMachineArn}"}
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                }
            },
            "Parameters": {
                "StateMachineArn": {
                    "Type": "String",
                    "Description": "ARN of the Step Functions state machine"
                }
            }
        }

        with open(output_dir / "eventbridge.yaml", "w") as f:
            import yaml
            yaml.dump(template, f, default_flow_style=False, sort_keys=False)

        return str(output_dir)

    def _generate_scheduled_rule(self) -> Dict[str, Any]:
        """Generate scheduled rule (cron)."""
        # Convert cron to EventBridge format
        cron_expr = self.config.schedule

        return {
            "ScheduleExpression": f"cron({cron_expr})"
        }

    def _generate_event_pattern_rule(self) -> Dict[str, Any]:
        """Generate event pattern rule (S3, DynamoDB, etc.)."""
        # Example: S3 file arrival
        event_pattern = {
            "source": ["aws.s3"],
            "detail-type": ["Object Created"],
            "detail": {
                "bucket": {
                    "name": [self.config.extra_config.get("s3_bucket", "data-bucket")]
                },
                "object": {
                    "key": [{"prefix": self.config.extra_config.get("s3_prefix", "data/")}]
                }
            }
        }

        return {
            "EventPattern": json.dumps(event_pattern)
        }


def generate_example_pipeline():
    """
    Generate example Step Functions pipeline with all features.

    Demonstrates:
    - Sequential tasks
    - Parallel execution
    - Conditional branching
    - Error handling
    - EventBridge triggering
    """
    pipeline = {
        "Comment": "Example Sparkle pipeline with all Step Functions features",
        "StartAt": "ValidateInput",
        "States": {
            "ValidateInput": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": "ValidateInputFunction",
                    "Payload.$": "$"
                },
                "Next": "CheckEnvironment"
            },
            "CheckEnvironment": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.environment",
                        "StringEquals": "prod",
                        "Next": "ProductionPath"
                    },
                    {
                        "Variable": "$.environment",
                        "StringEquals": "dev",
                        "Next": "DevelopmentPath"
                    }
                ],
                "Default": "DevelopmentPath"
            },
            "ProductionPath": {
                "Type": "Parallel",
                "Branches": [
                    {
                        "StartAt": "IngestData",
                        "States": {
                            "IngestData": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::databricks:runNow.sync",
                                "End": True
                            }
                        }
                    },
                    {
                        "StartAt": "ValidateSchema",
                        "States": {
                            "ValidateSchema": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::lambda:invoke",
                                "End": True
                            }
                        }
                    }
                ],
                "Next": "TransformData"
            },
            "DevelopmentPath": {
                "Type": "Task",
                "Resource": "arn:aws:states:::databricks:runNow.sync",
                "Next": "TransformData"
            },
            "TransformData": {
                "Type": "Task",
                "Resource": "arn:aws:states:::databricks:runNow.sync",
                "Retry": [
                    {
                        "ErrorEquals": ["States.Timeout"],
                        "IntervalSeconds": 60,
                        "MaxAttempts": 3,
                        "BackoffRate": 2.0
                    }
                ],
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "ResultPath": "$.error",
                        "Next": "NotifyFailure"
                    }
                ],
                "Next": "WriteToGold"
            },
            "WriteToGold": {
                "Type": "Task",
                "Resource": "arn:aws:states:::databricks:runNow.sync",
                "Next": "NotifySuccess"
            },
            "NotifySuccess": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": "arn:aws:sns:us-east-1:123456789012:pipeline-success",
                    "Message.$": "$"
                },
                "End": True
            },
            "NotifyFailure": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": "arn:aws:sns:us-east-1:123456789012:pipeline-failure",
                    "Message.$": "$"
                },
                "Next": "FailState"
            },
            "FailState": {
                "Type": "Fail",
                "Error": "PipelineExecutionFailed",
                "Cause": "Pipeline execution failed. Check logs for details."
            }
        }
    }

    return pipeline
