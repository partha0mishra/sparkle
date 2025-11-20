"""
CLI for Sparkle Orchestration.

Provides commands for generating deployment artifacts for different orchestrators.
"""

import click
import json
from pathlib import Path
from typing import Optional
from .factory import Pipeline
from .config_loader import load_pipeline_config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    """Sparkle Orchestration CLI."""
    pass


@cli.command()
@click.argument("pipeline_name")
@click.option("--env", default="prod", help="Environment (dev/qa/prod)")
@click.option("--orchestrator", required=True,
              type=click.Choice(["databricks", "airflow", "dagster", "prefect", "mage"]),
              help="Target orchestrator")
@click.option("--output", "-o", help="Output directory for generated files")
def generate_deploy(pipeline_name: str, env: str, orchestrator: str, output: Optional[str]):
    """
    Generate deployment artifacts for a pipeline.

    Example:
        sparkle-orchestration generate-deploy customer_silver_daily --orchestrator databricks
    """
    try:
        logger.info(f"Generating {orchestrator} deployment for {pipeline_name} ({env})")

        # Load pipeline
        pipeline = Pipeline.get(pipeline_name, env)

        # Build pipeline
        pipeline.build()

        # Generate deployment
        output_path = pipeline.deploy(orchestrator, output)

        logger.info(f"✅ Deployment artifacts generated at: {output_path}")

    except Exception as e:
        logger.error(f"❌ Failed to generate deployment: {e}")
        raise click.ClickException(str(e))


@cli.command()
@click.argument("pipeline_name")
@click.option("--env", default="prod", help="Environment (dev/qa/prod)")
def validate(pipeline_name: str, env: str):
    """
    Validate a pipeline configuration.

    Example:
        sparkle-orchestration validate customer_silver_daily --env prod
    """
    try:
        logger.info(f"Validating {pipeline_name} ({env})")

        # Load config
        config = load_pipeline_config(pipeline_name, env)

        # Load pipeline
        pipeline = Pipeline.get(pipeline_name, env, config=config)

        # Validate
        errors = pipeline.validate()

        if errors:
            logger.error("❌ Validation failed:")
            for error in errors:
                logger.error(f"  - {error}")
            raise click.ClickException("Validation failed")
        else:
            logger.info("✅ Configuration is valid")

    except Exception as e:
        logger.error(f"❌ Validation error: {e}")
        raise click.ClickException(str(e))


@cli.command()
@click.argument("pipeline_name")
@click.option("--env", default="prod", help="Environment (dev/qa/prod)")
@click.option("--output", "-o", help="Output file path")
def lineage(pipeline_name: str, env: str, output: Optional[str]):
    """
    Generate lineage visualization for a pipeline.

    Example:
        sparkle-orchestration lineage customer_silver_daily --output lineage.json
    """
    try:
        logger.info(f"Generating lineage for {pipeline_name} ({env})")

        # Load pipeline
        pipeline = Pipeline.get(pipeline_name, env)

        # Build pipeline
        pipeline.build()

        # Get lineage
        lineage_data = pipeline.get_lineage()

        # Output
        if output:
            with open(output, "w") as f:
                json.dump(lineage_data, f, indent=2)
            logger.info(f"✅ Lineage saved to: {output}")
        else:
            print(json.dumps(lineage_data, indent=2))

    except Exception as e:
        logger.error(f"❌ Failed to generate lineage: {e}")
        raise click.ClickException(str(e))


@cli.command()
def list_pipelines():
    """
    List all available pipeline types.

    Example:
        sparkle-orchestration list-pipelines
    """
    pipeline_types = Pipeline.list()

    logger.info(f"Available pipeline types ({len(pipeline_types)}):")
    for pipeline_type in pipeline_types:
        print(f"  - {pipeline_type}")


@cli.command()
@click.argument("pipeline_type")
@click.argument("pipeline_name")
@click.option("--output", "-o", help="Output directory")
def init(pipeline_type: str, pipeline_name: str, output: Optional[str]):
    """
    Initialize a new pipeline configuration.

    Example:
        sparkle-orchestration init bronze_raw_ingestion customer_ingestion
    """
    try:
        if output is None:
            output = Path.cwd() / "config" / "orchestration" / pipeline_name

        output_path = Path(output)
        output_path.mkdir(parents=True, exist_ok=True)

        # Create template config for each environment
        for env in ["dev", "qa", "prod"]:
            config_template = {
                "pipeline_name": pipeline_name,
                "pipeline_type": pipeline_type,
                "env": env,
                "source_system": "CHANGEME",
                "source_connection": "CHANGEME",
                "source_table": "CHANGEME",
                "destination_catalog": "${CATALOG}",
                "destination_schema": "${SCHEMA}",
                "destination_table": pipeline_name,
                "tags": {
                    "pipeline": pipeline_name,
                    "env": env,
                    "owner": "CHANGEME"
                }
            }

            config_file = output_path / f"{env}.json"
            with open(config_file, "w") as f:
                json.dump(config_template, f, indent=2)

            logger.info(f"Created: {config_file}")

        logger.info(f"✅ Initialized {pipeline_name} configuration")

    except Exception as e:
        logger.error(f"❌ Failed to initialize: {e}")
        raise click.ClickException(str(e))


@cli.command()
@click.argument("pipeline_name")
@click.option("--env", default="prod", help="Environment (dev/qa/prod)")
@click.option("--dry-run", is_flag=True, help="Preview without executing")
def run(pipeline_name: str, env: str, dry_run: bool):
    """
    Execute a pipeline locally.

    Example:
        sparkle-orchestration run customer_silver_daily --env dev
    """
    try:
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Running {pipeline_name} ({env})")

        # Load pipeline
        pipeline = Pipeline.get(pipeline_name, env)

        # Build pipeline
        pipeline.build()

        if dry_run:
            logger.info("Tasks to execute:")
            for task in pipeline.tasks:
                logger.info(f"  - {task.task_name}")
            return

        # Execute pipeline
        result = pipeline.run()

        # Display metrics
        metrics = pipeline.get_metrics()
        logger.info(f"✅ Pipeline completed in {metrics.get('duration_seconds', 0):.2f}s")
        logger.info(f"Status: {metrics.get('status')}")
        logger.info(f"Tasks executed: {metrics.get('num_tasks')}")

    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        raise click.ClickException(str(e))


if __name__ == "__main__":
    cli()
