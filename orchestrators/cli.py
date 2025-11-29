import argparse
import sys
from .factory import PipelineFactory

def main():
    parser = argparse.ArgumentParser(description="Sparkle Orchestration CLI")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # generate-deploy
    deploy_parser = subparsers.add_parser("generate-deploy", help="Generate deployment artifacts")
    deploy_parser.add_argument("--pipeline", required=True, help="Pipeline name")
    deploy_parser.add_argument("--env", default="prod", help="Environment (dev/qa/prod)")
    deploy_parser.add_argument("--orchestrator", required=True, choices=["databricks", "airflow", "dagster", "prefect", "mage"], help="Target orchestrator")
    deploy_parser.add_argument("--output", help="Output path for artifacts")

    # run
    run_parser = subparsers.add_parser("run", help="Run pipeline locally")
    run_parser.add_argument("--pipeline", required=True, help="Pipeline name")
    run_parser.add_argument("--env", default="dev", help="Environment")

    args = parser.parse_args()

    if args.command == "generate-deploy":
        try:
            pipeline = PipelineFactory.get(args.pipeline, args.env)
            output = pipeline.deploy(args.orchestrator, args.output)
            print(f"Deployment artifacts generated at: {output}")
        except Exception as e:
            print(f"Error generating deployment: {e}")
            sys.exit(1)

    elif args.command == "run":
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName(f"Sparkle-{args.pipeline}").getOrCreate()
            
            pipeline = PipelineFactory.get(args.pipeline, args.env, spark)
            result = pipeline.run()
            print(f"Pipeline execution result: {result}")
        except Exception as e:
            print(f"Error running pipeline: {e}")
            sys.exit(1)
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
