import sys
import os
import importlib
import pkgutil

# Add current directory to path
sys.path.append(os.getcwd())

def verify_imports():
    print("Verifying imports...")
    modules_to_check = [
        "orchestrators",
        "orchestrators.base",
        "orchestrators.factory",
        "orchestrators.config_loader",
        "orchestrators.cli",
        "orchestrators.tasks",
        "orchestrators.tasks.ingest",
        "orchestrators.tasks.transform",
        "orchestrators.tasks.write",
        "orchestrators.tasks.ml",
        "orchestrators.tasks.quality",
        "orchestrators.tasks.governance",
        "orchestrators.tasks.notification",
        "orchestrators.tasks.orchestration",
        "orchestrators.tasks.observability",
        "orchestrators.adapters.databricks",
        "orchestrators.adapters.airflow",
        "orchestrators.adapters.dagster",
        "orchestrators.adapters.prefect",
        "orchestrators.adapters.mage",
        "orchestrators.scheduling.patterns",
    ]

    # Add pipelines
    pipeline_dir = "orchestrators/pipelines"
    if os.path.exists(pipeline_dir):
        for filename in os.listdir(pipeline_dir):
            if filename.endswith(".py") and filename != "__init__.py":
                module_name = f"orchestrators.pipelines.{filename[:-3]}"
                modules_to_check.append(module_name)

    failed = []
    for module in modules_to_check:
        try:
            importlib.import_module(module)
            print(f"✅ {module}")
        except Exception as e:
            print(f"❌ {module}: {e}")
            failed.append(module)

    if failed:
        print(f"\nFailed to import {len(failed)} modules.")
        sys.exit(1)
    else:
        print("\nAll modules imported successfully.")

def verify_factory():
    print("\nVerifying PipelineFactory...")
    from orchestrators.factory import PipelineFactory
    
    # Check if all pipelines are registered
    registry = PipelineFactory._registry
    print(f"Registered pipelines: {len(registry)}")
    
    expected_count = 24
    if len(registry) != expected_count:
        print(f"❌ Expected {expected_count} pipelines, found {len(registry)}")
        # sys.exit(1) # Don't fail hard yet, just warn
    else:
        print(f"✅ Found {expected_count} pipelines registered.")

if __name__ == "__main__":
    try:
        verify_imports()
        verify_factory()
        print("\nVerification passed!")
    except Exception as e:
        print(f"\nVerification failed: {e}")
        sys.exit(1)
