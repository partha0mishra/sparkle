#!/usr/bin/env python3
"""
Validation script for orchestrator components.
Verifies all 84 components are properly structured and registered.
"""
import sys
from pathlib import Path

# Add sparkle to path
sys.path.insert(0, str(Path(__file__).parent))

def validate_orchestrators():
    """Validate all orchestrator components."""
    results = {
        "A. Core Pipeline Templates": {"expected": 24, "found": 0, "items": []},
        "B. Multi-Orchestrator Adapters": {"expected": 25, "found": 0, "items": []},
        "C. Task Building Blocks": {"expected": 22, "found": 0, "items": []},
        "D. Scheduling & Trigger Patterns": {"expected": 13, "found": 0, "items": []},
    }

    print("=" * 80)
    print("ORCHESTRATOR COMPONENT VALIDATION")
    print("=" * 80)
    print()

    # A. Validate Core Pipeline Templates
    print("A. Core Pipeline Templates")
    print("-" * 80)
    try:
        from orchestrators.factory import PipelineFactory

        pipelines = list(PipelineFactory._registry.keys())
        results["A. Core Pipeline Templates"]["found"] = len(pipelines)
        results["A. Core Pipeline Templates"]["items"] = pipelines

        print(f"✓ Found {len(pipelines)} pipeline templates:")
        for i, pipeline in enumerate(sorted(pipelines), 1):
            print(f"  {i:2d}. {pipeline}")
        print()

    except Exception as e:
        print(f"✗ Error loading pipelines: {e}")
        print()

    # B. Validate Multi-Orchestrator Adapters
    print("B. Multi-Orchestrator Adapters")
    print("-" * 80)
    try:
        import orchestrators.adapters as adapters
        import inspect

        adapter_count = 0
        adapter_list = []

        # Count adapters from each module
        for module_name in ['databricks', 'airflow', 'dagster', 'prefect', 'mage']:
            module = getattr(adapters, module_name, None)
            if module:
                module_adapters = [
                    name for name, obj in inspect.getmembers(module, inspect.isclass)
                    if name.endswith('Adapter') and obj.__module__.startswith('orchestrators.adapters')
                ]
                adapter_count += len(module_adapters)
                adapter_list.extend([f"{module_name}.{name}" for name in module_adapters])

        results["B. Multi-Orchestrator Adapters"]["found"] = adapter_count
        results["B. Multi-Orchestrator Adapters"]["items"] = adapter_list

        print(f"✓ Found {adapter_count} adapter classes:")
        for i, adapter in enumerate(sorted(adapter_list), 1):
            print(f"  {i:2d}. {adapter}")
        print()

    except Exception as e:
        print(f"✗ Error loading adapters: {e}")
        print()

    # C. Validate Task Building Blocks
    print("C. Task Building Blocks")
    print("-" * 80)
    try:
        import orchestrators.tasks as tasks

        # Get all exported tasks from __all__
        task_list = tasks.__all__

        results["C. Task Building Blocks"]["found"] = len(task_list)
        results["C. Task Building Blocks"]["items"] = task_list

        print(f"✓ Found {len(task_list)} task building blocks:")
        for i, task in enumerate(task_list, 1):
            print(f"  {i:2d}. {task}")
        print()

    except Exception as e:
        print(f"✗ Error loading tasks: {e}")
        print()

    # D. Validate Scheduling & Trigger Patterns
    print("D. Scheduling & Trigger Patterns")
    print("-" * 80)
    try:
        import orchestrators.scheduling as scheduling

        # Get all exported patterns from __all__
        pattern_list = scheduling.__all__

        results["D. Scheduling & Trigger Patterns"]["found"] = len(pattern_list)
        results["D. Scheduling & Trigger Patterns"]["items"] = pattern_list

        print(f"✓ Found {len(pattern_list)} scheduling patterns:")
        for i, pattern in enumerate(pattern_list, 1):
            print(f"  {i:2d}. {pattern}")
        print()

    except Exception as e:
        print(f"✗ Error loading scheduling patterns: {e}")
        print()

    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)

    total_expected = sum(r["expected"] for r in results.values())
    total_found = sum(r["found"] for r in results.values())

    all_passed = True
    for section, data in results.items():
        status = "✓" if data["found"] >= data["expected"] else "✗"
        if data["found"] < data["expected"]:
            all_passed = False
        print(f"{status} {section}: {data['found']}/{data['expected']}")

    print()
    print(f"TOTAL: {total_found}/{total_expected} components")
    print()

    if all_passed:
        print("✓ All orchestrator components validated successfully!")
        return 0
    else:
        print("✗ Some components are missing. Please review the output above.")
        return 1


if __name__ == "__main__":
    sys.exit(validate_orchestrators())
