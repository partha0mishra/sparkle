#!/usr/bin/env python3
"""
Static validation script for orchestrator components.
Validates file structure without importing (doesn't require pyspark).
"""
import re
from pathlib import Path


def count_classes_in_file(file_path: Path, pattern: str) -> list:
    """Count classes matching pattern in a Python file."""
    try:
        content = file_path.read_text()
        matches = re.findall(pattern, content, re.MULTILINE)
        return matches
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []


def validate_orchestrators():
    """Validate all orchestrator components via static analysis."""
    base_path = Path(__file__).parent / "orchestrators"

    results = {
        "A. Core Pipeline Templates": {"expected": 24, "found": 0, "items": []},
        "B. Multi-Orchestrator Adapters": {"expected": 25, "found": 0, "items": []},
        "C. Task Building Blocks": {"expected": 22, "found": 0, "items": []},
        "D. Scheduling & Trigger Patterns": {"expected": 13, "found": 0, "items": []},
    }

    print("=" * 80)
    print("ORCHESTRATOR COMPONENT VALIDATION (Static Analysis)")
    print("=" * 80)
    print()

    # A. Validate Core Pipeline Templates (count .py files in pipelines/)
    print("A. Core Pipeline Templates")
    print("-" * 80)
    pipelines_path = base_path / "pipelines"
    if pipelines_path.exists():
        pipeline_files = [
            f.stem for f in pipelines_path.glob("*.py")
            if f.stem != "__init__" and f.stem != "__pycache__"
        ]
        results["A. Core Pipeline Templates"]["found"] = len(pipeline_files)
        results["A. Core Pipeline Templates"]["items"] = pipeline_files

        print(f"✓ Found {len(pipeline_files)} pipeline files:")
        for i, name in enumerate(sorted(pipeline_files), 1):
            print(f"  {i:2d}. {name}")
        print()
    else:
        print(f"✗ Pipelines directory not found: {pipelines_path}")
        print()

    # B. Validate Multi-Orchestrator Adapters (count Adapter classes in adapters/)
    print("B. Multi-Orchestrator Adapters")
    print("-" * 80)
    adapters_path = base_path / "adapters"
    if adapters_path.exists():
        adapter_classes = []
        for adapter_file in adapters_path.glob("*.py"):
            if adapter_file.stem == "__init__":
                continue
            classes = count_classes_in_file(adapter_file, r'^class (\w+Adapter)[:\(]')
            adapter_classes.extend([f"{adapter_file.stem}.{c}" for c in classes])

        results["B. Multi-Orchestrator Adapters"]["found"] = len(adapter_classes)
        results["B. Multi-Orchestrator Adapters"]["items"] = adapter_classes

        print(f"✓ Found {len(adapter_classes)} adapter classes:")
        for i, name in enumerate(sorted(adapter_classes), 1):
            print(f"  {i:2d}. {name}")
        print()
    else:
        print(f"✗ Adapters directory not found: {adapters_path}")
        print()

    # C. Validate Task Building Blocks (parse __init__.py for __all__)
    print("C. Task Building Blocks")
    print("-" * 80)
    tasks_init = base_path / "tasks" / "__init__.py"
    if tasks_init.exists():
        content = tasks_init.read_text()
        # Extract __all__ list
        all_match = re.search(r'__all__\s*=\s*\[(.*?)\]', content, re.DOTALL)
        if all_match:
            # Parse task names from __all__
            tasks = re.findall(r'"([^"]+)"', all_match.group(1))
            results["C. Task Building Blocks"]["found"] = len(tasks)
            results["C. Task Building Blocks"]["items"] = tasks

            print(f"✓ Found {len(tasks)} task building blocks:")
            for i, task in enumerate(tasks, 1):
                print(f"  {i:2d}. {task}")
            print()
        else:
            print("✗ Could not parse __all__ from tasks/__init__.py")
            print()
    else:
        print(f"✗ Tasks init file not found: {tasks_init}")
        print()

    # D. Validate Scheduling & Trigger Patterns (parse scheduling __init__.py)
    print("D. Scheduling & Trigger Patterns")
    print("-" * 80)
    scheduling_init = base_path / "scheduling" / "__init__.py"
    if scheduling_init.exists():
        content = scheduling_init.read_text()
        # Extract __all__ list
        all_match = re.search(r'__all__\s*=\s*\[(.*?)\]', content, re.DOTALL)
        if all_match:
            # Parse pattern names from __all__
            patterns = re.findall(r'"([^"]+)"', all_match.group(1))
            results["D. Scheduling & Trigger Patterns"]["found"] = len(patterns)
            results["D. Scheduling & Trigger Patterns"]["items"] = patterns

            print(f"✓ Found {len(patterns)} scheduling patterns:")
            for i, pattern in enumerate(patterns, 1):
                print(f"  {i:2d}. {pattern}")
            print()
        else:
            print("✗ Could not parse __all__ from scheduling/__init__.py")
            print()
    else:
        print(f"✗ Scheduling init file not found: {scheduling_init}")
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
        diff = data["found"] - data["expected"]
        diff_str = f" ({diff:+d})" if diff != 0 else ""
        if data["found"] < data["expected"]:
            all_passed = False
        print(f"{status} {section}: {data['found']}/{data['expected']}{diff_str}")

    print()
    print(f"TOTAL: {total_found}/{total_expected} components ({total_found - total_expected:+d})")
    print()

    if all_passed:
        print("✓ All orchestrator components validated successfully!")
        if total_found > total_expected:
            print(f"  Note: Found {total_found - total_expected} extra components (bonus adapters)")
        return 0
    else:
        print("✗ Some components are missing. Please review the output above.")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(validate_orchestrators())
