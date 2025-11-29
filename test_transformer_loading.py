#!/usr/bin/env python3
"""
Test script to verify transformer loading in component registry.
"""

import sys
from pathlib import Path

# Add sparkle-studio backend to path
backend_path = Path(__file__).parent / "sparkle-studio" / "backend"
sys.path.insert(0, str(backend_path))

# Add sparkle path
sparkle_path = Path(__file__).parent
sys.path.insert(0, str(sparkle_path))

print("=" * 60)
print("Testing Transformer Loading in Component Registry")
print("=" * 60)

try:
    # Mock the settings
    from unittest.mock import Mock
    import core
    core.config = Mock()
    core.config.settings = Mock()
    core.config.settings.SPARKLE_ENGINE_PATH = str(sparkle_path)

    # Import and build registry
    from component_registry import ComponentRegistry

    print("\n1. Building component registry...")
    registry = ComponentRegistry()
    registry = registry.build()
    print("   ✓ Registry built successfully")

    # Get statistics
    stats = registry.get_stats()
    print("\n2. Component Statistics:")
    for category, count in stats.items():
        print(f"   {category}: {count}")

    # Check transformers specifically
    print("\n3. Transformer Details:")
    transformers = registry.get_by_category("transformer")
    print(f"   Total transformers: {len(transformers)}")

    if transformers:
        # Group by sub_group
        from collections import defaultdict
        by_subgroup = defaultdict(list)

        for name, manifest in transformers.items():
            sub_group = manifest.sub_group or "Unknown"
            by_subgroup[sub_group].append(name)

        print("\n   By Sub-Group:")
        for sub_group in sorted(by_subgroup.keys()):
            count = len(by_subgroup[sub_group])
            print(f"     {sub_group}: {count} transformers")
            # Show first 3 as examples
            for name in sorted(by_subgroup[sub_group])[:3]:
                manifest = transformers[name]
                print(f"       - {manifest.display_name}")
                print(f"         Tags: {', '.join(manifest.tags[:3])}")
    else:
        print("   ⚠ No transformers found!")
        print("\n   Checking what went wrong...")

        # Try to import transformers directly
        try:
            import transformers
            print("   ✓ transformers module imports successfully")
            print(f"   Available: {dir(transformers)}")

            # Check a specific transformer
            from transformers.cleaning import drop_exact_duplicates
            print(f"   ✓ Sample function: {drop_exact_duplicates}")
            print(f"   Has _is_transformer: {hasattr(drop_exact_duplicates, '_is_transformer')}")
            print(f"   Docstring: {drop_exact_duplicates.__doc__[:100]}...")

        except Exception as e:
            print(f"   ✗ Error importing transformers: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 60)
    print("Test Complete!")
    print("=" * 60)

except Exception as e:
    print(f"\n✗ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
