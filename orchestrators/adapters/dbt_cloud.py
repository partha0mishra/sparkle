"""
dbt Cloud Adapter (5 components).

Generates dbt Cloud project structure from Sparkle pipeline configs.
"""

import json
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional
from ..base import BasePipeline
import logging

logger = logging.getLogger(__name__)


class dbtProjectAdapter:
    """
    Generates dbt project structure from pipeline configuration.

    Creates:
    - dbt_project.yml
    - profiles.yml
    - models/
    - tests/
    - macros/
    - analyses/
    """

    def __init__(self, pipeline: BasePipeline):
        self.pipeline = pipeline
        self.config = pipeline.config

    def generate_deployment(self, output_path: Optional[str] = None) -> str:
        """
        Generate dbt Cloud project structure.

        Args:
            output_path: Output directory for dbt project

        Returns:
            Path to generated project
        """
        if output_path is None:
            output_path = Path.cwd() / "deployments" / "dbt" / self.config.pipeline_name

        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate dbt_project.yml
        self._generate_project_yml(output_dir)

        # Generate profiles.yml
        self._generate_profiles_yml(output_dir)

        # Generate models
        self._generate_models(output_dir)

        # Generate schema.yml
        self._generate_schema_yml(output_dir)

        # Generate README
        self._generate_readme(output_dir)

        logger.info(f"Generated dbt project at: {output_dir}")

        return str(output_dir)

    def _generate_project_yml(self, output_dir: Path):
        """Generate dbt_project.yml."""
        project_yml = {
            "name": self.config.pipeline_name,
            "version": "1.0.0",
            "config-version": 2,

            "profile": self.config.pipeline_name,

            "model-paths": ["models"],
            "analysis-paths": ["analyses"],
            "test-paths": ["tests"],
            "seed-paths": ["seeds"],
            "macro-paths": ["macros"],
            "snapshot-paths": ["snapshots"],

            "target-path": "target",
            "clean-targets": ["target", "dbt_packages"],

            "models": {
                self.config.pipeline_name: {
                    "+materialized": "table",
                    "+schema": self.config.destination_schema,
                    "+tags": list(self.config.tags.keys()) if self.config.tags else []
                }
            }
        }

        with open(output_dir / "dbt_project.yml", "w") as f:
            yaml.dump(project_yml, f, default_flow_style=False, sort_keys=False)

    def _generate_profiles_yml(self, output_dir: Path):
        """Generate profiles.yml for Databricks."""
        profiles_yml = {
            self.config.pipeline_name: {
                "target": self.config.env,
                "outputs": {
                    self.config.env: {
                        "type": "databricks",
                        "catalog": self.config.destination_catalog or "${DBT_CATALOG}",
                        "schema": self.config.destination_schema or "${DBT_SCHEMA}",
                        "host": "${DBT_HOST}",
                        "http_path": "${DBT_HTTP_PATH}",
                        "token": "${DBT_TOKEN}",
                        "threads": 4
                    }
                }
            }
        }

        with open(output_dir / "profiles.yml", "w") as f:
            yaml.dump(profiles_yml, f, default_flow_style=False, sort_keys=False)

    def _generate_models(self, output_dir: Path):
        """Generate dbt model SQL files."""
        models_dir = output_dir / "models"
        models_dir.mkdir(exist_ok=True)

        # Generate model based on pipeline type
        if self.config.pipeline_type in ["silver_batch_transformation", "silver_streaming_transformation"]:
            self._generate_silver_model(models_dir)
        elif self.config.pipeline_type in ["gold_daily_aggregate", "gold_realtime_dashboard"]:
            self._generate_gold_model(models_dir)
        elif self.config.pipeline_type == "dimension_daily_scd2":
            self._generate_scd2_model(models_dir)

    def _generate_silver_model(self, models_dir: Path):
        """Generate silver layer model."""
        model_name = self.config.destination_table

        sql = f"""
-- Silver layer model: {model_name}
-- Generated from Sparkle pipeline: {self.config.pipeline_name}

{{{{
  config(
    materialized='incremental',
    unique_key={self._format_key_list(self.config.primary_key)},
    partition_by={self._format_partition_by()},
    tags={self._format_tags()}
  )
}}}}

with source as (
    select * from {{{{ source('{self._get_source_schema()}', '{self._get_source_table()}') }}}}
    {{%- if is_incremental() %}}
    where {self.config.watermark_column or 'updated_at'} > (select max({self.config.watermark_column or 'updated_at'}) from {{{{ this }}}})
    {{%- endif %}}
),

cleaned as (
    select
        *,
        -- Add audit columns
        current_timestamp() as _dbt_loaded_at,
        '{{{{ invocation_id }}}}' as _dbt_invocation_id
    from source
)

select * from cleaned
"""

        with open(models_dir / f"{model_name}.sql", "w") as f:
            f.write(sql.strip())

    def _generate_gold_model(self, models_dir: Path):
        """Generate gold layer aggregation model."""
        model_name = self.config.destination_table

        # Extract aggregation config if available
        agg_config = self.config.extra_config.get("aggregations", {})
        group_by = agg_config.get("group_by", [])
        measures = agg_config.get("measures", [])

        sql = f"""
-- Gold layer model: {model_name}
-- Generated from Sparkle pipeline: {self.config.pipeline_name}

{{{{
  config(
    materialized='table',
    partition_by={self._format_partition_by()},
    tags={self._format_tags()}
  )
}}}}

with source as (
    select * from {{{{ ref('{self._get_source_table()}') }}}}
),

aggregated as (
    select
        {self._format_group_by_columns(group_by)},
        {self._format_measures(measures)}
    from source
    {self._format_group_by_clause(group_by)}
)

select * from aggregated
"""

        with open(models_dir / f"{model_name}.sql", "w") as f:
            f.write(sql.strip())

    def _generate_scd2_model(self, models_dir: Path):
        """Generate SCD Type 2 dimension model."""
        model_name = self.config.destination_table

        sql = f"""
-- SCD Type 2 Dimension: {model_name}
-- Generated from Sparkle pipeline: {self.config.pipeline_name}

{{{{
  config(
    materialized='incremental',
    unique_key=['surrogate_key'],
    tags={self._format_tags()}
  )
}}}}

{{{{ config(pre_hook="call system.create_or_replace_sequence({{{{ target.schema }}}}.{model_name}_seq)") }}}}

with source as (
    select * from {{{{ ref('{self._get_source_table()}') }}}}
),

with_surrogate_key as (
    select
        nextval('{model_name}_seq') as surrogate_key,
        {', '.join(self.config.business_key or ['id'])} as business_key,
        *,
        current_date() as effective_date,
        cast('9999-12-31' as date) as end_date,
        true as is_current
    from source
)

select * from with_surrogate_key
"""

        with open(models_dir / f"{model_name}.sql", "w") as f:
            f.write(sql.strip())

    def _generate_schema_yml(self, output_dir: Path):
        """Generate schema.yml with sources and models."""
        models_dir = output_dir / "models"
        models_dir.mkdir(exist_ok=True)

        schema_yml = {
            "version": 2,
            "sources": [
                {
                    "name": self._get_source_schema(),
                    "schema": self._get_source_schema(),
                    "tables": [
                        {
                            "name": self._get_source_table(),
                            "description": f"Source table for {self.config.pipeline_name}"
                        }
                    ]
                }
            ],
            "models": [
                {
                    "name": self.config.destination_table,
                    "description": f"Model generated from {self.config.pipeline_name}",
                    "columns": self._generate_column_docs()
                }
            ]
        }

        with open(models_dir / "schema.yml", "w") as f:
            yaml.dump(schema_yml, f, default_flow_style=False, sort_keys=False)

    def _generate_column_docs(self) -> List[Dict[str, str]]:
        """Generate column documentation."""
        columns = []

        if self.config.primary_key:
            for col in self.config.primary_key:
                columns.append({
                    "name": col,
                    "description": f"Primary key column",
                    "tests": ["not_null", "unique"]
                })

        if self.config.watermark_column:
            columns.append({
                "name": self.config.watermark_column,
                "description": "Watermark column for incremental processing",
                "tests": ["not_null"]
            })

        return columns

    def _generate_readme(self, output_dir: Path):
        """Generate README.md."""
        readme = f"""# {self.config.pipeline_name}

dbt project generated from Sparkle pipeline configuration.

## Overview

- **Pipeline Type**: {self.config.pipeline_type}
- **Environment**: {self.config.env}
- **Source**: {self.config.source_table or self.config.source_system}
- **Destination**: {self.config.destination_catalog}.{self.config.destination_schema}.{self.config.destination_table}

## Running

```bash
# Install dependencies
dbt deps

# Run models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Configuration

This project uses profiles.yml for connection configuration. Set these environment variables:

- `DBT_HOST`: Databricks workspace URL
- `DBT_HTTP_PATH`: SQL Warehouse HTTP path
- `DBT_TOKEN`: Personal access token
- `DBT_CATALOG`: Unity Catalog name
- `DBT_SCHEMA`: Schema name

## Tags

{', '.join(f'`{k}: {v}`' for k, v in (self.config.tags or {}).items())}
"""

        with open(output_dir / "README.md", "w") as f:
            f.write(readme)

    def _get_source_schema(self) -> str:
        """Extract source schema from source_table."""
        if self.config.source_table and "." in self.config.source_table:
            parts = self.config.source_table.split(".")
            return parts[-2] if len(parts) >= 2 else "bronze"
        return "bronze"

    def _get_source_table(self) -> str:
        """Extract source table name."""
        if self.config.source_table:
            return self.config.source_table.split(".")[-1]
        return "source_table"

    def _format_key_list(self, keys: Optional[List[str]]) -> str:
        """Format key list for dbt."""
        if not keys:
            return "'id'"
        if len(keys) == 1:
            return f"'{keys[0]}'"
        return str(keys)

    def _format_partition_by(self) -> str:
        """Format partition_by for dbt."""
        if not self.config.partition_columns:
            return "None"
        if len(self.config.partition_columns) == 1:
            return f"'{self.config.partition_columns[0]}'"
        return str(self.config.partition_columns)

    def _format_tags(self) -> str:
        """Format tags for dbt."""
        if not self.config.tags:
            return "[]"
        return str(list(self.config.tags.keys()))

    def _format_group_by_columns(self, columns: List[str]) -> str:
        """Format group by columns."""
        if not columns:
            return "*"
        return ",\n        ".join(columns)

    def _format_measures(self, measures: List[Dict[str, str]]) -> str:
        """Format measure expressions."""
        if not measures:
            return "count(*) as row_count"

        formatted = []
        for measure in measures:
            name = measure.get("name", "measure")
            expr = measure.get("expression", "count(*)")
            formatted.append(f"{expr} as {name}")

        return ",\n        ".join(formatted)

    def _format_group_by_clause(self, columns: List[str]) -> str:
        """Format GROUP BY clause."""
        if not columns:
            return ""
        return f"group by {', '.join(columns)}"


class dbtModelAdapter:
    """
    Generates individual dbt model files from transformer chains.

    Maps Sparkle transformers to dbt SQL logic.
    """

    def __init__(self, pipeline: BasePipeline):
        self.pipeline = pipeline
        self.config = pipeline.config

    def generate_model(self, output_path: str) -> str:
        """
        Generate dbt model SQL from transformer chain.

        Args:
            output_path: Path to write model file

        Returns:
            Path to generated model
        """
        transformers = self.config.transformers or []

        # Build CTE chain from transformers
        ctes = ["source as (\n    select * from {{ ref('source_table') }}\n)"]

        for i, transformer in enumerate(transformers):
            cte_name = f"step_{i+1}_{transformer['name']}"
            prev_cte = f"step_{i}_{transformers[i-1]['name']}" if i > 0 else "source"

            sql = self._transformer_to_sql(transformer, prev_cte)
            ctes.append(f"{cte_name} as (\n{sql}\n)")

        final_cte = f"step_{len(transformers)}_{transformers[-1]['name']}" if transformers else "source"

        ctes_joined = ',\n\n'.join(ctes)
        model_sql = f"""
-- Generated model from transformer chain

with {ctes_joined}

select * from {final_cte}
"""

        with open(output_path, "w") as f:
            f.write(model_sql.strip())

        return output_path

    def _transformer_to_sql(self, transformer: Dict[str, Any], prev_cte: str) -> str:
        """
        Convert transformer to SQL logic.

        Args:
            transformer: Transformer configuration
            prev_cte: Previous CTE name

        Returns:
            SQL string
        """
        name = transformer["name"]
        params = transformer.get("params", {})

        # Map common transformers to SQL
        if name == "drop_exact_duplicates":
            return f"    select distinct * from {prev_cte}"

        elif name == "standardize_nulls":
            null_values = params.get("null_values", ["", "NULL", "null"])
            return f"    select * from {prev_cte}\n    -- TODO: Implement null standardization"

        elif name == "add_audit_columns":
            user = params.get("user", "dbt")
            return f"""    select
        *,
        current_timestamp() as created_at,
        '{user}' as created_by,
        current_timestamp() as updated_at,
        '{user}' as updated_by
    from {prev_cte}"""

        elif name == "add_hash_key":
            source_cols = params.get("source_columns", ["id"])
            hash_col = params.get("hash_column", "hash_key")
            concat_expr = " || '-' || ".join(source_cols)
            return f"""    select
        *,
        md5({concat_expr}) as {hash_col}
    from {prev_cte}"""

        else:
            return f"    select * from {prev_cte}\n    -- TODO: Implement {name}"


class dbtTestAdapter:
    """
    Generates dbt tests from data quality expectations.

    Maps Great Expectations to dbt tests.
    """

    def __init__(self, pipeline: BasePipeline):
        self.pipeline = pipeline
        self.config = pipeline.config

    def generate_tests(self, output_dir: str) -> str:
        """
        Generate dbt test files.

        Args:
            output_dir: Directory for test files

        Returns:
            Path to tests directory
        """
        tests_dir = Path(output_dir) / "tests"
        tests_dir.mkdir(parents=True, exist_ok=True)

        # Generate generic tests in schema.yml (already done in dbtProjectAdapter)

        # Generate custom tests
        if self.config.extra_config.get("quality_checks"):
            self._generate_custom_tests(tests_dir)

        return str(tests_dir)

    def _generate_custom_tests(self, tests_dir: Path):
        """Generate custom singular tests."""
        quality_checks = self.config.extra_config.get("quality_checks", {})

        # Null check test
        if "null_check_columns" in quality_checks:
            self._generate_null_check_test(tests_dir, quality_checks["null_check_columns"])

        # Uniqueness test
        if "uniqueness_check" in quality_checks:
            self._generate_uniqueness_test(tests_dir, quality_checks["uniqueness_check"])

        # Email format test
        if quality_checks.get("email_format_check"):
            self._generate_email_format_test(tests_dir)

    def _generate_null_check_test(self, tests_dir: Path, columns: List[str]):
        """Generate null check test."""
        test_name = f"test_{self.config.destination_table}_null_checks"

        sql = f"""
-- Test for null values in critical columns

select *
from {{{{ ref('{self.config.destination_table}') }}}}
where
    {' or '.join(f'{col} is null' for col in columns)}
"""

        with open(tests_dir / f"{test_name}.sql", "w") as f:
            f.write(sql.strip())

    def _generate_uniqueness_test(self, tests_dir: Path, columns: List[str]):
        """Generate uniqueness test."""
        test_name = f"test_{self.config.destination_table}_uniqueness"

        sql = f"""
-- Test for duplicates

select
    {', '.join(columns)},
    count(*) as duplicate_count
from {{{{ ref('{self.config.destination_table}') }}}}
group by {', '.join(columns)}
having count(*) > 1
"""

        with open(tests_dir / f"{test_name}.sql", "w") as f:
            f.write(sql.strip())

    def _generate_email_format_test(self, tests_dir: Path):
        """Generate email format validation test."""
        test_name = f"test_{self.config.destination_table}_email_format"

        sql = f"""
-- Test for invalid email formats

select *
from {{{{ ref('{self.config.destination_table}') }}}}
where email is not null
  and not regexp_like(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{{2,}}$')
"""

        with open(tests_dir / f"{test_name}.sql", "w") as f:
            f.write(sql.strip())


class dbtMacroAdapter:
    """
    Generates reusable dbt macros for common transformations.

    Creates macros library for Sparkle transformers.
    """

    def __init__(self, pipeline: BasePipeline):
        self.pipeline = pipeline
        self.config = pipeline.config

    def generate_macros(self, output_dir: str) -> str:
        """
        Generate dbt macro files.

        Args:
            output_dir: Directory for macro files

        Returns:
            Path to macros directory
        """
        macros_dir = Path(output_dir) / "macros"
        macros_dir.mkdir(parents=True, exist_ok=True)

        # Generate common macros
        self._generate_standardize_nulls_macro(macros_dir)
        self._generate_add_audit_columns_macro(macros_dir)
        self._generate_hash_key_macro(macros_dir)
        self._generate_clean_string_macro(macros_dir)

        return str(macros_dir)

    def _generate_standardize_nulls_macro(self, macros_dir: Path):
        """Generate standardize_nulls macro."""
        macro = """
{% macro standardize_nulls(column_name, null_values=['', 'NULL', 'null', 'N/A', 'NA', 'None']) -%}
    case
        {% for null_val in null_values %}
        when {{ column_name }} = '{{ null_val }}' then null
        {% endfor %}
        else {{ column_name }}
    end
{%- endmacro %}
"""

        with open(macros_dir / "standardize_nulls.sql", "w") as f:
            f.write(macro.strip())

    def _generate_add_audit_columns_macro(self, macros_dir: Path):
        """Generate add_audit_columns macro."""
        macro = """
{% macro add_audit_columns(user='dbt') -%}
    current_timestamp() as created_at,
    '{{ user }}' as created_by,
    current_timestamp() as updated_at,
    '{{ user }}' as updated_by
{%- endmacro %}
"""

        with open(macros_dir / "add_audit_columns.sql", "w") as f:
            f.write(macro.strip())

    def _generate_hash_key_macro(self, macros_dir: Path):
        """Generate hash_key macro."""
        macro = """
{% macro hash_key(columns) -%}
    md5(concat(
        {%- for col in columns -%}
            coalesce(cast({{ col }} as string), '')
            {%- if not loop.last -%}, '-', {%- endif -%}
        {%- endfor -%}
    ))
{%- endmacro %}
"""

        with open(macros_dir / "hash_key.sql", "w") as f:
            f.write(macro.strip())

    def _generate_clean_string_macro(self, macros_dir: Path):
        """Generate clean_string macro."""
        macro = """
{% macro clean_string(column_name) -%}
    trim(regexp_replace({{ column_name }}, '[\\s]+', ' '))
{%- endmacro %}
"""

        with open(macros_dir / "clean_string.sql", "w") as f:
            f.write(macro.strip())


class dbtExposureAdapter:
    """
    Generates dbt exposures for downstream consumers.

    Documents how models are used by dashboards, reports, ML models.
    """

    def __init__(self, pipeline: BasePipeline):
        self.pipeline = pipeline
        self.config = pipeline.config

    def generate_exposure(self, output_path: str) -> str:
        """
        Generate dbt exposure definition.

        Args:
            output_path: Path to write exposure file

        Returns:
            Path to generated exposure
        """
        exposure = {
            "version": 2,
            "exposures": [
                {
                    "name": f"{self.config.pipeline_name}_exposure",
                    "type": self._determine_exposure_type(),
                    "maturity": "high",
                    "url": self.config.extra_config.get("dashboard_url", ""),
                    "description": f"Exposure for {self.config.pipeline_name}",
                    "depends_on": [
                        f"ref('{self.config.destination_table}')"
                    ],
                    "owner": {
                        "name": self.config.tags.get("team", "data-team") if self.config.tags else "data-team",
                        "email": self.config.alert_channels[0] if self.config.alert_channels else "data-team@company.com"
                    },
                    "tags": list(self.config.tags.keys()) if self.config.tags else []
                }
            ]
        }

        with open(output_path, "w") as f:
            yaml.dump(exposure, f, default_flow_style=False, sort_keys=False)

        return output_path

    def _determine_exposure_type(self) -> str:
        """Determine exposure type from pipeline type."""
        if "dashboard" in self.config.pipeline_type:
            return "dashboard"
        elif "ml_" in self.config.pipeline_type:
            return "ml"
        elif "gold" in self.config.pipeline_type:
            return "analysis"
        else:
            return "application"
