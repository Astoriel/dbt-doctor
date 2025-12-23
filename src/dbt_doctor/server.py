"""
dbt-doctor MCP Server — AI-driven quality & governance for dbt projects.

All 12 MCP Tools:
  Audit:      audit_project, check_test_coverage, analyze_dag, get_project_health
  Profiling:  profile_model, execute_query, detect_schema_drift
  Generation: suggest_tests, update_model_yaml, generate_model_docs
  Context:    list_models, get_model_details
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

from .analyzers.auditor import ProjectAuditor
from .analyzers.dag_analyzer import DagAnalyzer
from .analyzers.drift_detector import SchemaDriftDetector
from .analyzers.profiler import DataProfiler
from .connectors.base import BaseConnector
from .core.manifest import DbtManifestParser
from .core.profiles import DbtProfileParser
from .core.project import DbtProjectParser
from .generators.doc_generator import DocGenerator
from .generators.test_suggester import TestSuggester
from .generators.yaml_writer import YamlWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("dbt_doctor")

# ── Singleton server instance ──────────────────────────────────────────────
mcp = FastMCP("dbt-doctor")

# ── Module-level singletons (set by main() before mcp.run()) ──────────────
_manifest: DbtManifestParser | None = None
_connector: BaseConnector | None = None
_auditor = ProjectAuditor()
_dag_analyzer = DagAnalyzer()


def _get_manifest() -> DbtManifestParser:
    if _manifest is None:
        raise RuntimeError("dbt-doctor not initialized. Pass --project-dir to the server.")
    _manifest.load()
    return _manifest


def _get_connector() -> BaseConnector:
    if _connector is None:
        raise RuntimeError(
            "Database connector not available. "
            "Check that profiles.yml is reachable and credentials are correct."
        )
    return _connector


# ═══════════════════════════════════════════════════════════════════════════
# CONTEXT TOOLS
# ═══════════════════════════════════════════════════════════════════════════


@mcp.tool()
def list_models() -> str:
    """
    List all dbt models in the project.

    Returns a summary of every model including name, materialization, description status,
    and documentation coverage. Use this as your starting point before any other tool.
    """
    parser = _get_manifest()
    models = parser.get_models()
    if not models:
        return "No models found. Run `dbt compile` in your project first."

    lines = [f"📦 dbt Project — {len(models)} models found:\n"]
    for m in sorted(models, key=lambda x: x["name"]):
        doc_status = "✅" if m["has_description"] else "❌"
        col_coverage = (
            f"{m['documented_column_count']}/{m['column_count']}"
            if m["column_count"] > 0
            else "no cols"
        )
        lines.append(
            f"  {doc_status} {m['name']:<40} [{m['materialized']:<8}]  "
            f"cols: {col_coverage}  schema: {m['schema']}"
        )
    return "\n".join(lines)


@mcp.tool()
def get_model_details(model_name: str) -> str:
    """
    Get full details for a specific dbt model: SQL code, columns, lineage, and config.

    Use this before writing documentation or suggesting tests for a specific model.
    Also use this to understand what a model does before profiling it.

    Args:
        model_name: Exact name of the dbt model (e.g. 'fct_orders' or 'stg_users').
    """
    parser = _get_manifest()
    details = parser.get_model_details(model_name)
    if not details:
        return f"Model '{model_name}' not found. Run `list_models` to see available models."

    lines = [
        f"Model: {details['name']}",
        f"  Description: {details['description'] or '(not documented)'}",
        f"  Path:        {details['path']}",
        f"  Schema:      {details['schema']}  |  Alias: {details['alias']}",
        f"  Materialized: {details['materialized']}",
        f"  Tags: {', '.join(details['tags']) if details['tags'] else 'none'}",
        "",
        "─── Columns ───────────────────────────────────────────",
    ]
    if details["columns"]:
        for col_name, col in details["columns"].items():
            desc = col.get("description", "")
            tests = col.get("tests", [])
            test_str = ", ".join(t if isinstance(t, str) else next(iter(t)) for t in tests)
            lines.append(
                f"  {col_name:<35}  desc={'✅' if desc else '❌'}  tests=[{test_str or 'none'}]"
            )
    else:
        lines.append("  (no columns in manifest — run `dbt parse` to populate)")

    lines.append("\n─── Upstream Dependencies ──────────────────────────────")
    if details["depends_on"]:
        for dep in details["depends_on"]:
            lines.append(f"  ← {dep}")
    else:
        lines.append("  (no upstream dependencies — this is a root model)")

    lines.append("\n─── Raw SQL ────────────────────────────────────────────")
    sql = details.get("raw_sql", "")
    lines.append(sql[:2000] + ("\n... [truncated]" if len(sql) > 2000 else ""))

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# AUDIT TOOLS
# ═══════════════════════════════════════════════════════════════════════════


@mcp.tool()
def audit_project() -> str:
    """
    Run a comprehensive quality audit of the entire dbt project.

    Checks:
    - Model-level documentation coverage (% of models with descriptions)
    - Column-level documentation coverage (% of columns with descriptions)
    - Test coverage (% of models with at least one dbt test)
    - Naming convention adherence (snake_case, layer prefixes)

    Returns an overall health score (0-100%) and a list of specific issues.
    Use this as the FIRST step before any documentation or test generation work.
    """
    parser = _get_manifest()
    models = parser.get_models()
    if not models:
        return "No models found. Run `dbt compile` first."

    # Enrich models with full column info from manifest
    enriched = []
    for m in models:
        details = parser.get_model_details(m["name"])
        if details:
            enriched.append(
                {
                    **m,
                    "columns": details.get("columns", {}),
                    "description": details.get("description", ""),
                }
            )
        else:
            enriched.append(m)

    report = _auditor.audit(enriched)
    return _auditor.format_report(report)


@mcp.tool()
def check_test_coverage() -> str:
    """
    Show detailed test coverage statistics for all models.

    Returns a ranked list of models sorted by test coverage, from worst to best.
    Use this to identify which models need tests the most, then call `suggest_tests`
    on the worst offenders.
    """
    parser = _get_manifest()
    models = parser.get_models()
    if not models:
        return "No models found."

    enriched = []
    for m in models:
        details = parser.get_model_details(m["name"]) or {}
        enriched.append(
            {
                **m,
                "columns": details.get("columns", {}),
                "description": details.get("description", ""),
            }
        )

    report = _auditor.audit(enriched)

    lines = [
        f"  🧪 Test Coverage Report ({report.test_score:.1f}% of models have ≥1 test)\n",
        f"  {'Model':<40} {'Test Cov':>8}  {'Doc Cov':>7}  Columns",
        "  " + "─" * 65,
    ]
    for mr in report.model_results:
        test_pct = round(mr.test_coverage * 100)
        doc_pct = round(mr.doc_coverage * 100)
        status = "🔴" if test_pct < 20 else ("🟡" if test_pct < 60 else "🟢")
        lines.append(
            f"  {status} {mr.name:<38} {test_pct:>7}%  {doc_pct:>6}%  "
            f"{mr.tested_columns}/{mr.total_columns}"
        )

    return "\n".join(lines)


@mcp.tool()
def analyze_dag() -> str:
    """
    Analyze the dbt project DAG (Directed Acyclic Graph) structure.

    Detects:
    - Orphan models (no downstream consumers or exposures)
    - Root models (no model dependencies, only sources)
    - High fan-out models (one model feeding many downstream consumers)
    - Maximum chain depth (long lineage chains)

    Use this to find structural issues and potential refactoring opportunities.
    """
    parser = _get_manifest()
    models = parser.get_models()
    exposures = parser.get_all_exposures()
    if not models:
        return "No models found."

    dag_report = _dag_analyzer.analyze(models, exposures)
    return _dag_analyzer.format_report(dag_report)


@mcp.tool()
def get_project_health() -> str:
    """
    Get a comprehensive health dashboard for the dbt project.

    This is the ENTRY POINT tool — call this first to get an overview of:
    - Overall quality score
    - Top coverage issues
    - DAG structural issues
    - Recommended next actions

    Think of it as a doctor's overall diagnosis before deciding on treatments.
    """
    parser = _get_manifest()
    models = parser.get_models()
    if not models:
        return "No models found. Run `dbt compile` in your dbt project directory first."

    enriched = []
    for m in models:
        details = parser.get_model_details(m["name"]) or {}
        enriched.append(
            {
                **m,
                "columns": details.get("columns", {}),
                "description": details.get("description", ""),
            }
        )

    report = _auditor.audit(enriched)
    exposures = parser.get_all_exposures()
    dag_report = _dag_analyzer.analyze(models, exposures)

    lines = [
        "╔" + "═" * 53 + "╗",
        "║  🩺 dbt-doctor — Project Health Dashboard         ║",
        "╠" + "═" * 53 + "╣",
        f"║  Overall Score:    {report.overall_score:>5.1f}%                          ║",
        "╟" + "─" * 53 + "╢",
        f"║  Documentation:    {report.model_doc_score:>5.1f}%  model descriptions      ║",
        f"║                    {report.column_doc_score:>5.1f}%  column descriptions     ║",
        f"║  Test Coverage:    {report.test_score:>5.1f}%  models with ≥1 test     ║",
        f"║  Naming Score:     {report.naming_score:>5.1f}%                          ║",
        "╟" + "─" * 53 + "╢",
        f"║  DAG: depth={dag_report.max_chain_depth:<2}  orphans={len(dag_report.orphan_models):<3}  "
        f"high-fanout={len(dag_report.high_fanout_models):<2}     ║",
        "╚" + "═" * 53 + "╝",
        "",
        "  📋 Recommended Actions:",
    ]

    priority = 1
    if report.overall_score < 50:
        lines.append(
            f"  {priority}. 🔴 Low overall quality. Start with: `audit_project` for details"
        )
        priority += 1
    if report.test_score < 60:
        worst = report.worst_models[:3]
        worst_names = ", ".join(m.name for m in worst)
        lines.append(f"  {priority}. Run `suggest_tests` on: {worst_names}")
        priority += 1
    if dag_report.orphan_models:
        lines.append(
            f"  {priority}. Review {len(dag_report.orphan_models)} orphan models "
            f"(first: {dag_report.orphan_models[0]})"
        )
        priority += 1
    if report.column_doc_score < 30:
        lines.append(f"  {priority}. Use `generate_model_docs` for E2E auto-documentation")
        priority += 1
    if _connector is not None:
        lines.append(
            f"  {priority}. ✅ DB connected — run `detect_schema_drift` to check for drift"
        )
    else:
        lines.append(
            f"  {priority}. ⚠️  No DB connection — profiling tools unavailable. "
            "Check profiles.yml setup."
        )

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# PROFILING TOOLS
# ═══════════════════════════════════════════════════════════════════════════


@mcp.tool()
def profile_model(model_name: str) -> str:
    """
    Profile a dbt model's output table in the data warehouse.

    Automatically locates the model's target schema and table from the manifest,
    then runs statistical analysis: null rates, unique counts, min/max values.

    Returns per-column statistics to help design accurate dbt tests.
    ALWAYS call this before `suggest_tests` or `generate_model_docs`.

    Args:
        model_name: Exact dbt model name (e.g. 'fct_orders').
    """
    parser = _get_manifest()
    details = parser.get_model_details(model_name)
    if not details:
        return f"Model '{model_name}' not found."

    schema = details.get("schema") or ""
    table = details.get("alias") or model_name

    if not schema:
        return (
            f"Could not determine schema for model '{model_name}'. "
            "Make sure the manifest is up to date (`dbt compile`)."
        )

    connector = _get_connector()
    profiler = DataProfiler(connector)
    try:
        profile = profiler.profile_table(schema, table)
        return profiler.format_profile(profile)
    except Exception as exc:
        return f"Profiling failed for {schema}.{table}: {exc}"


@mcp.tool()
def execute_query(query: str) -> str:
    """
    Execute a read-only SQL SELECT query against the connected data warehouse.

    Use this to manually inspect data, verify assumptions, or investigate anomalies
    before writing tests. Only SELECT statements are allowed.

    Args:
        query: A read-only (SELECT) SQL query to run.
    """
    connector = _get_connector()
    try:
        rows = connector.execute_query(query)
    except ValueError as exc:
        return f"❌ Rejected: {exc}"
    except Exception as exc:
        return f"❌ Query failed: {exc}"

    if not rows:
        return "Query succeeded — 0 rows returned."

    output_lines = [f"✅ {len(rows)} row(s) returned (max 200):"]
    for i, row in enumerate(rows[:20], 1):
        output_lines.append(f"  Row {i}: {json.dumps(row, default=str)}")
    if len(rows) > 20:
        output_lines.append(f"  ... and {len(rows) - 20} more rows (showing first 20)")
    return "\n".join(output_lines)


@mcp.tool()
def detect_schema_drift(model_name: str) -> str:
    """
    Detect schema drift between the actual database and the dbt manifest documentation.

    Compares columns in the DB vs columns documented in manifest.json and finds:
    - New columns added to DB but not yet documented in dbt
    - Documented columns that no longer exist in DB (stale docs)
    - Columns where the data type has changed

    This is a UNIQUE feature — run this regularly to keep your dbt docs in sync.

    Args:
        model_name: Exact dbt model name (e.g. 'stg_orders').
    """
    parser = _get_manifest()
    details = parser.get_model_details(model_name)
    if not details:
        return f"Model '{model_name}' not found in manifest."

    schema = details.get("schema") or ""
    table = details.get("alias") or model_name
    manifest_columns: dict[str, Any] = details.get("columns") or {}

    if not schema:
        return f"Cannot determine schema for '{model_name}'."

    connector = _get_connector()
    detector = SchemaDriftDetector(connector)
    try:
        drift_report = detector.detect(schema, table, manifest_columns)
        return detector.format_report(drift_report)
    except Exception as exc:
        return f"Schema drift check failed: {exc}"


# ═══════════════════════════════════════════════════════════════════════════
# GENERATION TOOLS
# ═══════════════════════════════════════════════════════════════════════════


@mcp.tool()
def suggest_tests(model_name: str) -> str:
    """
    Suggest data quality tests for a dbt model based on actual data statistics.

    This tool profiles the model's data in the warehouse and intelligently recommends:
    - not_null: when null rate is 0%
    - unique: when all values are distinct
    - accepted_values: when a column has very few distinct values
    - Additional heuristics for ID columns, timestamp columns, etc.

    Each suggestion includes a confidence level (HIGH/MEDIUM) and reasoning.
    After reviewing, call `update_model_yaml` to apply the suggestions.

    Args:
        model_name: Exact dbt model name.
    """
    parser = _get_manifest()
    details = parser.get_model_details(model_name)
    if not details:
        return f"Model '{model_name}' not found in manifest."

    schema = details.get("schema") or ""
    table = details.get("alias") or model_name

    if not schema:
        return f"Cannot determine schema for '{model_name}'."

    connector = _get_connector()
    profiler = DataProfiler(connector)
    suggester = TestSuggester()

    try:
        profile = profiler.profile_table(schema, table)
        suggestions = suggester.suggest(profile, model_name)
        return suggester.format_suggestions(suggestions)
    except Exception as exc:
        return f"Could not generate suggestions for '{model_name}': {exc}"


@mcp.tool()
def update_model_yaml(
    model_name: str,
    description: str,
    columns: list[dict],
) -> str:
    """
    Write or update documentation and tests in the model's schema.yml file.

    Uses ruamel.yaml to preserve existing human comments and other models in the same file.
    Merges new column docs/tests non-destructively — never removes existing content.

    Args:
        model_name: Exact dbt model name (e.g. 'stg_orders').
        description: A clear, business-level description of what this model represents.
        columns: List of column dicts. Each: {'name': str, 'description': str, 'tests': list}
                 Tests can be strings ('not_null') or dicts ({'accepted_values': {'values': [...]}}).

    Example columns:
        [
          {'name': 'order_id', 'description': 'Unique order identifier', 'tests': ['not_null', 'unique']},
          {'name': 'status', 'tests': [{'accepted_values': {'values': ['placed', 'shipped']}}]}
        ]
    """
    parser = _get_manifest()
    details = parser.get_model_details(model_name)
    if not details:
        return f"Model '{model_name}' not found in manifest. Cannot determine file path."

    model_path = details.get("path")
    if not model_path:
        return f"Could not determine SQL file path for '{model_name}'."

    project_dir = _manifest.project_dir if _manifest else Path(".")
    writer = YamlWriter(str(project_dir))

    result = writer.upsert_model_yaml(
        model_name=model_name,
        model_sql_path=model_path,
        description=description,
        columns=columns,
    )

    if "error" in result:
        return f"❌ Failed to update YAML: {result['error']}"

    return (
        f"✅ Successfully updated documentation for '{model_name}'!\n"
        f"   Written to: {result['file_path']}\n"
        f"   Columns documented: {len(columns)}\n"
        f"   Run `dbt parse` to refresh the manifest."
    )


@mcp.tool()
def generate_model_docs(model_name: str, description: str = "") -> str:
    """
    Full E2E documentation pipeline for a single dbt model.

    This is the KILLER FEATURE of dbt-doctor. In one call it:
    1. Profiles the model's data in the warehouse
    2. Generates intelligent test suggestions from the real statistics
    3. Prepares a complete schema.yml entry

    Returns a PREVIEW of what will be written (dry-run).
    After reviewing, call `update_model_yaml` with the suggested columns to apply.

    Use this when you want the AI to automatically document a model end-to-end.

    Args:
        model_name: Exact dbt model name.
        description: Optional model-level description. If empty, only column tests are suggested.
    """
    parser = _get_manifest()
    details = parser.get_model_details(model_name)
    if not details:
        return f"Model '{model_name}' not found in manifest."

    schema = details.get("schema") or ""
    table = details.get("alias") or model_name
    model_path = details.get("path") or ""

    if not schema:
        return f"Cannot determine schema for '{model_name}'."

    project_dir = _manifest.project_dir if _manifest else Path(".")
    connector = _get_connector()
    profiler = DataProfiler(connector)
    suggester = TestSuggester()
    writer = YamlWriter(str(project_dir))
    generator = DocGenerator(profiler, suggester, writer)

    try:
        preview = generator.generate(
            model_name=model_name,
            model_sql_path=model_path,
            schema=schema,
            table=table,
            description=description,
        )
        return generator.format_preview(preview)
    except Exception as exc:
        return f"Doc generation failed for '{model_name}': {exc}"


# ═══════════════════════════════════════════════════════════════════════════
# ENTRYPOINT
# ═══════════════════════════════════════════════════════════════════════════


def _build_connector(project_dir: str) -> BaseConnector | None:
    """Try to build a database connector from dbt profiles.yml."""
    proj_parser = DbtProjectParser(project_dir)
    if not proj_parser.load():
        logger.warning("Could not load dbt_project.yml — DB tools will be unavailable.")
        return None

    profile_name = proj_parser.profile_name
    if not profile_name:
        logger.warning("No profile name in dbt_project.yml.")
        return None

    logger.info("Found profile '%s' in dbt_project.yml", profile_name)
    prof_parser = DbtProfileParser(profile_name)
    creds = prof_parser.get_credentials()
    if not creds:
        logger.warning("Could not extract credentials from profiles.yml.")
        return None

    db_type = creds.get("type", "postgres").lower()

    if db_type == "postgres":
        try:
            from .connectors.postgres import PostgresConnector

            conn = PostgresConnector(creds)
            logger.info("PostgreSQL connector initialized.")
            return conn
        except Exception as exc:
            logger.error("Failed to create PostgreSQL connector: %s", exc)
    elif db_type == "duckdb":
        try:
            from .connectors.duckdb import DuckDBConnector

            db_path = creds.get("path", ":memory:")
            conn = DuckDBConnector(db_path)
            logger.info("DuckDB connector initialized (path=%s).", db_path)
            return conn
        except Exception as exc:
            logger.error("Failed to create DuckDB connector: %s", exc)
    else:
        logger.warning(
            "Unsupported DB type '%s'. Only 'postgres' and 'duckdb' are supported. "
            "Profiling tools will be unavailable.",
            db_type,
        )
    return None


def main() -> None:
    """CLI entrypoint for dbt-doctor MCP server."""
    arg_parser = argparse.ArgumentParser(
        prog="dbt-doctor",
        description=(
            "dbt-doctor: AI-driven quality & governance MCP Server for dbt projects.\n"
            "Audit coverage, profile data, detect drift, and auto-generate documentation."
        ),
    )
    arg_parser.add_argument(
        "--project-dir",
        type=str,
        required=True,
        help="Absolute path to your dbt project directory (contains dbt_project.yml).",
    )
    arg_parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging verbosity (default: INFO).",
    )

    args, _ = arg_parser.parse_known_args()

    # Configure logging
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    global _manifest, _connector

    project_dir = args.project_dir
    logger.info("Starting dbt-doctor for project at: %s", project_dir)

    _manifest = DbtManifestParser(project_dir)
    if not _manifest.load():
        logger.warning(
            "manifest.json not found or invalid. Read-only context tools will still work "
            "once you run `dbt compile` in your project."
        )

    _connector = _build_connector(project_dir)
    if _connector:
        logger.info("DB connected — all 12 tools available.")
    else:
        logger.info(
            "DB not connected — profiling/drift tools unavailable. "
            "Audit and context tools are still fully functional."
        )

    logger.info("dbt-doctor ready. Listening on stdio...")
    mcp.run()


if __name__ == "__main__":
    main()
