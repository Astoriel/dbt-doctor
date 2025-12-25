# Changelog

All notable changes to `dbt-doctor` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-12-23

### Added
- Initial release of the `dbt-doctor` MCP server.
- **Analyzers**:
  - `DataProfiler`: DuckDB-powered fast data profiling for mart tables (row count, distinct count, null rates, cardinality).
  - `ProjectAuditor`: Rules-based auditor for undocumented models, missing descriptions, and naming convention overrides.
  - `DagAnalyzer`: Extract upstream and downstream lineage from the dbt manifest.
  - `TestCoverageAnalyzer`: Calculate coverage score and find uncovered column constraints.
- **Server Tools (MCP)**:
  - `list_models_tool`: Get all models in the project with key metadata.
  - `get_model_details_tool`: Fetch column data, SQL, and description for a specific dbt model.
  - `audit_dbt_project_tool`: Run a full project audit and get a health score.
  - `check_test_coverage_tool`: Get detailed coverage metrics for all models.
  - `analyze_dag_tool`: See model dependencies including parents and children.
  - `get_project_health_summary_tool`: Aggregate metrics (coverage, data profiling, audit score) into one payload.
- **Core Engine**:
  - `ProjectParser`: Lightweight wrapper around `manifest.json` parsing.
  - DuckDB singleton connection manager for zero-copy memory operations.

### fixed
- Replaced redundant string-matching in tests with proper parameterized paths.
