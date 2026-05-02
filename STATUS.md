# Project Status

**Status:** Active alpha  
**Snapshot date:** 2025-12-25  
**Primary audience:** dbt users who want local MCP tools for project audit, documentation, profiling, and schema drift checks.

`dbt-doctor` is a focused MCP server for dbt quality workflows. The current implementation includes project audit tools, manifest parsing, data profiling through Postgres/DuckDB connectors, test suggestions, YAML updates, CI, and unit tests.

This repository should be read as an early open-source tool, not a mature replacement for the broader dbt platform. The most valuable parts to evaluate are the dbt manifest parsing, profiling/test-suggestion flow, and non-destructive YAML writer.

## Current Confidence

- Working: local MCP server, dbt manifest inspection, audit reports, DuckDB/Postgres profiling, YAML update flow, CI test matrix.
- Planned: broader adapter coverage, stronger SQL sandboxing, richer demo fixtures, compatibility notes versus other dbt MCP servers.
- Not claimed: enterprise governance, hosted service operation, full dbt Cloud parity.

