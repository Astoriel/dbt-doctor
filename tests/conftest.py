"""Shared pytest fixtures for dbt-doctor tests."""

import json
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_manifest_path() -> Path:
    """Return path to the sample manifest.json fixture file."""
    return FIXTURE_DIR / "sample_manifest.json"


@pytest.fixture
def sample_manifest_data(sample_manifest_path) -> dict:
    with open(sample_manifest_path, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def project_dir_with_manifest(sample_manifest_path, tmp_path) -> Path:
    """Create a temp dir that mimics a dbt project with manifest.json."""
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    manifest_dest = target_dir / "manifest.json"
    manifest_dest.write_bytes(sample_manifest_path.read_bytes())

    # Create a minimal dbt_project.yml
    (tmp_path / "dbt_project.yml").write_text(
        "name: jaffle_shop\nversion: '1.0.0'\nprofile: jaffle_shop\nmodel-paths: ['models']\n"
    )
    return tmp_path


@pytest.fixture
def duckdb_connector():
    """Provide a DuckDB in-memory connector with test data pre-loaded."""
    from dbt_doctor.connectors.duckdb import DuckDBConnector

    conn = DuckDBConnector(":memory:")

    # Create a test schema and table for profiling tests
    conn.execute_write("CREATE SCHEMA IF NOT EXISTS dbt_marts")
    conn.execute_write("""
        CREATE TABLE dbt_marts.fct_orders (
            order_id INTEGER,
            customer_id INTEGER,
            status VARCHAR,
            amount DECIMAL(10, 2)
        )
    """)
    conn.execute_write("""
        INSERT INTO dbt_marts.fct_orders VALUES
        (1, 101, 'placed', 29.99),
        (2, 102, 'shipped', 49.99),
        (3, 103, 'placed', 9.99),
        (4, 104, 'completed', 199.99),
        (5, 105, 'completed', 59.99),
        (6, 106, 'placed', 14.99),
        (7, 107, 'shipped', 79.99),
        (8, 108, 'completed', 39.99),
        (9, NULL, 'returned', 24.99),
        (10, 110, 'placed', 34.99)
    """)
    yield conn
    conn.close()
