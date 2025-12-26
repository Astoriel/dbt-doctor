"""Tests: DataProfiler via DuckDB in-memory connector."""

import pytest

from dbt_doctor.analyzers.profiler import DataProfiler


@pytest.fixture
def profile(duckdb_connector):
    """Pre-computed profile for fct_orders — avoids re-profiling in every test."""
    profiler = DataProfiler(duckdb_connector)
    return profiler.profile_table("dbt_marts", "fct_orders")


def test_profile_table_basic(profile):
    assert profile.table == "fct_orders"
    assert profile.total_rows == 10
    assert "order_id" in profile.column_profiles
    assert "status" in profile.column_profiles


@pytest.mark.parametrize(
    "column, expected_unique",
    [
        ("order_id", 10),   # all unique
        ("status", 4),      # placed / shipped / completed / returned
    ],
)
def test_cardinality(profile, column, expected_unique):
    assert profile.column_profiles[column].unique_count == expected_unique


@pytest.mark.parametrize(
    "column, expected_null_rate",
    [
        ("order_id", 0.0),    # no nulls
        ("customer_id", 10.0), # one null out of 10 rows
        ("status", 0.0),
    ],
)
def test_null_rates(profile, column, expected_null_rate):
    assert profile.column_profiles[column].null_rate == pytest.approx(
        expected_null_rate, abs=0.1
    )


def test_unique_rate_for_pk(profile):
    """order_id: 10 rows, all unique (IDs 1-10)."""
    assert profile.column_profiles["order_id"].unique_rate == pytest.approx(100.0, abs=0.1)


def test_pk_detection(profile):
    """order_id should be flagged as likely PK."""
    assert profile.column_profiles["order_id"].is_likely_pk is True


def test_non_pk_column_is_not_flagged(profile):
    """status has low cardinality — should NOT be flagged as PK."""
    assert profile.column_profiles["status"].is_likely_pk is False


def test_format_output_contains_expected_sections(duckdb_connector):
    profiler = DataProfiler(duckdb_connector)
    profile = profiler.profile_table("dbt_marts", "fct_orders")
    output = profiler.format_profile(profile)
    assert "fct_orders" in output
    assert "order_id" in output
    assert "null rate" in output


def test_profile_table_not_found(duckdb_connector):
    profiler = DataProfiler(duckdb_connector)
    with pytest.raises((ValueError, Exception)):
        profiler.profile_table("dbt_marts", "nonexistent_table")
