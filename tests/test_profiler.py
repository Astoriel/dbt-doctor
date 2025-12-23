"""Tests: DataProfiler via DuckDB in-memory connector."""

import pytest

from dbt_doctor.analyzers.profiler import DataProfiler


def test_profile_table_basic(duckdb_connector):
    profiler = DataProfiler(duckdb_connector)
    profile = profiler.profile_table("dbt_marts", "fct_orders")
    assert profile.table == "fct_orders"
    assert profile.total_rows == 10
    assert "order_id" in profile.column_profiles
    assert "status" in profile.column_profiles


def test_profile_null_rate(duckdb_connector):
    """customer_id has one NULL in our fixture data."""
    profiler = DataProfiler(duckdb_connector)
    profile = profiler.profile_table("dbt_marts", "fct_orders")
    customer_id_profile = profile.column_profiles["customer_id"]
    assert customer_id_profile.non_null_count == 9
    assert customer_id_profile.null_rate == pytest.approx(10.0, abs=0.1)


def test_profile_unique_rate(duckdb_connector):
    """order_id: 10 rows, all unique (IDs 1-10)."""
    profiler = DataProfiler(duckdb_connector)
    profile = profiler.profile_table("dbt_marts", "fct_orders")
    order_id_profile = profile.column_profiles["order_id"]
    assert order_id_profile.unique_count == 10
    assert order_id_profile.unique_rate == pytest.approx(100.0, abs=0.1)


def test_profile_low_cardinality(duckdb_connector):
    """status column: placed/shipped/completed/returned = 4 distinct values."""
    profiler = DataProfiler(duckdb_connector)
    profile = profiler.profile_table("dbt_marts", "fct_orders")
    status_profile = profile.column_profiles["status"]
    assert status_profile.unique_count == 4


def test_profile_pk_detection(duckdb_connector):
    """order_id should be detected as likely primary key."""
    profiler = DataProfiler(duckdb_connector)
    profile = profiler.profile_table("dbt_marts", "fct_orders")
    order_id_profile = profile.column_profiles["order_id"]
    assert order_id_profile.is_likely_pk is True


def test_profile_format_output(duckdb_connector):
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
