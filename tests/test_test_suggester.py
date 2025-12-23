"""Tests: TestSuggester rules — profile stats → dbt test recommendations."""

from dbt_doctor.analyzers.profiler import ColumnProfile, TableProfile
from dbt_doctor.generators.test_suggester import TestSuggester


def _make_profile(columns_data: dict) -> TableProfile:
    """Helper to build a TableProfile from column spec dict."""
    col_profiles = {}
    total_rows = 100
    for name, spec in columns_data.items():
        cp = ColumnProfile(
            name=name,
            data_type=spec.get("data_type", "text"),
            total_rows=total_rows,
            non_null_count=spec.get("non_null_count", total_rows),
            unique_count=spec.get("unique_count", total_rows),
        )
        cp.sample_values = spec.get("sample_values", [])
        col_profiles[name] = cp
    return TableProfile(schema="test", table="test_table", total_rows=total_rows, column_profiles=col_profiles)


def test_not_null_suggested_when_zero_nulls():
    profile = _make_profile({"user_id": {"non_null_count": 100, "unique_count": 100}})
    sugg = TestSuggester().suggest(profile, "test_model")
    tests = {t.test_name for t in sugg.column_suggestions.get("user_id", [])}
    assert "not_null" in tests


def test_unique_suggested_when_all_unique():
    profile = _make_profile({"user_id": {"non_null_count": 100, "unique_count": 100}})
    sugg = TestSuggester().suggest(profile, "test_model")
    tests = {t.test_name for t in sugg.column_suggestions.get("user_id", [])}
    assert "unique" in tests


def test_accepted_values_for_low_cardinality():
    profile = _make_profile({"status": {
        "non_null_count": 100,
        "unique_count": 3,
        "sample_values": ["placed", "shipped", "completed"],
    }})
    sugg = TestSuggester().suggest(profile, "test_model")
    tests = {t.test_name for t in sugg.column_suggestions.get("status", [])}
    assert "accepted_values" in tests


def test_accepted_values_includes_actual_values():
    profile = _make_profile({"status": {
        "non_null_count": 100,
        "unique_count": 2,
        "sample_values": ["active", "inactive"],
    }})
    sugg = TestSuggester().suggest(profile, "test_model")
    accepted_test = next(
        t for t in sugg.column_suggestions.get("status", [])
        if t.test_name == "accepted_values"
    )
    assert "active" in accepted_test.config.get("values", [])


def test_no_unique_suggested_when_not_unique():
    profile = _make_profile({"status": {"non_null_count": 100, "unique_count": 3}})
    sugg = TestSuggester().suggest(profile, "test_model")
    tests = {t.test_name for t in sugg.column_suggestions.get("status", [])}
    assert "unique" not in tests


def test_id_column_heuristic():
    """Columns ending in _id with high uniqueness should get unique suggestion."""
    profile = _make_profile({"order_id": {"non_null_count": 100, "unique_count": 95}})
    sugg = TestSuggester().suggest(profile, "test_model")
    col_sugg = sugg.column_suggestions.get("order_id", [])
    test_names = {t.test_name for t in col_sugg}
    # 95% unique → medium confidence unique suggestion
    assert "unique" in test_names


def test_to_yaml_columns_format():
    profile = _make_profile({"user_id": {"non_null_count": 100, "unique_count": 100}})
    sugg = TestSuggester().suggest(profile, "test_model")
    columns = TestSuggester().to_yaml_columns(sugg)
    assert isinstance(columns, list)
    if columns:
        assert "name" in columns[0]
        assert "tests" in columns[0]


def test_no_suggestions_for_high_null_column():
    """A column with 50% nulls should NOT get a not_null suggestion."""
    profile = _make_profile({"notes": {"non_null_count": 50, "unique_count": 50}})
    sugg = TestSuggester().suggest(profile, "test_model")
    tests = {t.test_name for t in sugg.column_suggestions.get("notes", [])}
    assert "not_null" not in tests
