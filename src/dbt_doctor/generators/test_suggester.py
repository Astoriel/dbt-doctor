"""Generators: Test suggestion engine — data profile stats → concrete dbt test recommendations."""

from dataclasses import dataclass, field

from ..analyzers.profiler import TableProfile

# Threshold for "low cardinality" → suggest accepted_values
_LOW_CARDINALITY_THRESHOLD = 10
# Minimum row count before making suggestions (too few rows = unreliable stats)
_MIN_ROWS_FOR_SUGGESTIONS = 10


@dataclass
class TestSuggestion:
    column_name: str
    test_name: str  # e.g. "not_null", "unique", "accepted_values"
    config: dict = field(default_factory=dict)  # e.g. {"values": [...]}
    reason: str = ""
    confidence: str = "high"  # "high", "medium", "low"


@dataclass
class ModelTestSuggestions:
    model_name: str
    schema: str
    table: str
    total_rows: int
    column_suggestions: dict[str, list[TestSuggestion]] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


class TestSuggester:
    """
    Converts a DataProfiler result into concrete dbt test recommendations.

    Rules applied per column:
    - null_rate == 0%            → not_null (high confidence)
    - unique_count == total_rows → unique (high confidence)
    - unique_count <= 10         → accepted_values with actual distinct values (high confidence)
    - name ends _id              → not_null + unique (if stats confirm)
    - name ends _at / _date      → not_null (if null_rate < 1%)
    - numeric min/max available  → note for dbt_expectations accepted_range
    """

    def suggest(self, profile: TableProfile, model_name: str) -> ModelTestSuggestions:
        result = ModelTestSuggestions(
            model_name=model_name,
            schema=profile.schema,
            table=profile.table,
            total_rows=profile.total_rows,
        )

        if profile.total_rows < _MIN_ROWS_FOR_SUGGESTIONS:
            result.notes.append(
                f"⚠️  Only {profile.total_rows} rows in table — suggestions may not be reliable. "
                "Profile again after loading more data."
            )

        for col_name, col in profile.column_profiles.items():
            suggestions: list[TestSuggestion] = []

            # --- not_null ---
            if col.null_rate == 0.0:
                suggestions.append(
                    TestSuggestion(
                        column_name=col_name,
                        test_name="not_null",
                        reason=f"0% null rate ({col.non_null_count}/{col.total_rows} rows are filled)",
                        confidence="high",
                    )
                )
            elif col.null_rate < 1.0 and col_name.lower().endswith(("_at", "_date", "_ts")):
                suggestions.append(
                    TestSuggestion(
                        column_name=col_name,
                        test_name="not_null",
                        reason=f"Timestamp column with only {col.null_rate}% nulls — likely always expected",
                        confidence="medium",
                    )
                )

            # --- unique ---
            if col.unique_count == col.total_rows and col.total_rows > 0:
                suggestions.append(
                    TestSuggestion(
                        column_name=col_name,
                        test_name="unique",
                        reason=f"100% unique values ({col.unique_count:,} distinct in {col.total_rows:,} rows)",
                        confidence="high",
                    )
                )

            # --- accepted_values ---
            if (
                0 < col.unique_count <= _LOW_CARDINALITY_THRESHOLD
                and col.total_rows >= _MIN_ROWS_FOR_SUGGESTIONS
            ):
                # Try to get distinct values from sample if available
                sample_distinct = list({v for v in col.sample_values if v is not None})
                config: dict = {}
                if sample_distinct and len(sample_distinct) <= _LOW_CARDINALITY_THRESHOLD:
                    config = {"values": sorted(str(v) for v in sample_distinct)}
                suggestions.append(
                    TestSuggestion(
                        column_name=col_name,
                        test_name="accepted_values",
                        config=config,
                        reason=(
                            f"Low cardinality: {col.unique_count} distinct values. "
                            "Constrain to prevent unexpected categories."
                        ),
                        confidence="high" if config else "medium",
                    )
                )

            # --- Column name heuristics ---
            col_lower = col_name.lower()
            if col_lower.endswith("_id") or col_lower == "id":
                # Check that not_null + unique aren't already suggested
                existing_tests = {s.test_name for s in suggestions}
                if "not_null" not in existing_tests and col.null_rate <= 5.0:
                    suggestions.append(
                        TestSuggestion(
                            column_name=col_name,
                            test_name="not_null",
                            reason=f"ID column with {col.null_rate}% nulls — typically should be not_null",
                            confidence="medium",
                        )
                    )
                if "unique" not in existing_tests and col.unique_rate >= 90.0:
                    suggestions.append(
                        TestSuggestion(
                            column_name=col_name,
                            test_name="unique",
                            reason=f"ID column with {col.unique_rate}% unique values — likely should be unique",
                            confidence="medium",
                        )
                    )

            if suggestions:
                result.column_suggestions[col_name] = suggestions

        return result

    def format_suggestions(self, suggestions: ModelTestSuggestions) -> str:
        lines = [
            "═" * 60,
            f"  🧪 Test Suggestions: {suggestions.model_name}",
            f"     Table: {suggestions.schema}.{suggestions.table} ({suggestions.total_rows:,} rows)",
            "═" * 60,
        ]

        for note in suggestions.notes:
            lines.append(f"  {note}")

        if not suggestions.column_suggestions:
            lines.append("  ✅ No additional tests suggested (already well-covered or no data).")
        else:
            lines.append("")
            for col_name, tests in suggestions.column_suggestions.items():
                lines.append(f"  Column: {col_name}")
                for t in tests:
                    cfg_str = f"  config: {t.config}" if t.config else ""
                    lines.append(f"    → [{t.confidence.upper()}] {t.test_name}{cfg_str}")
                    lines.append(f"       Reason: {t.reason}")
            lines.append("")

        lines.append("  💡 To apply: call `update_model_yaml` with these suggestions,")
        lines.append("     or `generate_model_docs` for full E2E documentation.")
        lines.append("═" * 60)
        return "\n".join(lines)

    def to_yaml_columns(self, suggestions: ModelTestSuggestions) -> list[dict]:
        """Convert suggestions to the format expected by YamlWriter.upsert_model_yaml."""
        columns: list[dict] = []
        for col_name, tests in suggestions.column_suggestions.items():
            col_entry: dict = {"name": col_name, "tests": []}
            for t in tests:
                if t.config:
                    col_entry["tests"].append({t.test_name: t.config})
                else:
                    col_entry["tests"].append(t.test_name)
            columns.append(col_entry)
        return columns
