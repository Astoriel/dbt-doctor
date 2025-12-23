"""Generators: E2E documentation generator — profile → suggest → write in one call."""

import logging
from dataclasses import dataclass
from typing import Any

from ..analyzers.profiler import DataProfiler, TableProfile
from ..generators.test_suggester import ModelTestSuggestions, TestSuggester
from ..generators.yaml_writer import YamlWriter

logger = logging.getLogger(__name__)


@dataclass
class DocPreview:
    """Preview of what will be written before committing to disk."""

    model_name: str
    description: str
    columns: list[dict[str, Any]]
    profile: TableProfile | None
    suggestions: ModelTestSuggestions | None
    yaml_path: str


class DocGenerator:
    """
    End-to-end documentation pipeline:

    1. Profile the model in the data warehouse
    2. Generate test suggestions from the profile statistics
    3. Optionally write to schema.yml

    Design decision: `generate()` returns a *preview* (dry-run).
    Caller explicitly calls `apply()` to write to disk.
    This gives the AI (or user) a chance to review before mutating files.
    """

    def __init__(self, profiler: DataProfiler, suggester: TestSuggester, writer: YamlWriter) -> None:
        self._profiler = profiler
        self._suggester = suggester
        self._writer = writer

    def generate(
        self,
        model_name: str,
        model_sql_path: str,
        schema: str,
        table: str,
        description: str = "",
    ) -> DocPreview:
        """
        Run profiling + suggestion pipeline and return a preview for review.

        Parameters
        ----------
        model_name:
            The dbt model name.
        model_sql_path:
            Path to the .sql file (relative to project root).
        schema, table:
            Database location of the model's output.
        description:
            Optional model-level description. If not provided, only column-level
            tests are suggested.
        """
        try:
            profile = self._profiler.profile_table(schema, table)
        except Exception as exc:
            logger.warning("Could not profile %s.%s: %s. Skipping data-driven suggestions.", schema, table, exc)
            profile = None

        suggestions: ModelTestSuggestions | None = None
        if profile:
            suggestions = self._suggester.suggest(profile, model_name)

        columns: list[dict[str, Any]] = []
        if suggestions:
            columns = self._suggester.to_yaml_columns(suggestions)

        planned_path = self._writer.find_or_plan_schema_path(model_sql_path)

        return DocPreview(
            model_name=model_name,
            description=description,
            columns=columns,
            profile=profile,
            suggestions=suggestions,
            yaml_path=str(planned_path),
        )

    def apply(self, preview: DocPreview) -> dict[str, Any]:
        """Write the preview to schema.yml. Returns result dict."""
        return self._writer.upsert_model_yaml(
            model_name=preview.model_name,
            model_sql_path=preview.yaml_path,
            description=preview.description,
            columns=preview.columns,
        )

    def format_preview(self, preview: DocPreview) -> str:
        lines = [
            "═" * 60,
            f"  📝 Doc Preview: {preview.model_name}",
            f"     Will write to: {preview.yaml_path}",
            "═" * 60,
        ]
        if preview.profile:
            lines.append(f"  Table: {preview.profile.schema}.{preview.profile.table} ({preview.profile.total_rows:,} rows)")
        if preview.description:
            lines.append(f"  Description: {preview.description[:80]}...")
        if preview.columns:
            lines.append(f"\n  Suggested columns ({len(preview.columns)}):")
            for col in preview.columns[:10]:
                tests = ", ".join(
                    (t if isinstance(t, str) else next(iter(t))) for t in col.get("tests", [])
                )
                lines.append(f"    • {col['name']}: tests=[{tests}]")
        else:
            lines.append("\n  No column suggestions generated.")

        lines.append("\n  ✅ Call `update_model_yaml` to apply these changes to disk.")
        lines.append("═" * 60)
        return "\n".join(lines)
