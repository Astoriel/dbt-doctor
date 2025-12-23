"""Analyzers: Project auditor — documentation/test coverage, naming conventions, health score."""

import re
from dataclasses import dataclass, field
from typing import Any

# Naming convention patterns for dbt models (layer-based naming)
_LAYER_PREFIXES = {
    "staging": re.compile(r"^stg_"),
    "intermediate": re.compile(r"^int_"),
    "marts_fact": re.compile(r"^fct_"),
    "marts_dim": re.compile(r"^dim_"),
    "snapshot": re.compile(r"^scd_|_snapshot$"),
    "seed": re.compile(r".*"),  # Seeds can have any name
}
_VALID_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")  # snake_case lowercase


@dataclass
class ModelAuditResult:
    name: str
    path: str
    has_description: bool
    description_len: int
    total_columns: int
    documented_columns: int
    tested_columns: int
    naming_ok: bool
    naming_issue: str = ""

    @property
    def doc_coverage(self) -> float:
        if self.total_columns == 0:
            return 1.0 if self.has_description else 0.0
        return self.documented_columns / self.total_columns

    @property
    def test_coverage(self) -> float:
        if self.total_columns == 0:
            return 0.0
        return self.tested_columns / self.total_columns


@dataclass
class AuditReport:
    total_models: int = 0
    models_with_description: int = 0
    models_with_any_test: int = 0
    total_columns: int = 0
    documented_columns: int = 0
    tested_columns: int = 0
    naming_violations: list[dict[str, str]] = field(default_factory=list)
    model_results: list[ModelAuditResult] = field(default_factory=list)

    @property
    def model_doc_score(self) -> float:
        if self.total_models == 0:
            return 100.0
        return round(self.models_with_description / self.total_models * 100, 1)

    @property
    def column_doc_score(self) -> float:
        if self.total_columns == 0:
            return 100.0
        return round(self.documented_columns / self.total_columns * 100, 1)

    @property
    def test_score(self) -> float:
        if self.total_models == 0:
            return 100.0
        return round(self.models_with_any_test / self.total_models * 100, 1)

    @property
    def naming_score(self) -> float:
        if self.total_models == 0:
            return 100.0
        violations = len(self.naming_violations)
        return round(max(0.0, (self.total_models - violations) / self.total_models * 100), 1)

    @property
    def overall_score(self) -> float:
        """Weighted composite score (0-100)."""
        return round(
            self.model_doc_score * 0.25
            + self.column_doc_score * 0.30
            + self.test_score * 0.35
            + self.naming_score * 0.10,
            1,
        )

    @property
    def worst_models(self) -> list[ModelAuditResult]:
        """Top 10 models with lowest combined doc+test coverage."""
        return sorted(
            self.model_results,
            key=lambda m: m.doc_coverage + m.test_coverage,
        )[:10]


def _check_naming(model_name: str) -> tuple[bool, str]:
    """Return (ok, issue_description) for naming convention check."""
    if not _VALID_NAME_RE.match(model_name):
        return False, f"'{model_name}' is not snake_case lowercase"
    return True, ""


def _count_tested_columns(columns: dict[str, Any]) -> int:
    """Count columns that have at least one test defined."""
    count = 0
    for col_info in columns.values():
        tests = col_info.get("tests", [])
        if tests:
            count += 1
    return count


class ProjectAuditor:
    """
    Audits a dbt project for documentation coverage, test coverage,
    naming conventions, and produces an overall health score (0-100).
    """

    def audit(self, models: list[dict[str, Any]]) -> AuditReport:
        """Run a full audit on the list of model metadata dicts from ManifestParser."""
        report = AuditReport()
        report.total_models = len(models)

        for model in models:
            name = model.get("name", "")
            path = model.get("original_file_path", "")
            description = model.get("description", "")
            columns: dict[str, Any] = model.get("columns", {})

            has_desc = bool(description.strip())
            if has_desc:
                report.models_with_description += 1

            col_count = len(columns)
            doc_col_count = sum(
                1 for c in columns.values() if c.get("description", "").strip()
            )
            tested_col_count = _count_tested_columns(columns)

            # A model "has any test" if at least one column has a test
            if tested_col_count > 0:
                report.models_with_any_test += 1

            report.total_columns += col_count
            report.documented_columns += doc_col_count
            report.tested_columns += tested_col_count

            naming_ok, naming_issue = _check_naming(name)
            if not naming_ok:
                report.naming_violations.append(
                    {"model": name, "issue": naming_issue}
                )

            result = ModelAuditResult(
                name=name,
                path=path,
                has_description=has_desc,
                description_len=len(description),
                total_columns=col_count,
                documented_columns=doc_col_count,
                tested_columns=tested_col_count,
                naming_ok=naming_ok,
                naming_issue=naming_issue,
            )
            report.model_results.append(result)

        return report

    def format_report(self, report: AuditReport) -> str:
        """Render a human + LLM-friendly string from an AuditReport."""
        lines = [
            "═" * 55,
            "  🩺 dbt-doctor — Project Health Report",
            "═" * 55,
            f"  Overall Score:          {report.overall_score:>5.1f}%",
            "─" * 55,
            f"  Model Description Cov:  {report.model_doc_score:>5.1f}%  "
            f"({report.models_with_description}/{report.total_models} models)",
            f"  Column Description Cov: {report.column_doc_score:>5.1f}%  "
            f"({report.documented_columns}/{report.total_columns} columns)",
            f"  Test Coverage:          {report.test_score:>5.1f}%  "
            f"({report.models_with_any_test}/{report.total_models} models with ≥1 test)",
            f"  Naming Conventions:     {report.naming_score:>5.1f}%  "
            f"({len(report.naming_violations)} violations)",
            "",
        ]

        if report.naming_violations:
            lines.append("  ⚠️  Naming Violations:")
            for v in report.naming_violations[:5]:
                lines.append(f"    • {v['model']}: {v['issue']}")
            if len(report.naming_violations) > 5:
                lines.append(f"    ... and {len(report.naming_violations) - 5} more")
            lines.append("")

        if report.worst_models:
            lines.append("  📉 Models Needing Attention (lowest coverage):")
            for m in report.worst_models[:5]:
                doc_pct = round(m.doc_coverage * 100)
                test_pct = round(m.test_coverage * 100)
                lines.append(
                    f"    • {m.name:<35}  docs:{doc_pct:>3}%  tests:{test_pct:>3}%"
                )
            lines.append("")

        lines.append("  💡 Recommendations:")
        if report.model_doc_score < 80:
            lines.append("    → Run `get_model_details` + `update_model_yaml` on undocumented models")
        if report.test_score < 60:
            lines.append("    → Run `suggest_tests` on worst-covered models")
        if report.column_doc_score < 50:
            lines.append("    → Run `generate_model_docs` for E2E auto-documentation")
        if report.overall_score >= 90:
            lines.append("    🎉 Excellent project quality! Keep it up.")
        lines.append("═" * 55)
        return "\n".join(lines)
