"""Tests: ProjectAuditor coverage scores and naming violations."""

import pytest

from dbt_doctor.analyzers.auditor import ProjectAuditor


def _make_model(name, description="", columns=None):
    cols = columns or {}
    return {
        "name": name,
        "original_file_path": f"models/{name}.sql",
        "description": description,
        "columns": cols,
    }


def test_perfect_coverage():
    auditor = ProjectAuditor()
    models = [
        _make_model(
            "stg_orders",
            description="Staged orders",
            columns={
                "order_id": {"description": "PK", "tests": ["not_null", "unique"]},
                "status": {"description": "Order status", "tests": ["accepted_values"]},
            },
        )
    ]
    report = auditor.audit(models)
    assert report.model_doc_score == 100.0
    assert report.column_doc_score == 100.0
    assert report.test_score == 100.0
    assert report.overall_score > 90.0


def test_zero_coverage():
    auditor = ProjectAuditor()
    models = [
        _make_model("fct_orders", description="", columns={"order_id": {"description": "", "tests": []}}),
        _make_model("stg_users", description="", columns={"user_id": {"description": "", "tests": []}}),
    ]
    report = auditor.audit(models)
    assert report.model_doc_score == 0.0
    assert report.column_doc_score == 0.0
    assert report.test_score == 0.0
    assert report.overall_score < 20.0


def test_naming_violations():
    auditor = ProjectAuditor()
    models = [
        _make_model("BadModelName"),   # CamelCase — violation
        _make_model("stg_orders"),     # OK
        _make_model("My Model"),       # spaces — violation
    ]
    report = auditor.audit(models)
    assert len(report.naming_violations) == 2
    violation_names = {v["model"] for v in report.naming_violations}
    assert "BadModelName" in violation_names
    assert "My Model" in violation_names
    assert "stg_orders" not in violation_names


def test_partial_column_coverage():
    auditor = ProjectAuditor()
    models = [
        _make_model(
            "stg_orders",
            description="Has description",
            columns={
                "order_id": {"description": "PK", "tests": ["not_null"]},
                "status": {"description": "", "tests": []},
                "amount": {"description": "", "tests": []},
            },
        )
    ]
    report = auditor.audit(models)
    assert report.column_doc_score == pytest.approx(33.3, abs=1.0)
    assert report.test_score == 100.0  # model has ≥1 test


def test_worst_models_ordering():
    auditor = ProjectAuditor()
    models = [
        _make_model("model_a", description="Documented", columns={
            "id": {"description": "pk", "tests": ["not_null"]}
        }),
        _make_model("model_b", description="", columns={
            "id": {"description": "", "tests": []}
        }),
    ]
    report = auditor.audit(models)
    worst = report.worst_models
    assert worst[0].name == "model_b"  # least coverage first


def test_format_report_contains_score():
    auditor = ProjectAuditor()
    models = [_make_model("stg_orders", description="OK")]
    report = auditor.audit(models)
    formatted = auditor.format_report(report)
    assert "Overall Score" in formatted
    assert "%" in formatted

