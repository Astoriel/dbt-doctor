"""Tests for the MCP tools evaluate_model_descriptions / submit_description_scores."""

import json

import pytest

from dbt_doctor import server
from dbt_doctor.connectors.base import ColumnInfo
from dbt_doctor.core.manifest import DbtManifestParser


@pytest.fixture
def tools(project_dir_with_manifest, monkeypatch):
    monkeypatch.setattr(server, "_manifest", DbtManifestParser(str(project_dir_with_manifest)))
    monkeypatch.setattr(server, "_rubric_path", None)
    monkeypatch.setattr(server, "_connector", None)
    return project_dir_with_manifest


def all_columns(batch):
    return [c["name"] for c in batch["columns"]]


def test_full_flow_on_fixture_model(tools):
    batch = json.loads(server.evaluate_model_descriptions("stg_orders"))
    assert batch["model"] == "stg_orders"
    assert batch["next_offset"] is None
    assert "order_id" in all_columns(batch)
    assert batch["sql"]["kind"] == "compiled"
    assert "order_id" in batch["unique_columns"]
    by_name = {c["name"]: c for c in batch["columns"]}
    assert by_name["status"]["signals"]["empty_description"] is True

    ids = [q["id"] for q in batch["rubric"]["questions"]]
    empty = {n for n, c in by_name.items() if c["description"] == ""}
    # Empty columns are flagged by the batch itself and need no scores.
    assert {n for n, c in by_name.items() if c.get("status") == "missing"} == empty
    totals = {"order_id": 9, "customer_id": 2}  # one passes, one fails the 35/50 threshold
    scores = [
        {"column": name, "scores": dict.fromkeys(ids, totals[name])}
        for name in all_columns(batch)
        if name not in empty
    ]
    report = json.loads(server.submit_description_scores("stg_orders", scores))
    assert report["status"] == "complete"
    status = {c["column"]: c["status"] for c in report["columns"]}
    assert status["order_id"] == "ok" and status["customer_id"] == "rewrite"
    assert {n for n, s in status.items() if s == "missing"} == empty
    assert set(report["to_update"]) == empty | {"customer_id"}
    # The state file is cleaned up at the end.
    assert not list((tools / "target" / "dbt_doctor").glob("description_eval_*.json"))


def test_unknown_model(tools):
    assert "not found" in server.evaluate_model_descriptions("ghost")
    assert "not found" in server.submit_description_scores("ghost", [])


def test_invalid_scores_return_readable_error(tools):
    out = server.submit_description_scores("stg_orders", [{"column": "ghost", "scores": {}}])
    assert out.startswith("❌") and "unknown column 'ghost'" in out


def test_invalid_offset_returns_error(tools):
    assert server.evaluate_model_descriptions("stg_orders", offset=99).startswith("❌")


def test_custom_rubric_file_is_used_and_reloaded(tools, tmp_path, monkeypatch):
    rubric = tmp_path / "rubric.json"
    rubric.write_text(
        json.dumps(
            {
                "pass_ratio": 0.5,
                "max_score_per_question": 10,
                "questions": [{"id": "only_one", "text": "Is it good?"}],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(server, "_rubric_path", rubric)
    batch = json.loads(server.evaluate_model_descriptions("stg_orders"))
    assert [q["id"] for q in batch["rubric"]["questions"]] == ["only_one"]
    assert batch["rubric"]["pass_total"] == 5

    # Edits apply without restarting the server.
    rubric.write_text(rubric.read_text().replace("Is it good?", "Is it great?"), encoding="utf-8")
    batch = json.loads(server.evaluate_model_descriptions("stg_orders"))
    assert batch["rubric"]["questions"][0]["text"] == "Is it great?"


def test_broken_rubric_file_returns_error(tools, tmp_path, monkeypatch):
    bad = tmp_path / "bad.json"
    bad.write_text("{", encoding="utf-8")
    monkeypatch.setattr(server, "_rubric_path", bad)
    assert server.evaluate_model_descriptions("stg_orders").startswith("❌")


# ── database check ────────────────────────────────────────────────────────


class StubConnector:
    def __init__(self, columns=None, error=None):
        self.columns, self.error, self.calls = columns or [], error, []

    def get_table_columns(self, schema, table):
        self.calls.append((schema, table))
        if self.error:
            raise self.error
        return self.columns


STG_ORDERS = ["order_id", "customer_id", "status", "amount", "order_date"]


def test_tools_describe_the_table_and_report_undeclared_columns(tools, monkeypatch):
    stub = StubConnector([ColumnInfo(n, "varchar") for n in [*STG_ORDERS, "extra_col"]])
    monkeypatch.setattr(server, "_connector", stub)

    batch = json.loads(server.evaluate_model_descriptions("stg_orders"))
    assert stub.calls[0][0] == "dbt_staging"
    assert batch["database_check"]["undeclared_columns"] == ["extra_col"]
    assert batch["total_columns"] == 6

    ids = [q["id"] for q in batch["rubric"]["questions"]]
    scores = [{"column": n, "scores": dict.fromkeys(ids, 9)} for n in ("order_id", "customer_id")]
    report = json.loads(server.submit_description_scores("stg_orders", scores))
    by_col = {c["column"]: c for c in report["columns"]}
    assert by_col["extra_col"]["status"] == "missing" and by_col["extra_col"]["declared"] is False
    assert "extra_col" in report["to_update"]
    assert report["database_check"]["status"] == "ok"


@pytest.mark.parametrize(
    "connector, expected",
    [
        (None, "not connected"),
        (StubConnector(error=RuntimeError("boom")), "could not describe"),
        (StubConnector(columns=[]), "not found in the database"),
    ],
)
def test_database_check_failures_do_not_break_the_review(tools, monkeypatch, connector, expected):
    monkeypatch.setattr(server, "_connector", connector)
    batch = json.loads(server.evaluate_model_descriptions("stg_orders"))
    assert batch["database_check"]["status"] == "unavailable"
    assert expected in batch["database_check"]["detail"]
    assert batch["total_columns"] == 5  # falls back to the manifest columns
