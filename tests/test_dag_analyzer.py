"""Tests: DagAnalyzer — orphan detection, depth calculation, fan-out."""

from dbt_doctor.analyzers.dag_analyzer import DagAnalyzer


def _make_models(specs):
    return [{"name": n, "depends_on": deps} for n, deps in specs.items()]


def test_orphan_detection():
    """orphan_model has no downstream consumers."""
    models = _make_models(
        {
            "stg_orders": [],
            "fct_orders": ["model.project.stg_orders"],
            "orphan_model": [],  # no one depends on it
        }
    )
    dag = DagAnalyzer()
    report = dag.analyze(models)
    # stg_orders is consumed by fct_orders → not orphan
    # fct_orders has no consumers → orphan
    # orphan_model has no consumers → orphan
    assert "orphan_model" in report.orphan_models
    assert "fct_orders" in report.orphan_models
    assert "stg_orders" not in report.orphan_models


def test_root_models():
    """stg_orders has only source deps, fct_orders has model deps."""
    models = _make_models(
        {
            "stg_orders": ["source.project.raw_orders"],
            "fct_orders": ["model.project.stg_orders"],
        }
    )
    dag = DagAnalyzer()
    report = dag.analyze(models)
    assert "stg_orders" in report.root_models
    assert "fct_orders" not in report.root_models


def test_max_chain_depth():
    """Chain: a → b → c → d should have depth 4."""
    models = _make_models(
        {
            "a": [],
            "b": ["model.project.a"],
            "c": ["model.project.b"],
            "d": ["model.project.c"],
        }
    )
    dag = DagAnalyzer()
    report = dag.analyze(models)
    assert report.max_chain_depth == 4


def test_high_fanout_detection():
    """base feeds 6 downstream models — should be flagged as high fan-out."""
    models = _make_models(
        {
            "base": [],
            "m1": ["model.project.base"],
            "m2": ["model.project.base"],
            "m3": ["model.project.base"],
            "m4": ["model.project.base"],
            "m5": ["model.project.base"],
            "m6": ["model.project.base"],
        }
    )
    dag = DagAnalyzer()
    report = dag.analyze(models)
    fanout_names = {hf["model"] for hf in report.high_fanout_models}
    assert "base" in fanout_names


def test_format_report_has_key_sections():
    models = _make_models(
        {
            "stg_orders": [],
            "fct_orders": ["model.project.stg_orders"],
        }
    )
    dag = DagAnalyzer()
    report = dag.analyze(models)
    formatted = dag.format_report(report)
    assert "Max Chain Depth" in formatted
    assert "Orphan Models" in formatted


def test_empty_project():
    dag = DagAnalyzer()
    report = dag.analyze([])
    assert report.total_models == 0
    assert report.max_chain_depth == 0
    assert report.orphan_models == []
