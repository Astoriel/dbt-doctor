"""Tests: DbtManifestParser with sample manifest fixture."""



from dbt_doctor.core.manifest import DbtManifestParser


def test_load_manifest(project_dir_with_manifest):
    parser = DbtManifestParser(str(project_dir_with_manifest))
    assert parser.load() is True


def test_load_missing_manifest(tmp_path):
    parser = DbtManifestParser(str(tmp_path))
    assert parser.load() is False


def test_get_models_count(project_dir_with_manifest):
    parser = DbtManifestParser(str(project_dir_with_manifest))
    parser.load()
    models = parser.get_models()
    assert len(models) == 6


def test_get_models_fields(project_dir_with_manifest):
    parser = DbtManifestParser(str(project_dir_with_manifest))
    parser.load()
    models = parser.get_models()
    model = next(m for m in models if m["name"] == "stg_orders")
    assert model["schema"] == "dbt_staging"
    assert model["materialized"] == "view"
    assert model["has_description"] is True
    assert model["column_count"] == 5


def test_get_model_details(project_dir_with_manifest):
    parser = DbtManifestParser(str(project_dir_with_manifest))
    parser.load()
    details = parser.get_model_details("fct_orders")
    assert details is not None
    assert details["name"] == "fct_orders"
    assert details["schema"] == "dbt_marts"
    assert "stg_orders" in " ".join(details["depends_on"])


def test_get_model_details_not_found(project_dir_with_manifest):
    parser = DbtManifestParser(str(project_dir_with_manifest))
    parser.load()
    assert parser.get_model_details("does_not_exist") is None


def test_get_upstream_lineage(project_dir_with_manifest):
    parser = DbtManifestParser(str(project_dir_with_manifest))
    parser.load()
    lineage = parser.get_upstream_lineage("fct_orders")
    assert len(lineage) == 2
    assert any("stg_orders" in dep for dep in lineage)


def test_get_all_sources(project_dir_with_manifest):
    parser = DbtManifestParser(str(project_dir_with_manifest))
    parser.load()
    sources = parser.get_all_sources()
    assert len(sources) == 2
    names = {s["name"] for s in sources}
    assert "orders" in names


def test_manifest_cache(project_dir_with_manifest):
    """Manifest should not be re-loaded if file hasn't changed."""
    parser = DbtManifestParser(str(project_dir_with_manifest))
    parser.load()
    mtime_before = parser._cache._mtime

    # Second load should use cache
    parser.load()
    assert parser._cache._mtime == mtime_before
