"""Tests: YamlWriter — smart merge, comment preservation, new file creation."""

from pathlib import Path

from dbt_doctor.generators.yaml_writer import YamlWriter


def test_creates_new_yaml_file(tmp_path):
    # Create a fake .sql file to establish the path
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "fct_orders.sql").write_text("SELECT 1")

    writer = YamlWriter(str(tmp_path))
    result = writer.upsert_model_yaml(
        model_name="fct_orders",
        model_sql_path="models/fct_orders.sql",
        description="My model description",
        columns=[{"name": "order_id", "description": "PK", "tests": ["not_null", "unique"]}],
    )
    assert "file_path" in result
    assert "error" not in result
    written_path = Path(result["file_path"])
    assert written_path.exists()
    content = written_path.read_text()
    assert "fct_orders" in content
    assert "order_id" in content
    assert "not_null" in content


def test_updates_existing_yaml_preserves_other_models(tmp_path):
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "stg_users.sql").write_text("SELECT 1")

    # Write a schema.yml with an existing model
    schema_path = models_dir / "_stg_users.yml"
    schema_path.write_text("""version: 2
models:
  - name: another_model
    description: Should not be touched
  - name: stg_users
    description: ''
""")

    writer = YamlWriter(str(tmp_path))
    result = writer.upsert_model_yaml(
        model_name="stg_users",
        model_sql_path="models/stg_users.sql",
        description="Updated description",
        columns=[{"name": "user_id", "tests": ["not_null"]}],
    )
    assert "file_path" in result
    content = Path(result["file_path"]).read_text()
    # Both models should still be present
    assert "another_model" in content
    assert "Should not be touched" in content
    assert "stg_users" in content
    assert "Updated description" in content


def test_merge_does_not_remove_existing_tests(tmp_path):
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "stg_orders.sql").write_text("SELECT 1")

    schema_path = models_dir / "_stg_orders.yml"
    schema_path.write_text("""version: 2
models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
""")

    writer = YamlWriter(str(tmp_path))
    # Add a new test, should not remove existing
    result = writer.upsert_model_yaml(
        model_name="stg_orders",
        model_sql_path="models/stg_orders.sql",
        columns=[{"name": "order_id", "tests": ["relationships"]}],
    )
    content = Path(result["file_path"]).read_text()
    assert "not_null" in content
    assert "unique" in content
    assert "relationships" in content


def test_adds_new_column_to_existing(tmp_path):
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "stg_orders.sql").write_text("SELECT 1")

    schema_path = models_dir / "_stg_orders.yml"
    schema_path.write_text("""version: 2
models:
  - name: stg_orders
    columns:
      - name: order_id
        tests: [not_null]
""")

    writer = YamlWriter(str(tmp_path))
    result = writer.upsert_model_yaml(
        model_name="stg_orders",
        model_sql_path="models/stg_orders.sql",
        columns=[{"name": "status", "tests": ["accepted_values"]}],
    )
    content = Path(result["file_path"]).read_text()
    assert "order_id" in content   # old column preserved
    assert "status" in content     # new column added
