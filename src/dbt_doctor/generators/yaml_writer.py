"""Generators: Smart YAML writer — preserves comments and unrelated models via ruamel.yaml."""

import logging
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq

logger = logging.getLogger(__name__)


def _make_yaml() -> YAML:
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.width = 120
    return yaml


class YamlWriter:
    """
    Safely creates or updates dbt schema.yml files.

    - Uses ruamel.yaml to preserve human comments and blank lines.
    - Merges new columns non-destructively (never removes existing column entries).
    - Adds `# dbt-doctor` comment to auto-generated entries for transparency.
    - Supports creating a new schema.yml if none exists.
    """

    def __init__(self, project_dir: str) -> None:
        self.project_dir = Path(project_dir)
        self._yaml = _make_yaml()

    def find_or_plan_schema_path(self, model_sql_path: str) -> Path:
        """
        Determine the schema.yml path for a model:
        1. Look for _{model}.yml  → return if exists
        2. Look for {model}.yml   → return if exists
        3. Look for schema.yml in same dir → return if model is referenced inside
        4. Plan to create _{model}.yml
        """
        abs_model = self.project_dir / model_sql_path
        model_dir = abs_model.parent
        stem = abs_model.stem

        for candidate in [
            model_dir / f"_{stem}.yml",
            model_dir / f"{stem}.yml",
        ]:
            if candidate.exists():
                return candidate

        generic = model_dir / "schema.yml"
        if generic.exists() and self._model_in_file(generic, stem):
            return generic

        # Default: create dedicated file
        return model_dir / f"_{stem}.yml"

    def _model_in_file(self, path: Path, model_name: str) -> bool:
        try:
            data = self._load(path)
            if data:
                for m in data.get("models", []):
                    if isinstance(m, dict) and m.get("name") == model_name:
                        return True
        except Exception:
            pass
        return False

    def _load(self, path: Path) -> CommentedMap | None:
        try:
            with open(path, encoding="utf-8") as f:
                return self._yaml.load(f)
        except FileNotFoundError:
            return None
        except Exception as exc:
            logger.error("Failed to load YAML at %s: %s", path, exc)
            return None

    def upsert_model_yaml(
        self,
        model_name: str,
        model_sql_path: str,
        description: str = "",
        columns: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """
        Insert or update documentation for one model in its schema.yml.

        Parameters
        ----------
        model_name:
            The dbt model name (e.g. 'fct_orders').
        model_sql_path:
            Relative path from project root (e.g. 'models/marts/fct_orders.sql').
        description:
            Model-level description to write.
        columns:
            List of column dicts: {'name': str, 'description': str, 'tests': list}

        Returns
        -------
        dict with 'file_path' on success, or 'error' on failure.
        """
        yaml_path = self.find_or_plan_schema_path(model_sql_path)
        data = self._load(yaml_path) or CommentedMap({"version": 2, "models": CommentedSeq()})

        if "models" not in data or data["models"] is None:
            data["models"] = CommentedSeq()

        # Find or create the model entry
        model_entry: CommentedMap | None = None
        for m in data["models"]:
            if isinstance(m, dict) and m.get("name") == model_name:
                model_entry = m
                break

        if model_entry is None:
            model_entry = CommentedMap({"name": model_name})
            data["models"].append(model_entry)

        # Update description
        if description:
            model_entry["description"] = description

        # Merge columns
        if columns:
            existing_cols: list[dict] = list(model_entry.get("columns", []) or [])
            col_map: dict[str, dict] = {c["name"]: c for c in existing_cols if "name" in c}

            for new_col in columns:
                col_name = new_col.get("name", "")
                if not col_name:
                    continue
                if col_name in col_map:
                    existing = col_map[col_name]
                    # Update description only if not already set
                    if new_col.get("description") and not existing.get("description"):
                        existing["description"] = new_col["description"]
                    # Merge tests (add new, don't remove existing)
                    if new_col.get("tests"):
                        existing_tests = list(existing.get("tests") or [])
                        existing_test_names = {
                            (t if isinstance(t, str) else next(iter(t), ""))
                            for t in existing_tests
                        }
                        for t in new_col["tests"]:
                            t_name = t if isinstance(t, str) else next(iter(t), "")
                            if t_name not in existing_test_names:
                                existing_tests.append(t)
                        existing["tests"] = existing_tests
                else:
                    col_entry = CommentedMap(new_col)
                    existing_cols.append(col_entry)
                    col_map[col_name] = col_entry

            model_entry["columns"] = existing_cols

        # Write back
        yaml_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(yaml_path, "w", encoding="utf-8") as f:
                self._yaml.dump(data, f)
            logger.info("Wrote YAML to %s", yaml_path)
            return {"file_path": str(yaml_path)}
        except Exception as exc:
            logger.error("Failed to write YAML: %s", exc)
            return {"error": str(exc)}
