"""Core: Read existing dbt schema.yml files using ruamel.yaml."""

import logging
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML

logger = logging.getLogger(__name__)


class SchemaReader:
    """Reads dbt schema.yml files in a human-comment-preserving manner."""

    def __init__(self) -> None:
        self._yaml = YAML()
        self._yaml.preserve_quotes = True

    def find_schema_file(self, model_sql_path: Path, project_dir: Path) -> Path | None:
        """Locate the schema YAML for a given model SQL path."""
        abs_model_path = project_dir / model_sql_path
        model_dir = abs_model_path.parent
        model_stem = abs_model_path.stem

        candidates = [
            model_dir / f"_{model_stem}.yml",
            model_dir / f"{model_stem}.yml",
            model_dir / "schema.yml",
            model_dir / "_schema.yml",
        ]
        for candidate in candidates:
            if candidate.exists():
                # For a generic schema.yml, verify the model is referenced inside
                if candidate.name in ("schema.yml", "_schema.yml"):
                    if self._model_in_file(candidate, model_stem):
                        return candidate
                else:
                    return candidate
        return None

    def _model_in_file(self, yaml_path: Path, model_name: str) -> bool:
        try:
            data = self._load_file(yaml_path)
            if not data:
                return False
            for m in data.get("models", []):
                if isinstance(m, dict) and m.get("name") == model_name:
                    return True
        except Exception:
            pass
        return False

    def _load_file(self, path: Path) -> dict[str, Any] | None:
        try:
            with open(path, encoding="utf-8") as f:
                return self._yaml.load(f)
        except Exception as exc:
            logger.error("Failed to load YAML at %s: %s", path, exc)
            return None

    def get_documented_columns(self, model_name: str, yaml_path: Path) -> dict[str, Any]:
        """Return {col_name: {description, tests}} for columns already documented."""
        data = self._load_file(yaml_path)
        if not data:
            return {}
        for model in data.get("models", []):
            if isinstance(model, dict) and model.get("name") == model_name:
                result: dict[str, Any] = {}
                for col in model.get("columns", []):
                    if isinstance(col, dict) and col.get("name"):
                        result[col["name"]] = {
                            "description": col.get("description", ""),
                            "tests": col.get("tests", []),
                        }
                return result
        return {}

    def get_existing_tests(self, model_name: str, yaml_path: Path) -> list[Any]:
        """Return list of test names/dicts already defined on the model level."""
        data = self._load_file(yaml_path)
        if not data:
            return []
        for model in data.get("models", []):
            if isinstance(model, dict) and model.get("name") == model_name:
                return list(model.get("tests", []))
        return []
