"""Core: dbt manifest.json parser with mtime-based caching."""

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class ManifestCache:
    """Simple file-based cache that invalidates when file mtime changes."""

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}
        self._mtime: float = 0.0
        self._path: Path | None = None

    def is_valid(self, path: Path) -> bool:
        if self._path != path:
            return False
        try:
            return path.stat().st_mtime == self._mtime
        except OSError:
            return False

    def load(self, path: Path) -> dict[str, Any]:
        self._path = path
        self._mtime = path.stat().st_mtime
        with open(path, encoding="utf-8") as f:
            self._data = json.load(f)
        return self._data

    @property
    def data(self) -> dict[str, Any]:
        return self._data


class DbtManifestParser:
    """Parses dbt target/manifest.json with automatic mtime-based caching."""

    def __init__(self, project_dir: str) -> None:
        self.project_dir = Path(project_dir)
        self.target_dir = self.project_dir / "target"
        self.manifest_path = self.target_dir / "manifest.json"
        self._cache = ManifestCache()
        self._test_index: dict[str, dict[str, list[str]]] = {}

    def load(self) -> bool:
        """Load manifest if not cached or file changed. Returns True on success."""
        if not self.manifest_path.exists():
            logger.warning(
                "manifest.json not found at %s. Run `dbt compile` first.", self.manifest_path
            )
            return False
        if self._cache.is_valid(self.manifest_path):
            logger.debug("Manifest cache is valid, skipping reload.")
            return True
        try:
            self._cache.load(self.manifest_path)
            self._test_index = self._build_test_index()
            logger.info("Manifest loaded from %s", self.manifest_path)
            return True
        except Exception as exc:
            logger.error("Error loading manifest.json: %s", exc)
            return False

    @property
    def _data(self) -> dict[str, Any]:
        return self._cache.data

    def _build_test_index(self) -> dict[str, dict[str, list[str]]]:
        """Map each model's unique_id to its tests.

        dbt stores tests as separate top-level nodes rather than nesting them under
        the model's own `columns`, so this has to be assembled from `depends_on`
        rather than read off the model node directly. Column-level (generic) tests
        are keyed by `column_name`; model-level (singular/custom) tests, which have
        no `column_name`, are keyed under `""`.
        """
        index: dict[str, dict[str, list[str]]] = {}
        for node in self._data.get("nodes", {}).values():
            if node.get("resource_type") != "test":
                continue
            parent_ids = node.get("depends_on", {}).get("nodes", [])
            if not parent_ids:
                continue
            test_metadata = node.get("test_metadata")
            test_name = test_metadata["name"] if test_metadata else node.get("name", "")
            column = node.get("column_name") or ""
            index.setdefault(parent_ids[0], {}).setdefault(column, []).append(test_name)
        return index

    # ------------------------------------------------------------------
    # Models
    # ------------------------------------------------------------------

    def get_models(self) -> list[dict[str, Any]]:
        """Return simplified list of all dbt models."""
        models: list[dict[str, Any]] = []
        for node in self._data.get("nodes", {}).values():
            if node.get("resource_type") == "model":
                models.append(
                    {
                        "name": node.get("name"),
                        "alias": node.get("alias", node.get("name")),
                        "description": node.get("description", ""),
                        "original_file_path": node.get("original_file_path"),
                        "database": node.get("database"),
                        "schema": node.get("schema"),
                        "materialized": node.get("config", {}).get("materialized", "view"),
                        "has_description": bool(node.get("description", "").strip()),
                        "column_count": len(node.get("columns", {})),
                        "documented_column_count": sum(
                            1
                            for c in node.get("columns", {}).values()
                            if c.get("description", "").strip()
                        ),
                        "depends_on": node.get("depends_on", {}).get("nodes", []),
                    }
                )
        return models

    def get_model_details(self, model_name: str) -> dict[str, Any] | None:
        """Return detailed info for a specific model by name."""
        for uid, node in self._data.get("nodes", {}).items():
            if node.get("resource_type") == "model" and node.get("name") == model_name:
                model_tests = self._test_index.get(uid, {})
                columns = {
                    col_name: {**col, "tests": model_tests.get(col_name, [])}
                    for col_name, col in node.get("columns", {}).items()
                }
                return {
                    "name": node.get("name"),
                    "description": node.get("description", ""),
                    "path": node.get("original_file_path"),
                    "database": node.get("database"),
                    "schema": node.get("schema"),
                    "alias": node.get("alias", node.get("name")),
                    "materialized": node.get("config", {}).get("materialized", "view"),
                    "raw_sql": node.get("raw_code", node.get("raw_sql", "")),
                    "compiled_sql": node.get("compiled_code", node.get("compiled_sql", "")),
                    "columns": columns,
                    "model_tests": model_tests.get("", []),
                    "depends_on": node.get("depends_on", {}).get("nodes", []),
                    "tags": node.get("tags", []),
                    "config": node.get("config", {}),
                }
        return None

    def get_upstream_lineage(self, model_name: str) -> list[str]:
        """Return upstream dependency node IDs for a model."""
        details = self.get_model_details(model_name)
        return details.get("depends_on", []) if details else []

    def get_all_sources(self) -> list[dict[str, Any]]:
        """Return all dbt sources from manifest."""
        sources: list[dict[str, Any]] = []
        for node in self._data.get("sources", {}).values():
            sources.append(
                {
                    "name": node.get("name"),
                    "source_name": node.get("source_name"),
                    "identifier": node.get("identifier", node.get("name")),
                    "database": node.get("database"),
                    "schema": node.get("schema"),
                    "description": node.get("description", ""),
                    "loaded_at_field": node.get("loaded_at_field"),
                }
            )
        return sources

    def get_all_exposures(self) -> list[dict[str, Any]]:
        """Return all dbt exposures from manifest."""
        exposures: list[dict[str, Any]] = []
        for node in self._data.get("exposures", {}).values():
            exposures.append(
                {
                    "name": node.get("name"),
                    "type": node.get("type"),
                    "description": node.get("description", ""),
                    "depends_on": node.get("depends_on", {}).get("nodes", []),
                }
            )
        return exposures

    def get_model_schema_path(self, model_name: str) -> str | None:
        """Get the original_file_path for a model to locate its schema.yml."""
        details = self.get_model_details(model_name)
        return details.get("path") if details else None

    def get_node_id_for_model(self, model_name: str) -> str | None:
        """Return the full node ID (e.g. model.project.name) for a model."""
        for node_id, node in self._data.get("nodes", {}).items():
            if node.get("resource_type") == "model" and node.get("name") == model_name:
                return node_id
        return None
