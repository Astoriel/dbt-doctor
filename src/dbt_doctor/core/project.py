"""Core: dbt_project.yml parser."""

import logging
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML as _YAML

logger = logging.getLogger(__name__)


class DbtProjectParser:
    """Parses the dbt_project.yml file to extract project-level configuration."""

    def __init__(self, project_dir: str) -> None:
        self.project_dir = Path(project_dir)
        self._data: dict[str, Any] = {}
        self._loaded = False

    def load(self) -> bool:
        project_yml = self.project_dir / "dbt_project.yml"
        if not project_yml.exists():
            logger.warning("dbt_project.yml not found at %s", project_yml)
            return False
        try:
            _yml = _YAML()
            with open(project_yml, encoding="utf-8") as f:
                self._data = _yml.load(f) or {}
            self._loaded = True
            return True
        except Exception as exc:
            logger.error("Failed to parse dbt_project.yml: %s", exc)
            return False

    @property
    def profile_name(self) -> str | None:
        return self._data.get("profile")

    @property
    def project_name(self) -> str | None:
        return self._data.get("name")

    @property
    def project_version(self) -> str | None:
        return str(self._data.get("version", ""))

    @property
    def model_paths(self) -> list[str]:
        return self._data.get("model-paths", self._data.get("source-paths", ["models"]))

    @property
    def vars(self) -> dict[str, Any]:
        return self._data.get("vars", {})
