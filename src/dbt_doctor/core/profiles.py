"""Core: ~/.dbt/profiles.yml parser with env_var support."""

import logging
import os
import re
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML as _YAML

logger = logging.getLogger(__name__)

# Regex for {{ env_var('VAR_NAME') }} or {{ env_var("VAR_NAME") }}
_ENV_VAR_RE = re.compile(r"\{\{\s*env_var\(['\"]([^'\"]+)['\"]\)\s*\}\}")


def _replace_env_vars(value: Any) -> Any:
    """Recursively replace dbt env_var() references with actual environment values."""
    if isinstance(value, str):
        def replacer(match: re.Match) -> str:
            var_name = match.group(1)
            resolved = os.environ.get(var_name, "")
            if not resolved:
                logger.warning("Environment variable '%s' not set (empty string used).", var_name)
            return resolved

        return _ENV_VAR_RE.sub(replacer, value)
    if isinstance(value, dict):
        return {k: _replace_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_replace_env_vars(v) for v in value]
    return value


class DbtProfileParser:
    """Reads ~/.dbt/profiles.yml and extracts DB credentials for a given profile."""

    _SEARCH_PATHS = [
        Path.home() / ".dbt" / "profiles.yml",
        Path.cwd() / "profiles.yml",
    ]

    def __init__(self, profile_name: str, target_name: str | None = None) -> None:
        self.profile_name = profile_name
        self.target_name = target_name

    def get_credentials(self) -> dict[str, Any] | None:
        """Return resolved credentials dict for the configured profile and target."""
        profile_path = self._find_profiles_file()
        if not profile_path:
            return None

        try:
            _yml = _YAML()
            _yml.preserve_quotes = True
            with open(profile_path, encoding="utf-8") as f:
                raw_profiles: dict[str, Any] = _yml.load(f) or {}
        except Exception as exc:
            logger.error("Failed to parse profiles.yml at %s: %s", profile_path, exc)
            return None

        profile = raw_profiles.get(self.profile_name)
        if not profile:
            logger.error(
                "Profile '%s' not found in %s. Available profiles: %s",
                self.profile_name,
                profile_path,
                list(raw_profiles.keys()),
            )
            return None

        target = self.target_name or profile.get("target")
        if not target:
            logger.error("No target specified for profile '%s'.", self.profile_name)
            return None

        outputs: dict[str, Any] = profile.get("outputs", {})
        raw_creds = outputs.get(target)
        if not raw_creds:
            logger.error(
                "Target '%s' not found in outputs for profile '%s'.", target, self.profile_name
            )
            return None

        return _replace_env_vars(raw_creds)

    def _find_profiles_file(self) -> Path | None:
        for path in self._SEARCH_PATHS:
            if path.exists():
                logger.debug("Found profiles.yml at %s", path)
                return path
        logger.error("profiles.yml not found. Searched: %s", self._SEARCH_PATHS)
        return None
