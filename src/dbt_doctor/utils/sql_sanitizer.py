"""Utils: SQL identifier sanitization to prevent SQL injection."""

import re

# Valid SQL identifiers: start with letter/underscore, contain alphanumeric/underscore, max 128 chars
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,127}$")


def validate_identifier(name: str) -> str:
    """
    Validate that ``name`` is a safe SQL identifier (table, schema, column name).

    Parameters
    ----------
    name:
        The identifier to validate.

    Returns
    -------
    str
        The validated identifier (unchanged).

    Raises
    ------
    ValueError
        If the identifier contains unsafe characters.
    """
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Unsafe SQL identifier: '{name}'. "
            "Only alphanumeric characters and underscores are allowed, "
            "and the name must start with a letter or underscore."
        )
    return name


def is_safe_identifier(name: str) -> bool:
    """Return True if ``name`` is a safe SQL identifier, False otherwise."""
    return bool(_IDENTIFIER_RE.match(name))
