"""Connectors: DuckDB connector (zero-config, great for testing & local dbt projects)."""

import contextlib
import logging
from typing import Any

from ..utils.sql_sanitizer import validate_identifier
from .base import BaseConnector, ColumnInfo

logger = logging.getLogger(__name__)

_READ_ONLY_PREFIXES = ("select", "with", "explain", "describe", "show", "pragma")
_WRITE_KEYWORDS = frozenset(
    ["insert", "update", "delete", "drop", "alter", "truncate", "create", "replace"]
)


def _assert_read_only(sql: str) -> None:
    stripped = sql.strip().lower()
    first_word = stripped.split()[0] if stripped.split() else ""
    if first_word not in _READ_ONLY_PREFIXES:
        raise ValueError(
            f"Only SELECT (read-only) queries are allowed. Got: '{first_word.upper()}'"
        )


class DuckDBConnector(BaseConnector):
    """
    DuckDB connector.

    Can operate in-memory (path=':memory:') for testing, or against a real .duckdb file.
    Also useful for querying Parquet files or dbt DuckDB adapter projects.
    """

    def __init__(self, database: str = ":memory:") -> None:
        try:
            import duckdb  # type: ignore[import]
            self._conn = duckdb.connect(database)
            logger.info("DuckDB connected to '%s'", database)
        except ImportError as exc:
            raise ImportError(
                "duckdb is required. Install with: pip install duckdb"
            ) from exc

    def execute_query(self, sql: str, params: tuple | None = None) -> list[dict[str, Any]]:
        _assert_read_only(sql)
        try:
            result = self._conn.execute(sql, list(params)) if params else self._conn.execute(sql)
            columns = [desc[0] for desc in result.description or []]
            return [dict(zip(columns, row, strict=False)) for row in result.fetchmany(200)]
        except Exception as exc:
            raise RuntimeError(f"DuckDB query failed: {exc}") from exc

    def execute_write(self, sql: str) -> None:
        """Internal-only write access used for setting up test fixtures."""
        self._conn.execute(sql)

    def get_table_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        validate_identifier(schema)
        validate_identifier(table)
        rows = self.execute_query(
            f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table}'
            ORDER BY ordinal_position
            """
        )
        return [
            ColumnInfo(
                name=r["column_name"],
                data_type=r["data_type"],
                is_nullable=str(r.get("is_nullable", "YES")).upper() == "YES",
            )
            for r in rows
        ]

    def check_table_exists(self, schema: str, table: str) -> bool:
        validate_identifier(schema)
        validate_identifier(table)
        rows = self.execute_query(
            f"""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = '{table}' LIMIT 1
            """
        )
        return len(rows) > 0

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._conn.close()
