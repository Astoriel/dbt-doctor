"""Connectors: PostgreSQL connector using psycopg (v3)."""

import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from ..utils.sql_sanitizer import validate_identifier
from .base import BaseConnector, ColumnInfo

logger = logging.getLogger(__name__)

_READ_ONLY_PREFIXES = ("select", "with", "explain")
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
    # Secondary check for embedded write keywords after a semicolon
    for part in stripped.split(";"):
        part = part.strip()
        if part and part.split()[0] in _WRITE_KEYWORDS:
            raise ValueError(f"Unsafe SQL detected: '{part[:40]}'")


class PostgresConnector(BaseConnector):
    """Connects to PostgreSQL using psycopg v3 with parameterized queries."""

    def __init__(self, credentials: dict[str, Any]) -> None:
        try:
            import psycopg  # type: ignore[import]

            self._psycopg = psycopg
        except ImportError as exc:
            raise ImportError(
                "psycopg is required for PostgreSQL support. "
                "Install with: pip install 'psycopg[binary]'"
            ) from exc

        self._conninfo = (
            f"host={credentials.get('host', 'localhost')} "
            f"port={credentials.get('port', 5432)} "
            f"user={credentials.get('user', '')} "
            f"password={credentials.get('password', '')} "
            f"dbname={credentials.get('dbname', credentials.get('database', ''))} "
        )
        self._default_schema: str = credentials.get("schema", "public")

    @contextmanager
    def _conn(self) -> Generator[Any, None, None]:
        conn = self._psycopg.connect(self._conninfo, row_factory=self._psycopg.rows.dict_row)
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(self, sql: str, params: tuple | None = None) -> list[dict[str, Any]]:
        _assert_read_only(sql)
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchmany(200)
            return [dict(r) for r in rows]

    def get_table_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        validate_identifier(schema)
        validate_identifier(table)
        rows = self.execute_query(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        return [
            ColumnInfo(
                name=r["column_name"],
                data_type=r["data_type"],
                is_nullable=r["is_nullable"].upper() == "YES",
            )
            for r in rows
        ]

    def check_table_exists(self, schema: str, table: str) -> bool:
        validate_identifier(schema)
        validate_identifier(table)
        rows = self.execute_query(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s LIMIT 1
            """,
            (schema, table),
        )
        return len(rows) > 0

    def close(self) -> None:
        pass  # Connections are closed per-context-manager; nothing to do here.
