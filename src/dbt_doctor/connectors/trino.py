"""Connectors: Trino connector using the official ``trino`` Python client."""

import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from ..utils.sql_sanitizer import validate_identifier
from .base import BaseConnector, ColumnInfo

logger = logging.getLogger(__name__)

_READ_ONLY_PREFIXES = ("select", "with", "show", "describe", "explain")
_WRITE_KEYWORDS = frozenset(
    ["insert", "update", "delete", "drop", "alter", "truncate", "create", "replace", "merge"]
)

# dbt-trino auth methods mapped onto what this connector supports.
_PASSWORD_METHODS = frozenset(["ldap", "password"])


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


class TrinoConnector(BaseConnector):
    """Connects to Trino using the ``trino`` client with parameterized queries.

    Reads the same keys as a dbt-trino profile: ``host``, ``port``, ``user``,
    ``database`` (the Trino catalog), ``schema``, ``http_scheme`` and ``method``
    (``none``, ``ldap``/``password`` or ``jwt``).
    """

    def __init__(self, credentials: dict[str, Any]) -> None:
        try:
            import trino  # type: ignore[import]

            self._trino = trino
        except ImportError as exc:
            raise ImportError(
                "trino is required for Trino support. Install with: pip install 'dbt-doctor[trino]'"
            ) from exc

        catalog = credentials.get("database") or credentials.get("catalog")
        if not catalog:
            raise ValueError("Trino profile must define 'database' (the Trino catalog).")

        self._host: str = credentials.get("host", "localhost")
        # Like dbt-trino: https by default for any authenticated method, so credentials
        # are never sent over plain HTTP unless the profile explicitly asks for it.
        method = str(credentials.get("method", "none")).lower()
        default_scheme = "http" if method == "none" else "https"
        self._http_scheme: str = credentials.get("http_scheme", default_scheme)
        self._port: int = int(
            credentials.get("port", 443 if self._http_scheme == "https" else 8080)
        )
        self._user: str = credentials.get("user", "dbt-doctor")
        self._catalog: str = catalog
        self._default_schema: str | None = credentials.get("schema")
        self._auth = self._build_auth(credentials)

    def _build_auth(self, credentials: dict[str, Any]) -> Any:
        method = str(credentials.get("method", "none")).lower()
        if method == "none":
            return None
        if method in _PASSWORD_METHODS:
            return self._trino.auth.BasicAuthentication(self._user, credentials.get("password", ""))
        if method == "jwt":
            return self._trino.auth.JWTAuthentication(credentials.get("jwt_token", ""))
        raise ValueError(
            f"Unsupported Trino auth method '{method}'. Supported: none, ldap/password, jwt."
        )

    @contextmanager
    def _conn(self) -> Generator[Any, None, None]:
        conn = self._trino.dbapi.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            catalog=self._catalog,
            schema=self._default_schema,
            http_scheme=self._http_scheme,
            auth=self._auth,
        )
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(self, sql: str, params: tuple | None = None) -> list[dict[str, Any]]:
        _assert_read_only(sql)
        # Trino rejects a trailing semicolon.
        sql = sql.strip().rstrip(";")
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchmany(200)
            names = [d[0] for d in cur.description or []]
            return [dict(zip(names, row, strict=False)) for row in rows]

    def get_table_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        validate_identifier(schema)
        validate_identifier(table)
        rows = self.execute_query(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
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
            WHERE table_schema = ? AND table_name = ? LIMIT 1
            """,
            (schema, table),
        )
        return len(rows) > 0

    def close(self) -> None:
        pass  # Connections are closed per-context-manager; nothing to do here.
