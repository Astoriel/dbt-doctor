"""Tests for TrinoConnector using a fake ``trino`` client (no server required)."""

import sys
import types

import pytest

from dbt_doctor.connectors.trino import TrinoConnector


class FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self.description = None
        self._rows = []

    def execute(self, sql, params=None):
        self._conn.executed.append((sql, params))
        columns, rows = self._conn.result
        self.description = [(c,) for c in columns]
        self._rows = rows

    def fetchmany(self, size):
        return self._rows[:size]


class FakeConnection:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.executed = []
        self.result = ([], [])
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


@pytest.fixture
def fake_trino(monkeypatch):
    """Install a fake ``trino`` module and record every connection it creates."""
    connections: list[FakeConnection] = []
    next_result: dict = {"value": ([], [])}

    def connect(**kwargs):
        conn = FakeConnection(**kwargs)
        conn.result = next_result["value"]
        connections.append(conn)
        return conn

    module = types.ModuleType("trino")
    module.dbapi = types.SimpleNamespace(connect=connect)
    module.auth = types.SimpleNamespace(
        BasicAuthentication=lambda user, pw: ("basic", user, pw),
        JWTAuthentication=lambda token: ("jwt", token),
    )
    monkeypatch.setitem(sys.modules, "trino", module)
    module.connections = connections
    module.next_result = next_result
    return module


PROFILE = {"type": "trino", "host": "trino.example.com", "database": "hive", "schema": "marts"}


def test_connects_with_profile_values(fake_trino):
    fake_trino.next_result["value"] = (["x"], [(1,)])
    TrinoConnector({**PROFILE, "port": 8443, "http_scheme": "https", "user": "ana"}).execute_query(
        "SELECT 1"
    )
    kwargs = fake_trino.connections[0].kwargs
    assert kwargs["host"] == "trino.example.com"
    assert kwargs["port"] == 8443
    assert kwargs["user"] == "ana"
    assert kwargs["catalog"] == "hive"
    assert kwargs["schema"] == "marts"
    assert kwargs["http_scheme"] == "https"
    assert kwargs["auth"] is None


@pytest.mark.parametrize(
    "extra, scheme, port",
    [
        ({}, "http", 8080),
        ({"method": "none"}, "http", 8080),
        ({"method": "ldap"}, "https", 443),
        ({"method": "jwt"}, "https", 443),
        ({"method": "ldap", "http_scheme": "http"}, "http", 8080),
    ],
)
def test_default_scheme_and_port_follow_auth_method(fake_trino, extra, scheme, port):
    connector = TrinoConnector({**PROFILE, **extra})
    assert connector._http_scheme == scheme
    assert connector._port == port


def test_default_port_depends_on_scheme(fake_trino):
    assert TrinoConnector(PROFILE)._port == 8080
    assert TrinoConnector({**PROFILE, "http_scheme": "https"})._port == 443


@pytest.mark.parametrize(
    "extra, expected",
    [
        ({"method": "ldap", "user": "u", "password": "p"}, ("basic", "u", "p")),
        ({"method": "password", "user": "u", "password": "p"}, ("basic", "u", "p")),
        ({"method": "jwt", "jwt_token": "tok"}, ("jwt", "tok")),
        ({"method": "none"}, None),
    ],
)
def test_auth_methods(fake_trino, extra, expected):
    assert TrinoConnector({**PROFILE, **extra})._auth == expected


def test_unsupported_auth_method_raises(fake_trino):
    with pytest.raises(ValueError, match="Unsupported Trino auth method"):
        TrinoConnector({**PROFILE, "method": "kerberos"})


def test_missing_catalog_raises(fake_trino):
    with pytest.raises(ValueError, match="catalog"):
        TrinoConnector({"type": "trino", "host": "h"})


def test_missing_driver_raises_helpful_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "trino", None)  # makes `import trino` fail
    with pytest.raises(ImportError, match=r"dbt-doctor\[trino\]"):
        TrinoConnector(PROFILE)


def test_execute_query_returns_dicts_and_closes_connection(fake_trino):
    fake_trino.next_result["value"] = (["id", "name"], [(1, "a"), (2, "b")])
    rows = TrinoConnector(PROFILE).execute_query("SELECT id, name FROM t;")
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    conn = fake_trino.connections[0]
    assert conn.closed
    # Trino rejects trailing semicolons, so they must be stripped.
    assert conn.executed[0][0] == "SELECT id, name FROM t"


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE t",
        "INSERT INTO t VALUES (1)",
        "DELETE FROM t",
        "CREATE TABLE t AS SELECT 1",
        "SELECT 1; DROP TABLE t",
        "",
    ],
)
def test_write_queries_are_rejected(fake_trino, sql):
    with pytest.raises(ValueError):
        TrinoConnector(PROFILE).execute_query(sql)
    assert fake_trino.connections == []


@pytest.mark.parametrize(
    "sql", ["SHOW TABLES", "DESCRIBE t", "EXPLAIN SELECT 1", "WITH a AS (SELECT 1) SELECT * FROM a"]
)
def test_read_queries_are_allowed(fake_trino, sql):
    fake_trino.next_result["value"] = (["x"], [(1,)])
    assert TrinoConnector(PROFILE).execute_query(sql) == [{"x": 1}]


def test_get_table_columns(fake_trino):
    fake_trino.next_result["value"] = (
        ["column_name", "data_type", "is_nullable"],
        [("id", "bigint", "NO"), ("name", "varchar", "YES")],
    )
    cols = TrinoConnector(PROFILE).get_table_columns("marts", "orders")
    assert [(c.name, c.data_type, c.is_nullable) for c in cols] == [
        ("id", "bigint", False),
        ("name", "varchar", True),
    ]
    sql, params = fake_trino.connections[0].executed[0]
    assert "information_schema.columns" in sql
    assert params == ("marts", "orders")


@pytest.mark.parametrize("rows, expected", [([(1,)], True), ([], False)])
def test_check_table_exists(fake_trino, rows, expected):
    fake_trino.next_result["value"] = (["_col0"], rows)
    assert TrinoConnector(PROFILE).check_table_exists("marts", "orders") is expected


def test_unsafe_identifiers_are_rejected(fake_trino):
    connector = TrinoConnector(PROFILE)
    with pytest.raises(ValueError, match="Unsafe SQL identifier"):
        connector.get_table_columns("marts; DROP TABLE x", "orders")
    with pytest.raises(ValueError, match="Unsafe SQL identifier"):
        connector.check_table_exists("marts", "orders--")
    assert fake_trino.connections == []
