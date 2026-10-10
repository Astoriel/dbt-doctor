"""Tests for adapter selection in server._build_connector."""

import sys
import types

import pytest

from dbt_doctor import server
from dbt_doctor.connectors.trino import TrinoConnector


class FakeProjectParser:
    profile_name = "my_profile"

    def __init__(self, _project_dir):
        pass

    def load(self):
        return True


def _patch_profile(monkeypatch, creds):
    class FakeProfileParser:
        def __init__(self, _profile_name):
            pass

        def get_credentials(self):
            return creds

    monkeypatch.setattr(server, "DbtProjectParser", FakeProjectParser)
    monkeypatch.setattr(server, "DbtProfileParser", FakeProfileParser)


@pytest.fixture
def fake_trino(monkeypatch):
    module = types.ModuleType("trino")
    module.dbapi = types.SimpleNamespace(connect=lambda **_: None)
    module.auth = types.SimpleNamespace()
    monkeypatch.setitem(sys.modules, "trino", module)


def test_trino_type_builds_trino_connector(monkeypatch, fake_trino):
    _patch_profile(monkeypatch, {"type": "trino", "host": "h", "database": "hive"})
    assert isinstance(server._build_connector("."), TrinoConnector)


def test_trino_type_is_case_insensitive(monkeypatch, fake_trino):
    _patch_profile(monkeypatch, {"type": "Trino", "host": "h", "database": "hive"})
    assert isinstance(server._build_connector("."), TrinoConnector)


def test_trino_without_catalog_returns_none(monkeypatch, fake_trino):
    _patch_profile(monkeypatch, {"type": "trino", "host": "h"})
    assert server._build_connector(".") is None


def test_trino_without_driver_returns_none(monkeypatch):
    monkeypatch.setitem(sys.modules, "trino", None)
    _patch_profile(monkeypatch, {"type": "trino", "host": "h", "database": "hive"})
    assert server._build_connector(".") is None


def test_unsupported_type_returns_none(monkeypatch):
    _patch_profile(monkeypatch, {"type": "snowflake"})
    assert server._build_connector(".") is None
