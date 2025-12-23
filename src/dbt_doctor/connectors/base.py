"""Connectors: Abstract base class for database connectors."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class ColumnInfo:
    """Metadata about a database column."""

    name: str
    data_type: str
    is_nullable: bool = True


class BaseConnector(ABC):
    """Abstract base for all database connectors used by dbt-doctor."""

    @abstractmethod
    def execute_query(self, sql: str, params: tuple | None = None) -> list[dict[str, Any]]:
        """Execute a SELECT query and return rows as list of dicts.

        Parameters
        ----------
        sql:
            SQL query to execute. Should be read-only (SELECT).
        params:
            Optional tuple of parameters for parameterized queries.

        Returns
        -------
        list[dict[str, Any]]
            List of row dicts, empty list if no results.

        Raises
        ------
        ValueError
            If the query is not a read-only SELECT statement.
        RuntimeError
            If the query execution fails.
        """

    @abstractmethod
    def get_table_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        """Return column metadata for a given schema.table."""

    @abstractmethod
    def check_table_exists(self, schema: str, table: str) -> bool:
        """Return True if schema.table exists in the database."""

    @abstractmethod
    def close(self) -> None:
        """Release any held connections or resources."""

    def __enter__(self) -> "BaseConnector":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
