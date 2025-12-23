"""Analyzers: Data profiler — batch column statistics via a single SQL query."""

import logging
from dataclasses import dataclass, field
from typing import Any

from ..connectors.base import BaseConnector
from ..utils.sql_sanitizer import validate_identifier

logger = logging.getLogger(__name__)


@dataclass
class ColumnProfile:
    name: str
    data_type: str
    total_rows: int = 0
    non_null_count: int = 0
    unique_count: int = 0
    min_val: Any = None
    max_val: Any = None
    sample_values: list[Any] = field(default_factory=list)

    @property
    def null_rate(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return round((self.total_rows - self.non_null_count) / self.total_rows * 100, 2)

    @property
    def unique_rate(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return round(self.unique_count / self.total_rows * 100, 2)

    @property
    def is_likely_pk(self) -> bool:
        """True if column looks like a primary key (unique + not null)."""
        return (
            self.null_rate == 0.0 and self.unique_count == self.total_rows and self.total_rows > 0
        )


@dataclass
class TableProfile:
    schema: str
    table: str
    total_rows: int
    column_profiles: dict[str, ColumnProfile] = field(default_factory=dict)
    sample_rows: list[dict[str, Any]] = field(default_factory=list)


_NUMERIC_TYPES = frozenset(
    [
        "integer",
        "bigint",
        "smallint",
        "numeric",
        "float",
        "double",
        "real",
        "decimal",
        "int",
        "int4",
        "int8",
        "float4",
        "float8",
        "number",
    ]
)
_TEMPORAL_TYPES = frozenset(["date", "timestamp", "timestamptz", "datetime", "time"])


def _is_numeric(data_type: str) -> bool:
    return any(t in data_type.lower() for t in _NUMERIC_TYPES)


def _is_temporal(data_type: str) -> bool:
    return any(t in data_type.lower() for t in _TEMPORAL_TYPES)


class DataProfiler:
    """
    Generates statistical column profiles for a database table.

    Uses a batch SQL query (one query per table) for efficiency,
    rather than issuing one query per column.
    """

    def __init__(self, connector: BaseConnector) -> None:
        self._connector = connector

    def profile_table(self, schema: str, table: str) -> TableProfile:
        validate_identifier(schema)
        validate_identifier(table)

        # 1. Get column metadata
        columns = self._connector.get_table_columns(schema, table)
        if not columns:
            raise ValueError(f"Table {schema}.{table} not found or has no columns.")

        # 2. Count total rows
        total_rows_rows = self._connector.execute_query(
            f"SELECT COUNT(*) AS cnt FROM {schema}.{table}"
        )
        total_rows = int((total_rows_rows[0] or {}).get("cnt", 0))

        # 3. Build a single aggregation query for all columns
        parts: list[str] = []
        for col in columns:
            cname = col.name
            parts.append(f"COUNT({cname}) AS {cname}__non_null")
            parts.append(f"COUNT(DISTINCT {cname}) AS {cname}__unique")
            if _is_numeric(col.data_type) or _is_temporal(col.data_type):
                parts.append(f"CAST(MIN({cname}) AS VARCHAR) AS {cname}__min")
                parts.append(f"CAST(MAX({cname}) AS VARCHAR) AS {cname}__max")

        agg_sql = f"SELECT {', '.join(parts)} FROM {schema}.{table}"
        agg_rows = self._connector.execute_query(agg_sql)
        agg = agg_rows[0] if agg_rows else {}

        # 4. Get sample rows (up to 5)
        sample_rows = self._connector.execute_query(f"SELECT * FROM {schema}.{table} LIMIT 5")

        # 5. Build profiles
        profile = TableProfile(
            schema=schema, table=table, total_rows=total_rows, sample_rows=sample_rows
        )
        for col in columns:
            cname = col.name
            cp = ColumnProfile(
                name=cname,
                data_type=col.data_type,
                total_rows=total_rows,
                non_null_count=int(agg.get(f"{cname}__non_null", 0) or 0),
                unique_count=int(agg.get(f"{cname}__unique", 0) or 0),
            )
            if f"{cname}__min" in agg:
                cp.min_val = agg[f"{cname}__min"]
                cp.max_val = agg[f"{cname}__max"]
            # Sample values from first row
            if sample_rows:
                cp.sample_values = [r.get(cname) for r in sample_rows[:3]]

            profile.column_profiles[cname] = cp

        return profile

    def format_profile(self, profile: TableProfile) -> str:
        lines = [
            "═" * 60,
            f"  📊 Data Profile: {profile.schema}.{profile.table}",
            f"     Total Rows: {profile.total_rows:,}",
            "═" * 60,
        ]
        for col in profile.column_profiles.values():
            lines.append(f"\n  Column: {col.name}  [{col.data_type}]")
            lines.append(
                f"    Non-null: {col.non_null_count:,} / {col.total_rows:,}  "
                f"(null rate: {col.null_rate}%)"
            )
            lines.append(f"    Unique:   {col.unique_count:,}  (unique rate: {col.unique_rate}%)")
            if col.min_val is not None:
                lines.append(f"    Min: {col.min_val}  |  Max: {col.max_val}")
            if col.is_likely_pk:
                lines.append("    ✅ Likely primary key (100% non-null, 100% unique)")
        lines.append("\n" + "═" * 60)
        return "\n".join(lines)
