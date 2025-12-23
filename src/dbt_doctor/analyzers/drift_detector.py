"""Analyzers: Schema drift detector — compare DB columns vs manifest/schema.yml."""

from dataclasses import dataclass, field
from typing import Any

from ..connectors.base import BaseConnector, ColumnInfo
from ..utils.sql_sanitizer import validate_identifier


@dataclass
class ColumnDrift:
    column_name: str
    drift_type: str  # "added_in_db", "removed_from_db", "type_changed"
    db_type: str | None = None
    manifest_type: str | None = None
    description: str = ""


@dataclass
class DriftReport:
    schema: str
    table: str
    is_clean: bool
    added_in_db: list[ColumnDrift] = field(default_factory=list)
    removed_from_db: list[ColumnDrift] = field(default_factory=list)
    type_changes: list[ColumnDrift] = field(default_factory=list)

    @property
    def total_drifts(self) -> int:
        return len(self.added_in_db) + len(self.removed_from_db) + len(self.type_changes)


class SchemaDriftDetector:
    """
    Detects drift between what is defined in the dbt manifest (schema.yml / manifest.json)
    and the actual columns present in the database.

    This catches:
    - New columns in DB not yet documented in dbt
    - Documented columns removed from DB (broken references)
    - Data type changes between manifest and DB
    """

    def __init__(self, connector: BaseConnector) -> None:
        self._connector = connector

    def detect(
        self,
        schema: str,
        table: str,
        manifest_columns: dict[str, Any],
    ) -> DriftReport:
        """
        Compare DB columns against manifest column definitions.

        Parameters
        ----------
        schema, table:
            Target database table.
        manifest_columns:
            Dict from manifest: {col_name: {data_type, description, ...}}
            (from DbtManifestParser.get_model_details()['columns'])
        """
        validate_identifier(schema)
        validate_identifier(table)

        db_columns: list[ColumnInfo] = self._connector.get_table_columns(schema, table)
        db_col_map: dict[str, ColumnInfo] = {c.name.lower(): c for c in db_columns}
        manifest_col_map: dict[str, dict] = {k.lower(): v for k, v in manifest_columns.items()}

        drifts: list[ColumnDrift] = []

        # Columns in DB but not in manifest → undocumented new columns
        for db_name, db_col in db_col_map.items():
            if db_name not in manifest_col_map:
                drifts.append(
                    ColumnDrift(
                        column_name=db_col.name,
                        drift_type="added_in_db",
                        db_type=db_col.data_type,
                        description=f"Column '{db_col.name}' exists in DB but is not documented in dbt.",
                    )
                )

        # Columns in manifest but not in DB → stale documentation
        for manifest_name in manifest_col_map:
            if manifest_name not in db_col_map:
                drifts.append(
                    ColumnDrift(
                        column_name=manifest_name,
                        drift_type="removed_from_db",
                        manifest_type=manifest_col_map[manifest_name].get("data_type"),
                        description=f"Column '{manifest_name}' is documented in dbt but missing from DB.",
                    )
                )

        # Type changes — only when manifest has explicit data_type info
        for col_name in db_col_map:
            if col_name in manifest_col_map:
                manifest_type = manifest_col_map[col_name].get("data_type", "")
                db_type = db_col_map[col_name].data_type
                if (
                    manifest_type
                    and manifest_type.lower() not in db_type.lower()
                    and db_type.lower() not in manifest_type.lower()
                ):
                    drifts.append(
                        ColumnDrift(
                            column_name=col_name,
                            drift_type="type_changed",
                            db_type=db_type,
                            manifest_type=manifest_type,
                            description=(
                                f"Type mismatch for '{col_name}': "
                                f"DB={db_type}, manifest={manifest_type}"
                            ),
                        )
                    )

        added = [d for d in drifts if d.drift_type == "added_in_db"]
        removed = [d for d in drifts if d.drift_type == "removed_from_db"]
        changed = [d for d in drifts if d.drift_type == "type_changed"]

        return DriftReport(
            schema=schema,
            table=table,
            is_clean=not drifts,
            added_in_db=added,
            removed_from_db=removed,
            type_changes=changed,
        )

    def format_report(self, report: DriftReport) -> str:
        lines = [
            "═" * 55,
            f"  🔄 Schema Drift: {report.schema}.{report.table}",
            "═" * 55,
        ]
        if report.is_clean:
            lines.append("  ✅ No drift detected! DB matches dbt documentation.")
        else:
            lines.append(f"  ⚠️  {report.total_drifts} drift(s) detected:")
            if report.added_in_db:
                lines.append(f"\n  📥 New columns in DB (undocumented): {len(report.added_in_db)}")
                for drift in report.added_in_db:
                    lines.append(f"    + {drift.column_name}  [{drift.db_type}]")
                    lines.append("      → Run `update_model_yaml` to document this column")
            if report.removed_from_db:
                lines.append(f"\n  🗑️  Stale in dbt docs (removed from DB): {len(report.removed_from_db)}")
                for drift in report.removed_from_db:
                    lines.append(f"    - {drift.column_name}  [{drift.manifest_type or 'unknown type'}]")
                    lines.append("      → Remove from schema.yml manually")
            if report.type_changes:
                lines.append(f"\n  🔀 Type changes: {len(report.type_changes)}")
                for drift in report.type_changes:
                    lines.append(
                        f"    ~ {drift.column_name}: DB={drift.db_type}, manifest={drift.manifest_type}"
                    )
        lines.append("═" * 55)
        return "\n".join(lines)
