"""Analyzers: semantic quality of column descriptions, judged by the client LLM.

dbt-doctor never calls an LLM itself. This module prepares the context (descriptions,
compiled SQL, deterministic signals and the scoring rubric) for the client LLM to judge,
then validates the scores it sends back, applies the pass threshold and tracks progress
for tables that need more than one batch.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..connectors.base import ColumnInfo

DEFAULT_RUBRIC_PATH = Path(__file__).resolve().parent.parent / "description_rubric.json"

# Character budget for one batch payload, and the knobs that keep batches useful.
DEFAULT_BATCH_CHARS = 30_000
MIN_COLUMNS_PER_BATCH = 5
SQL_INLINE_LIMIT = 12_000
_SNIPPET_LIMIT = 400

_QUESTION_ID_RE = re.compile(r"^[a-z][a-z0-9_]*$")


class RubricError(ValueError):
    """The rubric file is missing, unreadable or invalid."""


class ScoreValidationError(ValueError):
    """The scores sent back by the client are invalid."""


# ═══════════════════════════════════════════════════════════════════════════
# RUBRIC
# ═══════════════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class Question:
    id: str
    text: str


@dataclass(frozen=True)
class Rubric:
    version: int
    pass_ratio: float
    max_score_per_question: int
    questions: tuple[Question, ...]

    @property
    def max_total(self) -> int:
        return self.max_score_per_question * len(self.questions)

    @property
    def threshold(self) -> float:
        return round(self.pass_ratio * self.max_total, 6)

    def passes(self, total: float) -> bool:
        return total + 1e-9 >= self.threshold


def load_rubric(path: str | Path | None = None) -> Rubric:
    """Load and validate the rubric JSON (the packaged default when ``path`` is None)."""
    rubric_path = Path(path) if path else DEFAULT_RUBRIC_PATH
    try:
        raw = json.loads(rubric_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise RubricError(f"Cannot read rubric file {rubric_path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise RubricError(f"Rubric file {rubric_path} is not valid JSON: {exc}") from exc

    if not isinstance(raw, dict):
        raise RubricError("Rubric must be a JSON object.")

    version = raw.get("version", 1)
    if not isinstance(version, int) or isinstance(version, bool):
        raise RubricError("'version' must be an integer.")

    pass_ratio = raw.get("pass_ratio")
    if (
        isinstance(pass_ratio, bool)
        or not isinstance(pass_ratio, (int, float))
        or not 0 < pass_ratio <= 1
    ):
        raise RubricError("'pass_ratio' must be a number greater than 0 and at most 1.")

    max_score = raw.get("max_score_per_question")
    if isinstance(max_score, bool) or not isinstance(max_score, int) or max_score < 1:
        raise RubricError("'max_score_per_question' must be a positive integer.")

    raw_questions = raw.get("questions")
    if not isinstance(raw_questions, list) or not raw_questions:
        raise RubricError("'questions' must be a non-empty list.")

    questions: list[Question] = []
    seen: set[str] = set()
    for i, q in enumerate(raw_questions, start=1):
        if not isinstance(q, dict):
            raise RubricError(f"Question #{i} must be an object with 'id' and 'text'.")
        qid, text = q.get("id"), q.get("text")
        if not isinstance(qid, str) or not _QUESTION_ID_RE.match(qid):
            raise RubricError(
                f"Question #{i} has an invalid 'id'. Use lowercase letters, digits and "
                "underscores, starting with a letter."
            )
        if qid in seen:
            raise RubricError(f"Duplicate question id '{qid}'.")
        if not isinstance(text, str) or not text.strip():
            raise RubricError(f"Question '{qid}' must have a non-empty 'text'.")
        seen.add(qid)
        questions.append(Question(id=qid, text=text.strip()))

    return Rubric(
        version=version,
        pass_ratio=float(pass_ratio),
        max_score_per_question=max_score,
        questions=tuple(questions),
    )


# ═══════════════════════════════════════════════════════════════════════════
# DETERMINISTIC SIGNALS (hints for the LLM, not scores)
# ═══════════════════════════════════════════════════════════════════════════

_STOPWORDS = frozenset(
    [
        "the",
        "a",
        "an",
        "of",
        "for",
        "to",
        "and",
        "or",
        "is",
        "this",
        "that",
        "da",
        "de",
        "do",
        "das",
        "dos",
        "o",
        "e",
        "um",
        "uma",
    ]
)

# kind -> (column-name pattern, data-type pattern, pattern a good description would match)
_UNIT_RULES: dict[str, tuple[re.Pattern[str], re.Pattern[str] | None, re.Pattern[str]]] = {
    "currency": (
        re.compile(
            r"(amount|price|cost|revenue|fee|balance|discount|tax|salary|payment|gmv|mrr|arr)"
        ),
        None,
        re.compile(r"(usd|brl|eur|gbp|currency|cents|dollar|reais|r\$|\$|€|£)"),
    ),
    "timestamp": (
        re.compile(r"(_at|_ts|_timestamp|_datetime)$|^(timestamp|datetime)"),
        re.compile(r"timestamp|datetime"),
        re.compile(r"(utc|timezone|time zone|\btz\b|iso|gmt|local)"),
    ),
    "date": (
        re.compile(r"(_date|_dt)$|^date$"),
        re.compile(r"^date$"),
        re.compile(r"(yyyy|iso|format|utc|timezone|time zone|calendar|local)"),
    ),
    "boolean": (
        re.compile(r"^(is|has|was|can|should|did)_"),
        re.compile(r"bool"),
        re.compile(r"(true|false|flag|whether|yes|indicates|y/n|\b1\b|\b0\b)"),
    ),
    "percentage": (
        re.compile(r"(pct|percent|_rate|_ratio|_share)"),
        None,
        re.compile(r"(percent|%|ratio|fraction|proportion|between 0|0 to 1|0-1)"),
    ),
    "duration": (
        re.compile(r"(duration|elapsed|_seconds|_minutes|_hours|_days|_ms|_secs?)$"),
        None,
        re.compile(r"(second|minute|hour|day|millisecond|\bms\b|unit)"),
    ),
}

_LOGIC_RE = re.compile(
    r"\b(case\s+when|coalesce\s*\(|nullif\s*\(|ifnull\s*\(|nvl\s*\(|iff\s*\(|decode\s*\("
    r"|over\s*\(|(?:sum|count|avg|min|max|row_number|rank|lag|lead)\s*\()",
    re.IGNORECASE,
)
_LEADING_SELECT_RE = re.compile(r"^\s*select\s+(distinct\s+)?", re.IGNORECASE)


def _tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def _is_tautological(column: str, description: str) -> bool:
    """True when the description only restates the column name (ignoring filler words)."""
    col_tokens = _tokens(column)
    content = [t for t in _tokens(description) if t not in _STOPWORDS]
    if not content:
        return True
    return set(content) <= set(col_tokens) or "".join(content) == "".join(col_tokens)


def _missing_unit_kinds(column: str, data_type: str, description: str) -> list[str]:
    name, dtype, desc = column.lower(), (data_type or "").lower(), description.lower()
    kinds: list[str] = []
    for kind, (name_re, type_re, desc_re) in _UNIT_RULES.items():
        applies = bool(name_re.search(name)) or bool(type_re and dtype and type_re.search(dtype))
        if applies and not desc_re.search(desc):
            kinds.append(kind)
    return kinds


def extract_complex_expression(sql: str, column: str) -> str | None:
    """Return the SQL expression that produces ``column`` if it contains non-trivial logic.

    Textual heuristic: finds ``<expression> AS <column>`` and walks back to the start of
    the select item. Columns without an explicit alias are treated as plain passthroughs.
    """
    if not sql or not column:
        return None
    alias_re = re.compile(rf"\bas\s+[\"`\[]?{re.escape(column)}[\"`\]]?(?![\w$])", re.IGNORECASE)
    matches = list(alias_re.finditer(sql))
    for m in reversed(matches):  # the last occurrence is usually the final SELECT
        depth, start = 0, 0
        for i in range(m.start() - 1, -1, -1):
            ch = sql[i]
            if ch == ")":
                depth += 1
            elif ch == "(":
                if depth == 0:
                    start = i + 1
                    break
                depth -= 1
            elif ch == "," and depth == 0:
                start = i + 1
                break
        expression = _LEADING_SELECT_RE.sub("", sql[start : m.start()])
        expression = " ".join(expression.split())
        if expression and _LOGIC_RE.search(expression):
            if len(expression) > _SNIPPET_LIMIT:
                expression = expression[:_SNIPPET_LIMIT] + " ..."
            return expression
    return None


def compute_signals(
    column: str, data_type: str | None, description: str | None, sql: str | None
) -> dict[str, Any]:
    """Deterministic hints about one column. Only signals that fired are returned."""
    signals: dict[str, Any] = {}
    desc = (description or "").strip()
    if not desc:
        signals["empty_description"] = True
    else:
        if _is_tautological(column, desc):
            signals["repeats_column_name"] = True
        elif len(desc.split()) < 3:
            signals["very_short"] = True
        kinds = _missing_unit_kinds(column, data_type or "", desc)
        if kinds:
            signals["unit_or_format_not_stated"] = kinds
    expression = extract_complex_expression(sql or "", column)
    if expression:
        signals["complex_sql_expression"] = expression
    return signals


# ═══════════════════════════════════════════════════════════════════════════
# EVALUATOR
# ═══════════════════════════════════════════════════════════════════════════


_STATUSES = ("ok", "rewrite", "missing")


def _is_missing(description: str | None) -> bool:
    return not (description or "").strip()


def merge_columns(
    details: dict[str, Any], db_columns: list[ColumnInfo] | None, db_note: str | None = None
) -> tuple[list[tuple[str, dict[str, Any]]], dict[str, Any]]:
    """Combine the manifest columns with the table's real columns (the ``DESCRIBE`` view).

    The manifest only knows columns declared in schema.yml, so a column that exists in the
    table but is not documented would never show up. Manifest columns come first (YAML
    order), then the columns found only in the database, flagged ``declared=False``.
    Returns the merged list and a ``database_check`` summary.
    """
    manifest = details.get("columns", {})
    if db_columns is None:
        columns = [(n, {**c, "declared": True}) for n, c in manifest.items()]
        check = {
            "status": "unavailable",
            "detail": db_note or "database connector not available",
            "undeclared_columns": [],
            "not_in_database": [],
        }
        return columns, check

    db_by_name = {c.name.lower(): c for c in db_columns}
    columns: list[tuple[str, dict[str, Any]]] = []
    not_in_database: list[str] = []
    for name, col in manifest.items():
        merged = {**col, "declared": True}
        db = db_by_name.get(name.lower())
        if db is None:
            not_in_database.append(name)
            merged["in_database"] = False
        elif not col.get("data_type"):
            merged["data_type"] = db.data_type
        columns.append((name, merged))

    known = {n.lower() for n in manifest}
    undeclared: list[str] = []
    for db in db_columns:
        if db.name.lower() not in known:
            undeclared.append(db.name)
            columns.append(
                (
                    db.name,
                    {"description": "", "data_type": db.data_type, "tests": [], "declared": False},
                )
            )
    check = {
        "status": "ok",
        "detail": None,
        "undeclared_columns": undeclared,
        "not_in_database": not_in_database,
    }
    return columns, check


def _safe_filename(model_name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", model_name)


class DescriptionQualityEvaluator:
    """Builds evaluation batches for a model and collects the client's scores.

    Progress between batches lives in a temporary JSON file in ``state_dir``. It is
    deleted when a new evaluation starts (``offset=0``) and when it completes.
    """

    def __init__(self, rubric: Rubric, state_dir: Path) -> None:
        self.rubric = rubric
        self.state_dir = Path(state_dir)

    # ── state file ────────────────────────────────────────────────────────

    def _state_path(self, model_name: str) -> Path:
        return self.state_dir / f"description_eval_{_safe_filename(model_name)}.json"

    def _load_state(self, model_name: str) -> dict[str, Any]:
        path = self._state_path(model_name)
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        # A rubric edited mid-evaluation invalidates earlier scores.
        if state.get("rubric") != self._rubric_fingerprint():
            return {}
        return state

    def _save_state(self, model_name: str, results: dict[str, Any]) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        payload = {"model": model_name, "rubric": self._rubric_fingerprint(), "results": results}
        self._state_path(model_name).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _reset(self, model_name: str) -> None:
        self._state_path(model_name).unlink(missing_ok=True)

    def _rubric_fingerprint(self) -> dict[str, Any]:
        return {
            "version": self.rubric.version,
            "ids": [q.id for q in self.rubric.questions],
            "max": self.rubric.max_score_per_question,
            "ratio": self.rubric.pass_ratio,
        }

    # ── batch ─────────────────────────────────────────────────────────────

    def next_batch(
        self,
        details: dict[str, Any],
        offset: int = 0,
        max_chars: int = DEFAULT_BATCH_CHARS,
        db_columns: list[ColumnInfo] | None = None,
        db_note: str | None = None,
    ) -> dict[str, Any]:
        """Return the next batch of columns to judge, starting at ``offset``."""
        model = details["name"]
        columns, database_check = merge_columns(details, db_columns, db_note)
        total = len(columns)
        if total == 0:
            raise ValueError(
                f"Model '{model}' has no columns in the manifest or in the database. "
                "Document its columns in schema.yml and run `dbt parse` first."
            )
        if offset < 0 or offset >= total:
            raise ValueError(f"offset must be between 0 and {total - 1} for model '{model}'.")
        if offset == 0:
            self._reset(model)

        sql, sql_kind = self._pick_sql(details)
        unique_columns = [name for name, col in columns if "unique" in (col.get("tests") or [])]
        header: dict[str, Any] = {
            "model": model,
            "model_description": details.get("description", ""),
            "schema": details.get("schema"),
            "unique_columns": unique_columns,
            "database_check": database_check,
            "sql": {"kind": sql_kind, "code": sql},
            "rubric": {
                "scale": f"integer 0..{self.rubric.max_score_per_question}",
                "max_total": self.rubric.max_total,
                "pass_total": self.rubric.threshold,
                "questions": [{"id": q.id, "text": q.text} for q in self.rubric.questions],
            },
            "offset": offset,
            "total_columns": total,
            "instructions": self._instructions(model),
        }
        room = max_chars - len(json.dumps(header, ensure_ascii=False))

        batch: list[dict[str, Any]] = []
        used = 0
        for name, col in columns[offset:]:
            entry = {
                "name": name,
                "data_type": col.get("data_type"),
                "description": col.get("description", ""),
                "tests": col.get("tests", []),
                "signals": compute_signals(name, col.get("data_type"), col.get("description"), sql),
            }
            if col.get("in_database") is False:
                entry["signals"]["not_in_database"] = True
            if not col.get("declared", True):
                entry["declared"] = False
            if _is_missing(col.get("description")):
                entry["status"] = "missing"  # recorded automatically, no score needed
            size = len(json.dumps(entry, ensure_ascii=False))
            if len(batch) >= MIN_COLUMNS_PER_BATCH and used + size > room:
                break
            batch.append(entry)
            used += size

        end = offset + len(batch)
        header["columns"] = batch
        header["next_offset"] = end if end < total else None
        return header

    @staticmethod
    def _pick_sql(details: dict[str, Any]) -> tuple[str | None, str | None]:
        compiled = (details.get("compiled_sql") or "").strip()
        raw = (details.get("raw_sql") or "").strip()
        if compiled:
            sql, kind = compiled, "compiled"
        elif raw:
            sql, kind = raw, "raw (not compiled; run `dbt compile` for better results)"
        else:
            return None, None
        if len(sql) > SQL_INLINE_LIMIT:
            return None, "omitted (too large); rely on 'complex_sql_expression' signals"
        return sql, kind

    def _instructions(self, model: str) -> str:
        example_scores = {q.id: self.rubric.max_score_per_question for q in self.rubric.questions}
        example = json.dumps([{"column": "<name>", "scores": example_scores}])
        return (
            "Judge every column in 'columns' on each rubric question with an integer score "
            f"(0 = not satisfied, {self.rubric.max_score_per_question} = fully satisfied), using "
            "the description, the SQL, the signals and the model context. Signals are hints, "
            "not scores. Only score; do not write or suggest descriptions. Columns with "
            "'status': 'missing' have an empty description and are recorded automatically, so "
            "do not score them. Columns with 'declared': false exist in the table but are not "
            "documented in schema.yml (database_check lists them). If database_check.status "
            "is 'unavailable', tell the user the columns could not be checked against the "
            "database. Then call "
            f"submit_description_scores(model_name='{model}', scores={example}); send an empty "
            "list if every column of the batch is 'missing'. "
            "If 'next_offset' is not null, call evaluate_model_descriptions again with "
            "offset=next_offset. This tool never modifies any file in the dbt project."
        )

    # ── scores ────────────────────────────────────────────────────────────

    def submit(
        self,
        details: dict[str, Any],
        scores: list[dict[str, Any]],
        db_columns: list[ColumnInfo] | None = None,
        db_note: str | None = None,
    ) -> dict[str, Any]:
        """Validate and record scores for a batch. Returns progress, or the final report."""
        model = details["name"]
        merged, database_check = merge_columns(details, db_columns, db_note)
        names = [n for n, _ in merged]
        descriptions = {n: c.get("description", "") for n, c in merged}
        declared = {n: c.get("declared", True) for n, c in merged}

        absent = {n for n, d in descriptions.items() if _is_missing(d)}
        scored = self._validate(scores, names, absent)
        state = self._load_state(model)
        results: dict[str, Any] = state.get("results", {})
        for column, entry in scored.items():
            entry["description"] = descriptions[column]
            entry["declared"] = declared[column]
            results[column] = entry
        # Absent descriptions need no judgement: they are always flagged for update.
        for column in absent:
            results.setdefault(
                column,
                {
                    "column": column,
                    "description": descriptions[column],
                    "declared": declared[column],
                    "status": "missing",
                    "scores": None,
                    "total": None,
                },
            )

        pending = [n for n in names if n not in results]
        if pending:
            self._save_state(model, results)
            return {
                "status": "in_progress",
                "model": model,
                "evaluated": len(results),
                "total_columns": len(names),
                "next_offset": names.index(pending[0]),
            }

        report = [results[n] for n in names]
        self._reset(model)
        return {
            "status": "complete",
            "model": model,
            "max_total": self.rubric.max_total,
            "pass_total": self.rubric.threshold,
            "total_columns": len(names),
            "summary": {s: sum(1 for r in report if r["status"] == s) for s in _STATUSES},
            "to_update": [r["column"] for r in report if r["status"] != "ok"],
            "database_check": database_check,
            "columns": report,
            "next_step": (
                "Columns with status 'missing' or 'rewrite' (listed in 'to_update') are the "
                "ones a documentation-update step should change; leave 'ok' columns untouched. "
                "Columns with 'declared': false must be added to schema.yml, not just edited."
            ),
        }

    def _validate(
        self, scores: Any, names: list[str], absent: set[str]
    ) -> dict[str, dict[str, Any]]:
        if not isinstance(scores, list):
            raise ScoreValidationError("'scores' must be a list.")
        ids = [q.id for q in self.rubric.questions]
        max_score = self.rubric.max_score_per_question
        problems: list[str] = []
        accepted: dict[str, dict[str, Any]] = {}

        for i, item in enumerate(scores, start=1):
            if not isinstance(item, dict):
                problems.append(f"#{i}: each item must be an object.")
                continue
            column = item.get("column")
            if column not in names:
                problems.append(f"#{i}: unknown column '{column}'.")
                continue
            if column in accepted:
                problems.append(f"'{column}': sent more than once in this call.")
                continue
            if column in absent:
                continue  # empty description: recorded as 'missing', any score is ignored
            values = item.get("scores")
            if not isinstance(values, dict):
                problems.append(f"'{column}': 'scores' must be an object keyed by question id.")
                continue
            missing = [q for q in ids if q not in values]
            extra = [q for q in values if q not in ids]
            if missing or extra:
                problems.append(
                    f"'{column}': expected question ids {ids}"
                    + (f"; missing {missing}" if missing else "")
                    + (f"; unknown {extra}" if extra else "")
                )
                continue
            bad = [
                q
                for q, v in values.items()
                if isinstance(v, bool) or not isinstance(v, (int, float)) or not 0 <= v <= max_score
            ]
            if bad:
                problems.append(
                    f"'{column}': scores for {bad} must be numbers from 0 to {max_score}."
                )
                continue

            total = sum(values[q] for q in ids)
            accepted[column] = {
                "column": column,
                "scores": {q: values[q] for q in ids},
                "total": total,
                "status": "ok" if self.rubric.passes(total) else "rewrite",
            }

        if problems:
            shown = "\n".join(f"- {p}" for p in problems[:20])
            more = f"\n(+{len(problems) - 20} more)" if len(problems) > 20 else ""
            raise ScoreValidationError(
                "Scores rejected, nothing was recorded. Fix and resend the whole call:\n"
                f"{shown}{more}"
            )
        return accepted
