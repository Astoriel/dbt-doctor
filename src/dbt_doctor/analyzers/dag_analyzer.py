"""Analyzers: DAG analyzer — orphan models, depth, fan-out, root models."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class DagReport:
    total_models: int = 0
    orphan_models: list[str] = field(default_factory=list)
    root_models: list[str] = field(default_factory=list)
    max_chain_depth: int = 0
    high_fanout_models: list[dict[str, Any]] = field(default_factory=list)
    long_chains: list[list[str]] = field(default_factory=list)


class DagAnalyzer:
    """
    Analyzes the dbt project DAG to find structural issues:
    - Orphan models (no downstream consumers — no Exposure, no downstream model)
    - Root models (depend on nothing other than sources)
    - Max chain depth (long lineage chains increase compute cost)
    - High fan-out models (one model feeding many others)
    """

    def analyze(self, models: list[dict[str, Any]], exposures: list[dict[str, Any]] | None = None) -> DagReport:
        """
        Parameters
        ----------
        models:
            List of model dicts from ManifestParser.get_models().
            Each must have 'name' and 'depends_on' (list of node IDs).
        exposures:
            Optional list of exposures from get_all_exposures().
        """
        report = DagReport(total_models=len(models))
        exposures = exposures or []

        # Build lookup structures
        model_names = {m["name"] for m in models}
        {m["name"]: m for m in models}

        # Build forward adjacency: name → list of names that depend on it
        reverse_deps: dict[str, list[str]] = {name: [] for name in model_names}
        for m in models:
            for dep_id in m.get("depends_on", []):
                # dep_id is like "model.project.dep_name" or "source.project.source_name"
                dep_name = dep_id.split(".")[-1]
                if dep_name in reverse_deps:
                    reverse_deps[dep_name].append(m["name"])

        # Collect all nodes that appear in exposure depends_on
        exposure_deps: set[str] = set()
        for exp in exposures:
            for dep_id in exp.get("depends_on", []):
                exposure_deps.add(dep_id.split(".")[-1])

        # Find orphans: models that have no downstream model AND no exposure pointing to them
        for name in model_names:
            downstream = reverse_deps.get(name, [])
            if not downstream and name not in exposure_deps:
                report.orphan_models.append(name)

        # Find root models: models whose depends_on only references sources (no model deps)
        for m in models:
            model_deps = [
                dep_id.split(".")[-1]
                for dep_id in m.get("depends_on", [])
                if dep_id.startswith("model.")
            ]
            if not model_deps:
                report.root_models.append(m["name"])

        # Find high fan-out (a model that feeds many downstream models)
        for name, consumers in reverse_deps.items():
            if len(consumers) >= 5:
                report.high_fanout_models.append(
                    {"model": name, "downstream_count": len(consumers), "consumers": consumers[:10]}
                )
        report.high_fanout_models.sort(key=lambda x: -x["downstream_count"])

        # Calculate max DAG depth via iterative BFS from roots
        report.max_chain_depth = self._calculate_max_depth(models, reverse_deps)

        return report

    def _calculate_max_depth(
        self, models: list[dict[str, Any]], reverse_deps: dict[str, list[str]]
    ) -> int:
        """Calculate the longest path in the DAG using topological sort + DP."""
        # Build forward deps map: name → list of direct model deps
        forward: dict[str, list[str]] = {}
        model_names = {m["name"] for m in models}
        for m in models:
            name = m["name"]
            forward[name] = [
                dep_id.split(".")[-1]
                for dep_id in m.get("depends_on", [])
                if dep_id.startswith("model.") and dep_id.split(".")[-1] in model_names
            ]

        # Memoized depth calc
        memo: dict[str, int] = {}

        def depth(name: str, visited: set[str]) -> int:
            if name in memo:
                return memo[name]
            if name in visited:
                return 0  # Cycle guard (shouldn't happen in valid dbt DAG)
            visited = visited | {name}
            parents = forward.get(name, [])
            if not parents:
                memo[name] = 1
                return 1
            max_parent = max(depth(p, visited) for p in parents)
            result = max_parent + 1
            memo[name] = result
            return result

        if not model_names:
            return 0
        return max(depth(name, set()) for name in model_names)

    def format_report(self, report: DagReport) -> str:
        lines = [
            "═" * 55,
            "  🔗 dbt-doctor — DAG Analysis Report",
            "═" * 55,
            f"  Total Models:       {report.total_models}",
            f"  Max Chain Depth:    {report.max_chain_depth}",
            f"  Orphan Models:      {len(report.orphan_models)}",
            f"  Root Models:        {len(report.root_models)}",
            f"  High Fan-out:       {len(report.high_fanout_models)}",
            "",
        ]
        if report.orphan_models:
            lines.append("  ⚠️  Orphan Models (no downstream consumers):")
            for name in report.orphan_models[:10]:
                lines.append(f"    • {name}")
            if len(report.orphan_models) > 10:
                lines.append(f"    ... and {len(report.orphan_models) - 10} more")
            lines.append("")

        if report.high_fanout_models:
            lines.append("  📊 High Fan-out Models:")
            for hf in report.high_fanout_models[:5]:
                lines.append(f"    • {hf['model']}: feeds {hf['downstream_count']} models")
            lines.append("")

        if report.max_chain_depth > 10:
            lines.append(
                f"  ⚠️  Chain depth of {report.max_chain_depth} is high. "
                "Consider intermediate layers to break long chains."
            )
        lines.append("═" * 55)
        return "\n".join(lines)
