"""
Playbook: Cross-System Data Merge
===================================
Reusable workflow for consolidating data from multiple siloed systems
with different formats into a unified analytical dataset.

Usage:
    merge = CrossSystemMergePlaybook()
    merge.add_source("customers", "data/customers.csv")
    merge.add_source("orders", "data/orders.json")
    result = merge.execute(primary="customers", joins=[...])
"""

from datetime import datetime
from typing import Any

from src.consolidator import DataConsolidator
from src.profiler import SourceProfiler
from src.quality import DataQualityEngine


class CrossSystemMergePlaybook:
    PLAYBOOK_NAME = "cross_system_merge"
    DESCRIPTION = "Consolidate data from multiple siloed systems into a unified dataset"

    def __init__(self):
        self.consolidator = DataConsolidator()
        self.profiler = SourceProfiler()
        self.quality_engine = DataQualityEngine()
        self.source_configs: list[dict[str, str]] = []
        self.results: dict[str, Any] = {}

    def add_source(self, name: str, file_path: str) -> None:
        self.source_configs.append({"name": name, "file_path": file_path})

    def execute(
        self,
        primary_source: str,
        join_configs: list[dict[str, Any]],
    ) -> dict[str, Any]:
        start = datetime.now()

        print(f"\n  CROSS-SYSTEM MERGE: {len(self.source_configs)} sources")

        for config in self.source_configs:
            print(f"  Loading: {config['name']} ({config['file_path']})")
            self.consolidator.load_source(config["name"], config["file_path"])

        print(f"  Consolidating with primary source: {primary_source}")
        consolidated = self.consolidator.consolidate(primary_source, join_configs)

        print(f"  Running quality checks on consolidated data...")
        quality = self.quality_engine.run_all_checks(consolidated, "consolidated_output")

        self.results = {
            "playbook": self.PLAYBOOK_NAME,
            "sources_merged": len(self.source_configs),
            "primary_source": primary_source,
            "output_rows": len(consolidated),
            "output_columns": len(consolidated.columns),
            "quality_score": quality.get("overall_score", 0),
            "consolidation_summary": self.consolidator.get_consolidation_summary(),
            "executed_at": datetime.now().isoformat(),
            "duration": str(datetime.now() - start),
        }

        return self.results

    def generate_report(self) -> str:
        lines = []
        lines.append(f"{'='*60}")
        lines.append(f"  CROSS-SYSTEM MERGE RESULT")
        lines.append(f"{'='*60}")
        lines.append(f"  Sources Merged: {self.results.get('sources_merged', 0)}")
        lines.append(f"  Output: {self.results.get('output_rows', 0)} rows x {self.results.get('output_columns', 0)} columns")
        lines.append(f"  Quality: {self.results.get('quality_score', 0)}/100")
        lines.append(f"  Duration: {self.results.get('duration', 'N/A')}")
        return "\n".join(lines)
