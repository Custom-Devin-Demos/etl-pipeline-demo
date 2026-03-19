import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any

import pandas as pd


class DataConsolidator:
    def __init__(self):
        self.sources: dict[str, pd.DataFrame] = {}
        self.consolidated: pd.DataFrame | None = None
        self.lineage: list[dict[str, Any]] = []

    def load_source(self, name: str, file_path: str) -> pd.DataFrame:
        ext = os.path.splitext(file_path)[1].lower()

        if ext == ".csv":
            df = pd.read_csv(file_path)
        elif ext == ".json":
            with open(file_path, "r") as f:
                raw = json.load(f)
            if isinstance(raw, list):
                df = pd.json_normalize(raw)
            elif isinstance(raw, dict):
                for key in ["data", "inventory", "items", "records", "results"]:
                    if key in raw and isinstance(raw[key], list):
                        df = pd.json_normalize(raw[key])
                        break
                else:
                    df = pd.json_normalize(raw)
            else:
                df = pd.DataFrame([raw])
        elif ext == ".xml":
            df = self._parse_xml(file_path)
        else:
            raise ValueError(f"Unsupported format: {ext}")

        self.sources[name] = df
        self.lineage.append({
            "action": "load_source",
            "source_name": name,
            "file_path": file_path,
            "format": ext.replace(".", ""),
            "rows": len(df),
            "columns": list(df.columns),
            "timestamp": datetime.now().isoformat(),
        })
        return df

    def _parse_xml(self, file_path: str) -> pd.DataFrame:
        tree = ET.parse(file_path)
        root = tree.getroot()

        records = []
        for element in root.iter():
            if len(element) > 0:
                record = {}
                has_leaf = False
                for child in element:
                    if child.text and child.text.strip():
                        record[child.tag] = child.text.strip()
                        has_leaf = True
                    elif len(child) > 0:
                        for subchild in child:
                            if subchild.text and subchild.text.strip():
                                record[f"{child.tag}_{subchild.tag}"] = subchild.text.strip()
                                has_leaf = True
                if has_leaf and len(record) >= 3:
                    records.append(record)

        return pd.DataFrame(records) if records else pd.DataFrame()

    def consolidate(
        self,
        primary_source: str,
        join_configs: list[dict[str, Any]],
    ) -> pd.DataFrame:
        if primary_source not in self.sources:
            raise ValueError(f"Primary source '{primary_source}' not loaded")

        result = self.sources[primary_source].copy()
        self.lineage.append({
            "action": "start_consolidation",
            "primary_source": primary_source,
            "primary_rows": len(result),
            "timestamp": datetime.now().isoformat(),
        })

        for config in join_configs:
            source_name = config["source"]
            join_key = config.get("on")
            left_key = config.get("left_on", join_key)
            right_key = config.get("right_on", join_key)
            how = config.get("how", "left")
            suffix = config.get("suffix", f"_{source_name}")

            if source_name not in self.sources:
                self.lineage.append({
                    "action": "skip_join",
                    "source": source_name,
                    "reason": "source not loaded",
                    "timestamp": datetime.now().isoformat(),
                })
                continue

            right_df = self.sources[source_name]
            before_rows = len(result)
            cols_before = set(result.columns)

            result = result.merge(
                right_df,
                left_on=left_key,
                right_on=right_key,
                how=how,
                suffixes=("", suffix),
            )

            self.lineage.append({
                "action": "join",
                "source": source_name,
                "join_type": how,
                "join_keys": f"{left_key} = {right_key}",
                "rows_before": before_rows,
                "rows_after": len(result),
                "columns_added": [c for c in result.columns if c not in cols_before],
                "timestamp": datetime.now().isoformat(),
            })

        self.consolidated = result
        self.lineage.append({
            "action": "consolidation_complete",
            "final_rows": len(result),
            "final_columns": len(result.columns),
            "sources_merged": len(join_configs) + 1,
            "timestamp": datetime.now().isoformat(),
        })

        return result

    def get_consolidation_summary(self) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "sources_loaded": {},
            "consolidated_shape": None,
            "lineage": self.lineage,
        }

        for name, df in self.sources.items():
            summary["sources_loaded"][name] = {
                "rows": len(df),
                "columns": len(df.columns),
                "column_names": list(df.columns),
            }

        if self.consolidated is not None:
            summary["consolidated_shape"] = {
                "rows": len(self.consolidated),
                "columns": len(self.consolidated.columns),
                "column_names": list(self.consolidated.columns),
            }

        return summary

    def generate_report(self) -> str:
        lines = []
        lines.append(f"{'='*70}")
        lines.append(f"  DATA CONSOLIDATION REPORT")
        lines.append(f"{'='*70}")
        lines.append("")

        lines.append("  SOURCE SYSTEMS:")
        lines.append(f"  {'─'*50}")
        for name, df in self.sources.items():
            lines.append(f"  [{name}]")
            lines.append(f"    Rows: {len(df):,}  |  Columns: {len(df.columns)}")
            lines.append(f"    Columns: {', '.join(df.columns[:8])}{'...' if len(df.columns) > 8 else ''}")
            lines.append("")

        if self.consolidated is not None:
            lines.append("  CONSOLIDATED OUTPUT:")
            lines.append(f"  {'─'*50}")
            lines.append(f"  Total Rows: {len(self.consolidated):,}")
            lines.append(f"  Total Columns: {len(self.consolidated.columns)}")
            lines.append(f"  Columns: {', '.join(self.consolidated.columns[:10])}{'...' if len(self.consolidated.columns) > 10 else ''}")
            lines.append("")

        lines.append("  DATA LINEAGE:")
        lines.append(f"  {'─'*50}")
        for entry in self.lineage:
            action = entry.get("action", "unknown")
            ts = entry.get("timestamp", "")[:19]
            if action == "load_source":
                lines.append(f"  [{ts}] LOAD: {entry['source_name']} ({entry['format']}) - {entry['rows']} rows")
            elif action == "join":
                lines.append(f"  [{ts}] JOIN: {entry['source']} ({entry['join_type']}) on {entry['join_keys']}")
                lines.append(f"           Rows: {entry['rows_before']} -> {entry['rows_after']}")
            elif action == "consolidation_complete":
                lines.append(f"  [{ts}] DONE: {entry['final_rows']} rows, {entry['final_columns']} columns from {entry['sources_merged']} sources")

        return "\n".join(lines)
