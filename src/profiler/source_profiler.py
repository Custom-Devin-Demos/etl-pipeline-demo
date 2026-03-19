import json
import os
from datetime import datetime
from typing import Any

import pandas as pd


class SourceProfiler:
    def __init__(self):
        self.profile_results: dict[str, Any] = {}

    def profile_csv(self, file_path: str) -> dict[str, Any]:
        df = pd.read_csv(file_path)
        return self._profile_dataframe(df, file_path, "csv")

    def profile_json(self, file_path: str) -> dict[str, Any]:
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

        return self._profile_dataframe(df, file_path, "json")

    def profile_xml(self, file_path: str) -> dict[str, Any]:
        import xml.etree.ElementTree as ET

        tree = ET.parse(file_path)
        root = tree.getroot()

        records = []
        for element in root.iter():
            if len(element) > 0 and any(child.text and child.text.strip() for child in element):
                record = {}
                for child in element:
                    if child.text and child.text.strip():
                        record[child.tag] = child.text.strip()
                    elif len(child) > 0:
                        for subchild in child:
                            if subchild.text and subchild.text.strip():
                                record[f"{child.tag}_{subchild.tag}"] = subchild.text.strip()
                if record:
                    records.append(record)

        df = pd.DataFrame(records) if records else pd.DataFrame()
        return self._profile_dataframe(df, file_path, "xml")

    def profile_auto(self, file_path: str) -> dict[str, Any]:
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".csv":
            return self.profile_csv(file_path)
        elif ext == ".json":
            return self.profile_json(file_path)
        elif ext == ".xml":
            return self.profile_xml(file_path)
        else:
            raise ValueError(f"Unsupported file format: {ext}")

    def _profile_dataframe(self, df: pd.DataFrame, source: str, format_type: str) -> dict[str, Any]:
        profile: dict[str, Any] = {
            "source": source,
            "format": format_type,
            "profiled_at": datetime.now().isoformat(),
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": {},
            "summary": {},
        }

        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].apply(lambda x: json.dumps(x, default=str) if isinstance(x, (list, dict)) else x)

        total_cells = len(df) * len(df.columns)
        total_nulls = int(df.isnull().sum().sum())
        try:
            total_duplicates = int(df.duplicated().sum())
        except TypeError:
            total_duplicates = 0

        profile["summary"] = {
            "total_cells": total_cells,
            "total_null_cells": total_nulls,
            "null_percentage": round(total_nulls / total_cells * 100, 2) if total_cells > 0 else 0,
            "duplicate_rows": total_duplicates,
            "duplicate_percentage": round(total_duplicates / len(df) * 100, 2) if len(df) > 0 else 0,
            "memory_usage_bytes": int(df.memory_usage(deep=True).sum()),
        }

        for col in df.columns:
            col_profile = self._profile_column(df[col])
            profile["columns"][col] = col_profile

        self.profile_results[source] = profile
        return profile

    def _profile_column(self, series: pd.Series) -> dict[str, Any]:
        col_info: dict[str, Any] = {
            "dtype": str(series.dtype),
            "null_count": int(series.isnull().sum()),
            "null_percentage": round(series.isnull().sum() / len(series) * 100, 2) if len(series) > 0 else 0,
            "unique_count": int(series.nunique()),
            "unique_percentage": round(series.nunique() / len(series) * 100, 2) if len(series) > 0 else 0,
        }

        non_null = series.dropna()

        if pd.api.types.is_numeric_dtype(series):
            col_info["stats"] = {
                "mean": round(float(non_null.mean()), 4) if len(non_null) > 0 else None,
                "median": round(float(non_null.median()), 4) if len(non_null) > 0 else None,
                "std": round(float(non_null.std()), 4) if len(non_null) > 1 else None,
                "min": float(non_null.min()) if len(non_null) > 0 else None,
                "max": float(non_null.max()) if len(non_null) > 0 else None,
                "zeros": int((non_null == 0).sum()),
                "negatives": int((non_null < 0).sum()),
            }
        elif pd.api.types.is_string_dtype(series):
            lengths = non_null.astype(str).str.len()
            col_info["stats"] = {
                "min_length": int(lengths.min()) if len(lengths) > 0 else None,
                "max_length": int(lengths.max()) if len(lengths) > 0 else None,
                "avg_length": round(float(lengths.mean()), 2) if len(lengths) > 0 else None,
            }

            if col_info["unique_percentage"] > 90:
                col_info["inferred_role"] = "identifier"
            elif col_info["unique_count"] <= 20:
                col_info["inferred_role"] = "categorical"
                col_info["top_values"] = non_null.value_counts().head(10).to_dict()
            else:
                col_info["inferred_role"] = "text"

            email_pattern = non_null.astype(str).str.match(r"^[\w.+-]+@[\w-]+\.[\w.]+$")
            if email_pattern.sum() > len(non_null) * 0.8:
                col_info["semantic_type"] = "email"

            phone_pattern = non_null.astype(str).str.match(r"^[\d\s\-\(\)\+]+$")
            if phone_pattern.sum() > len(non_null) * 0.8 and col_info.get("inferred_role") != "identifier":
                col_info["semantic_type"] = "phone"

            date_parseable = 0
            for val in non_null.head(20):
                try:
                    pd.to_datetime(val)
                    date_parseable += 1
                except (ValueError, TypeError):
                    pass
            sample_size = min(20, len(non_null))
            if sample_size > 0 and date_parseable / sample_size > 0.8:
                col_info["semantic_type"] = "date"

        return col_info

    def generate_report(self, profile: dict[str, Any]) -> str:
        lines = []
        lines.append(f"{'='*70}")
        lines.append(f"  SOURCE DATA PROFILE REPORT")
        lines.append(f"{'='*70}")
        lines.append(f"  Source: {profile['source']}")
        lines.append(f"  Format: {profile['format'].upper()}")
        lines.append(f"  Profiled: {profile['profiled_at']}")
        lines.append(f"{'='*70}")
        lines.append("")

        summary = profile["summary"]
        lines.append("  DATASET OVERVIEW")
        lines.append(f"  {'-'*40}")
        lines.append(f"  Rows:            {profile['row_count']:,}")
        lines.append(f"  Columns:         {profile['column_count']}")
        lines.append(f"  Total Cells:     {summary['total_cells']:,}")
        lines.append(f"  Null Cells:      {summary['total_null_cells']:,} ({summary['null_percentage']}%)")
        lines.append(f"  Duplicate Rows:  {summary['duplicate_rows']} ({summary['duplicate_percentage']}%)")
        lines.append(f"  Memory Usage:    {summary['memory_usage_bytes']:,} bytes")
        lines.append("")

        lines.append("  COLUMN DETAILS")
        lines.append(f"  {'-'*40}")

        for col_name, col_info in profile["columns"].items():
            lines.append(f"  [{col_name}]")
            lines.append(f"    Type: {col_info['dtype']}")
            lines.append(f"    Nulls: {col_info['null_count']} ({col_info['null_percentage']}%)")
            lines.append(f"    Unique: {col_info['unique_count']} ({col_info['unique_percentage']}%)")

            if "semantic_type" in col_info:
                lines.append(f"    Semantic Type: {col_info['semantic_type']}")
            if "inferred_role" in col_info:
                lines.append(f"    Inferred Role: {col_info['inferred_role']}")

            if "stats" in col_info:
                stats = col_info["stats"]
                for k, v in stats.items():
                    if v is not None and k != "zeros" and k != "negatives":
                        lines.append(f"    {k}: {v}")
                if stats.get("negatives", 0) > 0:
                    lines.append(f"    WARNING: {stats['negatives']} negative values detected")

            if "top_values" in col_info:
                lines.append(f"    Top Values: {dict(list(col_info['top_values'].items())[:5])}")
            lines.append("")

        return "\n".join(lines)
