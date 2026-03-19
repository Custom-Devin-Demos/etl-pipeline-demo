"""
Playbook: New Data Source Onboarding
=====================================
Reusable workflow for onboarding any new data source into the ETL platform.
This playbook automates the entire process from discovery to pipeline deployment.

Usage:
    playbook = DataOnboardingPlaybook()
    result = playbook.execute(source_path="path/to/data.csv")
"""

import os
from datetime import datetime
from typing import Any

import pandas as pd

from src.profiler import SourceProfiler, SchemaDetector
from src.quality import DataQualityEngine


class DataOnboardingPlaybook:
    PLAYBOOK_NAME = "data_onboarding"
    DESCRIPTION = "End-to-end onboarding of a new data source with profiling, quality checks, and schema generation"

    STEPS = [
        "discover_source",
        "profile_data",
        "detect_schema",
        "assess_quality",
        "generate_recommendations",
        "produce_onboarding_report",
    ]

    def __init__(self):
        self.profiler = SourceProfiler()
        self.schema_detector = SchemaDetector()
        self.quality_engine = DataQualityEngine()
        self.results: dict[str, Any] = {}

    def execute(self, source_path: str) -> dict[str, Any]:
        start = datetime.now()
        self.results = {"playbook": self.PLAYBOOK_NAME, "source": source_path, "steps": {}}

        print(f"\n{'='*60}")
        print(f"  PLAYBOOK: {self.DESCRIPTION}")
        print(f"{'='*60}")

        print(f"\n  Step 1/6: Discovering source...")
        source_info = self._discover_source(source_path)
        self.results["steps"]["discover"] = source_info

        print(f"  Step 2/6: Profiling data...")
        profile = self.profiler.profile_auto(source_path)
        self.results["steps"]["profile"] = profile

        print(f"  Step 3/6: Detecting schema...")
        schema = self.schema_detector.detect_schema(profile)
        self.results["steps"]["schema"] = schema

        print(f"  Step 4/6: Assessing data quality...")
        ext = os.path.splitext(source_path)[1].lower()
        if ext == ".csv":
            df = pd.read_csv(source_path)
        elif ext == ".json":
            import json
            with open(source_path) as f:
                raw = json.load(f)
            if isinstance(raw, list):
                df = pd.json_normalize(raw)
            elif isinstance(raw, dict):
                for key in ["data", "inventory", "items"]:
                    if key in raw and isinstance(raw[key], list):
                        df = pd.json_normalize(raw[key])
                        break
                else:
                    df = pd.json_normalize(raw)
            else:
                df = pd.DataFrame([raw])
        else:
            df = pd.DataFrame()

        quality = self.quality_engine.run_all_checks(df, source_path)
        self.results["steps"]["quality"] = quality

        print(f"  Step 5/6: Generating recommendations...")
        recommendations = self._generate_recommendations(profile, schema, quality)
        self.results["steps"]["recommendations"] = recommendations

        print(f"  Step 6/6: Producing onboarding report...")
        elapsed = datetime.now() - start
        self.results["execution_time"] = str(elapsed)
        self.results["completed_at"] = datetime.now().isoformat()

        return self.results

    def _discover_source(self, source_path: str) -> dict[str, Any]:
        stat = os.stat(source_path)
        return {
            "file_path": source_path,
            "file_size_bytes": stat.st_size,
            "file_extension": os.path.splitext(source_path)[1],
            "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        }

    def _generate_recommendations(
        self,
        profile: dict[str, Any],
        schema: dict[str, Any],
        quality: dict[str, Any],
    ) -> list[dict[str, str]]:
        recs = []

        if quality.get("overall_score", 100) < 80:
            recs.append({
                "priority": "high",
                "area": "data_quality",
                "recommendation": f"Quality score is {quality['overall_score']}/100. Address failed checks before production use.",
            })

        if profile.get("summary", {}).get("duplicate_rows", 0) > 0:
            recs.append({
                "priority": "medium",
                "area": "deduplication",
                "recommendation": "Implement deduplication logic in the transformation layer.",
            })

        for col_name, col_info in profile.get("columns", {}).items():
            if col_info.get("null_percentage", 0) > 20:
                recs.append({
                    "priority": "medium",
                    "area": "completeness",
                    "recommendation": f"Column '{col_name}' has {col_info['null_percentage']}% nulls. Define imputation strategy.",
                })

        if schema.get("primary_key_candidates"):
            recs.append({
                "priority": "info",
                "area": "schema",
                "recommendation": f"Suggested primary key: {', '.join(schema['primary_key_candidates'])}",
            })

        if schema.get("foreign_key_candidates"):
            recs.append({
                "priority": "info",
                "area": "relationships",
                "recommendation": f"Potential foreign keys detected: {', '.join(schema['foreign_key_candidates'])}. Validate against related tables.",
            })

        return recs

    def generate_report(self) -> str:
        lines = []
        lines.append(f"{'='*60}")
        lines.append(f"  DATA ONBOARDING REPORT")
        lines.append(f"{'='*60}")
        lines.append(f"  Source: {self.results.get('source', 'N/A')}")
        lines.append(f"  Completed: {self.results.get('completed_at', 'N/A')}")
        lines.append(f"  Duration: {self.results.get('execution_time', 'N/A')}")
        lines.append("")

        quality = self.results.get("steps", {}).get("quality", {})
        lines.append(f"  Quality Score: {quality.get('overall_score', 'N/A')}/100 (Grade: {quality.get('grade', 'N/A')})")
        lines.append("")

        recs = self.results.get("steps", {}).get("recommendations", [])
        if recs:
            lines.append("  RECOMMENDATIONS:")
            for r in recs:
                lines.append(f"  [{r['priority'].upper():6s}] [{r['area']}] {r['recommendation']}")

        return "\n".join(lines)
