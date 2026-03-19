"""
Playbook: Data Quality Gate
============================
Reusable workflow that enforces quality standards before data enters
the target system. Acts as a gate/checkpoint in the ETL pipeline.

Usage:
    gate = QualityGatePlaybook(min_score=80)
    result = gate.execute(df=my_dataframe, source_name="sales_data")
"""

from datetime import datetime
from typing import Any

import pandas as pd

from src.quality import DataQualityEngine


class QualityGatePlaybook:
    PLAYBOOK_NAME = "quality_gate"
    DESCRIPTION = "Enforce data quality standards with configurable thresholds"

    def __init__(self, min_score: float = 75, block_on_critical: bool = True):
        self.min_score = min_score
        self.block_on_critical = block_on_critical
        self.quality_engine = DataQualityEngine()
        self.results: dict[str, Any] = {}

    def execute(self, df: pd.DataFrame, source_name: str = "unknown") -> dict[str, Any]:
        start = datetime.now()

        print(f"\n  QUALITY GATE: Evaluating '{source_name}'...")
        print(f"  Minimum acceptable score: {self.min_score}")

        quality_report = self.quality_engine.run_all_checks(df, source_name)

        score = quality_report.get("overall_score", 0)
        critical = quality_report.get("critical_failures", 0)

        gate_passed = score >= self.min_score
        if self.block_on_critical and critical > 0:
            gate_passed = False

        if gate_passed:
            decision = "APPROVED"
            action = "Data cleared for loading to target system"
        elif self.block_on_critical and critical > 0:
            decision = "REJECTED"
            action = "Data blocked due to critical quality failures. Fix critical issues before retry."
        elif score >= self.min_score * 0.8:
            decision = "CONDITIONAL"
            action = "Data may proceed with quality remediation applied"
        else:
            decision = "REJECTED"
            action = "Data blocked from target system. Fix quality issues before retry."

        self.results = {
            "playbook": self.PLAYBOOK_NAME,
            "source": source_name,
            "gate_passed": gate_passed,
            "decision": decision,
            "action": action,
            "quality_score": score,
            "quality_grade": quality_report.get("grade", "F"),
            "min_score": self.min_score,
            "critical_failures": critical,
            "total_checks": quality_report.get("total_checks", 0),
            "checks_passed": quality_report.get("passed", 0),
            "checks_failed": quality_report.get("failed", 0),
            "recommendations": quality_report.get("recommendations", []),
            "executed_at": datetime.now().isoformat(),
            "duration": str(datetime.now() - start),
        }

        icon = "++" if gate_passed else "!!"
        print(f"  {icon} GATE {decision}: Score {score}/100 (min: {self.min_score})")

        return self.results

    def generate_report(self) -> str:
        lines = []
        lines.append(f"{'='*60}")
        lines.append(f"  QUALITY GATE RESULT")
        lines.append(f"{'='*60}")

        decision = self.results.get("decision", "N/A")
        lines.append(f"  Decision: {decision}")
        lines.append(f"  Score: {self.results.get('quality_score', 0)}/{self.results.get('min_score', 0)} minimum")
        lines.append(f"  Grade: {self.results.get('quality_grade', 'N/A')}")
        lines.append(f"  Action: {self.results.get('action', 'N/A')}")
        lines.append("")
        lines.append(f"  Checks: {self.results.get('checks_passed', 0)} passed / {self.results.get('checks_failed', 0)} failed")
        lines.append(f"  Critical: {self.results.get('critical_failures', 0)}")

        return "\n".join(lines)
