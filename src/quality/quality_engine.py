from datetime import datetime
from typing import Any

import pandas as pd


class QualityRule:
    def __init__(self, name: str, description: str, severity: str = "warning"):
        self.name = name
        self.description = description
        self.severity = severity
        self.passed = False
        self.details: dict[str, Any] = {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule": self.name,
            "description": self.description,
            "severity": self.severity,
            "passed": self.passed,
            "details": self.details,
        }


class DataQualityEngine:
    SEVERITY_WEIGHTS = {"critical": 0, "error": 25, "warning": 50, "info": 75}

    def __init__(self):
        self.rules: list[QualityRule] = []
        self.results: list[dict[str, Any]] = []

    def run_all_checks(self, df: pd.DataFrame, source_name: str = "unknown") -> dict[str, Any]:
        self.rules = []
        self.results = []

        self._check_completeness(df)
        self._check_uniqueness(df)
        self._check_consistency(df)
        self._check_validity(df)
        self._check_timeliness(df)
        self._check_accuracy(df)

        total = len(self.rules)
        passed = sum(1 for r in self.rules if r.passed)
        failed = total - passed

        critical_failures = sum(1 for r in self.rules if not r.passed and r.severity == "critical")
        error_failures = sum(1 for r in self.rules if not r.passed and r.severity == "error")

        if total > 0:
            base_score = (passed / total) * 100
            penalty = critical_failures * 15 + error_failures * 8
            score = max(0, round(base_score - penalty, 1))
        else:
            score = 100.0

        report = {
            "source": source_name,
            "checked_at": datetime.now().isoformat(),
            "overall_score": score,
            "grade": self._score_to_grade(score),
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "critical_failures": critical_failures,
            "error_failures": error_failures,
            "rules": [r.to_dict() for r in self.rules],
            "recommendations": self._generate_recommendations(),
        }

        return report

    def _check_completeness(self, df: pd.DataFrame) -> None:
        rule = QualityRule("completeness_overall", "Check overall data completeness", "error")
        total_cells = df.shape[0] * df.shape[1]
        null_cells = int(df.isnull().sum().sum())
        completeness_pct = round((1 - null_cells / total_cells) * 100, 2) if total_cells > 0 else 100
        rule.passed = completeness_pct >= 95
        rule.details = {
            "total_cells": total_cells,
            "null_cells": null_cells,
            "completeness_percentage": completeness_pct,
        }
        self.rules.append(rule)

        for col in df.columns:
            null_count = int(df[col].isnull().sum())
            if null_count > 0:
                col_rule = QualityRule(
                    f"completeness_{col}",
                    f"Check completeness for column '{col}'",
                    "warning" if null_count / len(df) < 0.1 else "error",
                )
                col_rule.passed = null_count / len(df) < 0.05
                col_rule.details = {
                    "column": col,
                    "null_count": null_count,
                    "null_percentage": round(null_count / len(df) * 100, 2),
                }
                self.rules.append(col_rule)

    def _check_uniqueness(self, df: pd.DataFrame) -> None:
        rule = QualityRule("no_duplicate_rows", "Check for duplicate rows", "error")
        dup_count = int(df.duplicated().sum())
        rule.passed = dup_count == 0
        rule.details = {"duplicate_rows": dup_count, "duplicate_percentage": round(dup_count / len(df) * 100, 2) if len(df) > 0 else 0}
        self.rules.append(rule)

        for col in df.columns:
            if col.endswith("_id") or col == "id" or col.endswith("_key"):
                non_null = df[col].dropna()
                dup_ids = int(non_null.duplicated().sum())
                unique_ratio = non_null.nunique() / len(non_null) if len(non_null) > 0 else 1.0
                is_primary_key = unique_ratio == 1.0
                if is_primary_key:
                    severity = "critical"
                    description = f"Check uniqueness for primary key column '{col}'"
                else:
                    severity = "info"
                    description = f"Check uniqueness for foreign key column '{col}' (duplicates expected)"
                id_rule = QualityRule(f"uniqueness_{col}", description, severity)
                id_rule.passed = dup_ids == 0 if is_primary_key else True
                id_rule.details = {
                    "column": col,
                    "duplicate_values": dup_ids,
                    "unique_ratio": round(unique_ratio, 4),
                    "inferred_role": "primary_key" if is_primary_key else "foreign_key",
                }
                self.rules.append(id_rule)

    def _check_consistency(self, df: pd.DataFrame) -> None:
        for col in df.select_dtypes(include=["object"]).columns:
            non_null = df[col].dropna()
            if len(non_null) == 0:
                continue

            unique_vals = non_null.unique()
            if len(unique_vals) <= 50:
                inconsistencies = []
                val_set = set(str(v).strip().lower() for v in unique_vals)
                for v1 in val_set:
                    for v2 in val_set:
                        if v1 != v2 and (v1 in v2 or v2 in v1) and abs(len(v1) - len(v2)) <= 2:
                            inconsistencies.append((v1, v2))

                if inconsistencies:
                    rule = QualityRule(f"consistency_{col}", f"Check value consistency for '{col}'", "warning")
                    rule.passed = False
                    rule.details = {
                        "column": col,
                        "potential_inconsistencies": inconsistencies[:5],
                    }
                    self.rules.append(rule)

        for col in df.select_dtypes(include=["object"]).columns:
            non_null = df[col].dropna()
            if len(non_null) == 0:
                continue
            has_leading_trailing = (non_null.astype(str) != non_null.astype(str).str.strip()).sum()
            if has_leading_trailing > 0:
                rule = QualityRule(f"whitespace_{col}", f"Check for whitespace issues in '{col}'", "info")
                rule.passed = False
                rule.details = {"column": col, "affected_rows": int(has_leading_trailing)}
                self.rules.append(rule)

    def _check_validity(self, df: pd.DataFrame) -> None:
        for col in df.columns:
            if "email" in col.lower():
                non_null = df[col].dropna()
                if len(non_null) > 0:
                    rule = QualityRule(f"valid_email_{col}", f"Validate email format in '{col}'", "error")
                    email_pattern = r"^[\w.+-]+@[\w-]+\.[\w.]+$"
                    invalid = non_null[~non_null.astype(str).str.match(email_pattern)]
                    rule.passed = len(invalid) == 0
                    rule.details = {"column": col, "invalid_count": len(invalid), "sample_invalid": list(invalid.head(3))}
                    self.rules.append(rule)

            if "phone" in col.lower():
                non_null = df[col].dropna()
                if len(non_null) > 0:
                    rule = QualityRule(f"valid_phone_{col}", f"Validate phone format in '{col}'", "warning")
                    phone_pattern = r"^\d{3}-\d{4}$|^\d{3}-\d{3}-\d{4}$|^\(\d{3}\)\s?\d{3}-\d{4}$"
                    invalid = non_null[~non_null.astype(str).str.match(phone_pattern)]
                    rule.passed = len(invalid) == 0
                    rule.details = {"column": col, "invalid_count": len(invalid), "sample_invalid": list(invalid.head(3).astype(str))}
                    self.rules.append(rule)

        for col in df.select_dtypes(include=["float64", "int64"]).columns:
            if "price" in col.lower() or "amount" in col.lower() or "cost" in col.lower():
                non_null = df[col].dropna()
                negatives = (non_null < 0).sum()
                if negatives > 0:
                    rule = QualityRule(f"non_negative_{col}", f"Check for negative values in '{col}'", "error")
                    rule.passed = False
                    rule.details = {"column": col, "negative_count": int(negatives)}
                    self.rules.append(rule)

    def _check_timeliness(self, df: pd.DataFrame) -> None:
        date_cols = []
        for col in df.columns:
            if "date" in col.lower() or "time" in col.lower() or "created" in col.lower() or "updated" in col.lower():
                date_cols.append(col)

        for col in date_cols:
            try:
                dates = pd.to_datetime(df[col], errors="coerce")
                future_dates = (dates > pd.Timestamp.now()).sum()
                if future_dates > 0:
                    rule = QualityRule(f"no_future_dates_{col}", f"Check for future dates in '{col}'", "warning")
                    rule.passed = False
                    rule.details = {"column": col, "future_date_count": int(future_dates)}
                    self.rules.append(rule)
            except (ValueError, TypeError):
                pass

    def _check_accuracy(self, df: pd.DataFrame) -> None:
        numeric_cols = df.select_dtypes(include=["float64", "int64"]).columns
        for col in numeric_cols:
            non_null = df[col].dropna()
            if len(non_null) < 5:
                continue
            q1 = non_null.quantile(0.25)
            q3 = non_null.quantile(0.75)
            iqr = q3 - q1
            lower = q1 - 3 * iqr
            upper = q3 + 3 * iqr
            outliers = ((non_null < lower) | (non_null > upper)).sum()
            if outliers > 0:
                rule = QualityRule(f"outliers_{col}", f"Check for statistical outliers in '{col}'", "info")
                rule.passed = outliers / len(non_null) < 0.05
                rule.details = {
                    "column": col,
                    "outlier_count": int(outliers),
                    "outlier_percentage": round(outliers / len(non_null) * 100, 2),
                    "lower_bound": round(float(lower), 2),
                    "upper_bound": round(float(upper), 2),
                }
                self.rules.append(rule)

    def _score_to_grade(self, score: float) -> str:
        if score >= 95:
            return "A"
        elif score >= 85:
            return "B"
        elif score >= 70:
            return "C"
        elif score >= 50:
            return "D"
        return "F"

    def _generate_recommendations(self) -> list[str]:
        recommendations = []
        for rule in self.rules:
            if rule.passed:
                continue
            if "completeness" in rule.name and rule.severity in ("error", "critical"):
                col = rule.details.get("column", "multiple columns")
                recommendations.append(f"Address missing values in '{col}' - consider imputation or requiring at source")
            elif "duplicate" in rule.name:
                recommendations.append("Remove or investigate duplicate rows - define deduplication strategy")
            elif "uniqueness" in rule.name:
                col = rule.details.get("column", "ID column")
                role = rule.details.get("inferred_role", "unknown")
                if role == "primary_key":
                    recommendations.append(f"Fix duplicate IDs in '{col}' - this breaks referential integrity")
                else:
                    recommendations.append(f"Foreign key '{col}' has expected duplicates - verify referential integrity with parent table")
            elif "valid_email" in rule.name:
                recommendations.append("Add email validation at ingestion point")
            elif "valid_phone" in rule.name:
                recommendations.append("Standardize phone number format (e.g., XXX-XXX-XXXX)")
            elif "non_negative" in rule.name:
                col = rule.details.get("column", "financial column")
                recommendations.append(f"Investigate negative values in '{col}' - may indicate data entry errors or refunds")
            elif "outliers" in rule.name:
                col = rule.details.get("column", "numeric column")
                recommendations.append(f"Review outliers in '{col}' for data accuracy")
        return list(set(recommendations))

    def generate_report(self, quality_result: dict[str, Any]) -> str:
        lines = []
        lines.append(f"{'='*70}")
        lines.append(f"  DATA QUALITY REPORT")
        lines.append(f"{'='*70}")
        lines.append(f"  Source: {quality_result['source']}")
        lines.append(f"  Checked: {quality_result['checked_at']}")
        lines.append(f"{'='*70}")
        lines.append("")

        grade = quality_result["grade"]
        score = quality_result["overall_score"]
        grade_bar = "#" * int(score / 2) + "-" * (50 - int(score / 2))
        lines.append(f"  QUALITY SCORE: {score}/100  Grade: {grade}")
        lines.append(f"  [{grade_bar}]")
        lines.append("")

        lines.append(f"  Total Checks: {quality_result['total_checks']}")
        lines.append(f"  Passed:       {quality_result['passed']}")
        lines.append(f"  Failed:       {quality_result['failed']}")
        lines.append(f"  Critical:     {quality_result['critical_failures']}")
        lines.append(f"  Errors:       {quality_result['error_failures']}")
        lines.append("")

        failed_rules = [r for r in quality_result["rules"] if not r["passed"]]
        if failed_rules:
            lines.append("  FAILED CHECKS:")
            lines.append(f"  {'-'*50}")
            for r in failed_rules:
                icon = "!!" if r["severity"] in ("critical", "error") else "  "
                lines.append(f"  {icon} [{r['severity'].upper():8s}] {r['rule']}")
                for k, v in r["details"].items():
                    lines.append(f"       {k}: {v}")
            lines.append("")

        if quality_result["recommendations"]:
            lines.append("  RECOMMENDATIONS:")
            lines.append(f"  {'-'*50}")
            for i, rec in enumerate(quality_result["recommendations"], 1):
                lines.append(f"  {i}. {rec}")

        return "\n".join(lines)
