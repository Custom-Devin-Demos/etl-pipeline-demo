import json
from datetime import datetime
from typing import Any


class PipelineStep:
    def __init__(self, name: str, step_type: str, config: dict[str, Any]):
        self.name = name
        self.step_type = step_type
        self.config = config
        self.status = "pending"
        self.output: Any = None
        self.duration_ms: int = 0
        self.error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.step_type,
            "config": self.config,
            "status": self.status,
            "duration_ms": self.duration_ms,
            "error": self.error,
        }


class PipelineBuilder:
    def __init__(self):
        self.steps: list[PipelineStep] = []
        self.pipeline_config: dict[str, Any] = {}

    def generate_pipeline_from_sources(
        self,
        source_profiles: list[dict[str, Any]],
        quality_reports: list[dict[str, Any]],
        target_model: dict[str, Any],
    ) -> dict[str, Any]:
        self.steps = []

        for profile in source_profiles:
            source_name = profile.get("source", "unknown")
            fmt = profile.get("format", "csv")
            self.steps.append(PipelineStep(
                f"extract_{self._safe_name(source_name)}",
                "extract",
                {
                    "source": source_name,
                    "format": fmt,
                    "row_count": profile.get("row_count", 0),
                    "strategy": "full_load" if profile.get("row_count", 0) < 100000 else "chunked",
                    "chunk_size": 10000 if profile.get("row_count", 0) >= 100000 else None,
                },
            ))

        for qr in quality_reports:
            source_name = qr.get("source", "unknown")
            recommendations = qr.get("recommendations", [])
            transform_ops = self._recommendations_to_transforms(recommendations, qr)
            self.steps.append(PipelineStep(
                f"validate_{self._safe_name(source_name)}",
                "validate",
                {
                    "source": source_name,
                    "quality_score": qr.get("overall_score", 0),
                    "quality_grade": qr.get("grade", "F"),
                    "min_acceptable_score": 60,
                    "halt_on_critical": True,
                },
            ))
            if transform_ops:
                self.steps.append(PipelineStep(
                    f"clean_{self._safe_name(source_name)}",
                    "transform",
                    {
                        "source": source_name,
                        "operations": transform_ops,
                    },
                ))

        if len(source_profiles) > 1:
            self.steps.append(PipelineStep(
                "consolidate_sources",
                "consolidate",
                {
                    "strategy": "merge_on_keys",
                    "sources": [p.get("source", "unknown") for p in source_profiles],
                    "join_keys": self._detect_join_keys(source_profiles),
                },
            ))

        if target_model:
            self.steps.append(PipelineStep(
                "transform_to_target",
                "transform",
                {
                    "target_schema": target_model.get("schema_type", "star_schema"),
                    "fact_table": target_model.get("fact_table", {}).get("name", "fact_table"),
                    "dimension_tables": [d["name"] for d in target_model.get("dimension_tables", [])],
                },
            ))

        self.steps.append(PipelineStep(
            "load_to_target",
            "load",
            {
                "target_type": "data_warehouse",
                "write_mode": "upsert",
                "batch_size": 5000,
                "enable_audit_columns": True,
            },
        ))

        self.steps.append(PipelineStep(
            "post_load_validation",
            "validate",
            {
                "checks": ["row_count_match", "schema_match", "null_check", "referential_integrity"],
            },
        ))

        self.pipeline_config = {
            "generated_at": datetime.now().isoformat(),
            "pipeline_name": "zero_touch_etl_pipeline",
            "version": "1.0.0",
            "total_steps": len(self.steps),
            "steps": [s.to_dict() for s in self.steps],
            "execution_config": {
                "parallelism": min(4, len(source_profiles)),
                "retry_policy": {"max_retries": 3, "backoff_seconds": 30},
                "timeout_minutes": 60,
                "notifications": {"on_failure": True, "on_success": True},
            },
        }

        return self.pipeline_config

    def generate_pipeline_code(self) -> str:
        lines = []
        lines.append('"""')
        lines.append("Auto-generated ETL Pipeline")
        lines.append(f"Generated: {self.pipeline_config.get('generated_at', 'N/A')}")
        lines.append(f"Steps: {self.pipeline_config.get('total_steps', 0)}")
        lines.append('"""')
        lines.append("import logging")
        lines.append("import time")
        lines.append("from datetime import datetime")
        lines.append("")
        lines.append("import pandas as pd")
        lines.append("")
        lines.append("logger = logging.getLogger(__name__)")
        lines.append("")
        lines.append("")
        lines.append("class GeneratedPipeline:")
        lines.append("    def __init__(self):")
        lines.append("        self.start_time = None")
        lines.append("        self.dataframes = {}")
        lines.append("        self.metrics = {}")
        lines.append("")
        lines.append("    def run(self):")
        lines.append('        self.start_time = datetime.now()')
        lines.append('        logger.info("Pipeline started at %s", self.start_time)')
        lines.append("")

        for step in self.steps:
            method_name = step.name.lower().replace("-", "_").replace(" ", "_")
            lines.append(f"        self._{method_name}()")

        lines.append("")
        lines.append("        elapsed = datetime.now() - self.start_time")
        lines.append('        logger.info("Pipeline completed in %s", elapsed)')
        lines.append("        return self.metrics")
        lines.append("")

        for step in self.steps:
            method_name = step.name.lower().replace("-", "_").replace(" ", "_")
            lines.append(f"    def _{method_name}(self):")
            lines.append(f'        logger.info("Executing step: {step.name}")')
            lines.append(f"        start = time.time()")

            if step.step_type == "extract":
                fmt = step.config.get("format", "csv")
                source = step.config.get("source", "unknown")
                safe = self._safe_name(source)
                if fmt == "csv":
                    lines.append(f'        self.dataframes["{safe}"] = pd.read_csv("{source}")')
                elif fmt == "json":
                    lines.append(f'        self.dataframes["{safe}"] = pd.read_json("{source}")')
                else:
                    lines.append(f'        # Extract from {fmt} source: {source}')
                    lines.append(f'        pass')
            elif step.step_type == "validate":
                lines.append(f"        # Run quality validation checks")
                lines.append(f"        pass")
            elif step.step_type == "transform":
                ops = step.config.get("operations", [])
                if ops:
                    for op in ops:
                        lines.append(f'        # Transform: {op}')
                else:
                    lines.append(f"        # Apply target model transformations")
                lines.append(f"        pass")
            elif step.step_type == "consolidate":
                lines.append(f"        # Consolidate multiple data sources")
                lines.append(f"        pass")
            elif step.step_type == "load":
                lines.append(f"        # Load to target data warehouse")
                lines.append(f"        pass")

            lines.append(f'        self.metrics["{step.name}"] = {{"duration_s": round(time.time() - start, 3)}}')
            lines.append(f'        logger.info("Step {step.name} completed")')
            lines.append("")

        return "\n".join(lines)

    def generate_report(self) -> str:
        if not self.pipeline_config:
            return "No pipeline generated yet."

        lines = []
        lines.append(f"{'='*70}")
        lines.append(f"  AUTOMATED PIPELINE DESIGN")
        lines.append(f"{'='*70}")
        lines.append(f"  Pipeline: {self.pipeline_config.get('pipeline_name', 'N/A')}")
        lines.append(f"  Generated: {self.pipeline_config.get('generated_at', 'N/A')}")
        lines.append(f"  Total Steps: {self.pipeline_config.get('total_steps', 0)}")
        lines.append("")

        lines.append("  EXECUTION FLOW:")
        lines.append(f"  {'─'*50}")

        steps = self.pipeline_config.get("steps", [])
        for i, step in enumerate(steps):
            connector = "  |" if i < len(steps) - 1 else "  "
            icon = {"extract": ">>", "validate": "??", "transform": "~~", "consolidate": "++", "load": "<<"}
            step_icon = icon.get(step["type"], "  ")
            lines.append(f"  {step_icon} Step {i+1}: {step['name']} [{step['type'].upper()}]")
            for k, v in step["config"].items():
                if k not in ("operations",):
                    lines.append(f"  {connector}   {k}: {v}")
            if i < len(steps) - 1:
                lines.append(f"  {connector}")
                lines.append(f"  v")

        exec_config = self.pipeline_config.get("execution_config", {})
        if exec_config:
            lines.append("")
            lines.append("  EXECUTION CONFIG:")
            lines.append(f"  Parallelism: {exec_config.get('parallelism', 1)}")
            retry = exec_config.get("retry_policy", {})
            lines.append(f"  Retries: {retry.get('max_retries', 0)} (backoff: {retry.get('backoff_seconds', 0)}s)")
            lines.append(f"  Timeout: {exec_config.get('timeout_minutes', 60)} min")

        return "\n".join(lines)

    def _recommendations_to_transforms(self, recommendations: list[str], qr: dict[str, Any]) -> list[str]:
        transforms = []
        for rec in recommendations:
            rec_lower = rec.lower()
            if "missing values" in rec_lower or "imputation" in rec_lower:
                transforms.append("fill_missing_values")
            if "duplicate" in rec_lower:
                transforms.append("remove_duplicates")
            if "email" in rec_lower:
                transforms.append("validate_email_format")
            if "phone" in rec_lower:
                transforms.append("standardize_phone_format")
            if "negative" in rec_lower:
                transforms.append("handle_negative_values")
            if "outlier" in rec_lower:
                transforms.append("cap_outliers")
        return list(set(transforms))

    def _detect_join_keys(self, profiles: list[dict[str, Any]]) -> list[str]:
        all_columns: dict[str, int] = {}
        for p in profiles:
            for col_name in p.get("columns", {}):
                if col_name.endswith("_id") or col_name.endswith("_key"):
                    all_columns[col_name] = all_columns.get(col_name, 0) + 1
        return [col for col, count in all_columns.items() if count > 1]

    def _safe_name(self, name: str) -> str:
        import os
        import re
        base = os.path.splitext(os.path.basename(name))[0]
        return re.sub(r"[^a-zA-Z0-9_]", "_", base).lower()
