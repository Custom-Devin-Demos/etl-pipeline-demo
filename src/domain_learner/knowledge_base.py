import json
import os
from datetime import datetime
from typing import Any


class DomainKnowledgeBase:
    def __init__(self, storage_dir: str = "demo/domain_knowledge"):
        self.storage_dir = storage_dir
        self.knowledge: dict[str, Any] = {
            "domain": None,
            "entities": {},
            "relationships": [],
            "business_rules": [],
            "data_patterns": [],
            "glossary": {},
            "learned_at": None,
        }
        os.makedirs(storage_dir, exist_ok=True)

    def learn_from_profiles(self, profiles: list[dict[str, Any]]) -> dict[str, Any]:
        entities: dict[str, dict[str, Any]] = {}
        all_columns: dict[str, list[str]] = {}

        for profile in profiles:
            source = profile.get("source", "unknown")
            table_name = os.path.splitext(os.path.basename(source))[0]

            entity: dict[str, Any] = {
                "source": source,
                "row_count": profile.get("row_count", 0),
                "attributes": {},
            }

            columns = profile.get("columns", {})
            for col_name, col_info in columns.items():
                entity["attributes"][col_name] = {
                    "dtype": col_info.get("dtype", "unknown"),
                    "semantic_type": col_info.get("semantic_type"),
                    "inferred_role": col_info.get("inferred_role"),
                    "nullable": col_info.get("null_count", 0) > 0,
                }

                if col_name not in all_columns:
                    all_columns[col_name] = []
                all_columns[col_name].append(table_name)

            entities[table_name] = entity

        relationships = []
        for col_name, tables in all_columns.items():
            if len(tables) > 1 and (col_name.endswith("_id") or col_name.endswith("_key")):
                for i in range(len(tables)):
                    for j in range(i + 1, len(tables)):
                        relationships.append({
                            "entity_a": tables[i],
                            "entity_b": tables[j],
                            "join_column": col_name,
                            "relationship_type": "foreign_key",
                        })

        domain = self._infer_domain(entities)
        glossary = self._build_glossary(entities)
        business_rules = self._infer_business_rules(entities, profiles)
        patterns = self._detect_patterns(profiles)

        self.knowledge = {
            "domain": domain,
            "entities": entities,
            "relationships": relationships,
            "business_rules": business_rules,
            "data_patterns": patterns,
            "glossary": glossary,
            "learned_at": datetime.now().isoformat(),
        }

        return self.knowledge

    def _infer_domain(self, entities: dict[str, Any]) -> str:
        all_columns = set()
        for entity in entities.values():
            all_columns.update(entity.get("attributes", {}).keys())

        domain_signals: dict[str, int] = {
            "e-commerce": 0,
            "healthcare": 0,
            "finance": 0,
            "logistics": 0,
            "manufacturing": 0,
            "retail": 0,
        }

        ecommerce_terms = ["product", "customer", "order", "transaction", "cart", "price", "payment", "shipping", "return", "inventory"]
        healthcare_terms = ["patient", "diagnosis", "prescription", "treatment", "claim", "provider"]
        finance_terms = ["account", "balance", "interest", "loan", "credit", "debit"]
        logistics_terms = ["shipment", "warehouse", "carrier", "tracking", "delivery", "freight"]

        for col in all_columns:
            col_lower = col.lower()
            for term in ecommerce_terms:
                if term in col_lower:
                    domain_signals["e-commerce"] += 1
                    domain_signals["retail"] += 1
            for term in healthcare_terms:
                if term in col_lower:
                    domain_signals["healthcare"] += 1
            for term in finance_terms:
                if term in col_lower:
                    domain_signals["finance"] += 1
            for term in logistics_terms:
                if term in col_lower:
                    domain_signals["logistics"] += 1

        for entity_name in entities:
            name_lower = entity_name.lower()
            for term in ecommerce_terms:
                if term in name_lower:
                    domain_signals["e-commerce"] += 3

        if max(domain_signals.values()) == 0:
            return "general"

        return max(domain_signals, key=lambda k: domain_signals[k])

    def _build_glossary(self, entities: dict[str, Any]) -> dict[str, str]:
        glossary: dict[str, str] = {}

        term_definitions = {
            "customer_id": "Unique identifier for a customer entity",
            "product_id": "Unique identifier for a product in the catalog",
            "transaction_id": "Unique identifier for a business transaction",
            "order_id": "Unique identifier for a purchase order",
            "quantity": "Number of units involved in a transaction",
            "unit_price": "Price per single unit of a product",
            "total_amount": "Calculated total value of a transaction",
            "email": "Electronic mail address for communication",
            "phone": "Telephone contact number",
            "registration_date": "Date when the entity was first registered",
            "loyalty_tier": "Customer loyalty program classification level",
            "payment_method": "Method of payment used for transaction",
            "status": "Current state or condition of the record",
            "category": "Classification group for products",
            "brand": "Product manufacturer or brand name",
            "supplier_id": "Unique identifier for a supplier entity",
            "warehouse_id": "Unique identifier for a storage facility",
            "sku": "Stock Keeping Unit - unique product variant identifier",
            "reorder_point": "Minimum inventory level triggering restocking",
            "tracking_number": "Shipment tracking reference number",
            "carrier": "Shipping or logistics provider company",
            "refund_amount": "Monetary value returned to customer",
            "return_date": "Date when product was returned",
        }

        all_cols = set()
        for entity in entities.values():
            all_cols.update(entity.get("attributes", {}).keys())

        for col in all_cols:
            col_lower = col.lower()
            for term, definition in term_definitions.items():
                if term in col_lower:
                    glossary[col] = definition
                    break

        return glossary

    def _infer_business_rules(self, entities: dict[str, Any], profiles: list[dict[str, Any]]) -> list[dict[str, str]]:
        rules = []

        for profile in profiles:
            columns = profile.get("columns", {})
            for col_name, col_info in columns.items():
                if "price" in col_name.lower() or "amount" in col_name.lower() or "cost" in col_name.lower():
                    stats = col_info.get("stats", {})
                    if stats.get("negatives", 0) > 0:
                        rules.append({
                            "rule": f"{col_name} should be non-negative (except for refunds/adjustments)",
                            "type": "validation",
                            "column": col_name,
                        })
                    else:
                        rules.append({
                            "rule": f"{col_name} must be a positive numeric value",
                            "type": "constraint",
                            "column": col_name,
                        })

                if col_name.endswith("_id") and col_info.get("null_count", 0) == 0:
                    rules.append({
                        "rule": f"{col_name} is required (NOT NULL) for referential integrity",
                        "type": "constraint",
                        "column": col_name,
                    })

                if col_info.get("semantic_type") == "email":
                    rules.append({
                        "rule": f"{col_name} must follow valid email format (RFC 5322)",
                        "type": "format",
                        "column": col_name,
                    })

        return rules

    def _detect_patterns(self, profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
        patterns = []

        for profile in profiles:
            source = profile.get("source", "unknown")
            summary = profile.get("summary", {})

            if summary.get("duplicate_rows", 0) > 0:
                patterns.append({
                    "pattern": "duplicate_data",
                    "source": source,
                    "description": f"Source contains {summary['duplicate_rows']} duplicate rows",
                    "action": "Apply deduplication during transformation",
                })

            if summary.get("null_percentage", 0) > 10:
                patterns.append({
                    "pattern": "sparse_data",
                    "source": source,
                    "description": f"Source has {summary['null_percentage']}% null values",
                    "action": "Implement null handling strategy (default values, imputation, or filtering)",
                })

        return patterns

    def save_knowledge(self) -> str:
        filepath = os.path.join(self.storage_dir, "domain_knowledge.json")
        with open(filepath, "w") as f:
            json.dump(self.knowledge, f, indent=2, default=str)
        return filepath

    def generate_report(self) -> str:
        lines = []
        lines.append(f"{'='*70}")
        lines.append(f"  DOMAIN KNOWLEDGE REPORT")
        lines.append(f"{'='*70}")
        lines.append(f"  Inferred Domain: {self.knowledge.get('domain', 'N/A')}")
        lines.append(f"  Learned: {self.knowledge.get('learned_at', 'N/A')}")
        lines.append("")

        entities = self.knowledge.get("entities", {})
        lines.append(f"  DISCOVERED ENTITIES ({len(entities)}):")
        lines.append(f"  {'─'*50}")
        for name, entity in entities.items():
            attrs = entity.get("attributes", {})
            lines.append(f"  [{name}] ({entity.get('row_count', 0)} records)")
            for attr_name, attr_info in attrs.items():
                role = attr_info.get("inferred_role", "")
                sem = attr_info.get("semantic_type", "")
                extra = f" ({sem})" if sem else f" ({role})" if role else ""
                lines.append(f"    - {attr_name}: {attr_info.get('dtype', 'unknown')}{extra}")
            lines.append("")

        relationships = self.knowledge.get("relationships", [])
        if relationships:
            lines.append(f"  DISCOVERED RELATIONSHIPS ({len(relationships)}):")
            lines.append(f"  {'─'*50}")
            for rel in relationships:
                lines.append(f"  {rel['entity_a']} <--[{rel['join_column']}]--> {rel['entity_b']}")
            lines.append("")

        rules = self.knowledge.get("business_rules", [])
        if rules:
            lines.append(f"  INFERRED BUSINESS RULES ({len(rules)}):")
            lines.append(f"  {'─'*50}")
            for rule in rules:
                lines.append(f"  [{rule['type'].upper()}] {rule['rule']}")
            lines.append("")

        glossary = self.knowledge.get("glossary", {})
        if glossary:
            lines.append(f"  DOMAIN GLOSSARY ({len(glossary)} terms):")
            lines.append(f"  {'─'*50}")
            for term, definition in sorted(glossary.items()):
                lines.append(f"  {term:30s} {definition}")
            lines.append("")

        patterns = self.knowledge.get("data_patterns", [])
        if patterns:
            lines.append(f"  DATA PATTERNS ({len(patterns)}):")
            lines.append(f"  {'─'*50}")
            for p in patterns:
                lines.append(f"  [{p['pattern']}] {p['description']}")
                lines.append(f"    Action: {p['action']}")

        return "\n".join(lines)
