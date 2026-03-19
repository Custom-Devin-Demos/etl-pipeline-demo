# Zero-Touch ETL Demo

End-to-end demonstration of Devin's automated data engineering capabilities for the Zero-Touch ETL initiative.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full interactive demo
python -m demo.run_demo

# Run in auto mode (non-interactive, no pauses)
DEMO_AUTO_RUN=1 python -m demo.run_demo
```

## Demo Phases

### Phase 1: Source Data Understanding & Profiling
Devin auto-discovers and profiles data sources across formats (CSV, JSON, XML, API):
- Statistical profiling (nulls, distributions, cardinality)
- Semantic type detection (emails, phones, dates, IDs)
- Column role inference (identifier, categorical, text)
- Auto-generated DDL from discovered schemas

### Phase 2: Data Quality Measurement
Comprehensive quality scoring across six dimensions:
- **Completeness**: null analysis per-column and overall
- **Uniqueness**: duplicate detection, ID integrity validation
- **Consistency**: value pattern and whitespace checks
- **Validity**: format validation (email, phone, numeric ranges)
- **Timeliness**: future date and staleness detection
- **Accuracy**: statistical outlier detection (IQR-based)

Quality scores (0-100) with letter grades directly gate pipeline execution.

### Phase 3: Requirements to Target Data Model
Automatic translation from source data to optimized target:
- Star schema inference (fact vs dimension tables)
- Relationship detection (FK candidates)
- Aggregation table design for analytics
- SCD Type 2 slowly-changing dimension support
- Production-ready DDL generation

### Phase 4: Automated Pipeline Development
Pipeline auto-generated from profiles + quality reports + target model:
- Adapts extraction strategy to data volume (full vs chunked)
- Cleaning steps derived from quality findings
- Multi-source consolidation with detected join keys
- Post-load validation as built-in step
- Generates executable Python pipeline code

### Phase 5: Multi-Agent Orchestration & Personas
Four specialized agent personas coordinated sequentially:

| Agent | Role | Key Actions |
|-------|------|-------------|
| **Architect** | Source analysis, model design | Analyzes schemas, designs star schema, generates DDL |
| **Developer** | Pipeline implementation | Builds extractors, transforms, loaders, pipeline code |
| **QA** | Quality validation | Runs assessments, validates contracts, signs off |
| **PM** | Project coordination | Tracks progress, executive summaries, risk management |

Each agent has full audit trail of actions and decisions.

### Phase 6: Reusable Playbooks
Three executable playbook templates:

1. **Data Onboarding** (`demo/playbooks/data_onboarding.py`): End-to-end new source integration
2. **Quality Gate** (`demo/playbooks/quality_gate.py`): Configurable quality threshold enforcement
3. **Cross-System Merge** (`demo/playbooks/cross_system_merge.py`): Multi-format data consolidation

### Phase 7: Domain Learning & Greenfield Scenarios
Devin learns the business domain from data:
- Auto-detects domain (e-commerce, healthcare, finance, etc.)
- Discovers entities, attributes, and relationships
- Infers business rules from data patterns
- Builds domain glossary
- Persists knowledge to `demo/domain_knowledge/` for future sessions

### Phase 8: Data Consolidation Across Siloed Systems
Merges data from multiple formats and systems:
- CSV flat files (legacy systems)
- JSON (modern APIs/services)
- XML (enterprise/legacy integration)
- API responses (real-time feeds)

Full data lineage tracking for every load, join, and transformation.

## Sample Data Sources

| Source | Format | Description |
|--------|--------|-------------|
| `csv/customers.csv` | CSV | Customer master data with loyalty tiers |
| `csv/transactions.csv` | CSV | Purchase transactions with payments |
| `csv/products.csv` | CSV | Product catalog with pricing |
| `json/inventory.json` | JSON | Warehouse inventory levels |
| `json/supplier_orders.json` | JSON | Supplier purchase orders |
| `xml/shipping_records.xml` | XML | Legacy shipping manifests |
| `api_mock/returns_api.json` | JSON | Returns/refunds API response |

## Devin vs Claude Code Comparison

| Capability | Devin | Claude Code |
|---|---|---|
| Multi-agent orchestration | Native (4 personas) | N/A |
| Persistent domain learning | Yes (knowledge base) | No (stateless) |
| Reusable playbooks | Built-in, executable | Manual scripts |
| Auto pipeline generation | End-to-end from data | Partial, needs guidance |
| Quality gate enforcement | Integrated in pipeline | Separate tooling |
| Multi-format consolidation | Automatic (CSV/JSON/XML/API) | Manual per-format |
| Target model design | Auto DDL generation | Manual design |
| Session persistence | Yes (cross-session) | No |
| CI/CD integration | Native | Limited |
| Greenfield onboarding | Zero-touch from data | Requires documentation |

## Project Structure

```
demo/
  run_demo.py                    # Main demo runner (8 phases)
  sample_data/
    csv/                         # CSV sources (customers, transactions, products)
    json/                        # JSON sources (inventory, supplier orders)
    xml/                         # XML sources (shipping records)
    api_mock/                    # API response mocks (returns)
  playbooks/
    data_onboarding.py           # New source onboarding playbook
    quality_gate.py              # Quality gate enforcement playbook
    cross_system_merge.py        # Cross-system merge playbook
  domain_knowledge/              # Persisted domain knowledge (generated)
src/
  profiler/                      # Source data profiling & schema detection
  quality/                       # Data quality engine (6 dimensions)
  model_designer/                # Target data model designer (star schema)
  pipeline_generator/            # Automated pipeline code generation
  orchestrator/                  # Multi-agent orchestration framework
  consolidator/                  # Cross-system data consolidation
  domain_learner/                # Domain knowledge learning & persistence
```
