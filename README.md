# Tail-Spend Supplier Enrichment Agentic Backend

Multi-agent pipeline that enriches supplier records (Tail-Spend) with
source-grounded "what they do", savings levers, market competitors, and
contract structure. Built around a hard-evidence regime: every exported
row carries either a real supplier-website citation, a secondary-listing
citation, or a controlled `manual review required` neutral statement —
never category-template prose.

## Stack

- Python 3.11
- AWS Bedrock — Claude Sonnet 4.5 (`us.anthropic.claude-sonnet-4-5-20250929-v1:0`) via boto3
- DDGS for live web search (with a deterministic second-pass URL resolver)
- pandas + openpyxl for Excel I/O

## Layout

```
config.py                     # Constants (allowed bases, sentinels, prompt version)
orchestrator.py               # End-to-end pipeline (load → URL → LLM → QA → workbook)
agents/
  url_agent.py                # URL discovery, classification, second-pass resolver
  market_intelligence_agent.py# LLM enrichment (default / partner_source / secondary_listing)
  qa_governance_agent.py      # Standardize + evidence calibration
  final_validator.py          # Hard pre-export quality gate
  supplier_identity_agent.py  # Vendor normalization
  spend_competitor_agent.py   # Within-spend competitor logic
tests/
  test_evidence_gating.py               # Unit-level QA regression (13 tests)
  test_first100_live_research_pilot.py  # Live 100-row pilot harness
  test_first100_quality_gates.py        # 8 hard quality gates (pytest)
outputs/                      # Generated workbooks, caches, audits
```

## Quality Gates

Eight hard gates the first-100 pilot (and full run) MUST pass before
delivery:

1. No row has a banned `category inference` research basis.
2. No row's `What they do` contains generic category-template phrases.
3. Every row has a non-blank `Exact URLs Leveraged for Study` value.
4. No forbidden URL placeholder markers (`NO EXACT SOURCE FOUND`, etc.).
5. Every Tier-A row has at least one non-weak supplier-grade URL.
6. Every Tier-B row uses an allowed basis (`secondary listing`,
   `supplier website + secondary listing`, or `manual review required`).
7. Every Low/Medium-confidence row has `AI Review Flag = Yes`.
8. Every Tier-B row has `AI Review Flag = Yes`.

Run them:

```powershell
python -m pytest tests/test_first100_quality_gates.py -v
```

## First-100 Pilot Run

```powershell
python tests/test_first100_live_research_pilot.py
```

Outputs:
- `outputs/first100_live_research_output.xlsx` — final workbook
- `outputs/first100_live_research_quality_report.md` — pass/fail report
- `outputs/first100_enrichment_cache.generated.jsonl` — LLM cache
- `outputs/first100_exact_urls.generated.json` — URL cache

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
# Configure .env with AWS_REGION + Bedrock-capable IAM credentials.
```

## Status

First-100 live pilot: **PASS** (0 failures, 8/8 gates green).
Approved for the full 2,726-row run on go-ahead.
