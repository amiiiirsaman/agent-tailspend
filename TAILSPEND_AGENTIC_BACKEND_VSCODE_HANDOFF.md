# Tail-Spend Supplier Enrichment Agentic Backend: VS Code Handoff

**Author:** Manus AI  
**Project:** Alaska Airlines and Hawaiian Airlines tail-spend supplier enrichment  
**Primary deliverable:** Modular Python agentic backend with exact URL source traceability and first-10-row regression validation.

## Executive Summary

This handoff package converts the prior monolithic enrichment workflow into a modular, VS Code-ready Python backend. The backend is organized as a set of specialized procurement-intelligence agents that collectively enrich supplier rows with **vendor details**, **competitor analysis**, **typical contract structure**, **top savings levers**, and **exact URLs leveraged for the study**. The pipeline is intentionally cache-first so it can reproduce the delivered workbook outputs deterministically while still supporting live LLM and live URL retrieval when enabled.

The package was tested against the first 10 vendors in the source workbook and compared to the final AI-enriched Excel workbook. The regression test produced **100 exact matches out of 100 field comparisons**, validating that the modular code reproduces the final enriched first-10 rows when using the same enrichment and URL caches.

| Validation Item | Result |
|---|---:|
| Source rows tested | 10 |
| Enriched columns compared | 10 |
| Field comparisons | 100 |
| Exact matches | 100 |
| Mismatches | 0 |
| Match rate | 100.00% |

## Folder Structure

The backend package is located at `/home/ubuntu/tailspend_agentic_backend`. It is designed to be opened directly in VS Code as a project folder.

| Path | Purpose | Output |
|---|---|---|
| `config.py` | Centralizes workbook paths, model settings, cache paths, and output file names. | Shared runtime configuration. |
| `orchestrator.py` | Runs the end-to-end agentic enrichment pipeline and writes an enriched workbook. | Excel workbook plus agent design JSON. |
| `agents/common.py` | Provides shared normalization, keying, cache, and formatting utilities. | Reusable helper functions. |
| `agents/supplier_identity_agent.py` | Normalizes supplier names and generates duplicate/consolidation-review hints. | Canonical supplier records and duplicate review notes. |
| `agents/spend_competitor_agent.py` | Calculates in-file same-category competitors from the full spend dataset. | `Competitors within Spend` values. |
| `agents/market_intelligence_agent.py` | Produces supplier summary, market competitors, contract model, savings levers, confidence, research basis, and review flag. | AI/category enrichment fields. |
| `agents/url_agent.py` | Retrieves and validates exact supplier/category source URLs; rejects generic search links. | `Exact URLs Leveraged for Study` and source audit metadata. |
| `agents/qa_governance_agent.py` | Standardizes generated outputs and supports field-level regression comparison. | Validated Excel-ready field mappings. |
| `tests/test_first10_regression.py` | Runs first-10-vendor regression validation against the delivered final workbook. | Markdown report, CSV comparison detail, and first-10 workbook. |
| `requirements.txt` | Python package dependencies for VS Code setup. | Installable dependency list. |

## Agent Design

The backend follows a practical multi-agent design in which each agent has a narrow responsibility and deterministic boundaries. This improves auditability because supplier identity, internal spend competitors, market intelligence, URL traceability, and quality governance can each be inspected independently.

| Agent | Script | Role | Output |
|---|---|---|---|
| Supplier Identity Agent | `agents/supplier_identity_agent.py` | Normalizes supplier names, creates canonical row records, and detects likely duplicate supplier names for consolidation review. | Canonical records and consolidation review hints. |
| Spend-Internal Competitor Agent | `agents/spend_competitor_agent.py` | Uses the spend file itself to identify other suppliers in the same L2 category, ranked by total spend. | Internal peer supplier list per row. |
| Market Competitor and Contract Intelligence Agent | `agents/market_intelligence_agent.py` | Uses cache-first LLM outputs and category fallback logic to populate supplier summary, external competitors, commercial model, and savings levers. | AI enrichment fields for procurement review. |
| Exact URL Agent | `agents/url_agent.py` | Provides exact source URLs from the validated URL cache and can perform live exact URL search for cache misses. Generic search-result pages are rejected. | Exact URLs or manual-validation marker. |
| QA & Governance Agent | `agents/qa_governance_agent.py` | Standardizes field values, normalizes confidence/review flags, and supports regression comparison. | Excel-ready fields and validation results. |

## Runtime Modes

The backend supports two main operating patterns. The default is **cache-first reproducibility**, which is the recommended mode when validating or regenerating the workbook from the delivered enrichment results. Optional live retrieval can be enabled when expanding or refreshing the study.

| Mode | How to Run | Best Use | Output |
|---|---|---|---|
| Cache-first reproduction | `python3.11 orchestrator.py --limit 10 --output outputs/first10_agentic_output.xlsx` | Reproduce prior enrichment values from validated caches without new LLM/search variability. | Deterministic Excel output. |
| Full workbook regeneration | `python3.11 orchestrator.py --output outputs/full_agentic_output.xlsx` | Regenerate all rows using existing caches and deterministic fallback where needed. | Full enriched workbook. |
| Live URL refresh | `python3.11 orchestrator.py --live-urls --output outputs/full_agentic_output_live_urls.xlsx` | Attempt live exact URL lookup for missing source URLs. | Enriched workbook with refreshed URL fields. |
| Regression validation | `python3.11 tests/test_first10_regression.py` | Verify first-10 modular output matches the final delivered workbook. | Markdown report and CSV comparison detail. |

## Setup Instructions for VS Code

Open the folder `/home/ubuntu/tailspend_agentic_backend` in VS Code. Use Python 3.11 and install dependencies in the environment you plan to use. If running in the current sandbox, the required packages are already available or can be installed with the command below.

```bash
cd /home/ubuntu/tailspend_agentic_backend
sudo pip3 install -r requirements.txt
```

If you want live LLM enrichment rather than cache-first reproduction, ensure that `OPENAI_API_KEY` is available in the environment. The implementation defaults to `gpt-4.1-mini`, but the model can be changed with `TAILSPEND_MODEL`. For a production-grade workflow, this design can be pointed to a Sonnet-class or equivalent procurement-capable LLM through the same agent interface.

```bash
export TAILSPEND_MODEL=gpt-4.1-mini
python3.11 orchestrator.py --limit 10 --output outputs/first10_agentic_output.xlsx
```

## First-10 Regression Test

The regression test intentionally uses the **full source workbook as spend context** while only producing the first 10 rows. This matters because `Competitors within Spend` in the final workbook was computed against all 2,726 rows, not only the first 10-row slice. Without full-file context, internal peer supplier fields would differ even if the AI fields were correct.

```bash
cd /home/ubuntu/tailspend_agentic_backend
python3.11 tests/test_first10_regression.py
```

| Test Artifact | Path | Output |
|---|---|---|
| First-10 enriched workbook | `outputs/first10_agentic_output.xlsx` | Modular pipeline output for first 10 source rows. |
| Regression report | `outputs/first10_regression_report.md` | Summary showing 100/100 exact matches. |
| Regression details | `outputs/first10_regression_details.csv` | Row/column-level comparison detail. |

## Exact URL Agent Notes

The `ExactURLAgent` is deliberately conservative. It loads the validated exact URL cache by supplier/category key and returns the exact links used in the final workbook. If no exact URL was validated, it returns `NO EXACT SUPPLIER URL RETRIEVED - MANUAL VALIDATION REQUIRED` instead of inserting a generic search link. When live lookup is enabled, the agent rejects search-engine, maps, and social-media search URLs, then scores organic results based on supplier-name token relevance.

| URL Agent Behavior | Description | Output |
|---|---|---|
| Cache lookup | Uses `vendor | L1 | L2` to retrieve previously validated exact URLs. | Semicolon-separated exact URLs. |
| Generic-link rejection | Removes Google, Bing, DuckDuckGo, search pages, maps pages, and weak social search URLs. | Cleaner citation trail. |
| Manual validation flag | Marks rows without exact supplier URLs rather than fabricating source evidence. | Manual-validation text in final column. |
| Optional live search | Uses `ddgs` to retrieve organic candidate URLs and relevance-score them. | Exact URLs where confidence threshold is met. |

## Important Files Consumed by the Package

The package expects the original workbook, delivered final workbook, enrichment cache, and exact URL cache to remain available at their current locations unless overridden through environment variables.

| Input | Default Path | Override Variable |
|---|---|---|
| Source workbook | `/home/ubuntu/upload/Tailspend_ProjectReimagine_20260512.xlsx` | `TAILSPEND_INPUT` |
| Final enriched workbook | `/home/ubuntu/tailspend_enrichment_output/Alaska_Airlines_Tailspend_AI_Enriched_exact_URLs.xlsx` | `TAILSPEND_FINAL_WORKBOOK` |
| Enrichment cache | `/home/ubuntu/tailspend_enrichment_output/enrichment_cache.jsonl` | `TAILSPEND_ENRICHMENT_CACHE` |
| Exact URL cache | `/home/ubuntu/tailspend_enrichment_output/exact_urls_ddgs_cache.json` | `TAILSPEND_EXACT_URL_CACHE` |

## Governance Notes

The enrichment fields should be treated as **procurement intelligence and sourcing hypotheses**, not final supplier due diligence. Rows marked for AI review or manual URL validation should be checked before executive negotiation, supplier consolidation, or sourcing-event decisions. High-spend categories, regulated aviation operations, safety-critical suppliers, and ambiguous supplier names should receive human validation even when the model confidence is high.
