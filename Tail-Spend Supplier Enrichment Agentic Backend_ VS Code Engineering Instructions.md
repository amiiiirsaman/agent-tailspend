# Tail-Spend Supplier Enrichment Agentic Backend: VS Code Engineering Instructions

**Author:** Manus AI  
**Project:** Alaska Airlines and Hawaiian Airlines tail-spend supplier intelligence enrichment  
**Audience:** Python engineers, procurement analytics teams, AI engineering teams, and VS Code users who need to run, inspect, modify, or productionize the supplier-enrichment backend.

## 1. Purpose of This Package

This package contains a modular Python backend that enriches a tail-spend supplier workbook with procurement intelligence. The pipeline was built to support the executive request to capture **what each vendor does**, identify **competitors and supplier alternatives**, describe **typical contract structures**, propose **top savings levers**, and attach **exact URLs leveraged for the study** wherever source links were substantiated.

The backend was intentionally split into multiple agent scripts rather than one large notebook or monolithic script. Each agent owns a narrow part of the workflow, which makes the system easier to debug, test, rerun, and modify in VS Code. The package is also cache-first by default, meaning it can reproduce the final delivered workbook results deterministically when the same cache files are present.

> **Important operating principle:** Do not insert generic search-engine links into the final source column. The URL agent must either provide exact supplier/category source URLs or mark the row as requiring manual validation.

## 2. Recommended VS Code Setup

Open the folder `/home/ubuntu/tailspend_agentic_backend` directly in VS Code. The project is a plain Python package and does not require a web server, database, or container to run locally. Use Python 3.11 because the scripts were tested in that runtime.

```bash
cd /home/ubuntu/tailspend_agentic_backend
sudo pip3 install -r requirements.txt
```

If you want the Market Intelligence Agent to generate new live LLM outputs instead of using the existing enrichment cache and category fallbacks, make sure the runtime environment has an `OPENAI_API_KEY`. The model is controlled through `TAILSPEND_MODEL` and currently defaults to `gpt-4.1-mini`. For production sourcing work, the same script structure can be pointed to a Sonnet-class or equivalent model by changing the model configuration and provider wrapper.

| Setup Item | Instruction | Expected Output |
|---|---|---|
| Open folder | Open `/home/ubuntu/tailspend_agentic_backend` in VS Code. | VS Code shows `agents/`, `tests/`, `orchestrator.py`, and `config.py`. |
| Install dependencies | Run `sudo pip3 install -r requirements.txt`. | Python can import `pandas`, `openpyxl`, `openai`, and `ddgs`. |
| Confirm inputs | Check paths in `config.py`. | The source workbook, final workbook, enrichment cache, and URL cache paths resolve correctly. |
| Run validation | Execute `python3.11 tests/test_first10_regression.py`. | The regression report shows 100/100 exact matches. |

## 3. Project Structure

The codebase is designed around one orchestrator and five specialized agents. The orchestrator controls execution order and workbook output. The agents perform supplier identity normalization, internal peer analysis, market intelligence generation, exact URL sourcing, and QA standardization.

| File or Folder | What It Does | When to Modify It | Output |
|---|---|---|---|
| `config.py` | Stores workbook paths, sheet names, model name, cache paths, output paths, and final Excel column order. | Modify when file locations, model names, sheet names, or output destinations change. | Shared configuration constants. |
| `orchestrator.py` | Loads the workbook, calls agents in order, writes the enriched Excel workbook, and exports backend design metadata. | Modify when changing pipeline order, adding a new agent, or changing workbook output sheets. | Enriched Excel workbook and JSON design metadata. |
| `agents/common.py` | Contains shared string cleaning, supplier keying, JSON cache loading, spend formatting, and utility helpers. | Modify carefully when normalization or cache key rules need to change globally. | Utility functions used by all agents. |
| `agents/supplier_identity_agent.py` | Normalizes supplier rows and identifies likely consolidation/duplicate review hints. | Modify when improving supplier-name matching or deduplication logic. | Canonical supplier records and consolidation hints. |
| `agents/spend_competitor_agent.py` | Uses the spend file to identify in-file supplier peers within the same L2 category. | Modify when internal competitor ranking should use a different logic than same-L2 spend. | `Competitors within Spend`. |
| `agents/market_intelligence_agent.py` | Produces supplier description, savings levers, external competitors, contract structure, confidence, research basis, and review flag. | Modify when changing prompts, fallback templates, LLM model behavior, or category intelligence. | Core AI enrichment fields. |
| `agents/url_agent.py` | Retrieves exact URL references from cache or optional live lookup and rejects weak generic search links. | Modify when improving source scoring, search providers, or URL validation rules. | `Exact URLs Leveraged for Study`. |
| `agents/qa_governance_agent.py` | Standardizes AI output, fills missing fields conservatively, and supports row-level comparison. | Modify when changing final field naming, confidence handling, or governance rules. | Excel-ready field map and QA comparison output. |
| `tests/test_first10_regression.py` | Runs the modular pipeline on the first 10 source rows and compares output to the final workbook. | Modify when adding new final columns or changing expected regression rules. | Regression report, detail CSV, and first-10 workbook. |

## 4. End-to-End Execution Flow

The pipeline begins by loading the source Excel workbook and creating a working dataframe. It then calculates spend-based context from the full workbook, even when only a limited number of rows are being enriched. This is important because internal competitor output depends on the full supplier population, not just the test slice.

After the dataframe is loaded, the Supplier Identity Agent creates canonical row records and cache keys. The Market Intelligence Agent then enriches each unique supplier/category combination using cache-first output, live LLM output if enabled, or structured category fallback. The Spend Competitor Agent provides same-category in-file peer suppliers. The URL Agent appends exact source URLs or a manual-validation marker. Finally, the QA Governance Agent standardizes fields before the orchestrator writes the Excel workbook.

| Step | Component | What Happens | Output |
|---:|---|---|---|
| 1 | `orchestrator.py` | Reads the source workbook and optional row limit. | Source dataframe. |
| 2 | `SupplierIdentityAgent` | Normalizes supplier name, L1 category, L2 category, and spend into a canonical record. | Supplier/category cache key. |
| 3 | `SpendCompetitorAgent` | Calculates top same-L2 peer suppliers from the full spend context. | Internal competitor field. |
| 4 | `MarketIntelligenceAgent` | Retrieves or generates vendor summary, external competitors, contract structure, and savings levers. | AI enrichment fields. |
| 5 | `ExactURLAgent` | Looks up exact source URLs by supplier/category key and optionally performs live lookup for missing links. | Exact URL field or manual-validation marker. |
| 6 | `QAGovernanceAgent` | Standardizes field names, confidence, research basis, and review flags. | Final Excel-ready row values. |
| 7 | `orchestrator.py` | Writes workbook sheets and design metadata. | Enriched Excel workbook. |

## 5. Agent-by-Agent Engineering Instructions

### 5.1 Supplier Identity Agent

The Supplier Identity Agent lives in `agents/supplier_identity_agent.py`. Its job is to convert raw workbook rows into stable, canonical supplier records that can be used by downstream agents. It reads columns such as `Supplier Name`, `Cleansed Vendor Name`, SpendSphere Level 1, SpendSphere Level 2, and spend. It creates the normalized fields that drive cache lookup, duplicate review, and supplier/category matching.

This agent should remain deterministic. Do not call an LLM from this agent. If supplier matching needs to become more sophisticated, add deterministic fuzzy matching or a separate matching utility and keep the output explainable. The cache key produced by this agent is important because the enrichment and URL caches depend on it.

| Interface | Detail | Output |
|---|---|---|
| Primary class | `SupplierIdentityAgent` | Instantiable agent object. |
| Main method | `normalize_row(row)` | Dictionary with supplier name, cleansed vendor, L1, L2, spend, and cache key. |
| Supporting method | `duplicate_suggestions(vendor_names)` | Consolidation-review hints for likely duplicate vendor names. |
| Downstream dependency | Used by `orchestrator.py`, `MarketIntelligenceAgent`, and `ExactURLAgent`. | Stable supplier/category keys. |

### 5.2 Spend-Internal Competitor Agent

The Spend-Internal Competitor Agent lives in `agents/spend_competitor_agent.py`. This agent does not search the web. It only uses the workbook itself to identify suppliers that appear in the same SpendSphere Level 2 category. The output answers the executive question, “Do competitors or peer suppliers already exist in our spend file?”

For regression testing, this agent must use the full source workbook as context, not only the first 10 rows. If a developer tests only a row slice without passing full context, the competitor field may be logically valid for the slice but will not match the delivered workbook. The orchestrator’s `context_df` argument exists to handle that scenario.

| Interface | Detail | Output |
|---|---|---|
| Primary class | `SpendCompetitorAgent` | Instantiable internal peer-analysis agent. |
| Main method | `competitors_within_spend(df)` | Mapping from row index to top same-L2 suppliers and spend values. |
| Ranking logic | Same L2 category, excluding the row’s supplier, sorted by spend. | Internal peer supplier list. |
| Key caution | Use full workbook context for regression and production runs. | Consistent `Competitors within Spend` values. |

### 5.3 Market Intelligence Agent

The Market Intelligence Agent lives in `agents/market_intelligence_agent.py`. This is the main AI procurement-intelligence agent. It produces the human-readable enrichment fields: what the supplier likely does, market competitors, typical contract structure, savings levers, confidence, research basis, and review flag.

The agent is designed to work in three tiers. First, it checks the enrichment cache to reproduce validated prior outputs. Second, if live LLM mode is available and configured, it can generate new outputs from supplier and category context. Third, if live LLM output is unavailable or intentionally disabled, it uses structured category fallback templates so the workbook is still complete. This layered behavior prevents the pipeline from failing on obscure suppliers while preserving traceability through confidence and review flags.

When modifying this agent, keep the output schema stable unless you also update `QAGovernanceAgent`, `APPENDED_COLUMNS` in `config.py`, the workbook writer, and the regression test. If you change prompts, expect regression output to differ unless the cache is still used.

| Interface | Detail | Output |
|---|---|---|
| Primary class | `MarketIntelligenceAgent` | Instantiable AI enrichment agent. |
| Main method | `enrich_batch(records)` | List of enrichment dictionaries for supplier/category records. |
| Single-row method | `enrich_one(record)` | One enrichment dictionary. |
| Required fields | `what_they_do`, `top_3_savings_levers`, `market_competitors`, `contract_structure`, `confidence`, `research_basis`, `review_flag`. | Standardized procurement intelligence fields. |
| Fallback behavior | Uses category templates if cache or live LLM is unavailable. | Complete workbook with confidence/review markers. |

### 5.4 Exact URL Agent

The Exact URL Agent lives in `agents/url_agent.py`. This agent exists because the source column must be credible. It should provide exact links that were found for the supplier/category study, or it should clearly mark the row for manual validation. It must not fill the workbook with generic search URLs just to avoid blanks.

The default behavior is cache-first. The agent loads the validated exact URL cache and retrieves links using the supplier/category cache key. If a row has no validated exact URL, it returns `NO EXACT SUPPLIER URL RETRIEVED - MANUAL VALIDATION REQUIRED`. If `--live-urls` is passed to the orchestrator, the agent can perform live organic lookup through the configured search package, score candidate URLs by supplier-name relevance, reject generic search pages, and return candidate links that meet the threshold.

Developers should treat this as a governance-sensitive agent. If source scoring is relaxed too much, the workbook can end up with unrelated pages. If it is too strict, more rows will require manual validation. For executive-facing work, conservative behavior is better than false precision.

| Interface | Detail | Output |
|---|---|---|
| Primary class | `ExactURLAgent` | Instantiable source-citation agent. |
| Main method | `enrich_record(row, live=False)` | Dictionary containing exact URL text and source status. |
| Cache behavior | Uses exact URL cache keyed by supplier/category. | Semicolon-separated exact source URLs. |
| Live behavior | Optional organic lookup and relevance scoring. | Candidate exact URLs for cache misses. |
| Guardrail | Reject search-engine links and weak generic source links. | Cleaner final citation column. |
| Failure mode | Returns manual-validation marker, not fabricated links. | Honest row-level source status. |

### 5.5 QA & Governance Agent

The QA & Governance Agent lives in `agents/qa_governance_agent.py`. It converts raw enrichment dictionaries into stable Excel-ready fields. It also ensures that missing values are filled conservatively and that confidence and review flags follow the expected format.

This agent is the right place to change output field names or validation rules. For example, if the business wants `AI Review Flag` to become `Procurement Review Required`, change the mapping here and update `config.py` and the regression test. Do not scatter final column-name logic across multiple agents.

| Interface | Detail | Output |
|---|---|---|
| Primary class | `QAGovernanceAgent` | Instantiable QA and mapping agent. |
| Main method | `standardize_market_output(enrichment, record)` | Cleaned and complete enrichment dictionary. |
| Mapping method | `excel_field_map(standardized)` | Final workbook column names and values. |
| Comparison method | `compare_rows(produced, expected, columns)` | Field-level match details for regression checks. |
| Governance role | Normalizes confidence, review flag, and missing values. | Stable Excel-ready output. |

### 5.6 Shared Utility Module

The shared utility module lives in `agents/common.py`. It is not an agent, but it is critical because all agents rely on its normalization and cache helpers. Functions here include text cleaning, supplier key generation, JSONL cache loading, URL cache support, and spend formatting.

Change this file cautiously. A small change to string normalization or cache-key logic can cause many cache misses and make the workbook differ from the delivered final output. If you need to change cache key logic, run the first-10 regression test immediately and inspect the detail CSV.

| Utility Area | Purpose | Output |
|---|---|---|
| Text cleaning | Normalize whitespace, casing assumptions, and missing values. | Stable text values. |
| Cache keying | Build supplier/category keys used by market and URL caches. | Cache lookup keys. |
| Cache loading | Load JSON and JSONL data for deterministic reproduction. | Python dictionaries/lists. |
| Formatting | Format spend values and semicolon-separated output strings. | Human-readable workbook values. |

## 6. Orchestrator Instructions

The orchestrator lives in `orchestrator.py`. This is the file to run when you want to create an enriched workbook. It loads data, instantiates agents, calls agents in the correct sequence, manages row-level output, and writes the workbook.

Use `--limit` for development and testing. Use `--live-urls` only when you intentionally want the URL Agent to attempt source discovery for missing rows. Do not use live modes when trying to reproduce the delivered workbook exactly because live search and live LLM outputs can vary over time.

```bash
cd /home/ubuntu/tailspend_agentic_backend
python3.11 orchestrator.py --limit 10 --output outputs/first10_agentic_output.xlsx
python3.11 orchestrator.py --output outputs/full_agentic_output.xlsx
python3.11 orchestrator.py --live-urls --output outputs/full_agentic_output_live_urls.xlsx
```

| Argument | Meaning | Recommended Use | Output |
|---|---|---|---|
| `--input` | Source workbook path. | Use when moving the workbook to a new location. | Source dataframe. |
| `--sheet` | Source worksheet name. | Use if the input workbook sheet changes. | Selected source sheet. |
| `--output` | Output workbook path. | Use for test and production output separation. | Enriched workbook. |
| `--limit` | Number of rows to process. | Use for development, first-10 tests, and debugging. | Partial enriched workbook. |
| `--live-urls` | Enables live URL lookup for cache misses. | Use only when refreshing or expanding source citations. | Workbook with refreshed source column. |
| `--json-design` | Path for agent design metadata JSON. | Use when documenting or integrating the backend. | Agent design JSON file. |

## 7. Configuration Instructions

The configuration file is `config.py`. This is the first file to inspect if a path breaks or the pipeline cannot find the workbook. It stores source paths, output paths, sheet names, model name, cache paths, batch size, and final appended-column order.

Most changes should be made through environment variables rather than hardcoding new paths. This keeps the repo portable between local VS Code, sandbox, and production environments.

| Setting | Meaning | Default Behavior | Override |
|---|---|---|---|
| `DEFAULT_INPUT_WORKBOOK` | Original supplier spend workbook. | Reads uploaded Excel file. | `TAILSPEND_INPUT` |
| `DEFAULT_FINAL_WORKBOOK` | Final delivered enriched workbook used for regression. | Reads exact-URL enriched workbook. | `TAILSPEND_FINAL_WORKBOOK` |
| `ENRICHMENT_CACHE` | Cache of market intelligence outputs. | Reuses validated AI enrichment. | `TAILSPEND_ENRICHMENT_CACHE` |
| `EXACT_URL_CACHE` | Cache of exact supplier URLs. | Reuses validated source URLs. | `TAILSPEND_EXACT_URL_CACHE` |
| `LLM_MODEL` | Model used when live enrichment is enabled. | Uses configured default. | `TAILSPEND_MODEL` |
| `APPENDED_COLUMNS` | Final enrichment columns and ordering. | Keeps URL column at the end. | Modify code only after updating tests. |

## 8. Regression Test Instructions

The regression test lives in `tests/test_first10_regression.py`. It runs the modular backend on the first 10 source rows and compares those results to the first 10 rows of the final delivered AI-enriched workbook. It validates both the pipeline and the column-level mapping.

The test uses the full source workbook as context for internal competitor logic. This is intentional. If only the first 10 rows are used as context, the `Competitors within Spend` field will not match the final workbook because the final workbook ranked competitors from all 2,726 rows.

```bash
cd /home/ubuntu/tailspend_agentic_backend
python3.11 tests/test_first10_regression.py
```

| Test Output | Path | Meaning |
|---|---|---|
| First-10 workbook | `outputs/first10_agentic_output.xlsx` | The enriched output generated by the modular backend. |
| Regression report | `outputs/first10_regression_report.md` | Human-readable pass/fail summary. |
| Regression detail CSV | `outputs/first10_regression_details.csv` | Row-by-row and column-by-column comparison results. |

The most recent validation passed with **100 exact matches out of 100 field comparisons**. If the test fails after code changes, open `outputs/first10_regression_details.csv` and inspect the `match`, `produced_value`, and `expected_value` columns. That file will show exactly which row and field changed.

## 9. How to Safely Modify the Backend

The safest engineering workflow is to modify one agent at a time and rerun the first-10 regression test after every meaningful change. If the change is intended to improve logic rather than reproduce the prior workbook exactly, document the expected mismatch and create a new baseline workbook.

| Change Type | File to Edit First | Also Update | Required Test |
|---|---|---|---|
| Change final column names | `agents/qa_governance_agent.py` | `config.py`, `tests/test_first10_regression.py`, workbook documentation. | First-10 regression. |
| Add a new enrichment field | `market_intelligence_agent.py` | `qa_governance_agent.py`, `config.py`, `orchestrator.py`, regression test. | First-10 regression and workbook open check. |
| Improve supplier deduplication | `supplier_identity_agent.py` | Potentially `common.py` if cache keying changes. | First-10 regression plus duplicate-review spot check. |
| Change internal competitor logic | `spend_competitor_agent.py` | Regression expected baseline if logic intentionally changes. | First-10 regression with full context. |
| Refresh exact URLs | `url_agent.py` | URL cache and source audit process. | First-10 regression and manual source spot check. |
| Change model or prompt | `market_intelligence_agent.py` | Enrichment cache baseline if outputs are regenerated. | First-10 regression and category review. |

## 10. Productionization Notes

For a production workflow, keep the agents modular but add orchestration controls around retry logic, rate limits, source audit logging, and approval workflows. The current package is suitable for local VS Code execution and deterministic workbook regeneration. A production version should persist source audit records, track exact prompt/model versions, distinguish cache-generated versus live-generated outputs, and require human review for low-confidence suppliers or rows with missing exact source URLs.

The pipeline is intentionally conservative about URL citations. A row with a manual-validation marker is preferable to a row with an unrelated source. For procurement decisions, especially in aviation-related categories, human review should remain part of the operating model.

| Production Need | Recommended Enhancement | Why It Matters |
|---|---|---|
| Auditability | Store prompt version, model version, timestamp, and exact source candidates per row. | Enables review and defensibility. |
| Source quality | Maintain a source audit table with accepted and rejected URLs. | Prevents weak references from entering executive files. |
| Human approval | Add a review queue for low-confidence and manual-validation rows. | Reduces risk in ambiguous supplier cases. |
| Refresh cadence | Version enrichment caches by run date. | Makes changes over time explainable. |
| Cost and runtime control | Add batch retry, rate limiting, and checkpointing. | Improves reliability for large workbooks. |

## 11. Quick Start Commands

Use the following commands when handing this package to another engineer. They cover installation, first-10 validation, and full workbook generation.

```bash
cd /home/ubuntu/tailspend_agentic_backend
sudo pip3 install -r requirements.txt
python3.11 tests/test_first10_regression.py
python3.11 orchestrator.py --output outputs/full_agentic_output.xlsx
```

| Command | Purpose | Expected Result |
|---|---|---|
| `sudo pip3 install -r requirements.txt` | Installs dependencies. | Python imports succeed. |
| `python3.11 tests/test_first10_regression.py` | Validates the modular backend against final first-10 rows. | 100/100 matches if caches and workbooks are unchanged. |
| `python3.11 orchestrator.py --output outputs/full_agentic_output.xlsx` | Regenerates the enriched workbook. | Full workbook written to `outputs/`. |
| `python3.11 orchestrator.py --live-urls --output outputs/full_agentic_output_live_urls.xlsx` | Attempts live URL refresh for missing source rows. | Workbook with refreshed URL field where exact links are found. |

## 12. Final Deliverables in This Package

The package includes the modular Python backend, a regression test, a VS Code handoff, and previously generated first-10 validation artifacts. If you move this project to another machine, also move or remap the input workbook and cache files referenced in `config.py`.

| Deliverable | Path | Description |
|---|---|---|
| VS Code handoff | `TAILSPEND_AGENTIC_BACKEND_VSCODE_HANDOFF.md` | This engineering instruction document. |
| Orchestrator | `orchestrator.py` | Main pipeline runner. |
| Agent scripts | `agents/*.py` | Modular agent implementations, including the URL agent. |
| Regression test | `tests/test_first10_regression.py` | First-10 validation against the final workbook. |
| Regression report | `outputs/first10_regression_report.md` | 100/100 field-match summary. |
| Regression detail CSV | `outputs/first10_regression_details.csv` | Detailed comparison output. |
| First-10 workbook | `outputs/first10_agentic_output.xlsx` | Test workbook produced by the modular backend. |
