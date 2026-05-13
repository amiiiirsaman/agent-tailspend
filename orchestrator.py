"""Agentic orchestration pipeline for Alaska/Hawaiian Airlines tail-spend enrichment.

Two execution modes are supported:

* ``--mode cache-replay`` (default): reproduce previously validated outputs from the
  enrichment cache and the URL cache. May use category fallback templates only when
  ``--allow-fallback`` is present.
* ``--mode live-research``: perform live web research per supplier/category row,
  fetch evidence, and call the LLM for evidence-grounded enrichment. Rows that
  cannot be researched are flagged (research-failed marker on the URL column,
  ``LIVE_RESEARCH_FAILED`` review flag) instead of being silently filled with
  category templates. Only when ``--allow-fallback`` is passed will category
  templates be used as a last resort, and they will be tagged
  ``research_basis = "category inference (fallback)"`` so audits can spot them.

Examples::

    python orchestrator.py --mode live-research \
        --source-workbook "Tailspend_Project Reimagine_20260512.xlsx" \
        --limit 10 --write-caches \
        --output outputs/first10_live_research_output.xlsx

    python orchestrator.py --mode cache-replay \
        --enrichment-cache outputs/enrichment_cache.generated.jsonl \
        --url-cache outputs/exact_urls.generated.json \
        --limit 10 --output outputs/first10_replay_output.xlsx
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

try:  # auto-load .env so OPENAI_API_KEY is available without manual export
    from dotenv import load_dotenv
    load_dotenv()
except Exception:  # pragma: no cover
    pass

from config import (
    APPENDED_COLUMNS,
    BATCH_SIZE,
    DEFAULT_INPUT_WORKBOOK,
    DEFAULT_OUTPUT_DIR,
    ENRICHMENT_CACHE,
    EXACT_URL_CACHE,
    EXECUTIVE_READY_SHEET,
    FAST_FALLBACK,
    GENERATED_ENRICHMENT_CACHE,
    GENERATED_URL_CACHE,
    LLM_MODEL,
    MANUAL_VALIDATION_SHEET,
    OUTPUT_WORKBOOK,
    PROMPT_VERSION,
    QUARANTINE_REASON_CONTRADICTION,
    QUARANTINE_REASON_GROUNDING_FAILED,
    QUARANTINE_REASON_LISTING_ONLY,
    QUARANTINE_REASON_NO_URL,
    QUARANTINE_REASON_NO_EVIDENCE_GRADE,
    RESEARCH_BASIS_MANUAL_REVIEW,
    REVIEW_FLAG_MANUAL,
    SOURCE_SHEET,
    SOURCE_URL_AUDIT,
    UNRESOLVED_NEUTRAL_STATEMENT,
    UNRESOLVED_URL_SENTINEL,
    URL_RESEARCH_FAILED_TEXT,
    URL_MANUAL_VALIDATION_TEXT,
)
from agents.common import clean_text, load_jsonl_cache, make_enrichment_key
from agents.final_validator import (
    FinalOutputValidationError,
    validate_final_enrichment_row,
    validate_no_final_category_inference,
)
from agents.grounding_agent import (
    check_claim_grounding,
    check_contradiction,
    evidence_snippets_from_packet,
)
from agents.market_intelligence_agent import MarketIntelligenceAgent
from agents.qa_governance_agent import QAGovernanceAgent
from agents.research_evidence_agent import ResearchEvidenceAgent
from agents.spend_competitor_agent import SpendCompetitorAgent
from agents.supplier_identity_agent import SupplierIdentityAgent
from agents.url_agent import (
    ExactURLAgent,
    URLResult,
    WEAK_EVIDENCE_DOMAINS,
    _host_in_set,
)


AGENTIC_BACKEND_DESIGN = {
    "objective": "Enrich airline tail-spend supplier records with vendor details, competitor analysis, typical contract structure, savings levers, and exact source URLs.",
    "modes": ["cache-replay", "live-research"],
    "agents": [
        {"agent": "Supplier Identity Agent", "script": "agents/supplier_identity_agent.py"},
        {"agent": "Spend-Internal Competitor Agent", "script": "agents/spend_competitor_agent.py"},
        {"agent": "Exact URL Agent", "script": "agents/url_agent.py"},
        {"agent": "Research Evidence Agent", "script": "agents/research_evidence_agent.py"},
        {"agent": "Market Competitor and Contract Intelligence Agent", "script": "agents/market_intelligence_agent.py"},
        {"agent": "QA & Governance Agent", "script": "agents/qa_governance_agent.py"},
    ],
}


# ----------------------------------------------------------------- io helpers

def load_source_rows(input_workbook: Path, sheet_name: str, limit: int | None = None) -> pd.DataFrame:
    df = pd.read_excel(input_workbook, sheet_name=sheet_name)
    unnamed_empty = [c for c in df.columns if str(c).startswith("Unnamed") and df[c].isna().all()]
    df = df.drop(columns=unnamed_empty)
    if limit is not None:
        df = df.head(limit).copy()
    return df


def build_unique_units(df: pd.DataFrame, identity_agent: SupplierIdentityAgent) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    units: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        record = identity_agent.normalize_row(row)
        if record["cache_key"] not in seen:
            seen.add(record["cache_key"])
            record["record_id"] = len(units)
            units.append(record)
    return units


def _persist_quarantine(market_agent: "MarketIntelligenceAgent",
                        unit: Dict[str, Any],
                        accepted_urls: List[str],
                        standardized: Dict[str, Any],
                        reason: str) -> None:
    """Write a quarantine decision to the LLM cache so cache-replay is deterministic.

    The cache loader uses last-wins semantics on cache_key; appending this record
    causes future replays to surface the quarantine outcome via the cached path
    in :class:`MarketIntelligenceAgent` instead of re-running grounding logic.
    """
    cache_key = unit.get("cache_key")
    if not cache_key:
        return
    record = dict(standardized)
    record["cache_key"] = cache_key
    record["model"] = market_agent.model
    record["prompt_version"] = "quarantine-v2"
    record["timestamp"] = _dt.datetime.utcnow().isoformat() + "Z"
    record["source_urls"] = list(accepted_urls or [])
    record["evidence_tier"] = standardized.get("evidence_tier", "")
    record["_quarantined"] = True
    record["_quarantine_reason"] = reason
    try:
        market_agent._persist_generated([record])
    except Exception:
        pass
    # Also update in-memory cache so any subsequent unit read in this run sees it.
    if hasattr(market_agent, "cache") and isinstance(market_agent.cache, dict):
        market_agent.cache[cache_key] = record


def _unresolved_neutral_output(unit: Dict[str, Any], url_result: "URLResult") -> Dict[str, Any]:
    """Manus v4: synthesize a controlled neutral row for suppliers that have
    no defensible non-weak URL after second-pass URL search.

    Every enrichment field carries the same controlled neutral statement (no
    LLM call, no banned phrases, no category-template wording). The row is
    flagged for manual review via confidence='Low' and review_flag='Yes'.
    The research_basis is the new Manus-v4 value
    'manual review required' so the export is internally consistent.
    """
    return {
        "record_id": unit.get("record_id"),
        "what_they_do": UNRESOLVED_NEUTRAL_STATEMENT,
        "top_3_savings_levers": UNRESOLVED_NEUTRAL_STATEMENT,
        "market_competitors": UNRESOLVED_NEUTRAL_STATEMENT,
        "contract_structure": UNRESOLVED_NEUTRAL_STATEMENT,
        "confidence": "Low",
        "research_basis": RESEARCH_BASIS_MANUAL_REVIEW,
        "review_flag": "Yes",
        "_unresolved": True,
        "_unresolved_search_attempts": list(getattr(url_result, "search_attempts", []) or []),
    }


def _persist_unresolved(market_agent: "MarketIntelligenceAgent",
                         unit: Dict[str, Any],
                         url_result: "URLResult",
                         standardized: Dict[str, Any]) -> None:
    """Write the unresolved row into the enrichment cache so cache-replay
    reproduces it deterministically without re-running URL search.
    """
    cache_key = unit.get("cache_key")
    if not cache_key:
        return
    record = dict(standardized)
    record["cache_key"] = cache_key
    record["model"] = market_agent.model
    record["prompt_version"] = f"{PROMPT_VERSION}+unresolved-v4"
    record["timestamp"] = _dt.datetime.utcnow().isoformat() + "Z"
    record["source_urls"] = list(url_result.accepted_urls or [])
    record["evidence_tier"] = url_result.evidence_tier or "C"
    record["_unresolved"] = True
    try:
        market_agent._persist_generated([record])
    except Exception:
        pass
    if hasattr(market_agent, "cache") and isinstance(market_agent.cache, dict):
        market_agent.cache[cache_key] = record


def _unresolved_url_text(url_result: "URLResult") -> str:
    """Build the URL-column text for an unresolved row.

    Best-effort weak URLs (semicolon-joined) when any accepted URL exists, so
    the audit trail is preserved in the workbook. Otherwise the controlled
    sentinel string. NEVER blank, NEVER a banned placeholder marker.
    """
    if url_result.accepted_urls:
        return "; ".join(url_result.accepted_urls)
    return UNRESOLVED_URL_SENTINEL


def write_workbook(enriched: pd.DataFrame, output_path: Path, input_workbook: Path,
                   source_sheet: str, mode: str) -> None:
    """Write the enriched workbook (Manus v3: single AI-Enriched sheet only).

    Before writing, every row is passed through ``validate_final_enrichment_row``
    which raises ``FinalOutputValidationError`` if any banned QA placeholder
    phrase remains in the user-facing enrichment columns. This is a hard gate;
    if it triggers, the bug must be fixed upstream rather than the output
    written with placeholder text.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Manus v3 hard validator: fail fast if any row still has placeholder text.
    for _idx, row in enriched.iterrows():
        validate_final_enrichment_row(row.to_dict())
    # Manus v4 hard validator: fail fast if any row carries a banned
    # category-inference basis, generic category-template text, missing
    # source URLs, or a Tier-B / Low / Medium row without a 'Yes' review flag.
    validate_no_final_category_inference(
        [row.to_dict() for _i, row in enriched.iterrows()]
    )

    confidence_series = enriched.get("AI Confidence", pd.Series(dtype=str)).astype(str)
    review_series = enriched.get("AI Review Flag", pd.Series(dtype=str)).astype(str)
    summary = pd.DataFrame([
        {"Metric": "Mode", "Value": mode},
        {"Metric": "Rows enriched (total)", "Value": len(enriched)},
        {"Metric": "Unique cleansed vendors", "Value": enriched["Cleansed Vendor Name"].nunique()},
        {"Metric": "Confidence: High", "Value": int((confidence_series == "High").sum())},
        {"Metric": "Confidence: Medium", "Value": int((confidence_series == "Medium").sum())},
        {"Metric": "Confidence: Low", "Value": int((confidence_series == "Low").sum())},
        {"Metric": "Review Flag: Yes", "Value": int((review_series == "Yes").sum())},
        {"Metric": "Review Flag: No", "Value": int((review_series == "No").sum())},
        {"Metric": "Evidence Tier A (official URL)",
         "Value": int((enriched.get("Evidence Tier", pd.Series(dtype=str)) == "A").sum())},
        {"Metric": "Evidence Tier B (listing-only)",
         "Value": int((enriched.get("Evidence Tier", pd.Series(dtype=str)) == "B").sum())},
        {"Metric": "Evidence Tier C (no URL)",
         "Value": int((enriched.get("Evidence Tier", pd.Series(dtype=str)) == "C").sum())},
        {"Metric": "LLM model configured", "Value": LLM_MODEL},
    ])
    agent_design = pd.DataFrame(AGENTIC_BACKEND_DESIGN["agents"])
    original = pd.read_excel(input_workbook, sheet_name=source_sheet)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # Manus v3: single executive sheet. No Manual Validation split.
        enriched.to_excel(writer, sheet_name="AI Enriched Tail Spend", index=False)
        summary.to_excel(writer, sheet_name="Validation Summary", index=False)
        agent_design.to_excel(writer, sheet_name="Agentic Backend Design", index=False)
        original.to_excel(writer, sheet_name="Original Data", index=False)


def write_url_cache(url_results: Dict[str, URLResult], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: Dict[str, Any] = {}
    for _key, result in url_results.items():
        d = result.to_dict()
        # Slim cache: drop full candidate audit, keep accepted urls + queries + status.
        # Key by the URL agent's own source_key (vendor|L1|L2) so cache-replay can find it.
        payload[result.source_key] = {
            "exact_urls": d["exact_urls"],
            "official_urls": d.get("official_urls", []),
            "listing_urls": d.get("listing_urls", []),
            "evidence_tier": d.get("evidence_tier", "C"),
            "exact_urls_text": d["exact_urls_text"],
            "url_count": d["url_count"],
            "source_status": d["source_status"],
            "search_queries_used": d["search_queries_used"],
            "vendor": d["vendor"],
            "l1": d["l1"],
            "l2": d["l2"],
        }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_url_audit(url_results_per_row: List[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows: List[Dict[str, Any]] = []
    for entry in url_results_per_row:
        result: URLResult = entry["result"]
        if result.candidates:
            for cand in result.candidates:
                rows.append({
                    "row_number_1_based": entry["row_index"] + 1,
                    "vendor": result.vendor,
                    "l1": result.l1,
                    "l2": result.l2,
                    "source_key": result.source_key,
                    "candidate_url": cand.url,
                    "title": cand.title[:200],
                    "snippet": cand.snippet[:300],
                    "score": cand.score,
                    "accepted": cand.accepted,
                    "reject_reason": cand.reject_reason,
                    "queries_used": "; ".join(result.queries_used),
                    "status": result.status,
                })
        else:
            rows.append({
                "row_number_1_based": entry["row_index"] + 1,
                "vendor": result.vendor,
                "l1": result.l1,
                "l2": result.l2,
                "source_key": result.source_key,
                "candidate_url": "",
                "title": "",
                "snippet": "",
                "score": "",
                "accepted": "",
                "reject_reason": "no candidates retrieved",
                "queries_used": "; ".join(result.queries_used),
                "status": result.status,
            })
    pd.DataFrame(rows).to_csv(path, index=False)


# ----------------------------------------------------------------- pipeline

def enrich_dataframe(
    df: pd.DataFrame,
    mode: str = "cache-replay",
    allow_fallback: bool = False,
    write_caches: bool = False,
    enrichment_cache_path: Path = ENRICHMENT_CACHE,
    url_cache_path: Path = EXACT_URL_CACHE,
    generated_enrichment_cache: Path = GENERATED_ENRICHMENT_CACHE,
    generated_url_cache: Path = GENERATED_URL_CACHE,
    source_audit_path: Path = SOURCE_URL_AUDIT,
    batch_size: int = BATCH_SIZE,
    context_df: pd.DataFrame | None = None,
) -> Dict[str, Any]:
    """Run the modular agentic enrichment pipeline.

    Returns a dict with keys: ``enriched`` (DataFrame), ``url_results`` (per row),
    ``unit_records`` (canonical per-supplier records), ``mode``, ``allow_fallback``.
    """
    if mode not in {"cache-replay", "live-research"}:
        raise ValueError(f"Unknown mode: {mode}")

    identity_agent = SupplierIdentityAgent()
    spend_agent = SpendCompetitorAgent()
    market_agent = MarketIntelligenceAgent(
        model=LLM_MODEL,
        cache=load_jsonl_cache(enrichment_cache_path),
        fast_fallback=FAST_FALLBACK,
        generated_cache_path=generated_enrichment_cache if (write_caches and mode == "live-research") else None,
    )
    url_agent = ExactURLAgent(cache_path=url_cache_path)
    evidence_agent = ResearchEvidenceAgent()
    qa_agent = QAGovernanceAgent()

    # In live-research mode we require an LLM client unless fallback is allowed.
    if mode == "live-research" and market_agent.client is None and not allow_fallback:
        raise RuntimeError(
            "live-research mode requires an LLM client. Configure AWS Bedrock "
            "(AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_REGION + BEDROCK_MODEL_ID) "
            "or set OPENAI_API_KEY. Pass --allow-fallback to permit deterministic "
            "category templates as a documented last resort."
        )

    # Reset generated caches so a fresh run produces fresh files.
    if write_caches and mode == "live-research":
        if generated_enrichment_cache.exists():
            generated_enrichment_cache.unlink()
        generated_enrichment_cache.parent.mkdir(parents=True, exist_ok=True)

    enriched = df.copy()
    for col in APPENDED_COLUMNS:
        if col not in enriched.columns:
            enriched[col] = ""
        enriched[col] = enriched[col].astype("object")

    spend_context = context_df if context_df is not None else enriched
    within_spend = spend_agent.competitors_within_spend(spend_context)
    duplicate_map = identity_agent.duplicate_suggestions(
        spend_context["Cleansed Vendor Name"].dropna().astype(str).unique().tolist()
    )

    units = build_unique_units(enriched, identity_agent)

    # ------------- Step A: URL research (per unit) -------------
    unit_url_results: Dict[str, URLResult] = {}
    for unit in units:
        unit_url_results[unit["cache_key"]] = url_agent.research(unit, mode=mode)

    # ------------- Step B: Evidence + Market intelligence (per unit) -------------
    # Manus v3: NEVER quarantine into placeholder text. Every row gets a real
    # source-grounded enrichment. On contradiction or grounding failure for a
    # Tier-A draft we re-prompt ONCE in snippet-override mode (trust page text
    # over the source-data L1/L2). After the retry we accept the rewrite as-is
    # and let confidence/review_flag carry the uncertainty signal.
    unit_outputs: Dict[str, Dict[str, Any]] = {}
    unit_tiers: Dict[str, str] = {}
    unit_quarantine_reasons: Dict[str, str] = {}  # internal audit only
    for unit in units:
        cache_key = unit["cache_key"]
        url_result = unit_url_results[cache_key]
        tier = url_result.evidence_tier or ("A" if url_result.accepted_urls else "C")
        unit_tiers[cache_key] = tier
        cached_market = market_agent._from_cache(unit)
        vendor_for_scrub = (
            unit.get("preferred_vendor_name") or unit.get("cleansed_vendor_name") or unit.get("vendor_name") or ""
        )

        # Replay from cache if available AND it isn't an old-style quarantine row.
        if cached_market is not None and not cached_market.get("_quarantined"):
            standardized = qa_agent.standardize_market_output(cached_market, unit)
            standardized = qa_agent.enforce_evidence_calibration(standardized, tier, vendor_for_scrub)
            unit_outputs[cache_key] = standardized
            continue

        if mode == "live-research" and market_agent.client is not None and tier == "A":
            # Manus v3: feed evidence_for_llm() (drops weak domains from snippets).
            llm_input_urls = url_agent.evidence_for_llm(url_result)
            evidence = evidence_agent.gather(
                vendor=vendor_for_scrub,
                l1=unit.get("l1", ""),
                l2=unit.get("l2", ""),
                urls=llm_input_urls,
            )
            # Pick prompt mode: "partner_source" when none of the accepted URLs
            # apex-matches the supplier (i.e. all evidence-grade sources are
            # partner/government pages, not the supplier's own apex domain).
            # Inspect URLResult.source_types directly: official_urls is the full
            # evidence-grade list (incl. .gov / partner pages) for backwards
            # compatibility, so it cannot distinguish those two cases.
            has_apex_official = any(
                stype == "official_supplier"
                for stype in url_result.source_types.values()
            )
            if url_result.evidence_grade_urls and not has_apex_official:
                first_mode = "partner_source"
            else:
                first_mode = "default"
            try:
                raw = market_agent.live_research_one(
                    unit, evidence, url_result.accepted_urls,
                    evidence_tier="A", prompt_mode=first_mode,
                )
            except Exception as exc:
                if allow_fallback:
                    raw = _unresolved_neutral_output(unit, url_result)
                    raw["_llm_error"] = type(exc).__name__
                else:
                    raise
            standardized = qa_agent.standardize_market_output(raw, unit)
            standardized = qa_agent.enforce_evidence_calibration(standardized, "A", vendor_for_scrub)

            # Post-LLM gates: contradiction (always) + grounding (bypassed when
            # an apex-matching official supplier source is present, since venue
            # / military pages have idiosyncratic snippet vocabulary).
            snippets = evidence_snippets_from_packet(evidence)
            contradiction = check_contradiction(
                standardized.get("what_they_do", ""), snippets, vendor=vendor_for_scrub,
            )
            grounding_required = not bool(url_result.official_urls)
            if grounding_required:
                grounding = check_claim_grounding(
                    standardized.get("what_they_do", ""), snippets, vendor=vendor_for_scrub,
                )
                grounded_ok = grounding.grounded
                grounding_reason = grounding.reason
            else:
                grounded_ok = True
                grounding_reason = ""

            retry_reason: Optional[str] = None
            if contradiction.contradicts:
                retry_reason = QUARANTINE_REASON_CONTRADICTION
            elif not grounded_ok:
                retry_reason = QUARANTINE_REASON_GROUNDING_FAILED

            if retry_reason and mode == "live-research" and market_agent.client is not None:
                # Re-prompt ONCE in snippet-override mode. We accept the rewrite
                # regardless and stamp confidence/review_flag accordingly so
                # the export carries real source-grounded text, never a
                # placeholder.
                try:
                    raw2 = market_agent.live_research_one(
                        unit, evidence, url_result.accepted_urls,
                        evidence_tier="A", prompt_mode="snippet_override",
                    )
                except Exception:
                    raw2 = None
                if raw2:
                    standardized = qa_agent.standardize_market_output(raw2, unit)
                    standardized = qa_agent.enforce_evidence_calibration(standardized, "A", vendor_for_scrub)
                    # Manus v3: trust the snippet_override prompt's own
                    # confidence/review_flag values (it is hard-coded to
                    # Medium / Yes). Do NOT post-override here, otherwise
                    # cache-replay determinism breaks because the cache stores
                    # the LLM's value and live overrides it in-memory.
                    standardized["_retry_reason"] = retry_reason
                unit_quarantine_reasons[cache_key] = retry_reason  # internal audit
        elif mode == "live-research" and market_agent.client is not None and tier in ("B", "C"):
            # Manus v4: split Tier B/C into two paths.
            #   * SECONDARY_LISTING path: tier == "B" AND at least one non-weak
            #     URL exists (after second-pass) -> hedged source-grounded
            #     enrichment with research_basis="secondary listing".
            #   * UNRESOLVED path: no non-weak URL after second-pass ->
            #     synthesize a controlled neutral row (no LLM call); basis is
            #     "manual review required". This replaces the legacy
            #     CATEGORY_ONLY_PROMPT path which produced banned
            #     "category inference - no official source" output.
            non_weak_urls = url_agent.evidence_for_llm(url_result) if tier == "B" else []
            non_weak_urls = [u for u in non_weak_urls if not _host_in_set(u, WEAK_EVIDENCE_DOMAINS)]
            if tier == "B" and non_weak_urls:
                evidence = evidence_agent.gather(
                    vendor=vendor_for_scrub,
                    l1=unit.get("l1", ""),
                    l2=unit.get("l2", ""),
                    urls=non_weak_urls,
                )
                try:
                    raw = market_agent.live_research_one(
                        unit, evidence, non_weak_urls,
                        evidence_tier="B", prompt_mode="secondary_listing",
                    )
                except Exception as exc:
                    if allow_fallback:
                        raw = _unresolved_neutral_output(unit, url_result)
                        raw["_llm_error"] = type(exc).__name__
                    else:
                        raise
                standardized = qa_agent.standardize_market_output(raw, unit)
                standardized = qa_agent.enforce_evidence_calibration(standardized, "B", vendor_for_scrub)
                unit_quarantine_reasons[cache_key] = QUARANTINE_REASON_LISTING_ONLY
            else:
                # Unresolved row -- no LLM call, controlled neutral output.
                standardized = _unresolved_neutral_output(unit, url_result)
                # Persist to cache so cache-replay reproduces it deterministically.
                _persist_unresolved(market_agent, unit, url_result, standardized)
                unit_quarantine_reasons[cache_key] = QUARANTINE_REASON_NO_URL
        else:
            # cache-replay path with no cache entry, OR live mode with no LLM but fallback allowed,
            # OR cached row marked _quarantined from an old run.
            # Manus v4: never use category_fallback_output (banned basis). Use
            # the unresolved synthesizer instead so the export contract holds.
            standardized = _unresolved_neutral_output(unit, url_result)
            unit_quarantine_reasons[cache_key] = QUARANTINE_REASON_NO_URL

        unit_outputs[cache_key] = standardized

    # ------------- Step C: Apply per-row outputs to the dataframe -------------
    url_audit_rows: List[Dict[str, Any]] = []
    for idx, row in enriched.iterrows():
        row_record = identity_agent.normalize_row(row)
        row_key = make_enrichment_key(row_record)
        standardized = unit_outputs.get(row_key) or qa_agent.research_failed_output(row_record)
        for column, value in qa_agent.excel_field_map(standardized).items():
            enriched.loc[idx, column] = value
        enriched.loc[idx, "Competitors within Spend"] = within_spend.get(idx, "")
        enriched.loc[idx, "AI Potential Consolidation Review"] = duplicate_map.get(
            clean_text(row.get("Cleansed Vendor Name")), ""
        )
        url_result = unit_url_results.get(row_key) or url_agent.research(row, mode=mode)
        # Manus v4: unresolved rows (basis = "manual review required") get
        # best-effort URLs joined or a controlled sentinel string. All other
        # rows use the normal exact_urls_text (which strips weak URLs).
        is_unresolved = bool(standardized.get("_unresolved")) or (
            clean_text(standardized.get("research_basis", "")).lower()
            == RESEARCH_BASIS_MANUAL_REVIEW
        )
        if is_unresolved:
            enriched.loc[idx, "Exact URLs Leveraged for Study"] = _unresolved_url_text(url_result)
        else:
            enriched.loc[idx, "Exact URLs Leveraged for Study"] = url_result.exact_urls_text
        enriched.loc[idx, "Evidence Tier"] = url_result.evidence_tier or unit_tiers.get(row_key, "C")
        url_audit_rows.append({"row_index": idx, "result": url_result})

    original_cols = [c for c in df.columns if c not in APPENDED_COLUMNS]
    final_cols = original_cols + [c for c in APPENDED_COLUMNS if c in enriched.columns]
    enriched = enriched[final_cols]

    if write_caches:
        write_url_cache(unit_url_results, generated_url_cache)
        write_url_audit(url_audit_rows, source_audit_path)

    return {
        "enriched": enriched,
        "mode": mode,
        "allow_fallback": allow_fallback,
        "url_results": unit_url_results,
        "unit_records": units,
        "url_audit_rows": url_audit_rows,
        "generated_enrichment_cache": generated_enrichment_cache if (write_caches and mode == "live-research") else None,
        "generated_url_cache": generated_url_cache if write_caches else None,
        "source_audit_path": source_audit_path if write_caches else None,
    }


# ----------------------------------------------------------------- CLI

def main() -> None:
    parser = argparse.ArgumentParser(description="Run the modular tail-spend enrichment agentic backend.")
    parser.add_argument("--mode", choices=["cache-replay", "live-research"], default="cache-replay",
                        help="Pipeline mode (default: cache-replay).")
    parser.add_argument("--source-workbook", "--input", dest="input", type=Path,
                        default=DEFAULT_INPUT_WORKBOOK, help="Source tail-spend workbook path.")
    parser.add_argument("--sheet", default=SOURCE_SHEET, help="Source worksheet name.")
    parser.add_argument("--output", type=Path, default=OUTPUT_WORKBOOK, help="Output enriched workbook path.")
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit for testing.")
    parser.add_argument("--allow-fallback", action="store_true",
                        help="Permit deterministic category templates if research/LLM is unavailable.")
    parser.add_argument("--write-caches", action="store_true",
                        help="Write generated enrichment + URL caches and source audit.")
    parser.add_argument("--enrichment-cache", type=Path, default=ENRICHMENT_CACHE,
                        help="Existing enrichment cache to consult (cache-replay).")
    parser.add_argument("--url-cache", type=Path, default=EXACT_URL_CACHE,
                        help="Existing URL cache to consult.")
    parser.add_argument("--generated-enrichment-cache", type=Path, default=GENERATED_ENRICHMENT_CACHE,
                        help="Path for newly generated enrichment cache.")
    parser.add_argument("--generated-url-cache", type=Path, default=GENERATED_URL_CACHE,
                        help="Path for newly generated URL cache.")
    parser.add_argument("--source-audit", type=Path, default=SOURCE_URL_AUDIT,
                        help="Path for the per-row source URL audit CSV.")
    parser.add_argument("--json-design", type=Path, default=DEFAULT_OUTPUT_DIR / "agentic_backend_design.json",
                        help="Agent design JSON output path.")
    # Legacy compatibility flag.
    parser.add_argument("--live-urls", action="store_true",
                        help="(Deprecated) Equivalent to --mode live-research.")
    args = parser.parse_args()

    mode = "live-research" if args.live_urls else args.mode

    df = load_source_rows(args.input, args.sheet, args.limit)
    result = enrich_dataframe(
        df,
        mode=mode,
        allow_fallback=args.allow_fallback,
        write_caches=args.write_caches,
        enrichment_cache_path=args.enrichment_cache,
        url_cache_path=args.url_cache,
        generated_enrichment_cache=args.generated_enrichment_cache,
        generated_url_cache=args.generated_url_cache,
        source_audit_path=args.source_audit,
    )
    write_workbook(result["enriched"], args.output, args.input, args.sheet, mode)
    args.json_design.write_text(json.dumps(AGENTIC_BACKEND_DESIGN, indent=2), encoding="utf-8")

    print(f"Mode: {mode}")
    print(f"Wrote enriched workbook: {args.output}")
    print(f"Rows enriched: {len(result['enriched'])}")
    if args.write_caches:
        if result.get("generated_url_cache"):
            print(f"Wrote URL cache: {result['generated_url_cache']}")
        if result.get("generated_enrichment_cache"):
            print(f"Wrote enrichment cache: {result['generated_enrichment_cache']}")
        if result.get("source_audit_path"):
            print(f"Wrote source audit: {result['source_audit_path']}")


if __name__ == "__main__":
    main()
