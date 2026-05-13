"""Live-research quality test for the first 10 vendors.

Acceptance criteria (per the project handoff):

* All 10 rows have populated `What they do`, `Top 3 Savings Levers`,
  `Market Competitors`, `AI Contract Structure`, `AI Review Flag`,
  `AI Confidence`, `AI Research Basis`, and `Exact URLs Leveraged for Study`.
* No generic search-result URLs in the URL column.
* No category-template fallback text appears unless `--allow-fallback` was
  passed (we run *without* it).
* Source audit file exists.
* Each LLM-generated cache row includes model, timestamp, confidence,
  research basis, and source URLs.
* A subsequent ``cache-replay`` run using the freshly generated cache
  reproduces the first 10 outputs deterministically.

This test invokes the orchestrator pipeline programmatically (no subprocess)
so it can be debugged from VS Code. Set ``OPENAI_API_KEY`` before running.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except Exception:
    pass

from config import (  # noqa: E402
    APPENDED_COLUMNS,
    DEFAULT_INPUT_WORKBOOK,
    DEFAULT_OUTPUT_DIR,
    GENERATED_ENRICHMENT_CACHE,
    GENERATED_URL_CACHE,
    LIVE_RESEARCH_REPORT,
    SOURCE_SHEET,
    SOURCE_URL_AUDIT,
    URL_MANUAL_VALIDATION_TEXT,
    URL_RESEARCH_FAILED_TEXT,
)
from orchestrator import enrich_dataframe, load_source_rows, write_workbook  # noqa: E402


REQUIRED_POPULATED = [
    "What they do",
    "Top 3 Savings Levers",
    "Market Competitors",
    "AI Contract Structure",
    "AI Review Flag",
    "AI Confidence",
    "AI Research Basis",
    "Exact URLs Leveraged for Study",
]

GENERIC_SEARCH_HOSTS = ("google.com/search", "bing.com/search", "duckduckgo.com",
                        "google.com/maps", "search?")
FALLBACK_MARKERS = ("category inference (fallback)",
                    "ambiguous supplier; category inference (fallback)")
BANNED_RESEARCH_BASIS = (
    "known company/category knowledge",
    "supplier website + category context",
    "supplier website only",
)
ALLOWED_RESEARCH_BASIS = {
    "supplier website",
    "supplier website + secondary listing",
    "category inference - no official source",
}


def _is_populated(value: Any) -> bool:
    s = "" if value is None else str(value).strip()
    return s != "" and s.lower() != "nan"


def _has_generic_search_url(value: Any) -> bool:
    if not isinstance(value, str):
        value = "" if value is None else str(value)
    low = value.lower()
    return any(m in low for m in GENERIC_SEARCH_HOSTS)


def _is_research_failed_marker(value: Any) -> bool:
    s = "" if value is None else str(value)
    return URL_RESEARCH_FAILED_TEXT in s or URL_MANUAL_VALIDATION_TEXT in s


def _row_uses_fallback(row: pd.Series) -> bool:
    rb = str(row.get("AI Research Basis", "")).lower()
    return any(marker in rb for marker in FALLBACK_MARKERS)


def _check_workbook(df: pd.DataFrame, allow_fallback: bool) -> Dict[str, Any]:
    """Manus v3 acceptance gates: single executive sheet; no placeholders.

    Round 4 passes only if the final first-10 output has zero manual-validation
    placeholder phrases, zero NO EXACT SOURCE FOUND rows, zero RESEARCH FAILED
    rows, zero contradictory source/description rows, zero rows where weak
    domains are the only evidence for product/service claims, and 10 of 10 rows
    with exact or defensible supplier-level URLs. Low or medium confidence is
    allowed only as a separate review flag; it must not replace enrichment text.
    """
    from agents.final_validator import (  # noqa: WPS433
        BANNED_FINAL_PHRASES,
        ENRICHMENT_FIELDS,
        FinalOutputValidationError,
        validate_final_enrichment_row,
    )
    from agents.url_agent import WEAK_EVIDENCE_DOMAINS  # noqa: WPS433

    failures: List[str] = []
    if len(df) != 10:
        failures.append(f"expected 10 rows, got {len(df)}")
    for col in REQUIRED_POPULATED:
        if col not in df.columns:
            failures.append(f"missing column: {col}")

    # ----- Gate 1: every row passes the final-output validator (no banned phrases).
    for i, row in df.iterrows():
        try:
            validate_final_enrichment_row(row.to_dict())
        except FinalOutputValidationError as exc:
            failures.append(f"row {i + 1}: {exc}")

    # ----- Gate 2: no banned phrases anywhere in the four enrichment fields
    #               (belt-and-suspenders on top of the validator).
    for i, row in df.iterrows():
        for field in ENRICHMENT_FIELDS:
            text = ("" if row.get(field) is None else str(row.get(field))).lower()
            for phrase in BANNED_FINAL_PHRASES:
                if phrase in text:
                    failures.append(
                        f"row {i + 1}: banned phrase '{phrase}' in column '{field}'"
                    )

    # ----- Gate 3: AI Review Flag must be exactly 'Yes' or 'No'.
    for i, row in df.iterrows():
        rev = str(row.get("AI Review Flag", "")).strip()
        if rev not in {"Yes", "No"}:
            failures.append(f"row {i + 1}: AI Review Flag must be Yes/No, got {rev!r}")

    # ----- Gate 4: every row's URL list must contain >=1 non-weak URL.
    for i, row in df.iterrows():
        urls = str(row.get("Exact URLs Leveraged for Study", ""))
        url_tokens = [u.strip() for u in urls.split(";") if u.strip().startswith("http")]
        if not url_tokens:
            failures.append(f"row {i + 1}: no URLs at all in '{urls[:120]}'")
            continue
        non_weak = []
        for u in url_tokens:
            host = u.split("//", 1)[-1].split("/", 1)[0].lower().removeprefix("www.")
            is_weak = any(host == w or host.endswith("." + w) for w in WEAK_EVIDENCE_DOMAINS)
            if not is_weak:
                non_weak.append(u)
        if not non_weak:
            failures.append(
                f"row {i + 1}: only weak-domain URLs as evidence: {urls[:160]}"
            )

    # ----- Gate 5: hard-case URL presence.
    hard_cases = {
        "3E CO": "3eco.com",
        "354TH": "eielson",
        "5TH AVENUE": "5thavenue.org",
        "617436BC": "fletransport.com",
    }
    for i, row in df.iterrows():
        vendor = str(row.get("Vendor Name", "")).upper()
        urls_lower = str(row.get("Exact URLs Leveraged for Study", "")).lower()
        for marker, expected_host in hard_cases.items():
            if marker in vendor and expected_host not in urls_lower:
                failures.append(
                    f"row {i + 1} ({marker}): expected '{expected_host}' in URLs, got '{urls_lower[:160]}'"
                )

    # ----- Gate 6: 100 PERCENT contradiction rule.
    for i, row in df.iterrows():
        vendor = str(row.get("Vendor Name", "")).upper()
        if "100 PERCENT" not in vendor:
            continue
        urls_lower = str(row.get("Exact URLs Leveraged for Study", "")).lower()
        wtd = str(row.get("What they do", "")).lower()
        if "100percent.com" in urls_lower:
            for bpo_word in ("bpo", "outsourced business process", "business process outsourcing", "managed services"):
                if bpo_word in wtd:
                    failures.append(
                        f"row {i + 1} (100 PERCENT): contradiction — '{bpo_word}' in description while 100percent.com is the source"
                    )
            product_words = ("goggle", "eyewear", "sunglass", "helmet", "cycling", "moto", "apparel", "mtb")
            if not any(w in wtd for w in product_words):
                failures.append(
                    f"row {i + 1} (100 PERCENT): description must contain >=1 of {product_words}"
                )

    # ----- Gate 7: research_basis still in allowed set.
    for i, row in df.iterrows():
        rb = str(row.get("AI Research Basis", "")).strip().lower()
        if rb not in {a.lower() for a in ALLOWED_RESEARCH_BASIS}:
            failures.append(f"row {i + 1}: research_basis not in allowed set: {rb!r}")

    # ----- Gate 8: required text columns populated (sanity). Exact URLs may
    # be empty for Tier-B / category-inference rows.
    for i, row in df.iterrows():
        tier = str(row.get("Evidence Tier", "")).strip().upper()
        for col in REQUIRED_POPULATED:
            if col not in df.columns:
                continue
            if col == "Exact URLs Leveraged for Study" and tier in ("B", "C"):
                continue
            if not _is_populated(row.get(col)):
                failures.append(f"row {i + 1}: empty column '{col}'")

    return {"row_count": len(df), "failures": failures}


def _check_generated_caches(enrichment_path: Path, url_cache_path: Path,
                            audit_path: Path, expected_unit_keys: List[str]) -> Dict[str, Any]:
    failures: List[str] = []
    if not enrichment_path.exists():
        failures.append(f"missing generated enrichment cache: {enrichment_path}")
    else:
        cache_rows = []
        with enrichment_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        cache_rows.append(json.loads(line))
                    except Exception as exc:
                        failures.append(f"cache parse error: {exc}")
        for row in cache_rows:
            for required in ("cache_key", "model", "timestamp", "prompt_version",
                             "confidence", "research_basis", "source_urls"):
                if required not in row:
                    failures.append(f"cache row {row.get('cache_key', '?')} missing field: {required}")
                    break
            # source_urls may legitimately be empty for category-inference rows
            # (no exact supplier URL was found). In that case research_basis must
            # be exactly 'category inference - no official source'.
            if not row.get("source_urls"):
                rb = str(row.get("research_basis", "")).lower()
                if "category inference - no official source" not in rb:
                    failures.append(
                        f"cache row {row.get('cache_key')} has empty source_urls "
                        f"but research_basis is not 'category inference - no official source': {rb!r}"
                    )

    if not url_cache_path.exists():
        failures.append(f"missing generated URL cache: {url_cache_path}")
    if not audit_path.exists():
        failures.append(f"missing source audit: {audit_path}")

    return {"failures": failures}


def _replay_determinism(first_run_df: pd.DataFrame, source_first10: pd.DataFrame,
                        full_source: pd.DataFrame,
                        enrichment_cache: Path, url_cache: Path) -> Dict[str, Any]:
    """Re-run in cache-replay mode using the just-generated caches and compare."""
    replay = enrich_dataframe(
        source_first10,
        mode="cache-replay",
        allow_fallback=False,
        write_caches=False,
        enrichment_cache_path=enrichment_cache,
        url_cache_path=url_cache,
        context_df=full_source,
    )
    replay_df = replay["enriched"]
    failures: List[str] = []
    cols = [c for c in REQUIRED_POPULATED if c in first_run_df.columns and c in replay_df.columns]
    for col in cols:
        for i in range(min(len(first_run_df), len(replay_df))):
            a = "" if first_run_df.iloc[i][col] is None else str(first_run_df.iloc[i][col]).strip()
            b = "" if replay_df.iloc[i][col] is None else str(replay_df.iloc[i][col]).strip()
            if a != b:
                failures.append(f"row {i + 1} column '{col}' differs between live and replay")
    return {"failures": failures, "compared_columns": cols}


def main() -> int:
    full_source = load_source_rows(DEFAULT_INPUT_WORKBOOK, SOURCE_SHEET, limit=None)
    source_first10 = full_source.head(10).copy()

    # 1) Live research run.
    live = enrich_dataframe(
        source_first10,
        mode="live-research",
        allow_fallback=False,
        write_caches=True,
        generated_enrichment_cache=GENERATED_ENRICHMENT_CACHE,
        generated_url_cache=GENERATED_URL_CACHE,
        source_audit_path=SOURCE_URL_AUDIT,
        context_df=full_source,
    )
    live_df = live["enriched"]
    workbook_path = DEFAULT_OUTPUT_DIR / "first10_live_research_output.xlsx"
    write_workbook(live_df, workbook_path, DEFAULT_INPUT_WORKBOOK, SOURCE_SHEET, "live-research")

    # 2) Workbook quality checks.
    wb_check = _check_workbook(live_df, allow_fallback=False)

    # 3) Generated caches & audit.
    cache_check = _check_generated_caches(
        GENERATED_ENRICHMENT_CACHE,
        GENERATED_URL_CACHE,
        SOURCE_URL_AUDIT,
        expected_unit_keys=[u["cache_key"] for u in live["unit_records"]],
    )

    # 4) Cache-replay determinism.
    replay_check = _replay_determinism(
        live_df, source_first10, full_source,
        enrichment_cache=GENERATED_ENRICHMENT_CACHE,
        url_cache=GENERATED_URL_CACHE,
    )

    all_failures = wb_check["failures"] + cache_check["failures"] + replay_check["failures"]
    passed = not all_failures

    LIVE_RESEARCH_REPORT.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# First-10 Live-Research Quality Test\n\n",
        f"**Result:** {'PASS' if passed else 'FAIL'}\n\n",
        f"- Rows: {wb_check['row_count']}\n",
        f"- Workbook failures: {len(wb_check['failures'])}\n",
        f"- Cache failures: {len(cache_check['failures'])}\n",
        f"- Replay failures: {len(replay_check['failures'])} (compared {len(replay_check['compared_columns'])} columns)\n",
        f"\n**Generated workbook:** `{workbook_path}`\n",
        f"**Generated enrichment cache:** `{GENERATED_ENRICHMENT_CACHE}`\n",
        f"**Generated URL cache:** `{GENERATED_URL_CACHE}`\n",
        f"**Source audit:** `{SOURCE_URL_AUDIT}`\n",
    ]
    if all_failures:
        lines.append("\n## Failures\n\n")
        for f in all_failures:
            lines.append(f"- {f}\n")
    LIVE_RESEARCH_REPORT.write_text("".join(lines), encoding="utf-8")

    print(f"Live-research quality report: {LIVE_RESEARCH_REPORT}")
    print(f"Workbook: {workbook_path}")
    print(f"Result: {'PASS' if passed else 'FAIL'} ({len(all_failures)} failures)")
    if all_failures:
        for f in all_failures[:10]:
            print(f"  - {f}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
