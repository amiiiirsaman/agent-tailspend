"""Live-research pilot test for the first 100 vendors.

Round 4 controlled production pilot. Same hard gates as the first-10
acceptance test, scaled to 100 rows:

1. No banned placeholder phrases in final enrichment fields.
2. No NO EXACT SOURCE FOUND or RESEARCH FAILED final rows.
3. No category-inference basis when supplier-level official sources are
   available (i.e. Tier-A rows must not have basis 'category inference').
4. No weak directory domains (Yelp, D&B, Bizapedia, etc.) as the only
   evidence in a row.
5. No source/description contradictions (heuristic check_contradiction).
6. Medium/Low-confidence rows must still contain source-grounded
   enrichment (validator rejects placeholders regardless of confidence).

Hard-case sub-gates from the first-10 test (3eco / eielson / 5thavenue /
fletransport URL presence; 100 PERCENT no-BPO contradiction) still apply
because rows 1-10 are inside the first 100.
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
    DEFAULT_INPUT_WORKBOOK,
    DEFAULT_OUTPUT_DIR,
    SOURCE_SHEET,
    URL_MANUAL_VALIDATION_TEXT,
    URL_RESEARCH_FAILED_TEXT,
)
from orchestrator import enrich_dataframe, load_source_rows, write_workbook  # noqa: E402


PILOT_SIZE = 100

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

# Manus v4: category-inference is no longer an allowed final basis.
ALLOWED_RESEARCH_BASIS = {
    "supplier website",
    "supplier website + secondary listing",
    "secondary listing",
    "manual review required",
}

# Pilot-scoped paths (don't clobber the first-10 artifacts).
PILOT_ENRICHMENT_CACHE = DEFAULT_OUTPUT_DIR / "first100_enrichment_cache.generated.jsonl"
PILOT_URL_CACHE = DEFAULT_OUTPUT_DIR / "first100_exact_urls.generated.json"
PILOT_AUDIT = DEFAULT_OUTPUT_DIR / "first100_source_url_audit.csv"
PILOT_WORKBOOK = DEFAULT_OUTPUT_DIR / "first100_live_research_output.xlsx"
PILOT_REPORT = DEFAULT_OUTPUT_DIR / "first100_live_research_quality_report.md"


def _is_populated(value: Any) -> bool:
    s = "" if value is None else str(value).strip()
    return s != "" and s.lower() != "nan"


def _check_workbook(df: pd.DataFrame) -> Dict[str, Any]:
    from agents.final_validator import (  # noqa: WPS433
        BANNED_FINAL_PHRASES,
        ENRICHMENT_FIELDS,
        FinalOutputValidationError,
        validate_final_enrichment_row,
    )
    from agents.url_agent import WEAK_EVIDENCE_DOMAINS  # noqa: WPS433
    from agents.grounding_agent import check_contradiction  # noqa: WPS433

    failures: List[str] = []
    if len(df) != PILOT_SIZE:
        failures.append(f"expected {PILOT_SIZE} rows, got {len(df)}")
    for col in REQUIRED_POPULATED:
        if col not in df.columns:
            failures.append(f"missing column: {col}")

    # Gate 1: validator (no banned phrases in enrichment fields).
    for i, row in df.iterrows():
        try:
            validate_final_enrichment_row(row.to_dict())
        except FinalOutputValidationError as exc:
            failures.append(f"row {i + 1}: {exc}")

    # Gate 2: belt-and-suspenders banned-phrase scan.
    for i, row in df.iterrows():
        for field in ENRICHMENT_FIELDS:
            text = ("" if row.get(field) is None else str(row.get(field))).lower()
            for phrase in BANNED_FINAL_PHRASES:
                if phrase in text:
                    failures.append(
                        f"row {i + 1}: banned phrase '{phrase}' in column '{field}'"
                    )

    # Gate 3: AI Review Flag is exactly Yes or No.
    for i, row in df.iterrows():
        rev = str(row.get("AI Review Flag", "")).strip()
        if rev not in {"Yes", "No"}:
            failures.append(f"row {i + 1}: AI Review Flag must be Yes/No, got {rev!r}")

    # Gate 4: every row must have >=1 non-weak URL OR research_failed marker
    # is NEVER present (placeholder text was banned in v3 — every row should
    # either resolve to URL evidence or be a Tier-C category-inference row
    # with source_urls left empty in the cache but a non-placeholder text in
    # the workbook column).
    for i, row in df.iterrows():
        urls_text = str(row.get("Exact URLs Leveraged for Study", ""))
        if URL_RESEARCH_FAILED_TEXT in urls_text or URL_MANUAL_VALIDATION_TEXT in urls_text:
            failures.append(
                f"row {i + 1}: forbidden URL marker in '{urls_text[:120]}'"
            )
            continue
        url_tokens = [u.strip() for u in urls_text.split(";") if u.strip().startswith("http")]
        # Tier-C / no-URL rows are allowed (category inference). When URLs do
        # exist, at least one must be non-weak.
        if url_tokens:
            non_weak = []
            for u in url_tokens:
                host = u.split("//", 1)[-1].split("/", 1)[0].lower().removeprefix("www.")
                is_weak = any(host == w or host.endswith("." + w) for w in WEAK_EVIDENCE_DOMAINS)
                if not is_weak:
                    non_weak.append(u)
            if not non_weak:
                failures.append(
                    f"row {i + 1}: only weak-domain URLs as evidence: {urls_text[:160]}"
                )

    # Gate 5 (Tier discipline): when Tier == 'A' (supplier-level official
    # source present), research_basis must NOT be 'category inference'.
    for i, row in df.iterrows():
        tier = str(row.get("Evidence Tier", "")).strip().upper()
        rb = str(row.get("AI Research Basis", "")).strip().lower()
        if tier == "A" and "category inference" in rb:
            failures.append(
                f"row {i + 1}: Tier A row used category-inference basis: {rb!r}"
            )

    # Gate 6: research_basis in allowed set.
    for i, row in df.iterrows():
        rb = str(row.get("AI Research Basis", "")).strip().lower()
        if rb not in {a.lower() for a in ALLOWED_RESEARCH_BASIS}:
            failures.append(f"row {i + 1}: research_basis not in allowed set: {rb!r}")

    # Gate 7 (Manus v4): every required column populated for every row,
    # including Exact URLs Leveraged for Study (unresolved rows carry the
    # controlled UNRESOLVED_URL_SENTINEL string, never blank).
    for i, row in df.iterrows():
        for col in REQUIRED_POPULATED:
            if col not in df.columns:
                continue
            if not _is_populated(row.get(col)):
                failures.append(f"row {i + 1}: empty column '{col}'")

    # Gate 8: hard-case URL presence (rows 1-10 are inside the pilot).
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
                    f"row {i + 1} ({marker}): expected '{expected_host}' in URLs"
                )

    # Gate 9: 100 PERCENT contradiction rule.
    for i, row in df.iterrows():
        vendor = str(row.get("Vendor Name", "")).upper()
        if "100 PERCENT" not in vendor:
            continue
        urls_lower = str(row.get("Exact URLs Leveraged for Study", "")).lower()
        wtd = str(row.get("What they do", "")).lower()
        if "100percent.com" in urls_lower:
            for bpo_word in (
                "bpo", "outsourced business process",
                "business process outsourcing", "managed services",
            ):
                if bpo_word in wtd:
                    failures.append(
                        f"row {i + 1} (100 PERCENT): contradiction '{bpo_word}' "
                        f"in description while 100percent.com is the source"
                    )

    # Gate 10: source/description contradiction (heuristic) for Tier-A rows
    # whose URLs include a likely-apex supplier domain. We don't have the
    # snippet text in the workbook, so this gate is a string-level check:
    # description must contain >= 1 token that intersects with the URL host
    # apex labels (excluding generic words). This catches the BPO-style
    # mismatch where description mentions a totally unrelated industry.
    for i, row in df.iterrows():
        tier = str(row.get("Evidence Tier", "")).strip().upper()
        if tier != "A":
            continue
        urls_text = str(row.get("Exact URLs Leveraged for Study", ""))
        wtd = str(row.get("What they do", "")).lower()
        contradiction = check_contradiction(
            wtd, [urls_text], vendor=str(row.get("Vendor Name", "")),
        )
        if contradiction.contradicts:
            failures.append(
                f"row {i + 1}: contradiction detected: {contradiction.reason}"
            )

    return {"row_count": len(df), "failures": failures}


def _check_caches(enrichment_path: Path, url_cache_path: Path,
                  audit_path: Path) -> Dict[str, Any]:
    failures: List[str] = []
    if not enrichment_path.exists():
        failures.append(f"missing generated enrichment cache: {enrichment_path}")
    else:
        with enrichment_path.open("r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception as exc:
                    failures.append(f"cache parse error line {line_no}: {exc}")
                    continue
                for required in (
                    "cache_key", "model", "timestamp", "prompt_version",
                    "confidence", "research_basis", "source_urls",
                ):
                    if required not in row:
                        failures.append(
                            f"cache row {row.get('cache_key', '?')} missing: {required}"
                        )
                        break
                if not row.get("source_urls"):
                    rb = str(row.get("research_basis", "")).lower()
                    if rb != "manual review required":
                        failures.append(
                            f"cache row {row.get('cache_key')} empty source_urls "
                            f"but basis is not 'manual review required': {rb!r}"
                        )
    if not url_cache_path.exists():
        failures.append(f"missing generated URL cache: {url_cache_path}")
    if not audit_path.exists():
        failures.append(f"missing source audit: {audit_path}")
    return {"failures": failures}


def _summary(df: pd.DataFrame) -> Dict[str, Any]:
    tiers = df.get("Evidence Tier", pd.Series(dtype=str)).astype(str).value_counts().to_dict()
    confidence = df.get("AI Confidence", pd.Series(dtype=str)).astype(str).value_counts().to_dict()
    review = df.get("AI Review Flag", pd.Series(dtype=str)).astype(str).value_counts().to_dict()
    basis = df.get("AI Research Basis", pd.Series(dtype=str)).astype(str).value_counts().to_dict()
    return {
        "tiers": tiers, "confidence": confidence,
        "review_flag": review, "research_basis": basis,
    }


def main() -> int:
    full_source = load_source_rows(DEFAULT_INPUT_WORKBOOK, SOURCE_SHEET, limit=None)
    pilot_source = full_source.head(PILOT_SIZE).copy()

    live = enrich_dataframe(
        pilot_source,
        mode="live-research",
        allow_fallback=False,
        write_caches=True,
        generated_enrichment_cache=PILOT_ENRICHMENT_CACHE,
        generated_url_cache=PILOT_URL_CACHE,
        source_audit_path=PILOT_AUDIT,
        context_df=full_source,
    )
    live_df = live["enriched"]
    write_workbook(live_df, PILOT_WORKBOOK, DEFAULT_INPUT_WORKBOOK, SOURCE_SHEET, "live-research")

    wb_check = _check_workbook(live_df)
    cache_check = _check_caches(PILOT_ENRICHMENT_CACHE, PILOT_URL_CACHE, PILOT_AUDIT)
    summary = _summary(live_df)

    all_failures = wb_check["failures"] + cache_check["failures"]
    passed = not all_failures

    PILOT_REPORT.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# First-100 Live-Research Pilot Quality Report\n\n",
        f"**Result:** {'PASS' if passed else 'FAIL'}\n\n",
        f"- Rows: {wb_check['row_count']}\n",
        f"- Workbook failures: {len(wb_check['failures'])}\n",
        f"- Cache failures: {len(cache_check['failures'])}\n\n",
        "## Distribution\n\n",
        f"- Evidence tiers: {summary['tiers']}\n",
        f"- Confidence: {summary['confidence']}\n",
        f"- Review flag: {summary['review_flag']}\n",
        f"- Research basis: {summary['research_basis']}\n\n",
        f"**Workbook:** `{PILOT_WORKBOOK}`\n",
        f"**Enrichment cache:** `{PILOT_ENRICHMENT_CACHE}`\n",
        f"**URL cache:** `{PILOT_URL_CACHE}`\n",
        f"**Source audit:** `{PILOT_AUDIT}`\n",
    ]
    if all_failures:
        lines.append("\n## Failures\n\n")
        for f in all_failures:
            lines.append(f"- {f}\n")
    PILOT_REPORT.write_text("".join(lines), encoding="utf-8")

    print(f"Pilot report: {PILOT_REPORT}")
    print(f"Workbook: {PILOT_WORKBOOK}")
    print(f"Result: {'PASS' if passed else 'FAIL'} ({len(all_failures)} failures)")
    print(f"Tiers: {summary['tiers']}")
    print(f"Confidence: {summary['confidence']}")
    print(f"Review flag: {summary['review_flag']}")
    if all_failures:
        for f in all_failures[:30]:
            print(f"  - {f}")
        if len(all_failures) > 30:
            print(f"  ... and {len(all_failures) - 30} more")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
