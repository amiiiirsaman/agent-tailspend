"""evidence-tier first-100 quality gates (pytest).

These eight gates run against the regenerated first-100 pilot workbook and
must ALL pass before the full 2,726-row run is approved. The gates encode
the hard blockers from the v4 spec:

    1. NO row has research_basis in the v3 banned set.
    2. NO row's What-they-do contains a generic category-template phrase.
    3. EVERY row has a non-blank `Exact URLs Leveraged for Study` value
       (either a real URL list OR the controlled UNRESOLVED_URL_SENTINEL).
    4. NO row uses any forbidden URL placeholder marker (NO EXACT SOURCE
       FOUND / RESEARCH FAILED / MANUAL VALIDATION REQUIRED).
    5. EVERY Tier-A row has at least one non-weak supplier-grade URL.
    6. Tier-B rows must use research_basis 'secondary listing' OR
       'supplier website + secondary listing' OR 'manual review required'
       (never 'category inference').
    7. EVERY row with confidence Low or Medium has AI Review Flag = Yes.
    8. EVERY Tier-B row has AI Review Flag = Yes (hedged-source policy).

Run:  python -m pytest tests/test_first100_quality_gates.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.final_validator import (  # noqa: E402
    BANNED_FINAL_BASIS,
    BANNED_GENERIC_DESCRIPTION_TEXT,
)
from agents.url_agent import WEAK_EVIDENCE_DOMAINS, _host_in_set  # noqa: E402
from config import (  # noqa: E402
    DEFAULT_OUTPUT_DIR,
    RESEARCH_BASIS_MANUAL_REVIEW,
    RESEARCH_BASIS_OFFICIAL,
    RESEARCH_BASIS_SECONDARY_LISTING,
    UNRESOLVED_URL_SENTINEL,
    URL_MANUAL_VALIDATION_TEXT,
    URL_RESEARCH_FAILED_TEXT,
)


PILOT_WORKBOOK = DEFAULT_OUTPUT_DIR / "first100_live_research_output.xlsx"


def _is_blank(value) -> bool:
    s = "" if value is None else str(value).strip()
    return s == "" or s.lower() == "nan"


@pytest.fixture(scope="module")
def pilot_df() -> pd.DataFrame:
    if not PILOT_WORKBOOK.exists():
        pytest.skip(f"Pilot workbook not found: {PILOT_WORKBOOK}")
    df = pd.read_excel(PILOT_WORKBOOK, sheet_name="AI Enriched Tail Spend")
    assert len(df) == 100, f"expected 100 rows, got {len(df)}"
    return df


def test_gate_1_no_banned_research_basis(pilot_df: pd.DataFrame) -> None:
    banned = {b.lower() for b in BANNED_FINAL_BASIS}
    bad = []
    for i, row in pilot_df.iterrows():
        rb = str(row.get("AI Research Basis", "")).strip().lower()
        if rb in banned:
            bad.append((i + 1, row.get("Vendor Name"), rb))
    assert not bad, f"{len(bad)} rows with banned basis: {bad[:10]}"


def test_gate_2_no_generic_category_text(pilot_df: pd.DataFrame) -> None:
    bad = []
    for i, row in pilot_df.iterrows():
        wtd = str(row.get("What they do", "")).lower()
        for phrase in BANNED_GENERIC_DESCRIPTION_TEXT:
            if phrase in wtd:
                bad.append((i + 1, row.get("Vendor Name"), phrase))
                break
    assert not bad, f"{len(bad)} rows with generic category text: {bad[:10]}"


def test_gate_3_every_row_has_url_text(pilot_df: pd.DataFrame) -> None:
    bad = []
    for i, row in pilot_df.iterrows():
        urls = row.get("Exact URLs Leveraged for Study")
        if _is_blank(urls):
            bad.append((i + 1, row.get("Vendor Name")))
    assert not bad, f"{len(bad)} rows with blank URL column: {bad[:10]}"


def test_gate_4_no_forbidden_url_placeholders(pilot_df: pd.DataFrame) -> None:
    forbidden = (URL_RESEARCH_FAILED_TEXT, URL_MANUAL_VALIDATION_TEXT)
    bad = []
    for i, row in pilot_df.iterrows():
        urls = str(row.get("Exact URLs Leveraged for Study", ""))
        for marker in forbidden:
            if marker and marker in urls:
                bad.append((i + 1, row.get("Vendor Name"), marker))
                break
    assert not bad, f"{len(bad)} rows with forbidden URL marker: {bad[:10]}"


def test_gate_5_tier_a_has_non_weak_url(pilot_df: pd.DataFrame) -> None:
    bad = []
    for i, row in pilot_df.iterrows():
        tier = str(row.get("Evidence Tier", "")).strip().upper()
        if tier != "A":
            continue
        urls_text = str(row.get("Exact URLs Leveraged for Study", ""))
        tokens = [u.strip() for u in urls_text.split(";") if u.strip().startswith("http")]
        non_weak = [u for u in tokens if not _host_in_set(u, WEAK_EVIDENCE_DOMAINS)]
        if not non_weak:
            bad.append((i + 1, row.get("Vendor Name"), urls_text[:120]))
    assert not bad, f"{len(bad)} Tier-A rows without non-weak URL: {bad[:10]}"


def test_gate_6_tier_b_basis_in_v4_set(pilot_df: pd.DataFrame) -> None:
    allowed = {
        RESEARCH_BASIS_OFFICIAL.lower() + " + secondary listing",
        RESEARCH_BASIS_SECONDARY_LISTING.lower(),
        RESEARCH_BASIS_MANUAL_REVIEW.lower(),
    }
    bad = []
    for i, row in pilot_df.iterrows():
        tier = str(row.get("Evidence Tier", "")).strip().upper()
        if tier != "B":
            continue
        rb = str(row.get("AI Research Basis", "")).strip().lower()
        if rb not in allowed:
            bad.append((i + 1, row.get("Vendor Name"), rb))
    assert not bad, f"{len(bad)} Tier-B rows with wrong basis: {bad[:10]}"


def test_gate_7_low_or_medium_has_review_flag_yes(pilot_df: pd.DataFrame) -> None:
    bad = []
    for i, row in pilot_df.iterrows():
        conf = str(row.get("AI Confidence", "")).strip().lower()
        flag = str(row.get("AI Review Flag", "")).strip().lower()
        if conf in {"low", "medium"} and flag != "yes":
            bad.append((i + 1, row.get("Vendor Name"), conf, flag))
    assert not bad, f"{len(bad)} Low/Medium rows missing Review Flag = Yes: {bad[:10]}"


def test_gate_8_tier_b_has_review_flag_yes(pilot_df: pd.DataFrame) -> None:
    bad = []
    for i, row in pilot_df.iterrows():
        tier = str(row.get("Evidence Tier", "")).strip().upper()
        flag = str(row.get("AI Review Flag", "")).strip().lower()
        if tier == "B" and flag != "yes":
            bad.append((i + 1, row.get("Vendor Name"), flag))
    assert not bad, f"{len(bad)} Tier-B rows missing Review Flag = Yes: {bad[:10]}"
