"""Final-output validator (QA).

Hard-fails the workbook write when any exported enrichment field still contains
an internal QA placeholder phrase. The contract: the user-facing workbook MUST
contain real source-grounded enrichment text in every row. Uncertainty is
allowed only via the AI Confidence and AI Review Flag columns.
"""
from __future__ import annotations

from typing import Any, Dict, List

from config import (
    ALLOWED_FINAL_RESEARCH_BASIS,
    RESEARCH_BASIS_CATEGORY_ONLY,
)


# Lowercased phrases that must NEVER appear in any exported enrichment field.
BANNED_FINAL_PHRASES: List[str] = [
    "manual validation required",
    "not substantiated from accepted public sources",
    "no exact source found",
    "research failed",
]

# evidence-tier: bases that are forbidden in the FINAL exported workbook.
BANNED_FINAL_BASIS: List[str] = [
    RESEARCH_BASIS_CATEGORY_ONLY.lower(),
    "category inference (fallback)",
    "category inference",
]

# evidence-tier: generic category-template phrases that must NEVER appear in the
# exported `What they do` column. They reveal that the row is a category
# placeholder rather than supplier-specific source-grounded text.
BANNED_GENERIC_DESCRIPTION_TEXT: List[str] = [
    "category:",
    "typical suppliers",
    "suppliers in this category",
]

# Columns produced by QAGovernanceAgent.excel_field_map() that must contain
# real text (not placeholders). Names are lowercase-sensitive matches to the
# excel_field_map keys.
ENRICHMENT_FIELDS: List[str] = [
    "What they do",
    "AI Contract Structure",
    "Top 3 Savings Levers",
    "Market Competitors",
]

SOURCE_URL_FIELD = "Exact URLs Leveraged for Study"
BASIS_FIELD = "AI Research Basis"
DESCRIPTION_FIELD = "What they do"
TIER_FIELD = "Evidence Tier"
CONFIDENCE_FIELD = "AI Confidence"
REVIEW_FLAG_FIELD = "AI Review Flag"


class FinalOutputValidationError(Exception):
    """Raised when an exported row still carries a banned placeholder phrase."""


def validate_final_enrichment_row(row: Dict[str, Any]) -> None:
    """Raise ``FinalOutputValidationError`` if any banned phrase appears in
    the row's enrichment fields.

    ``row`` is a dict-like mapping of column name to value (e.g. the result of
    ``DataFrame.iloc[i].to_dict()``).
    """
    for field in ENRICHMENT_FIELDS:
        value = row.get(field, "")
        if value is None:
            text = ""
        else:
            text = str(value).strip().lower()
        if not text:
            raise FinalOutputValidationError(
                f"Empty enrichment field '{field}' in row "
                f"{row.get('Cleansed Vendor Name') or row.get('Vendor Name') or '<unknown>'}"
            )
        for phrase in BANNED_FINAL_PHRASES:
            if phrase in text:
                raise FinalOutputValidationError(
                    f"Banned placeholder phrase '{phrase}' found in field "
                    f"'{field}' for row "
                    f"{row.get('Cleansed Vendor Name') or row.get('Vendor Name') or '<unknown>'}"
                )


def _vendor_label(row: Dict[str, Any]) -> str:
    return str(
        row.get("Vendor Name")
        or row.get("Cleansed Vendor Name")
        or "<unknown>"
    )


def _is_blank_url(value: Any) -> bool:
    if value is None:
        return True
    s = str(value).strip()
    if not s:
        return True
    return s.lower() in {"nan", "none", "null", "[]"}


def validate_no_final_category_inference(rows: List[Dict[str, Any]]) -> None:
    """evidence-tier hard gate: fail the export if any final row carries a banned
    category-inference basis, generic category-template text, missing source
    URLs, an out-of-allowlist research_basis, or a Tier-B / Low / Medium row
    without a 'Yes' review flag.

    ``rows`` is an iterable of dict-like row records (e.g. a DataFrame row
    converted via ``df.iloc[i].to_dict()``).
    """
    failures: List[str] = []
    allowed_bases = {b.lower() for b in ALLOWED_FINAL_RESEARCH_BASIS}
    for i, row in enumerate(rows, start=2):  # start=2 to mirror Excel row numbering
        vendor = _vendor_label(row)
        basis = str(row.get(BASIS_FIELD, "")).strip().lower()
        description = str(row.get(DESCRIPTION_FIELD, "")).strip().lower()
        urls = row.get(SOURCE_URL_FIELD, "")
        tier = str(row.get(TIER_FIELD, "")).strip().upper()
        confidence = str(row.get(CONFIDENCE_FIELD, "")).strip().lower()
        review = str(row.get(REVIEW_FLAG_FIELD, "")).strip().lower()

        if basis in BANNED_FINAL_BASIS:
            failures.append(f"row={i} vendor={vendor}: banned final basis {basis!r}")
        if basis and basis not in allowed_bases:
            failures.append(
                f"row={i} vendor={vendor}: research_basis {basis!r} not in allowed set"
            )

        for term in BANNED_GENERIC_DESCRIPTION_TEXT:
            if term in description:
                failures.append(
                    f"row={i} vendor={vendor}: banned generic description text {term!r}"
                )

        if _is_blank_url(urls):
            failures.append(f"row={i} vendor={vendor}: missing source URL field")

        if tier == "B" and review not in {"yes", "true", "1", "y"}:
            failures.append(
                f"row={i} vendor={vendor}: Tier B without 'Yes' review flag"
            )
        if confidence in {"low", "medium"} and review not in {"yes", "true", "1", "y"}:
            failures.append(
                f"row={i} vendor={vendor}: {confidence}-confidence without 'Yes' review flag"
            )

    if failures:
        raise FinalOutputValidationError(
            "Final enrichment export failed evidence-tier quality gate:\n"
            + "\n".join(failures)
        )
