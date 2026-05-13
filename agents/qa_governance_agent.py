"""QA and Governance Agent.

This agent standardizes generated enrichment fields, applies conservative review flags,
and supports first-10 regression validation against the delivered workbook.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List

from .common import clean_text
from config import (
    RESEARCH_BASIS_OFFICIAL,
    RESEARCH_BASIS_OFFICIAL_PLUS_LISTING,
    RESEARCH_BASIS_CATEGORY_ONLY,
    RESEARCH_BASIS_SECONDARY_LISTING,
    RESEARCH_BASIS_MANUAL_REVIEW,
    REVIEW_FLAG_MANUAL,
    QUARANTINE_WHAT_THEY_DO,
    QUARANTINE_GENERIC_NOT_SUBSTANTIATED,
    UNRESOLVED_NEUTRAL_STATEMENT,
)


_BANNED_RESEARCH_BASIS_PHRASES = [
    "known company/category knowledge",
    "supplier website + category context",
    "supplier website only",
    "third-party directory + category context",
    "category inference (fallback)",
    "category inference",
]


class QAGovernanceAgent:
    """Validate and standardize agent outputs for Excel delivery."""

    name = "QA & Governance Agent"

    required_enrichment_keys = [
        "what_they_do",
        "top_3_savings_levers",
        "market_competitors",
        "contract_structure",
        "confidence",
        "research_basis",
        "review_flag",
    ]

    def standardize_market_output(self, enrichment: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
        """Fill missing market-intelligence fields and normalize confidence/review values."""
        l2 = clean_text(record.get("l2"))
        output = dict(enrichment)
        output["what_they_do"] = clean_text(output.get("what_they_do")) or f"Likely provides {l2.lower()} products or services based on supplier name and category."
        output["top_3_savings_levers"] = clean_text(output.get("top_3_savings_levers")) or "Competitive bid; consolidate spend; standardize rates and SLAs"
        output["market_competitors"] = clean_text(output.get("market_competitors")) or f"Other {l2} providers; regional specialists; national suppliers"
        output["contract_structure"] = clean_text(output.get("contract_structure")) or f"Typically priced through negotiated rates, unit prices, subscriptions, or project fees for {l2} scope."
        confidence = clean_text(output.get("confidence")) or "Medium"
        confidence_norm = confidence.title()
        if confidence_norm not in {"High", "Medium", "Low"}:
            confidence_norm = "Medium"
        output["confidence"] = confidence_norm
        output["research_basis"] = clean_text(output.get("research_basis")) or RESEARCH_BASIS_CATEGORY_ONLY
        review_flag = clean_text(output.get("review_flag")) or ("Yes" if confidence_norm == "Low" else "No")
        # QA: AI Review Flag exported value MUST be exactly "Yes" or "No".
        # Internal "Manual Validation Required" string is collapsed to "Yes".
        if review_flag.lower().startswith("manual") or review_flag.lower().startswith("y"):
            output["review_flag"] = "Yes"
        else:
            output["review_flag"] = "No"
        return output

    def enforce_evidence_calibration(
        self,
        standardized: Dict[str, Any],
        evidence_tier: str,
        vendor: str = "",
    ) -> Dict[str, Any]:
        """Override calibration to match the row's evidence tier.

        - Tier A (official URL present): research_basis must be one of the
          'supplier website*' phrases. If model emitted a banned phrase, normalize
          to 'supplier website'. Confidence/review unchanged.
        - Tier B / Tier C: evidence-tier — if the LLM produced a evidence-tier allowed
          basis (secondary listing / manual review required) keep it and only
          force review_flag='Yes'. Otherwise (e.g. legacy 'category inference -
          no official source') retain the legacy override that forces
          confidence='Low', review_flag='Yes', research_basis=category-only,
          and strips vendor-specific sentences as a safety net. The
          orchestrator is responsible for choosing the right Tier B path
          (SECONDARY_LISTING_PROMPT or unresolved synthesizer); this method
          is the LAST line of defence.
        """
        out = dict(standardized)
        rb = clean_text(out.get("research_basis"))
        if evidence_tier == "A":
            allowed = {RESEARCH_BASIS_OFFICIAL, RESEARCH_BASIS_OFFICIAL_PLUS_LISTING}
            if rb not in allowed:
                # Map banned/unknown phrases to the safest allowed value.
                out["research_basis"] = RESEARCH_BASIS_OFFICIAL
            return out
        # Tier B / C.
        if rb in {RESEARCH_BASIS_SECONDARY_LISTING, RESEARCH_BASIS_MANUAL_REVIEW}:
            # Trust the evidence-tier path; only enforce review_flag.
            out["review_flag"] = "Yes"
            confidence = clean_text(out.get("confidence")).title() or "Low"
            if confidence not in {"Low", "Medium"}:
                confidence = "Medium" if rb == RESEARCH_BASIS_SECONDARY_LISTING else "Low"
            out["confidence"] = confidence
            return out
        # evidence-tier legacy fallback. Previously assigned RESEARCH_BASIS_CATEGORY_ONLY
        # ("category inference - no official source") which is now BANNED in
        # final exports. Map to MANUAL_REVIEW so the workbook stays compliant
        # while still flagging the row for manual review and Low confidence.
        # Also overwrite the four enrichment text fields with the controlled
        # neutral statement so any stray category-template text the LLM
        # produced never leaks into the workbook.
        out["confidence"] = "Low"
        out["review_flag"] = "Yes"
        out["research_basis"] = RESEARCH_BASIS_MANUAL_REVIEW
        out["_unresolved"] = True
        out["what_they_do"] = UNRESOLVED_NEUTRAL_STATEMENT
        out["top_3_savings_levers"] = UNRESOLVED_NEUTRAL_STATEMENT
        out["market_competitors"] = UNRESOLVED_NEUTRAL_STATEMENT
        out["contract_structure"] = UNRESOLVED_NEUTRAL_STATEMENT
        return out

    def quarantine_output(self, record: Dict[str, Any], reason: str = "") -> Dict[str, Any]:
        """Produce the prescribed Manual-Validation row payload (QA critique).

        Used for rows that have no evidence-grade URL, fail claim grounding,
        or contradict their evidence. All AI text columns are scrubbed to a
        controlled placeholder so the workbook reader cannot mistake a
        quarantined row for completed enrichment. Quantitative columns from
        the source workbook are unaffected (the orchestrator owns those).
        """
        return {
            "what_they_do": QUARANTINE_WHAT_THEY_DO,
            "top_3_savings_levers": QUARANTINE_GENERIC_NOT_SUBSTANTIATED,
            "market_competitors": QUARANTINE_GENERIC_NOT_SUBSTANTIATED,
            "contract_structure": QUARANTINE_GENERIC_NOT_SUBSTANTIATED,
            "confidence": "Low",
            "research_basis": RESEARCH_BASIS_CATEGORY_ONLY,
            "review_flag": REVIEW_FLAG_MANUAL,
            "_quarantined": True,
            "_quarantine_reason": reason or "",
        }

    @staticmethod
    def _strip_vendor_specific_sentences(text: str, vendor: str) -> str:
        """Remove any sentence that names the vendor as a subject of a fact.

        Heuristic: split on '. ', drop sentences whose lowercased form contains
        any distinctive vendor token (>=4 chars) immediately followed by an
        is/provides/offers/specializes verb. If everything gets stripped, fall
        back to a generic category placeholder.
        """
        if not text:
            return text
        norm_vendor = re.sub(r"[^a-z0-9 ]+", " ", vendor.lower()).split()
        tokens = [t for t in norm_vendor if len(t) >= 4]
        if not tokens:
            return text
        verb_pat = r"\b(is|are|was|were|provides|offers|specializes|operates|supplies|delivers|manufactures|develops|sells)\b"
        kept: List[str] = []
        for sentence in re.split(r"(?<=[.!?])\s+", text):
            low = sentence.lower()
            bad = False
            for tok in tokens:
                if tok in low and re.search(re.escape(tok) + r".{0,40}?" + verb_pat, low):
                    bad = True
                    break
            if not bad:
                kept.append(sentence)
        result = " ".join(kept).strip()
        if not result:
            result = "Category-level placeholder; specific supplier description requires manual research."
        return result

    def research_failed_output(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Produce a fully-populated, audit-clear row when live research fails.

        Used in live-research mode when no acceptable URL was found and fallback
        templates are not allowed. The fields stay populated (so the workbook is
        valid) but every column makes it obvious that the row needs human work.
        """
        l2 = clean_text(record.get("l2"))
        return {
            "what_they_do": QUARANTINE_WHAT_THEY_DO,
            "top_3_savings_levers": QUARANTINE_GENERIC_NOT_SUBSTANTIATED,
            "market_competitors": QUARANTINE_GENERIC_NOT_SUBSTANTIATED,
            "contract_structure": QUARANTINE_GENERIC_NOT_SUBSTANTIATED,
            "confidence": "Low",
            "research_basis": RESEARCH_BASIS_CATEGORY_ONLY,
            "review_flag": REVIEW_FLAG_MANUAL,
            "_research_failed": True,
        }

    def excel_field_map(self, standardized: Dict[str, Any]) -> Dict[str, Any]:
        """Map internal snake_case fields to final workbook column names."""
        return {
            "What they do": standardized.get("what_they_do", ""),
            "Top 3 Savings Levers": standardized.get("top_3_savings_levers", ""),
            "Market Competitors": standardized.get("market_competitors", ""),
            "AI Contract Structure": standardized.get("contract_structure", ""),
            "AI Confidence": standardized.get("confidence", ""),
            "AI Research Basis": standardized.get("research_basis", ""),
            "AI Review Flag": standardized.get("review_flag", ""),
        }

    def compare_rows(self, produced: Dict[str, Any], expected: Dict[str, Any], columns: Iterable[str]) -> List[Dict[str, Any]]:
        """Compare produced and expected row fields exactly after whitespace normalization."""
        details = []
        for column in columns:
            left = clean_text(produced.get(column))
            right = clean_text(expected.get(column))
            details.append({
                "column": column,
                "match": left == right,
                "produced": left,
                "expected": right,
            })
        return details
