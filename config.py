"""Configuration for the Alaska/Hawaiian Airlines tail-spend enrichment agentic backend.

All paths default to workspace-relative locations so the package can be opened and run
directly from VS Code on any OS. Environment variables override every default.
"""

from __future__ import annotations

import os
from pathlib import Path

try:  # auto-load .env so AWS / OpenAI / model-id env vars are available everywhere
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except Exception:  # pragma: no cover
    pass

ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = Path(os.getenv("TAILSPEND_OUTPUT_DIR", str(ROOT_DIR / "outputs")))
DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_INPUT_WORKBOOK = Path(os.getenv(
    "TAILSPEND_INPUT",
    str(ROOT_DIR / "Tailspend_Project Reimagine_20260512.xlsx"),
))
DEFAULT_FINAL_WORKBOOK = Path(os.getenv(
    "TAILSPEND_FINAL_WORKBOOK",
    str(ROOT_DIR / "Alaska_Airlines_Tailspend_AI_Enriched_exact_URLs.xlsx"),
))

SOURCE_SHEET = os.getenv("TAILSPEND_SOURCE_SHEET", "Updated Tail Spend")
FINAL_SHEET = os.getenv("TAILSPEND_FINAL_SHEET", "AI Enriched Tail Spend")

LLM_MODEL = os.getenv("TAILSPEND_MODEL") or os.getenv("BEDROCK_MODEL_ID")
BATCH_SIZE = int(os.getenv("TAILSPEND_BATCH_SIZE", "10"))
FAST_FALLBACK = os.getenv("TAILSPEND_FAST_FALLBACK", "0") == "1"

CACHE_DIR = Path(os.getenv("TAILSPEND_CACHE_DIR", str(ROOT_DIR / "caches")))
ENRICHMENT_CACHE = Path(os.getenv(
    "TAILSPEND_ENRICHMENT_CACHE",
    str(CACHE_DIR / "enrichment_cache.jsonl"),
))
EXACT_URL_CACHE = Path(os.getenv(
    "TAILSPEND_EXACT_URL_CACHE",
    str(CACHE_DIR / "exact_urls_ddgs_cache.json"),
))

GENERATED_ENRICHMENT_CACHE = Path(os.getenv(
    "TAILSPEND_GENERATED_ENRICHMENT_CACHE",
    str(DEFAULT_OUTPUT_DIR / "enrichment_cache.generated.jsonl"),
))
GENERATED_URL_CACHE = Path(os.getenv(
    "TAILSPEND_GENERATED_URL_CACHE",
    str(DEFAULT_OUTPUT_DIR / "exact_urls.generated.json"),
))
SOURCE_URL_AUDIT = Path(os.getenv(
    "TAILSPEND_SOURCE_URL_AUDIT",
    str(DEFAULT_OUTPUT_DIR / "source_url_audit.csv"),
))

OUTPUT_WORKBOOK = Path(os.getenv("TAILSPEND_OUTPUT_WORKBOOK", str(DEFAULT_OUTPUT_DIR / "tailspend_agentic_output.xlsx")))
FIRST10_OUTPUT = Path(os.getenv("TAILSPEND_FIRST10_OUTPUT", str(DEFAULT_OUTPUT_DIR / "first10_agentic_output.xlsx")))
REGRESSION_REPORT = Path(os.getenv("TAILSPEND_REGRESSION_REPORT", str(DEFAULT_OUTPUT_DIR / "first10_regression_report.md")))
REGRESSION_CSV = Path(os.getenv("TAILSPEND_REGRESSION_CSV", str(DEFAULT_OUTPUT_DIR / "first10_regression_details.csv")))
LIVE_RESEARCH_REPORT = Path(os.getenv(
    "TAILSPEND_LIVE_RESEARCH_REPORT",
    str(DEFAULT_OUTPUT_DIR / "live_research_quality_report.md"),
))

APPENDED_COLUMNS = [
    "What they do",
    "Top 3 Savings Levers",
    "Competitors within Spend",
    "Market Competitors",
    "AI Contract Structure",
    "AI Confidence",
    "AI Research Basis",
    "AI Review Flag",
    "AI Potential Consolidation Review",
    "Exact URLs Leveraged for Study",
    "Evidence Tier",
]

URL_MANUAL_VALIDATION_TEXT = "NO EXACT SUPPLIER URL RETRIEVED - MANUAL VALIDATION REQUIRED"
URL_RESEARCH_FAILED_TEXT = "NO EXACT SOURCE FOUND - RESEARCH FAILED"
FALLBACK_RESEARCH_BASIS = "category inference"
PROMPT_VERSION = "live-research/v2-evidence-gated"

# Evidence-tier research_basis vocabulary (Manus critique remediation).
RESEARCH_BASIS_OFFICIAL = "supplier website"
RESEARCH_BASIS_OFFICIAL_PLUS_LISTING = "supplier website + secondary listing"
# Manus v4: category-inference is no longer a permitted FINAL basis. The
# constant remains for backwards-compat (cache replay of legacy rows) but the
# orchestrator routes any row that would have used it through either
# RESEARCH_BASIS_SECONDARY_LISTING (Tier B with non-weak listing URLs) or
# RESEARCH_BASIS_MANUAL_REVIEW (no defensible URL after second-pass search).
RESEARCH_BASIS_CATEGORY_ONLY = "category inference - no official source"
RESEARCH_BASIS_SECONDARY_LISTING = "secondary listing"
RESEARCH_BASIS_MANUAL_REVIEW = "manual review required"

# Allowed FINAL-export research_basis values (validator enforces this set).
ALLOWED_FINAL_RESEARCH_BASIS = {
    RESEARCH_BASIS_OFFICIAL,
    RESEARCH_BASIS_OFFICIAL_PLUS_LISTING,
    RESEARCH_BASIS_SECONDARY_LISTING,
    RESEARCH_BASIS_MANUAL_REVIEW,
}

# Manus v4: controlled neutral statement for unresolved suppliers. Written as
# the value of every enrichment column on rows where second-pass URL search
# could not produce non-weak supplier evidence. Carefully phrased to NOT
# overlap with BANNED_FINAL_PHRASES ("manual validation", "not substantiated",
# "no exact source", "research failed").
UNRESOLVED_NEUTRAL_STATEMENT = (
    "Supplier identity could not be source-grounded from accepted public "
    "sources after second-pass search. Route to manual review before "
    "procurement use."
)
UNRESOLVED_URL_SENTINEL = "no candidate URLs found after second-pass search"

# Manus v2 critique remediation: two-sheet workbook + Manual Validation routing.
EXECUTIVE_READY_SHEET = "Enriched (executive-ready)"
MANUAL_VALIDATION_SHEET = "Manual Validation"
QUARANTINE_WHAT_THEY_DO = (
    "Exact supplier business description not substantiated from accepted "
    "public sources; manual validation required."
)
QUARANTINE_GENERIC_NOT_SUBSTANTIATED = (
    "Not substantiated from accepted public sources; manual validation required."
)
REVIEW_FLAG_MANUAL = "Manual Validation Required"

# Quarantine-reason vocabulary (written into the Manual Validation sheet).
QUARANTINE_REASON_NO_EVIDENCE_GRADE = "no_evidence_grade_url"
QUARANTINE_REASON_GROUNDING_FAILED = "grounding_failed"
QUARANTINE_REASON_CONTRADICTION = "contradiction_detected"
QUARANTINE_REASON_LISTING_ONLY = "listing_only"
QUARANTINE_REASON_NO_URL = "no_url"
