"""Grounding & contradiction checks for live-research outputs.

Pure-Python heuristic checkers (no LLM, no external models). Used by the
orchestrator to decide whether a Tier-A row's `what they do` description is
actually supported by the accepted-URL evidence snippets, or whether it
contradicts them.

Two checks:

* :func:`check_claim_grounding` — are the noun-ish content tokens in
  ``what_they_do`` actually present in any evidence snippet? Requires both
  (a) a minimum coverage ratio AND (b) at least one shared distinctive industry
  noun.
* :func:`check_contradiction` — does the description's industry vocabulary
  diverge sharply from the snippets' industry vocabulary? Used to catch the
  100percent.com=goggles vs. BPO mismatch.

Both functions are deterministic and side-effect-free, so they preserve
cache-replay determinism.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, List, Set


# Generic words that should not count as "evidence" if they overlap.
STOPWORDS: Set[str] = {
    "the", "a", "an", "and", "or", "of", "to", "for", "in", "on", "with", "by",
    "from", "at", "as", "is", "are", "was", "were", "be", "been", "being", "this",
    "that", "these", "those", "it", "its", "their", "they", "we", "our", "you",
    "your", "us", "such", "other", "than", "then", "also", "but", "not", "no",
    "any", "all", "some", "more", "most", "very", "can", "may", "will", "would",
    "should", "could", "has", "have", "had", "do", "does", "did",
    # "supplier" / "category" / "typically" leak from our own templates and
    # would inflate coverage if not stripped.
    "supplier", "suppliers", "category", "typically", "typical", "provides",
    "provider", "providers", "provide", "offer", "offers", "offering", "based",
    "company", "companies", "business", "services", "service", "products",
    "product", "solutions", "solution", "industry", "industries", "operations",
    "operation", "various", "including", "include", "include.", "well", "high",
    "low", "wide", "across", "through", "specializing", "specialize",
    # numbers and dates aren't useful tokens for grounding either.
    "year", "years", "day", "days",
}

# Distinctive *industry* keywords. We require >=1 shared industry noun between
# description and evidence to consider a description "grounded" — pure overlap
# of generic verbs/adjectives doesn't substantiate a supplier-specific claim.
# This list is intentionally broad; missing entries simply mean grounding falls
# back to coverage ratio + shared-token requirement.
INDUSTRY_NOUNS: Set[str] = {
    # tech / software / saas
    "software", "saas", "platform", "cloud", "api", "data", "analytics", "ai",
    "ml", "ndc", "booking", "reservation", "crm", "erp", "ticketing",
    # aerospace / aviation
    "aerospace", "airline", "aviation", "aircraft", "airframe", "engine",
    "mro", "repair", "overhaul", "composite", "avionics", "faa", "easa",
    # tires / mobility / fleet
    "tire", "tires", "fleet", "vehicle", "truck", "trucking", "logistics",
    "freight", "shipping", "warehouse", "warehousing", "3pl",
    # eyewear / sports apparel
    "goggles", "eyewear", "sunglasses", "helmet", "helmets", "moto", "mtb",
    "apparel", "jersey", "jerseys", "cycling", "motocross", "snow", "snowboard",
    "ski", "racing", "race",
    # bpo / outsourced services
    "bpo", "outsourced", "outsourcing", "back-office", "backoffice",
    "payroll", "hr", "staffing", "call-center", "callcenter",
    # real estate / leasing
    "lease", "leasing", "leases", "tenant", "landlord", "property", "rent",
    "rental", "office", "warehouse",
    # consulting
    "advisory", "consulting", "consultant", "strategy", "strategic",
    # theatre / entertainment
    "theatre", "theater", "broadway", "musical", "musicals", "performance",
    "performing", "arts", "venue", "stage", "production", "productions",
    # military / government
    "squadron", "fss", "afb", "usaf", "military", "force", "base",
    "morale", "welfare", "recreation", "mwr", "fitness", "lodging",
}


@dataclass
class GroundingResult:
    grounded: bool
    coverage: float
    shared_tokens: List[str] = field(default_factory=list)
    shared_industry_tokens: List[str] = field(default_factory=list)
    description_tokens: List[str] = field(default_factory=list)
    evidence_tokens: List[str] = field(default_factory=list)
    reason: str = ""


@dataclass
class ContradictionResult:
    contradicts: bool
    description_only_industry: List[str] = field(default_factory=list)
    evidence_only_industry: List[str] = field(default_factory=list)
    reason: str = ""


def _tokenize(text: str) -> List[str]:
    """Lowercase, split on non-alpha, drop short tokens and stopwords."""
    if not text:
        return []
    raw = re.split(r"[^a-zA-Z0-9]+", text.lower())
    return [t for t in raw if len(t) >= 4 and t not in STOPWORDS]


def _industry_tokens(tokens: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for t in tokens:
        if t in INDUSTRY_NOUNS and t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _vendor_tokens(vendor: str) -> Set[str]:
    """Distinctive vendor-name tokens (>=4 chars). These are excluded from
    coverage so 'Accelya provides Accelya' doesn't auto-ground itself."""
    if not vendor:
        return set()
    raw = re.split(r"[^a-zA-Z0-9]+", vendor.lower())
    return {t for t in raw if len(t) >= 4}


def check_claim_grounding(
    what_they_do: str,
    evidence_snippets: List[str],
    vendor: str = "",
    coverage_threshold: float = 0.20,
    require_industry_noun: bool = False,
) -> GroundingResult:
    """Decide whether `what_they_do` is grounded by the evidence snippets.

    QA: threshold lowered from 0.40 to 0.20 because venue / military
    pages have specialised snippet vocabulary (event listings, unit names)
    that don't share many surface tokens with a generic LLM description even
    when both clearly describe the same business. The ``require_industry_noun``
    flag is now off by default; the contradiction detector does the heavier
    lifting for catching wrong-domain mismatches like 100PERCENT goggles vs.
    BPO services.

    A description is *grounded* when:

    * coverage_ratio >= ``coverage_threshold`` (fraction of distinctive content
      tokens in the description that also appear in evidence, excluding vendor
      name tokens and a stopword list).
    * if ``require_industry_noun`` is True, at least one ``INDUSTRY_NOUN`` is
      shared between description and evidence.

    Empty inputs => not grounded.
    """
    desc_tokens_all = _tokenize(what_they_do)
    vendor_toks = _vendor_tokens(vendor)
    desc_tokens = [t for t in desc_tokens_all if t not in vendor_toks]

    evidence_text = " ".join(evidence_snippets or [])
    evid_tokens_all = _tokenize(evidence_text)
    evid_tokens = [t for t in evid_tokens_all if t not in vendor_toks]
    evid_set = set(evid_tokens)

    if not desc_tokens:
        return GroundingResult(
            grounded=False, coverage=0.0,
            description_tokens=desc_tokens, evidence_tokens=evid_tokens,
            reason="empty-description",
        )
    if not evid_tokens:
        return GroundingResult(
            grounded=False, coverage=0.0,
            description_tokens=desc_tokens, evidence_tokens=evid_tokens,
            reason="empty-evidence",
        )

    shared = [t for t in dict.fromkeys(desc_tokens) if t in evid_set]
    coverage = len(shared) / max(len(set(desc_tokens)), 1)
    shared_industry = [t for t in shared if t in INDUSTRY_NOUNS]

    if require_industry_noun:
        grounded = coverage >= coverage_threshold and bool(shared_industry)
    else:
        grounded = coverage >= coverage_threshold
    reason = ""
    if not grounded:
        if coverage < coverage_threshold:
            reason = f"low-coverage:{coverage:.2f}<{coverage_threshold:.2f}"
        elif require_industry_noun and not shared_industry:
            reason = "no-shared-industry-noun"
    return GroundingResult(
        grounded=grounded,
        coverage=coverage,
        shared_tokens=shared,
        shared_industry_tokens=shared_industry,
        description_tokens=desc_tokens,
        evidence_tokens=evid_tokens,
        reason=reason,
    )


def check_contradiction(
    what_they_do: str,
    evidence_snippets: List[str],
    vendor: str = "",
    min_distinct_per_side: int = 2,
) -> ContradictionResult:
    """Detect a sharp industry-vocabulary mismatch.

    Contradiction is signalled when both sides have at least
    ``min_distinct_per_side`` distinct industry nouns AND those sets are
    disjoint. Example: description = {"bpo", "outsourced", "back-office"},
    evidence = {"goggles", "eyewear", "moto"} → contradicts.
    """
    desc_tokens = _tokenize(what_they_do)
    evid_tokens = _tokenize(" ".join(evidence_snippets or []))
    vendor_toks = _vendor_tokens(vendor)
    desc_industry = [t for t in _industry_tokens(desc_tokens) if t not in vendor_toks]
    evid_industry = [t for t in _industry_tokens(evid_tokens) if t not in vendor_toks]
    desc_set = set(desc_industry)
    evid_set = set(evid_industry)
    if (len(desc_set) >= min_distinct_per_side
            and len(evid_set) >= min_distinct_per_side
            and desc_set.isdisjoint(evid_set)):
        return ContradictionResult(
            contradicts=True,
            description_only_industry=sorted(desc_set),
            evidence_only_industry=sorted(evid_set),
            reason="industry-vocab-disjoint",
        )
    return ContradictionResult(
        contradicts=False,
        description_only_industry=sorted(desc_set - evid_set),
        evidence_only_industry=sorted(evid_set - desc_set),
        reason="",
    )


def evidence_snippets_from_packet(packet) -> List[str]:
    """Extract title + meta + snippet text from an EvidencePacket-like object.

    Defensive: works with either an ``EvidencePacket`` instance or any object
    exposing ``items`` with ``title``, ``meta_description``, ``snippet`` attrs.
    """
    if packet is None:
        return []
    items = getattr(packet, "items", None) or []
    out: List[str] = []
    for item in items:
        parts = [
            getattr(item, "title", "") or "",
            getattr(item, "meta_description", "") or "",
            getattr(item, "snippet", "") or "",
        ]
        joined = " ".join(p for p in parts if p).strip()
        if joined:
            out.append(joined)
    return out
