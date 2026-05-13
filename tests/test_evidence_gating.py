"""Unit tests for evidence-tier gating (URL agent + QA enforcement).

These tests exercise the deterministic, non-LLM parts of the evidence gate so
they can run offline and fast (no Bedrock / DDGS / OpenAI calls required).

Coverage:
- ExactURLAgent.is_official_url: vendor-apex match, listing-host rejection,
  bad-host rejection.
- ExactURLAgent.classify_tier: A (official + listing), B (listing-only),
  C (no URLs).
- QAGovernanceAgent.enforce_evidence_calibration: Tier B/C overrides; Tier A
  banned-phrase normalization; vendor-specific sentence scrubbing.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.url_agent import (  # noqa: E402
    ExactURLAgent,
    SOURCE_TYPE_OFFICIAL_SUPPLIER,
    SOURCE_TYPE_OFFICIAL_GOVERNMENT,
    SOURCE_TYPE_IDENTITY_REGISTRY,
    SOURCE_TYPE_DIRECTORY_LISTING,
    SOURCE_TYPE_SOCIAL_MEDIA,
    SOURCE_TYPE_UNRELATED,
)
from agents.qa_governance_agent import QAGovernanceAgent  # noqa: E402
from agents.grounding_agent import (  # noqa: E402
    check_claim_grounding,
    check_contradiction,
)


def _agent() -> ExactURLAgent:
    # cache_path doesn't matter for these tests; pass a Path that may or may not exist.
    return ExactURLAgent(cache_path=Path(PROJECT_ROOT / "caches" / "exact_urls_ddgs_cache.json"))


def test_official_url_apex_match() -> list[str]:
    a = _agent()
    cases = [
        ("ABELS GOLD COAST TIRES", "https://www.abelsgoldcoasttires.com/", True),
        ("ACCELYA WORLD", "https://w3.accelya.com/about", True),
        ("100 PERCENT LLC", "https://www.100percent.com/", True),
        ("ABELS GOLD COAST TIRES", "https://www.dnb.com/foo", False),
        ("ABELS GOLD COAST TIRES", "https://www.bizapedia.com/x", False),
        ("354TH FORCE SUPPORT SQUADRON", "https://www.rallypoint.com/units/354-fss", False),
        ("FOO", "https://www.softonic.com/x", False),
    ]
    failures: list[str] = []
    for vendor, url, expected in cases:
        got = a.is_official_url(vendor, url)
        if got != expected:
            failures.append(f"is_official_url({vendor!r}, {url!r}) = {got}, expected {expected}")
    return failures


def test_classify_tier() -> list[str]:
    a = _agent()
    failures: list[str] = []
    # Tier A: official URL plus a listing URL
    tier, off, lst = a.classify_tier("ABELS GOLD COAST TIRES",
                                     ["https://www.abelsgoldcoasttires.com/", "https://www.dnb.com/x"])
    if tier != "A" or off != ["https://www.abelsgoldcoasttires.com/"] or lst != ["https://www.dnb.com/x"]:
        failures.append(f"Tier A classify failed: {tier},{off},{lst}")
    # Tier B: listing-only
    tier, off, lst = a.classify_tier("354TH FORCE SUPPORT SQUADRON",
                                     ["https://www.rallypoint.com/units/x"])
    if tier != "B" or off != [] or lst != ["https://www.rallypoint.com/units/x"]:
        failures.append(f"Tier B classify failed: {tier},{off},{lst}")
    # Tier C: empty
    tier, off, lst = a.classify_tier("ANY", [])
    if tier != "C" or off != [] or lst != []:
        failures.append(f"Tier C classify failed: {tier},{off},{lst}")
    return failures


def test_qa_enforce_tier_b_c() -> list[str]:
    qa = QAGovernanceAgent()
    failures: list[str] = []
    raw = {
        "what_they_do": "100 PERCENT LLC is a leading provider of outsourced BPO services.",
        "top_3_savings_levers": "x; y; z",
        "market_competitors": "ADP, Accenture, Genpact, Infosys, Wipro",
        "contract_structure": "Fixed fee.",
        "confidence": "High",
        "research_basis": "known company/category knowledge",
        "review_flag": "No",
    }
    out = qa.enforce_evidence_calibration(raw, "B", vendor="100 PERCENT LLC")
    if out["confidence"] != "Low":
        failures.append(f"Tier B did not force Low confidence: {out['confidence']}")
    if out["review_flag"] != "Yes":
        failures.append(f"Tier B did not force Yes review_flag (QA): {out['review_flag']}")
    if out["research_basis"] != "manual review required":
        failures.append(f"Tier B research_basis wrong: {out['research_basis']}")
    # evidence-tier: legacy fallback overwrites enrichment text with the controlled
    # neutral statement, so any vendor-specific sentence MUST be gone.
    if "100 percent" in out["what_they_do"].lower():
        failures.append(f"Tier B failed to scrub vendor-specific sentence: {out['what_they_do']}")
    return failures


def test_qa_enforce_tier_a_normalizes_banned_phrase() -> list[str]:
    qa = QAGovernanceAgent()
    failures: list[str] = []
    raw = {
        "what_they_do": "Snippet-grounded description.",
        "top_3_savings_levers": "x; y; z",
        "market_competitors": "A, B, C, D, E",
        "contract_structure": "Fixed fee.",
        "confidence": "High",
        "research_basis": "supplier website + category context",  # banned phrase
        "review_flag": "No",
    }
    out = qa.enforce_evidence_calibration(raw, "A", vendor="ANY")
    if out["research_basis"] != "supplier website":
        failures.append(f"Tier A banned phrase not normalized: {out['research_basis']}")
    if out["confidence"] != "High":
        failures.append(f"Tier A should not change High confidence: {out['confidence']}")
    return failures


def test_classify_source_type() -> list[str]:
    a = _agent()
    failures: list[str] = []
    cases = [
        ("ABELS GOLD COAST TIRES", "https://www.abelsgoldcoasttires.com/", SOURCE_TYPE_OFFICIAL_SUPPLIER),
        ("354TH FORCE SUPPORT SQUADRON", "https://www.eielson.af.mil/", SOURCE_TYPE_OFFICIAL_GOVERNMENT),
        ("ANY", "https://www.dnb.com/x", SOURCE_TYPE_IDENTITY_REGISTRY),
        ("ANY", "https://www.bizapedia.com/x", SOURCE_TYPE_IDENTITY_REGISTRY),
        ("ANY", "https://www.opencorporates.com/x", SOURCE_TYPE_IDENTITY_REGISTRY),
        ("ANY", "https://www.zoominfo.com/x", SOURCE_TYPE_IDENTITY_REGISTRY),
        ("ANY", "https://www.yelp.com/biz/x", SOURCE_TYPE_DIRECTORY_LISTING),
        ("ANY", "https://www.yellowpages.com/x", SOURCE_TYPE_DIRECTORY_LISTING),
        ("ANY", "https://en.softonic.com/x", SOURCE_TYPE_DIRECTORY_LISTING),
        ("ANY", "https://www.facebook.com/x", SOURCE_TYPE_SOCIAL_MEDIA),
        ("ANY", "https://www.linkedin.com/company/x", SOURCE_TYPE_SOCIAL_MEDIA),
        ("ANY", "https://twitter.com/x", SOURCE_TYPE_SOCIAL_MEDIA),
        ("ANY", "https://x.com/x", SOURCE_TYPE_SOCIAL_MEDIA),
        ("ANY", "https://www.example.com/random", SOURCE_TYPE_UNRELATED),
    ]
    for vendor, url, expected in cases:
        got = a.classify_source_type(vendor, url)
        if got != expected:
            failures.append(f"classify_source_type({vendor!r}, {url!r}) = {got}, expected {expected}")
    return failures


def test_classify_tier_uses_source_type() -> list[str]:
    """Tier A must require an EVIDENCE_GRADE source-type, not just any apex match."""
    a = _agent()
    failures: list[str] = []
    # ABELS apex match -> official_supplier -> Tier A
    tier, _, _ = a.classify_tier("ABELS GOLD COAST TIRES", ["https://www.abelsgoldcoasttires.com/"])
    if tier != "A":
        failures.append(f"abels apex should be Tier A, got {tier}")
    # Only D&B / Bizapedia (identity_registry_only) -> Tier B
    tier, off, _ = a.classify_tier("ANY", ["https://www.dnb.com/x", "https://www.bizapedia.com/y"])
    if tier != "B" or off != []:
        failures.append(f"identity-only should be Tier B, got {tier},{off}")
    # Only Softonic -> Tier B
    tier, off, _ = a.classify_tier("354TH FORCE SUPPORT SQUADRON",
                                   ["https://354th.en.softonic.com/"])
    if tier != "B" or off != []:
        failures.append(f"softonic-only should be Tier B, got {tier},{off}")
    # eielson.af.mil -> official_government -> Tier A
    tier, off, _ = a.classify_tier("354TH FORCE SUPPORT SQUADRON",
                                   ["https://www.eielson.af.mil/"])
    if tier != "A" or "https://www.eielson.af.mil/" not in off:
        failures.append(f".mil should be Tier A, got {tier},{off}")
    return failures


def test_grounding_basic() -> list[str]:
    failures: list[str] = []
    # Grounded: tire-shop description vs tire-shop snippet
    desc = "Family-owned commercial tire service provider offering mobile tire repair and fleet management."
    snip = ["ABELS Gold Coast Tires - commercial tire repair, fleet management, and mobile service near LAX."]
    r = check_claim_grounding(desc, snip, vendor="ABELS GOLD COAST TIRES")
    if not r.grounded:
        failures.append(f"tire/tire grounding should pass: {r}")
    # Not grounded: BPO description vs goggles snippet (the 100PERCENT case)
    desc2 = "Provides outsourced business process services supporting administrative functions for airline operations."
    snip2 = ["100% offers premium goggles, sunglasses, helmets, and apparel for moto, MTB, and cycling."]
    r2 = check_claim_grounding(desc2, snip2, vendor="100 PERCENT LLC")
    if r2.grounded:
        failures.append(f"BPO/goggles grounding should fail: {r2}")
    # Not grounded: empty evidence
    r3 = check_claim_grounding("anything goes here as long as it has words.", [], vendor="x")
    if r3.grounded:
        failures.append(f"empty evidence must not ground: {r3}")
    return failures


def test_contradiction_basic() -> list[str]:
    failures: list[str] = []
    # Contradiction: BPO desc vs goggles evidence
    desc = "Outsourced BPO and back-office services for airlines."
    snip = ["Premium goggles, eyewear, and helmets for moto and MTB athletes."]
    r = check_contradiction(desc, snip, vendor="100 PERCENT LLC")
    if not r.contradicts:
        failures.append(f"BPO vs goggles should be contradiction: {r}")
    # No contradiction: tire desc vs tire snippet
    desc2 = "Tire repair and fleet management services."
    snip2 = ["Commercial tire shop, fleet vehicle service."]
    r2 = check_contradiction(desc2, snip2, vendor="ABELS")
    if r2.contradicts:
        failures.append(f"tire/tire should NOT be contradiction: {r2}")
    return failures


def test_qa_quarantine_output() -> list[str]:
    # QA: quarantine_output remains for internal audit but is no longer
    # used by the orchestrator. We keep a smoke test on the function itself
    # without asserting it's wired into exports (it must NOT be).
    qa = QAGovernanceAgent()
    failures: list[str] = []
    out = qa.quarantine_output({"vendor_name": "x"}, reason="grounding_failed")
    if not out.get("_quarantined"):
        failures.append("quarantine flag missing")
    if out.get("_quarantine_reason") != "grounding_failed":
        failures.append(f"quarantine reason wrong: {out.get('_quarantine_reason')}")
    return failures


def test_evidence_for_llm_filters_weak_domains() -> list[str]:
    """QA: evidence_for_llm() must drop WEAK_EVIDENCE_DOMAINS hosts so
    the LLM never sees a yelp/yellowpages/bizapedia snippet as primary evidence."""
    from agents.url_agent import URLResult, WEAK_EVIDENCE_DOMAINS  # noqa: WPS433
    a = _agent()
    failures: list[str] = []
    r = URLResult(source_key="x", vendor="x", l1="", l2="")
    r.evidence_grade_urls = [
        "https://www.3eco.com/",
        "https://www.yelp.com/biz/3eco",
        "https://www.bizapedia.com/3eco",
    ]
    out = a.evidence_for_llm(r)
    for u in out:
        host = u.split("//", 1)[-1].split("/", 1)[0].lower().removeprefix("www.")
        for weak in WEAK_EVIDENCE_DOMAINS:
            if host == weak or host.endswith("." + weak):
                failures.append(f"weak host {host} leaked into LLM input")
    if "https://www.3eco.com/" not in out:
        failures.append("official 3eco URL was dropped")
    return failures


def test_seed_urls_in_constants() -> list[str]:
    """QA: SUPPLIER_SEED_URLS must contain audited entries for the 5 hard cases."""
    from agents.url_agent import SUPPLIER_SEED_URLS  # noqa: WPS433
    failures: list[str] = []
    expected_keys = {
        "3E CO ENVIRON ECOL & ENG LLC",
        "617436BC LTD DBA FREIGHT LINK",
        "354TH FORCE SUPPORT SQUADRON",
        "5TH AVENUE THEATRE",
        "121 AT BNA LLC",
    }
    missing = expected_keys - set(SUPPLIER_SEED_URLS.keys())
    if missing:
        failures.append(f"SUPPLIER_SEED_URLS missing keys: {sorted(missing)}")
    for key in expected_keys & set(SUPPLIER_SEED_URLS.keys()):
        urls = SUPPLIER_SEED_URLS[key]
        if not urls or not all(u.startswith("http") for u in urls):
            failures.append(f"SUPPLIER_SEED_URLS[{key!r}] invalid: {urls}")
    return failures


def test_build_supplier_queries_alias_expansion() -> list[str]:
    """QA: queries_for() must expand using VENDOR_ALIAS_DICT for known hard cases."""
    from agents.url_agent import VENDOR_ALIAS_DICT  # noqa: WPS433
    a = _agent()
    failures: list[str] = []
    queries = a.queries_for("3E CO ENVIRON ECOL & ENG LLC", "Professional Services", "Advisory & Management Consulting")
    aliases = VENDOR_ALIAS_DICT.get("3E CO ENVIRON ECOL & ENG LLC", [])
    if aliases and not any(alias in queries for alias in aliases):
        failures.append(f"queries_for did not include any alias from {aliases}: {queries}")
    return failures


def test_final_validator_blocks_banned_phrases() -> list[str]:
    """QA: validate_final_enrichment_row must raise on any banned phrase."""
    from agents.final_validator import (  # noqa: WPS433
        FinalOutputValidationError,
        validate_final_enrichment_row,
    )
    failures: list[str] = []
    good_row = {
        "What they do": "Real description.",
        "AI Contract Structure": "Fixed fee.",
        "Top 3 Savings Levers": "a; b; c",
        "Market Competitors": "x, y, z",
        "Cleansed Vendor Name": "GOOD",
    }
    try:
        validate_final_enrichment_row(good_row)
    except FinalOutputValidationError as exc:
        failures.append(f"good row should not raise: {exc}")
    bad_row = dict(good_row)
    bad_row["What they do"] = "Not substantiated from accepted public sources; manual validation required."
    try:
        validate_final_enrichment_row(bad_row)
        failures.append("bad row did not raise")
    except FinalOutputValidationError:
        pass
    return failures


def main() -> int:
    all_failures: list[str] = []
    tests = [
        ("test_official_url_apex_match", test_official_url_apex_match),
        ("test_classify_tier", test_classify_tier),
        ("test_qa_enforce_tier_b_c", test_qa_enforce_tier_b_c),
        ("test_qa_enforce_tier_a_normalizes_banned_phrase", test_qa_enforce_tier_a_normalizes_banned_phrase),
        ("test_classify_source_type", test_classify_source_type),
        ("test_classify_tier_uses_source_type", test_classify_tier_uses_source_type),
        ("test_grounding_basic", test_grounding_basic),
        ("test_contradiction_basic", test_contradiction_basic),
        ("test_qa_quarantine_output", test_qa_quarantine_output),
        ("test_evidence_for_llm_filters_weak_domains", test_evidence_for_llm_filters_weak_domains),
        ("test_seed_urls_in_constants", test_seed_urls_in_constants),
        ("test_build_supplier_queries_alias_expansion", test_build_supplier_queries_alias_expansion),
        ("test_final_validator_blocks_banned_phrases", test_final_validator_blocks_banned_phrases),
    ]
    for name, fn in tests:
        failures = fn()
        for f in failures:
            all_failures.append(f"{name}: {f}")
    if all_failures:
        print(f"FAIL ({len(all_failures)} failures)")
        for f in all_failures:
            print(f"  - {f}")
        return 1
    print(f"PASS ({len(tests)} tests)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
