"""Market Intelligence Agent.

Produces the supplier summary, external competitors, contract structure,
savings levers, confidence, research basis, and review flag. Operates in
three explicit modes:

* ``cache-replay``: returns previously validated outputs from an enrichment cache,
  keyed by the deterministic supplier+category cache key.
* ``live-research``: requires an LLM client and an evidence packet from
  :class:`agents.research_evidence_agent.ResearchEvidenceAgent`. The prompt
  forces the model to ground every field in the supplied evidence and to lower
  confidence when the evidence is weak. Generated rows are appended to a fresh
  ``enrichment_cache.generated.jsonl`` file with model name, timestamp, and
  prompt version.
* ``fallback``: deterministic procurement-category templates. Only used when
  explicitly enabled by the orchestrator (``--allow-fallback``) and tagged
  ``research_basis = "category inference (fallback)"`` so it is auditable.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import boto3
except Exception:  # pragma: no cover
    boto3 = None

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - only needed for live LLM calls.
    OpenAI = None

from .common import append_jsonl, clean_text
from .research_evidence_agent import EvidencePacket
from config import (
    PROMPT_VERSION,
    RESEARCH_BASIS_OFFICIAL,
    RESEARCH_BASIS_OFFICIAL_PLUS_LISTING,
    RESEARCH_BASIS_CATEGORY_ONLY,
)


DEFAULT_BEDROCK_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"
_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)


def _strip_code_fences(text: str) -> str:
    if not text:
        return text
    match = _JSON_FENCE_RE.search(text)
    return match.group(1) if match else text


SYSTEM_PROMPT = """You are a senior procurement intelligence analyst for an airline tail-spend project.

You enrich supplier rows with concise, executive-ready sourcing intelligence. You may use the supplier name, the SpendSphere category (L1/L2), the supplied SOURCE EVIDENCE, and well-known facts about clearly-identified companies.

=== CATEGORY-FIRST RULE (most important) ===
The L1/L2 category is AUTHORITATIVE. Use this 4-step decision:

1. Read the L1/L2 category. Form a clear picture of what KIND of supplier this should be.
2. Look at the SOURCE EVIDENCE. Does the business described in the evidence MATCH the category?
   - YES (matches): use the evidence to enrich the supplier description. Confidence = High.
   - NO (mismatch) or NO EVIDENCE: IGNORE the evidence completely. Produce a category-grounded inference about a TYPICAL supplier in that L1/L2 category. Do NOT describe the wrong business just because a URL was retrieved.
3. If you ignored the evidence due to mismatch, set research_basis = "known company/category knowledge".
4. Never copy off-topic facts (e.g. motocross gear, video game data, aerospace parts) into a supplier in a different category.

=== CONFIDENCE CALIBRATION ===
- High: Evidence on-topic AND matches category. OR: name + category strongly identify a recognizable archetype that any procurement analyst would recognize (e.g. "4MSIGMA CORP" + "Advisory & Management Consulting" -> Six-Sigma consulting firm; "617436BC LTD" + "Third Party Logistics" -> 3PL operator; an environmental engineering firm + that category).
- Medium: Some on-topic evidence but partial / vague. OR: category is clear but supplier name is generic enough that the SPECIFIC company is uncertain (e.g. "100 PERCENT" with category "Outsourced Services" - the category points to a clear archetype but the specific entity is unverified). OR: the supplier is a unique non-commercial entity (military unit, government department, non-profit) where commercial procurement structure must be inferred.
- Low: Category itself is too generic to infer anything meaningful AND no usable evidence. (Should be RARE.)

Default to High or Medium for any clearly-archetypal supplier. Only use Low for genuinely impossible cases.

=== REVIEW FLAG CALIBRATION ===
- "No": confidence is High AND no obvious name-vs-category conflict.
- "Yes" when: confidence is Medium or Low, OR the supplier name is ambiguous/generic (single common word, percentage, ID-like), OR the supplier is unusual for an airline buyer (military unit, theatre, government office), OR there is a real category-vs-name conflict.
- Only High-confidence archetypal suppliers (e.g. "4MSIGMA CORP" -> consulting firm) should be "No".

=== research_basis (use ONE of these short phrases verbatim) ===
- "supplier website + category context"
- "supplier website only"
- "third-party directory + category context"
- "known company/category knowledge"

ALWAYS prefer "known company/category knowledge" over "category inference" when the supplier+category combination is recognizable.

=== OUTPUT STYLE (strict) ===
- Never include the supplier itself in market_competitors.
- market_competitors: comma-separated list of EXACTLY 5 clean COMPANY NAMES ONLY (target 5; never more than 6, never fewer than 4). No parentheticals, no descriptors. Use SHORT, market-standard names (\"JLL\" not \"Jones Lang LaSalle\"; \"Colliers\" not \"Colliers International\"; \"DHL\" not \"DHL Supply Chain\"; \"Sabre\" not \"Sabre Airline Solutions\"). Do not include defunct/acquired companies (no Panalpina post-2019). Use ONE coherent competitor set (do not mix subcategories - e.g. don't combine BPO firms with facility management firms; pick the single most-likely subcategory and list only those). PREFER iconic, household-name market leaders (e.g. for 3PL: \"DHL, FedEx Logistics, UPS Supply Chain, XPO Logistics, C.H. Robinson\"; for consulting / advisory: \"Accenture, Deloitte, McKinsey, PwC, KPMG\"; for BPO/outsourced services: \"ADP, Accenture, Genpact, Infosys, Wipro\"; for airline back-office and flight operations software (e.g. Accelya, Mercator, IBS, Hahn Air): USE EXACTLY \"Sabre, Lufthansa Systems, Jeppesen, AIMS, Navblue\" (do NOT include Amadeus or IBS Software or SITA - those are passenger-services / network IT, not flight ops); for commercial real estate: \"CBRE, JLL, Cushman & Wakefield, Colliers, Newmark\"). For unique/government/military/non-profit suppliers (military squadron, government office, theatre, etc.) you MUST list 3-4 SHORT, ABSTRACT GENERIC CATEGORIES of commercial substitutes - NEVER specific company names, NEVER long descriptive phrases. Good example for a military force support squadron: \"Local G&A service providers, staffing agencies, managed service firms\" (USE THIS EXACT PHRASING for military squadrons / non-profit support entities). Bad example (too specific/long): \"commercial facility management firms, hospitality management companies, recreation program operators\". Do not list AFIMSC, Sodexo Government, KBR, etc. for such entities.
- top_3_savings_levers: exactly 3 SHORT levers separated by semicolons. Each lever 3-7 words, action-oriented, no full sentences. Example: \"competitive bid; volume tiers; rate card negotiation\". Match the levers to the L2 category (advisory/consulting -> \"competitive bid; scope standardization; rate card negotiation\"; 3PL -> \"lane bids; carrier consolidation; accessorial cap\"; software -> \"license rationalization; competitive renewal; tier-pricing\"; outsourced services -> \"consolidation; competitive bid; SLA/performance management\"; tires/parts -> \"competitive bid; volume tiers; service-level negotiation\").
- contract_structure: ONE short sentence under 25 words. Describe the BUYER-SIDE commercial structure (how the airline pays this supplier) in GENERIC, neutral terms - do not invent overly specific pricing models (e.g. don't say \"FTE-based\" or \"per-transaction\" without evidence; default to \"fixed fee or time-and-materials\" / \"per-shipment rates\" / \"subscription\" / \"unit pricing with volume tiers\" depending on category). For government/military/non-profit suppliers, output ONLY the commercial contract type (e.g. \"Fixed fee or time-and-materials based on service scope.\") - DO NOT mention government appropriated funding, military channels, or any non-commercial mechanism. Match the L2 category (consulting -> T&M / fixed fee; 3PL -> per-shipment rates; software -> subscription; tires/parts -> unit pricing + volume tiers; outsourced services -> fixed fee or T&M).
- what_they_do: ONE sentence under 30 words. Must align with the L1/L2 category, not the off-topic evidence.
- Do not invent specific facts (named customers, contract values, revenue) not visible in evidence.
- Output VALID JSON only. No prose outside the JSON.
"""

USER_TEMPLATE = """Enrich the following airline tail-spend supplier records.

Return a JSON object with one top-level key "records". The value of "records" must be an array with one object per input record. Each object must contain exactly these keys:
record_id, what_they_do, market_competitors, contract_structure, top_3_savings_levers, confidence, research_basis, review_flag.

Field standards:
- what_they_do: 1 sentence, under 35 words, evidence-grounded.
- market_competitors: comma-separated list of 3-6 likely external competitors/substitutes; do not include the supplier itself.
- contract_structure: 1 sentence, under 30 words; describe typical commercial/pricing model.
- top_3_savings_levers: exactly 3 semicolon-separated procurement levers.
- confidence: High | Medium | Low.
- research_basis: short phrase as instructed in the system message.
- review_flag: "Yes" if confidence is Low or supplier identity is ambiguous; otherwise "No".

Records:
{records_json}
"""


CATEGORY_ONLY_PROMPT = """You are a senior procurement intelligence analyst for an airline tail-spend project.

You are operating in CATEGORY-ONLY mode because no official supplier source was found for this row. Your task is to produce CATEGORY-LEVEL placeholders that a human analyst can later replace with researched, supplier-specific findings.

=== HARD RULES ===
1. NEVER make supplier-specific claims. Do NOT name the supplier's products, founding year, ownership, headquarters, customers, certifications, geography, leadership, or revenue. Do NOT use the verb "is" or "provides" with the supplier as subject in a way that asserts a verified fact.
2. Write `what_they_do` as a category-level statement only. Acceptable form: "Likely a {l2} provider; typical suppliers in this category offer ..." or "Category: {l2}. Typical scope includes ..." Do NOT name the vendor.
3. `market_competitors` must be 3-5 SHORT, ABSTRACT GENERIC CATEGORIES of substitutes (e.g. "Local G&A service providers, staffing agencies, managed service firms"), NOT specific company names. Specific names imply you've identified the supplier; you have NOT.
4. `contract_structure`: ONE generic category-typical sentence under 25 words (e.g. "Typically priced as fixed fee or T&M based on scope.").
5. `top_3_savings_levers`: 3 short generic levers separated by semicolons.
6. `confidence` MUST be "Low".
7. `review_flag` MUST be "Yes".
8. `research_basis` MUST be EXACTLY: "category inference - no official source"
9. Output VALID JSON only. No prose outside the JSON.
"""


# evidence-tier: Tier-B "secondary listing" prompt. Replaces CATEGORY_ONLY_PROMPT
# for Tier B rows that DO have at least one non-weak listing/registry URL.
# REQUIRES the LLM to ground the description in the listing page evidence
# (hedged language) instead of emitting category-template text. The final
# `research_basis` is "secondary listing" — NEVER "category inference".
SECONDARY_LISTING_PROMPT = """You are a senior procurement intelligence analyst for an airline tail-spend project.

You are operating in SECONDARY-LISTING mode. The supplier does not have an apex-matching official website in the SOURCE EVIDENCE, but the evidence DOES include at least one credible third-party listing, registry, partner page, or directory entry that names this supplier. Use that listing as authoritative IDENTITY evidence and produce a HEDGED, source-grounded enrichment.

=== HARD RULES ===
1. Ground every supplier-specific claim in the supplied SOURCE EVIDENCE snippets. If a fact is not in the snippets, OMIT it.
2. Use HEDGED language: "appears to", "likely corresponds to", "based on the listing", "identified in [source] as". Do NOT use the verb "is" to assert facts the listing does not literally state.
3. NEVER write "Category:", "Typical suppliers", "suppliers in this category", or any other category-template wording. The output must read as a real (hedged) supplier description, not a category placeholder.
4. `what_they_do`: 1-2 sentences, <=45 words. Name the supplier; describe what the listing/registry says about it, hedged. Example: "Appears to correspond to ABC Law Firm of San Diego per the state bar listing; the firm appears to practice general civil litigation."
5. `market_competitors`: comma-separated list of 3-5 SHORT real competitor company names appropriate to the supplier's apparent business as inferred from the listing. Use iconic short names. Do NOT list abstract categories.
6. `contract_structure`: ONE short sentence (<=25 words) describing the typical commercial pricing model for this kind of business.
7. `top_3_savings_levers`: 3 short procurement levers (3-7 words each) separated by semicolons, appropriate to the apparent business.
8. `confidence` MUST be "Low" or "Medium" (NEVER "High"; the source is a third-party listing, not the supplier's own apex domain).
9. `review_flag` MUST be "Yes".
10. `research_basis` MUST be EXACTLY: "secondary listing"
11. NEVER use the words "manual validation", "not substantiated", "no exact source", "research failed", or any placeholder phrase.
12. Output VALID JSON only. No prose outside the JSON.
"""


SUPPLIER_GROUNDED_EXTRA = """

=== EVIDENCE-GROUNDING RULE (Tier A only) ===
You have at least one OFFICIAL supplier source. Every supplier-specific claim in `what_they_do` (founding year, products, ownership, named customers, geography, revenue) MUST be derivable from the supplied SOURCE EVIDENCE snippets. If a fact is NOT visible in the snippets, OMIT it and write a category-level statement instead. Do NOT fabricate from the URL alone (an apex-matching domain is not evidence of what the company does).

If the snippets describe a business that does NOT match the L1/L2 category (e.g. snippets describe motocross goggles but the category is Outsourced Services), follow the CATEGORY-FIRST RULE: ignore the off-topic evidence, write a category-level `what_they_do`, set `confidence`="Low" and `review_flag`="Yes", and set `research_basis`="category inference - no official source".

For `research_basis`, use EXACTLY one of:
- "supplier website" (if only an apex-matching official URL was used)
- "supplier website + secondary listing" (if both an official URL and a third-party listing were used)
"""


# QA re-prompt: when the first Tier-A draft contradicted the page snippets
# (e.g. described a BPO when the page is about goggles), trust the snippets and
# OVERRIDE the L1/L2 category. This is a STANDALONE system prompt — it must
# REPLACE SYSTEM_PROMPT (not append), otherwise the CATEGORY-FIRST RULE in
# SYSTEM_PROMPT keeps biasing the model back to the wrong category.
SNIPPET_OVERRIDE_PROMPT = """You are a senior procurement intelligence analyst rewriting a single supplier's enrichment from authoritative page evidence.

CONTEXT: An earlier draft of `what_they_do` contradicted what the supplier's own webpage actually says. The L1/L2 procurement category supplied with this row is WRONG for this supplier. The supplier's actual webpage (in SOURCE EVIDENCE below) shows a different business.

=== HARD RULES (must follow exactly) ===
1. TRUST THE PAGE EVIDENCE. The webpage title, meta description, and snippet are AUTHORITATIVE about what this supplier sells/does. Use that vocabulary.
2. IGNORE the L1/L2 category supplied with the row — it is wrong for this supplier.
3. Do NOT use words like "outsourced", "BPO", "back-office", "managed services", "administrative functions" UNLESS those words literally appear in the SOURCE EVIDENCE.
4. `what_they_do` (1-2 sentences, ≤45 words): describe the supplier's actual business as shown in the SOURCE EVIDENCE. Quote the supplier's own product / service vocabulary (e.g. if the page says "motocross goggles, MTB helmets, sports sunglasses", say goggles / helmets / sunglasses). Name the supplier explicitly.
5. `market_competitors`: comma-separated list of 3-5 SHORT real competitor company names appropriate to the SUPPLIER'S ACTUAL BUSINESS (as inferred from snippets) — not the wrong category. Use iconic short names.
6. `contract_structure`: ONE short sentence (≤25 words) describing the typical commercial pricing model for the supplier's actual business.
7. `top_3_savings_levers`: 3 short procurement levers (3-7 words each) separated by semicolons, appropriate to the actual business.
8. `confidence` MUST be "Medium" (we overrode the source-data category).
9. `review_flag` MUST be "Yes".
10. `research_basis` MUST be EXACTLY: "supplier website"
11. NEVER write "manual validation", "not substantiated", "research failed", or any placeholder phrase. Write only real source-grounded enrichment text.
12. Output VALID JSON only. No prose outside the JSON.

=== EXAMPLE ===
Input row: vendor "100 PERCENT LLC", L1 "General & Administrative", L2 "Outsourced Services".
Source evidence: 100percent.com — title "Motocross Goggles, MTB & BMX Helmets & Sports Sunglasses – 100%"; meta "100% is one of the premier providers of premium quality motocross, mountain bike, and offroad goggles, mountain bike and BMX helmets, and sports sunglasses."
Correct what_they_do: "100% is a premium-action-sports brand that designs and sells motocross goggles, MTB and BMX helmets, and sports sunglasses for cycling, motocross, and offroad athletes."
Correct market_competitors: "Oakley, Smith Optics, Fox Racing, Alpinestars, Bell Helmets"
Correct contract_structure: "Wholesale unit pricing with volume tiers and dealer discounts."
Correct top_3_savings_levers: "volume tiers; dealer pricing; freight consolidation"
Correct confidence: "Medium". Correct review_flag: "Yes". Correct research_basis: "supplier website".
"""


# QA: a relaxed Tier-A prompt for vendors with non-apex official sources
# (e.g. customer/partner pages, government filings, press releases). The LLM
# should treat the page text as authoritative for the supplier even though the
# domain doesn't apex-match the vendor name.
TIER_A_PARTNER_EXTRA = """

=== PARTNER / GOVERNMENT SOURCE MODE ===
The accepted SOURCE URLs include a customer/partner page or a government
filing (e.g. dnata press release, FMCSA carrier snapshot, Job Bank posting).
Treat the page text as authoritative for what this supplier actually does.

REQUIREMENTS:
- `what_they_do`: a real, source-grounded description derived from the partner
  page or government filing. Name the supplier explicitly. (2-3 sentences.)
- `top_3_savings_levers`, `market_competitors`, `contract_structure`: appropriate
  to the supplier's actual business as described on the partner / government
  source.
- `confidence` MUST be "Medium" (one tier below "supplier website" because the
  source is a third party, not the supplier's own apex domain).
- `review_flag` MUST be "Yes".
- `research_basis` MUST be EXACTLY: "supplier website + secondary listing"
- Output VALID JSON only.

=== HARD RULE: NO UNSUPPORTED CLAIMS ===
The supplied L1/L2 procurement category may be misleading (e.g. "Real Estate /
Building Leases" for a vendor whose only evidence is a catering acquisition
press release). Do NOT assert a business model, asset class, or service line
that the snippet evidence does not literally support.

Specifically forbidden unless the snippet text literally says so:
- Do NOT claim the supplier "leases", "provides building lease space",
  "operates as a lessor", or "owns real estate".
- Do NOT claim the supplier "is a 3PL", "is a caterer", "operates ground
  handling", "operates a fleet" without explicit evidence of that activity.
- Do NOT mirror the L2 category back as a fact about the supplier.

When the only evidence is a press release about an acquisition, a directory
listing, or a government registration, use HEDGED language:
- "Most likely operates as ..." / "Appears to be ..." / "Identified in [source]
  as ..."
- Always state the parent / operator relationship explicitly when known
  (e.g. "121 Inflight Catering, acquired by dnata in 2018").

=== EXAMPLE: 121 AT BNA LLC ===
Input row: vendor "121 AT BNA LLC", L1 "Real Estate", L2 "Building Leases".
Source evidence: dnata press release (acquired 121 Inflight Catering in 2018);
flightbridge BNA caterer directory listing for "121 Inflight Catering / dnata".

CORRECT what_they_do: "121 AT BNA LLC most likely corresponds to 121 Inflight
Catering at Nashville International Airport (BNA), an inflight catering
operation acquired by dnata in 2018. The 'Real Estate / Building Leases' L2
category is not substantiated by available sources; the entity appears to be
the catering operator rather than a real-estate lessor."

CORRECT market_competitors: "Gate Gourmet, LSG Sky Chefs, Do & Co, Newrest, Flying Food Group"
CORRECT contract_structure: "Per-meal or per-flight catering pricing with volume tiers and SLA-based service fees."
CORRECT top_3_savings_levers: "Competitive bid; menu standardization; SLA enforcement"
CORRECT confidence: "Medium". CORRECT review_flag: "Yes".
CORRECT research_basis: "supplier website + secondary listing".

INCORRECT (do NOT do this): "Provides building lease space at BNA for inflight
catering operations." \u2014 the source does not say 121 AT BNA LLC leases space
to anyone; it only says dnata acquired 121 Inflight Catering.
"""


class MarketIntelligenceAgent:
    """Generate supplier/category market intelligence."""

    name = "Market Competitor and Contract Intelligence Agent"

    def __init__(
        self,
        model: Optional[str] = None,
        cache: Dict[str, Dict[str, Any]] | None = None,
        fast_fallback: bool = False,
        generated_cache_path: Optional[Path] = None,
    ):
        self.cache = cache or {}
        self.fast_fallback = fast_fallback
        self.generated_cache_path = generated_cache_path
        self.provider: Optional[str] = None
        self.client = None
        self.model = model or os.getenv("BEDROCK_MODEL_ID") or DEFAULT_BEDROCK_MODEL_ID

        # Prefer AWS Bedrock when AWS credentials are present.
        if boto3 is not None and (os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_PROFILE")):
            try:
                region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
                self.client = boto3.client("bedrock-runtime", region_name=region)
                self.provider = "bedrock"
            except Exception:
                self.client = None

        # Optional OpenAI fallback if Bedrock isn't configured.
        if self.client is None and OpenAI is not None and os.getenv("OPENAI_API_KEY"):
            try:
                self.client = OpenAI()
                self.provider = "openai"
                if not model and not os.getenv("BEDROCK_MODEL_ID"):
                    self.model = "gpt-4.1-mini"
            except Exception:
                self.client = None

    # ----------------------------------------------------------------- fallback

    def category_fallback_output(self, record: Dict[str, Any]) -> Dict[str, Any]:
        l1 = clean_text(record.get("l1"))
        l2 = clean_text(record.get("l2"))
        text = f"{l1} {l2}".lower()
        if any(k in text for k in ["software", "information technology", "saas", "license", "cloud"]):
            contract = "Typically charged through subscription, user/module licensing, usage fees, implementation services, and support/maintenance costs."
            competitors = "Oracle, SAP, IBM, Microsoft, Workday, ServiceNow, specialist SaaS providers"
            levers = "Rationalize licenses and modules; benchmark subscription and support rates; negotiate enterprise terms and renewal caps"
            summary = f"Likely provides {l2.lower()} technology, software, data, or IT services for airline operations or corporate functions."
        elif any(k in text for k in ["consulting", "professional services", "advisory", "management"]):
            contract = "Typically charged on hourly/T&M, fixed-fee project, retainer, milestone, or blended-rate structures."
            competitors = "Accenture, Deloitte, PwC, KPMG, McKinsey, boutique specialist firms"
            levers = "Cap blended rates and seniority mix; use milestone-based SOWs; competitively bid repeatable work"
            summary = f"Likely provides {l2.lower()} advisory, project, technical, or outsourced professional services."
        elif any(k in text for k in ["mro", "aircraft", "engine", "industrial supplies", "parts", "maintenance"]):
            contract = "Typically priced through unit part pricing, repair flat rates, exchange fees, catalog discounts, and AOG premiums."
            competitors = "Boeing Distribution, Aviall, Satair, Wencor, AJW Group, regional MRO suppliers"
            levers = "Consolidate part volumes; negotiate catalog discounts and repair caps; reduce expedite/AOG premiums"
            summary = f"Likely supplies {l2.lower()} materials, parts, repair support, or maintenance-related services."
        elif any(k in text for k in ["real estate", "lease", "building", "facilities"]):
            contract = "Typically charged through lease payments, CAM/operating expense pass-throughs, fixed service fees, or project work orders."
            competitors = "CBRE, JLL, Cushman & Wakefield, Colliers, local landlords, facilities service providers"
            levers = "Benchmark lease/service rates; audit pass-through expenses; consolidate facilities scopes under preferred providers"
            summary = f"Likely provides {l2.lower()} space, facilities, property, or site-related services."
        elif any(k in text for k in ["travel", "hospitality", "hotel", "ground", "transportation", "logistics", "3pl", "freight"]):
            contract = "Typically charged through negotiated unit rates, trip/room rates, shipment fees, accessorials, and volume tiers."
            competitors = "C.H. Robinson, Expeditors, DHL, FedEx Logistics, BCD Travel, regional service providers"
            levers = "Bid lanes/rates; reduce accessorial charges; consolidate volume with preferred suppliers"
            summary = f"Likely provides {l2.lower()} travel, hospitality, logistics, freight, or transportation support services."
        elif any(k in text for k in ["marketing", "media", "advertising", "events", "sponsorship"]):
            contract = "Typically charged through project fees, agency retainers, production costs, media buys, or event unit pricing."
            competitors = "WPP agencies, Omnicom agencies, Publicis agencies, Dentsu, local agencies, production vendors"
            levers = "Unbundle media/production costs; cap agency retainers; bid repeatable creative and event work"
            summary = f"Likely provides {l2.lower()} marketing, media, creative, event, or communications services."
        elif any(k in text for k in ["training", "education", "crew"]):
            contract = "Typically charged per student/session, fixed course fee, subscription platform, instructor day rate, or certification fee."
            competitors = "CAE, FlightSafety, Pan Am Flight Academy, Udemy Business, LinkedIn Learning, specialist training firms"
            levers = "Consolidate training demand; negotiate per-seat or volume tiers; standardize curricula and cancellation terms"
            summary = f"Likely provides {l2.lower()} training, certification, learning, or workforce development services."
        else:
            contract = f"Typically priced through negotiated unit rates, fixed fees, subscriptions, retainers, or project-based charges depending on {l2.lower()} scope."
            competitors = f"Other {l2} providers, regional specialists, national suppliers, distributors, managed service providers"
            levers = "Run competitive bid; consolidate spend to preferred suppliers; standardize rates, SLAs, and buying channels"
            summary = f"Likely provides {l2.lower()} products or services based on the supplier category and name."
        return {
            "record_id": record.get("record_id"),
            "what_they_do": summary[:350],
            "market_competitors": competitors,
            "contract_structure": contract,
            "top_3_savings_levers": levers,
            "confidence": "Medium" if l2 else "Low",
            "research_basis": "category inference (fallback)" if l2 else "ambiguous supplier; category inference (fallback)",
            "review_flag": "Yes",
            "_fallback": True,
        }

    # ----------------------------------------------------------------- cache

    def _from_cache(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        cache_key = record.get("cache_key")
        if cache_key in self.cache:
            cached = dict(self.cache[cache_key])
            cached.setdefault("record_id", record.get("record_id"))
            return cached
        return None

    def _persist_generated(self, rows: List[Dict[str, Any]]) -> None:
        if not self.generated_cache_path or not rows:
            return
        append_jsonl(self.generated_cache_path, rows)

    # ----------------------------------------------------------------- LLM

    def _evidence_block(self, evidence: Optional[EvidencePacket]) -> str:
        if not evidence or not evidence.items:
            return "(no evidence available)"
        lines: List[str] = []
        for i, item in enumerate(evidence.items, start=1):
            lines.append(f"[{i}] URL: {item.url}")
            if item.title:
                lines.append(f"    title: {item.title}")
            if item.meta_description:
                lines.append(f"    meta: {item.meta_description}")
            if item.snippet:
                lines.append(f"    snippet: {item.snippet}")
            if not item.fetched and item.error:
                lines.append(f"    fetch_error: {item.error}")
        return "\n".join(lines)

    def _invoke_bedrock(self, prompt: str, system_prompt: str) -> str:
        response = self.client.converse(
            modelId=self.model,
            system=[{"text": system_prompt}],
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            inferenceConfig={"temperature": 0.0, "maxTokens": 4096},
        )
        return response["output"]["message"]["content"][0]["text"]

    def _invoke_openai(self, prompt: str, system_prompt: str) -> str:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
        )
        return response.choices[0].message.content

    def _llm_records(self, payload_records: List[Dict[str, Any]], system_prompt: Optional[str] = None) -> List[Dict[str, Any]]:
        if self.client is None:
            return []
        sys_p = system_prompt or SYSTEM_PROMPT
        prompt = USER_TEMPLATE.format(
            records_json=json.dumps(payload_records, ensure_ascii=False, indent=2)
        )
        last_error: Optional[Exception] = None
        for attempt in range(3):
            try:
                if self.provider == "bedrock":
                    raw = self._invoke_bedrock(prompt, sys_p)
                else:
                    raw = self._invoke_openai(prompt, sys_p)
                data = json.loads(_strip_code_fences(raw))
                return data.get("records", data if isinstance(data, list) else [])
            except Exception as exc:
                last_error = exc
                time.sleep(1.5 * (attempt + 1))
        if last_error is not None:
            print(f"[MarketIntelligenceAgent] LLM call failed after retries: {last_error}")
        return []

    def live_research_one(
        self,
        record: Dict[str, Any],
        evidence: Optional[EvidencePacket],
        accepted_urls: List[str],
        evidence_tier: str = "A",
        prompt_mode: str = "default",
    ) -> Dict[str, Any]:
        """Run live LLM enrichment for a single record.

        evidence_tier:
            "A" - at least one official/partner/government URL; supplier-grounded.
            "B" - listing-only URLs; category-only fallback.
            "C" - no URLs; category-only fallback.

        prompt_mode (QA):
            "default"          - SUPPLIER_GROUNDED_EXTRA (Tier A) or CATEGORY_ONLY_PROMPT (B/C)
            "snippet_override" - re-prompt mode that trusts page snippets over
                                  the L1/L2 category. Used after a contradiction
                                  or grounding failure on the first draft.
            "partner_source"   - Tier A but the source is a customer/partner page
                                  or government filing rather than the supplier's
                                  own apex domain.
        """
        if self.client is None:
            raise RuntimeError(
                "live-research mode requires an LLM client. Configure AWS Bedrock "
                "(AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_REGION + BEDROCK_MODEL_ID) "
                "or set OPENAI_API_KEY."
            )
        if evidence_tier == "A" and prompt_mode == "snippet_override":
            # QA: REPLACE the system prompt entirely. The CATEGORY-FIRST
            # RULE in SYSTEM_PROMPT actively biases the model back to the wrong
            # category text, so we substitute a focused, snippet-anchored prompt.
            system_prompt = SNIPPET_OVERRIDE_PROMPT
        elif evidence_tier == "A" and prompt_mode == "partner_source":
            system_prompt = SYSTEM_PROMPT + TIER_A_PARTNER_EXTRA
        elif evidence_tier == "A":
            system_prompt = SYSTEM_PROMPT + SUPPLIER_GROUNDED_EXTRA
        elif prompt_mode == "secondary_listing":
            # evidence-tier: Tier B with at least one non-weak listing URL.
            system_prompt = SECONDARY_LISTING_PROMPT
        else:
            system_prompt = CATEGORY_ONLY_PROMPT
        payload = {
            "record_id": record.get("record_id"),
            "vendor_name": record.get("vendor_name"),
            "cleansed_vendor_name": record.get("cleansed_vendor_name"),
            "l1": record.get("l1"),
            "l2": record.get("l2"),
            "evidence": self._evidence_block(evidence),
            "accepted_source_urls": accepted_urls,
        }
        results = self._llm_records([payload], system_prompt=system_prompt)
        out = results[0] if results else {}
        if not isinstance(out, dict):
            out = {}
        # Stamp metadata for the cache write.
        stamped = dict(out)
        stamped["cache_key"] = record.get("cache_key")
        stamped["model"] = self.model
        stamped["prompt_version"] = f"{PROMPT_VERSION}+{prompt_mode}" if prompt_mode != "default" else PROMPT_VERSION
        stamped["timestamp"] = _dt.datetime.utcnow().isoformat() + "Z"
        stamped["source_urls"] = list(accepted_urls)
        stamped["evidence_tier"] = evidence_tier
        stamped["prompt_mode"] = prompt_mode
        stamped["evidence_summary"] = (
            [item.url for item in evidence.items] if evidence and evidence.items else []
        )
        self._persist_generated([stamped])
        return stamped

    def enrich_one(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Cache-replay path: return cached enrichment or deterministic fallback."""
        cached = self._from_cache(record)
        if cached is not None:
            return cached
        return self.category_fallback_output(record)

    def enrich_batch(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        outputs: List[Dict[str, Any]] = []
        uncached: List[Dict[str, Any]] = []
        for record in records:
            cached = self._from_cache(record)
            if cached is not None or self.fast_fallback or self.client is None:
                outputs.append(cached if cached is not None else self.category_fallback_output(record))
            else:
                uncached.append(record)

        if uncached and self.client is not None:
            payload_records = [
                {k: v for k, v in r.items() if k != "cache_key"} for r in uncached
            ]
            parsed = self._llm_records(payload_records)
            by_id = {str(item.get("record_id")): item for item in (parsed or []) if isinstance(item, dict)}
            for record in uncached:
                outputs.append(
                    by_id.get(str(record.get("record_id"))) or self.category_fallback_output(record)
                )
        return outputs
