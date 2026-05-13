"""Research Evidence Agent.

Fetches lightweight evidence packets for a list of accepted source URLs so the
Market Intelligence Agent can produce evidence-grounded enrichment in
``live-research`` mode.

For each URL we capture:
    * url
    * fetched (bool)
    * status_code
    * title
    * meta_description
    * snippet (first ~600 chars of visible body text)

The fetcher is intentionally conservative: short timeouts, a small fixed budget
of bytes per page, no JS rendering, polite User-Agent. Failures are recorded
rather than raised so the pipeline keeps progressing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List

try:
    import requests
except Exception:  # pragma: no cover
    requests = None

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover
    BeautifulSoup = None


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; TailspendResearchBot/1.0; "
    "+https://example.invalid/tailspend-research)"
)
MAX_BYTES = 250_000
MAX_SNIPPET = 600
MAX_TITLE = 200
MAX_META = 400


@dataclass
class EvidenceItem:
    url: str
    fetched: bool = False
    status_code: int = 0
    title: str = ""
    meta_description: str = ""
    snippet: str = ""
    error: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "fetched": self.fetched,
            "status_code": self.status_code,
            "title": self.title,
            "meta_description": self.meta_description,
            "snippet": self.snippet,
            "error": self.error,
        }


@dataclass
class EvidencePacket:
    vendor: str
    l1: str
    l2: str
    items: List[EvidenceItem] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "vendor": self.vendor,
            "l1": self.l1,
            "l2": self.l2,
            "items": [i.to_dict() for i in self.items],
        }

    def has_text(self) -> bool:
        return any((i.title or i.meta_description or i.snippet) for i in self.items)


def _collapse_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


class ResearchEvidenceAgent:
    """Fetch and summarize web evidence for accepted source URLs."""

    name = "Research Evidence Agent"

    def __init__(self, timeout: int = 8, user_agent: str = DEFAULT_USER_AGENT,
                 max_urls: int = 3):
        self.timeout = timeout
        self.user_agent = user_agent
        self.max_urls = max_urls

    def fetch_one(self, url: str) -> EvidenceItem:
        item = EvidenceItem(url=url)
        if requests is None or BeautifulSoup is None:
            item.error = "requests/bs4 unavailable"
            return item
        try:
            resp = requests.get(
                url,
                timeout=self.timeout,
                headers={"User-Agent": self.user_agent, "Accept": "text/html,*/*"},
                allow_redirects=True,
                stream=True,
            )
            item.status_code = resp.status_code
            ctype = (resp.headers.get("Content-Type") or "").lower()
            if resp.status_code >= 400:
                item.error = f"http-{resp.status_code}"
                return item
            if "html" not in ctype and "xml" not in ctype and ctype:
                item.error = f"non-html:{ctype.split(';')[0]}"
                return item
            raw = resp.raw.read(MAX_BYTES, decode_content=True) if hasattr(resp, "raw") else resp.content[:MAX_BYTES]
            if isinstance(raw, bytes):
                try:
                    text = raw.decode(resp.encoding or "utf-8", errors="ignore")
                except Exception:
                    text = raw.decode("utf-8", errors="ignore")
            else:
                text = str(raw)
            soup = BeautifulSoup(text, "html.parser")
            title_tag = soup.find("title")
            item.title = _collapse_ws(title_tag.get_text() if title_tag else "")[:MAX_TITLE]
            meta = soup.find("meta", attrs={"name": "description"}) or soup.find(
                "meta", attrs={"property": "og:description"}
            )
            if meta and meta.get("content"):
                item.meta_description = _collapse_ws(meta["content"])[:MAX_META]
            for tag in soup(["script", "style", "noscript", "nav", "footer", "header", "form"]):
                tag.decompose()
            body = soup.body or soup
            snippet = _collapse_ws(body.get_text(separator=" "))
            item.snippet = snippet[:MAX_SNIPPET]
            item.fetched = True
        except Exception as exc:
            item.error = type(exc).__name__
        return item

    def gather(self, vendor: str, l1: str, l2: str, urls: List[str]) -> EvidencePacket:
        packet = EvidencePacket(vendor=vendor, l1=l1, l2=l2)
        for url in urls[: self.max_urls]:
            packet.items.append(self.fetch_one(url))
        return packet
