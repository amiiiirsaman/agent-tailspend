"""Shared utility functions for supplier enrichment agents."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd


def clean_text(value: Any) -> str:
    """Return a safe trimmed string for Excel and JSON fields."""
    if pd.isna(value):
        return ""
    return str(value).strip()


def spend_fmt(value: Any) -> str:
    """Format a numeric spend value for executive-readable competitor notes."""
    try:
        return f"${float(value):,.0f}"
    except Exception:
        return ""


def normalize_for_match(name: str) -> str:
    """Normalize supplier legal suffixes and punctuation for fuzzy consolidation logic."""
    n = clean_text(name).upper()
    n = re.sub(r"\b(LLC|INC|CORP|CORPORATION|CO|COMPANY|LTD|LIMITED|LP|LLP|PLC|THE|DBA|D/B/A)\b", " ", n)
    n = re.sub(r"[^A-Z0-9 ]+", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def make_enrichment_key(record: Dict[str, Any]) -> str:
    """Create the same deterministic enrichment key used by the production workbook."""
    raw = "|".join([
        clean_text(record.get("vendor_name", "")),
        clean_text(record.get("cleansed_vendor_name", "")),
        clean_text(record.get("l1", "")),
        clean_text(record.get("l2", "")),
    ]).upper()
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def make_url_key(vendor: str, l1: str, l2: str) -> str:
    """Create the supplier/category key used by the exact URL cache."""
    return "|".join([clean_text(vendor), clean_text(l1), clean_text(l2)])


def load_jsonl_cache(path: Path) -> Dict[str, Dict[str, Any]]:
    """Load a JSONL cache keyed by the record's cache_key field."""
    cache: Dict[str, Dict[str, Any]] = {}
    if not path.exists():
        return cache
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                cache[str(obj.get("cache_key"))] = obj
            except Exception:
                continue
    return cache


def append_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    """Append JSON records to a JSONL cache."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_json_cache(path: Path) -> Dict[str, Any]:
    """Load a JSON object cache if available."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def row_to_record(row: pd.Series) -> Dict[str, Any]:
    """Convert an input worksheet row into the normalized record format shared by agents."""
    total_spend = row.get("Total Spend")
    try:
        total_spend_float = float(total_spend or 0)
    except Exception:
        total_spend_float = 0.0
    record = {
        "vendor_name": clean_text(row.get("Vendor Name")),
        "cleansed_vendor_name": clean_text(row.get("Cleansed Vendor Name")),
        "l1": clean_text(row.get("L1")),
        "l2": clean_text(row.get("L2")),
        "supplier_tiering": clean_text(row.get("Supplier Tiering")),
        "total_spend": total_spend_float,
        "hawaian_airlines_flag": clean_text(row.get("Hawaii Airlines")),
        "alaska_airlines_flag": clean_text(row.get("Alaska Airlines")),
    }
    record["cache_key"] = make_enrichment_key(record)
    record["total_spend_formatted"] = spend_fmt(total_spend_float)
    return record


def preferred_vendor_name(row_or_record: Any) -> str:
    """Select cleansed vendor name when available, otherwise use the original vendor name."""
    if isinstance(row_or_record, dict):
        cleansed = clean_text(row_or_record.get("cleansed_vendor_name") or row_or_record.get("Cleansed Vendor Name"))
        original = clean_text(row_or_record.get("vendor_name") or row_or_record.get("Vendor Name"))
    else:
        cleansed = clean_text(row_or_record.get("Cleansed Vendor Name"))
        original = clean_text(row_or_record.get("Vendor Name"))
    return cleansed or original
