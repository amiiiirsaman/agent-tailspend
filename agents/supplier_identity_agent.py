"""Supplier Identity Agent.

This agent normalizes supplier identity inputs and produces deterministic duplicate or
consolidation-review hints. It does not call an LLM; this keeps identity handling stable
and reproducible across runs.
"""

from __future__ import annotations

import difflib
from typing import Dict, Iterable, List

from .common import clean_text, normalize_for_match, preferred_vendor_name, row_to_record


class SupplierIdentityAgent:
    """Normalize supplier row inputs and detect likely duplicate supplier names."""

    name = "Supplier Identity Agent"

    def normalize_row(self, row):
        """Convert a raw Excel row into the canonical enrichment record."""
        record = row_to_record(row)
        record["preferred_vendor_name"] = preferred_vendor_name(record)
        record["normalized_vendor_match_key"] = normalize_for_match(record["preferred_vendor_name"])
        return record

    def normalize_rows(self, dataframe) -> List[Dict]:
        """Normalize all dataframe rows into canonical records."""
        return [self.normalize_row(row) for _, row in dataframe.iterrows()]

    def duplicate_suggestions(self, names: Iterable[str]) -> Dict[str, str]:
        """Return likely duplicate/consolidation review candidates by cleansed name."""
        clean_names = [clean_text(n) for n in names if clean_text(n)]
        normalized = {name: normalize_for_match(name) for name in clean_names}
        buckets: Dict[str, List[str]] = {}
        for name, norm in normalized.items():
            key = (norm[:4] if norm else name[:4]).upper()
            buckets.setdefault(key, []).append(name)

        suggestions: Dict[str, str] = {}
        for bucket_names in buckets.values():
            if len(bucket_names) < 2:
                continue
            ordered = sorted(bucket_names)
            for index, left in enumerate(ordered):
                matches = []
                left_norm = normalized[left]
                for right in ordered[index + 1 :]:
                    right_norm = normalized[right]
                    if not left_norm or not right_norm or left_norm == right_norm:
                        continue
                    ratio = difflib.SequenceMatcher(None, left_norm, right_norm).ratio()
                    if ratio >= 0.88:
                        matches.append(right)
                if matches:
                    suggestions[left] = "Potential consolidation review with: " + ", ".join(matches[:5])
        return suggestions
