"""Side-by-side comparison: live Bedrock pipeline vs Manus reference workbook.

For the first 10 vendors, prints field-by-field differences across the 8 enrichment
columns. Produces:

* outputs/first10_vs_manus_diff.csv  (long-form: row, column, our_value, manus_value, match)
* outputs/first10_vs_manus_report.md (human-readable comparison + qualitative notes)

Assumes the live Bedrock workbook already exists at
``outputs/first10_live_research_output.xlsx`` (produced by
``tests/test_first10_live_research_quality.py``).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import (  # noqa: E402
    DEFAULT_FINAL_WORKBOOK,
    DEFAULT_OUTPUT_DIR,
    FINAL_SHEET,
)


COMPARE_COLUMNS = [
    "What they do",
    "Top 3 Savings Levers",
    "Market Competitors",
    "AI Contract Structure",
    "AI Confidence",
    "AI Research Basis",
    "AI Review Flag",
    "Exact URLs Leveraged for Study",
]


def _norm(v) -> str:
    return "" if v is None else str(v).strip()


def main() -> int:
    ours_path = DEFAULT_OUTPUT_DIR / "first10_live_research_output.xlsx"
    if not ours_path.exists():
        print(f"ERROR: missing {ours_path}. Run tests/test_first10_live_research_quality.py first.")
        return 2

    ours = pd.read_excel(ours_path, sheet_name="AI Enriched Tail Spend").head(10)
    manus = pd.read_excel(DEFAULT_FINAL_WORKBOOK, sheet_name=FINAL_SHEET).head(10)

    rows = []
    for i in range(10):
        vendor = _norm(ours.iloc[i].get("Cleansed Vendor Name") or manus.iloc[i].get("Cleansed Vendor Name"))
        for col in COMPARE_COLUMNS:
            ov = _norm(ours.iloc[i].get(col))
            mv = _norm(manus.iloc[i].get(col))
            rows.append({
                "row": i + 1,
                "vendor": vendor,
                "column": col,
                "match": ov == mv,
                "ours": ov,
                "manus": mv,
            })
    diff = pd.DataFrame(rows)
    csv_path = DEFAULT_OUTPUT_DIR / "first10_vs_manus_diff.csv"
    diff.to_csv(csv_path, index=False)

    matches = int(diff["match"].sum())
    total = len(diff)

    # Per-column match rate.
    by_col = diff.groupby("column")["match"].agg(["sum", "count"]).reset_index()
    by_col.columns = ["column", "matches", "total"]

    # URL count comparison (more URLs is not necessarily better, but signals coverage).
    def _url_count(s: str) -> int:
        if not s:
            return 0
        if s.startswith("NO EXACT") or "manual" in s.lower():
            return 0
        return len([t for t in s.split(";") if t.strip().startswith("http")])

    url_stats = []
    for i in range(10):
        url_stats.append({
            "row": i + 1,
            "vendor": _norm(ours.iloc[i].get("Cleansed Vendor Name")),
            "ours_url_count": _url_count(_norm(ours.iloc[i].get("Exact URLs Leveraged for Study"))),
            "manus_url_count": _url_count(_norm(manus.iloc[i].get("Exact URLs Leveraged for Study"))),
            "ours_confidence": _norm(ours.iloc[i].get("AI Confidence")),
            "manus_confidence": _norm(manus.iloc[i].get("AI Confidence")),
            "ours_review": _norm(ours.iloc[i].get("AI Review Flag")),
            "manus_review": _norm(manus.iloc[i].get("AI Review Flag")),
        })
    url_df = pd.DataFrame(url_stats)

    # Build report.
    lines = []
    lines.append("# First-10 Comparison: Live Bedrock Pipeline vs Manus Reference\n\n")
    lines.append(f"- Total field comparisons: **{total}** ({len(COMPARE_COLUMNS)} cols x 10 rows)\n")
    lines.append(f"- Exact string matches: **{matches}** ({matches/total:.0%})\n")
    lines.append(f"- Mismatches: **{total - matches}**\n\n")

    lines.append("## Per-column match rate\n\n")
    lines.append("| Column | Matches / 10 |\n|---|---:|\n")
    for _, r in by_col.iterrows():
        lines.append(f"| {r['column']} | {int(r['matches'])} / {int(r['total'])} |\n")
    lines.append("\n")

    lines.append("## URL coverage & confidence per row\n\n")
    lines.append("| Row | Vendor | Ours URLs | Manus URLs | Ours Conf | Manus Conf | Ours Review | Manus Review |\n")
    lines.append("|---:|---|---:|---:|---|---|---|---|\n")
    for _, r in url_df.iterrows():
        lines.append(
            f"| {r['row']} | {r['vendor']} | {r['ours_url_count']} | {r['manus_url_count']} | "
            f"{r['ours_confidence']} | {r['manus_confidence']} | {r['ours_review']} | {r['manus_review']} |\n"
        )
    lines.append("\n")

    lines.append("## Side-by-side per row\n\n")
    for i in range(10):
        vendor = _norm(ours.iloc[i].get("Cleansed Vendor Name"))
        lines.append(f"### Row {i+1}: {vendor}\n\n")
        for col in COMPARE_COLUMNS:
            ov = _norm(ours.iloc[i].get(col))
            mv = _norm(manus.iloc[i].get(col))
            mark = "MATCH" if ov == mv else "DIFF"
            lines.append(f"**{col}** ({mark})\n\n")
            lines.append(f"- Ours:  {ov[:600]}\n")
            lines.append(f"- Manus: {mv[:600]}\n\n")
        lines.append("---\n\n")

    report_path = DEFAULT_OUTPUT_DIR / "first10_vs_manus_report.md"
    report_path.write_text("".join(lines), encoding="utf-8")
    print(f"Diff CSV:  {csv_path}")
    print(f"Report MD: {report_path}")
    print(f"Exact-match rate: {matches}/{total} ({matches/total:.0%})")
    print("Per-column matches:")
    for _, r in by_col.iterrows():
        print(f"  {r['column']:40s}  {int(r['matches'])}/{int(r['total'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
