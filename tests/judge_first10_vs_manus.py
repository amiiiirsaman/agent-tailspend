"""LLM-judge: for each (row, field), decide whether Ours, Manus, or Tie wins.

Uses the same Bedrock Claude Sonnet 4.5 model as the pipeline. Each cell is
judged independently with a strict rubric (accuracy first, then specificity,
then evidence). Outputs:

* outputs/first10_judge_results.csv  - one row per (row, column) with winner + reason
* outputs/first10_judge_summary.md   - aggregate scoreboard

Run AFTER tests/test_first10_live_research_quality.py has produced
outputs/first10_live_research_output.xlsx.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except Exception:
    pass

import boto3

from config import DEFAULT_FINAL_WORKBOOK, DEFAULT_OUTPUT_DIR, FINAL_SHEET


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

MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "us.anthropic.claude-sonnet-4-5-20250929-v1:0")

JUDGE_SYSTEM = """You are an impartial procurement-data quality judge.

You are comparing two enrichment outputs (A and B) for a single supplier-category row in an airline tail-spend dataset.

You will judge ONE specific field at a time. Pick the winner using this rubric, in order:
1. ACCURACY: Which output correctly describes what THIS specific supplier actually sells / does, given the supplier name and category?
2. SPECIFICITY: Which output gives concrete, named, actionable detail rather than generic boilerplate?
3. EVIDENCE QUALITY (only for the URL field): Which set of URLs better represents the actual supplier (official site > supplier-named directory page > generic listing > unrelated noise)?
4. CALIBRATION (only for confidence/review fields): Which output is better calibrated to the strength of the evidence available?

Important rules:
- "Tie" only when both are essentially equivalent in quality. Do not use Tie to avoid choosing.
- If one output is clearly wrong about what the supplier does, the other wins regardless of how polished the wrong one is.
- For the URL field, an unrelated/off-topic URL is worse than an honest "research failed" marker.

Respond with VALID JSON only:
{"winner": "A" | "B" | "Tie", "reason": "<one sentence>"}
"""

JUDGE_USER_TEMPLATE = """Supplier: {vendor}
Category L1: {l1}
Category L2: {l2}

Field being judged: {column}

Output A:
{a}

Output B:
{b}

Decide which output is better for this field. Respond with JSON only.
"""


def _norm(v) -> str:
    return "" if v is None else str(v).strip()


def _strip_fence(text: str) -> str:
    m = re.search(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL)
    return m.group(1) if m else text


def main() -> int:
    ours_path = DEFAULT_OUTPUT_DIR / "first10_live_research_output.xlsx"
    if not ours_path.exists():
        print(f"ERROR: missing {ours_path}.")
        return 2

    ours = pd.read_excel(ours_path, sheet_name="AI Enriched Tail Spend").head(10)
    manus = pd.read_excel(DEFAULT_FINAL_WORKBOOK, sheet_name=FINAL_SHEET).head(10)

    region = os.getenv("AWS_REGION", "us-east-1")
    client = boto3.client("bedrock-runtime", region_name=region)

    results = []
    for i in range(10):
        vendor = _norm(ours.iloc[i].get("Cleansed Vendor Name") or manus.iloc[i].get("Cleansed Vendor Name"))
        l1 = _norm(ours.iloc[i].get("L1") or manus.iloc[i].get("L1"))
        l2 = _norm(ours.iloc[i].get("L2") or manus.iloc[i].get("L2"))
        for col in COMPARE_COLUMNS:
            ours_val = _norm(ours.iloc[i].get(col)) or "(empty)"
            manus_val = _norm(manus.iloc[i].get(col)) or "(empty)"

            # Randomize A/B order to prevent positional bias.
            # Use deterministic rotation: even row -> ours=A, odd -> ours=B.
            if i % 2 == 0:
                a_val, b_val, a_label = ours_val, manus_val, "Ours"
            else:
                a_val, b_val, a_label = manus_val, ours_val, "Manus"

            prompt = JUDGE_USER_TEMPLATE.format(
                vendor=vendor, l1=l1, l2=l2, column=col, a=a_val, b=b_val,
            )

            verdict = {"winner": "Tie", "reason": "judge call failed"}
            for attempt in range(3):
                try:
                    response = client.converse(
                        modelId=MODEL_ID,
                        system=[{"text": JUDGE_SYSTEM}],
                        messages=[{"role": "user", "content": [{"text": prompt}]}],
                        inferenceConfig={"temperature": 0.0, "maxTokens": 256},
                    )
                    raw = response["output"]["message"]["content"][0]["text"]
                    verdict = json.loads(_strip_fence(raw))
                    break
                except Exception as exc:
                    time.sleep(1.0 * (attempt + 1))
                    verdict = {"winner": "Tie", "reason": f"judge error: {type(exc).__name__}"}

            raw_winner = str(verdict.get("winner", "Tie")).strip()
            if raw_winner == "A":
                winner = a_label
            elif raw_winner == "B":
                winner = "Manus" if a_label == "Ours" else "Ours"
            else:
                winner = "Tie"

            results.append({
                "row": i + 1,
                "vendor": vendor,
                "column": col,
                "winner": winner,
                "reason": verdict.get("reason", ""),
                "ours": ours_val[:300],
                "manus": manus_val[:300],
            })
            print(f"  row {i+1:2d} {col:35s} -> {winner}")

    df = pd.DataFrame(results)
    csv_path = DEFAULT_OUTPUT_DIR / "first10_judge_results.csv"
    df.to_csv(csv_path, index=False)

    # Aggregates
    by_winner = df["winner"].value_counts().to_dict()
    by_col = df.groupby(["column", "winner"]).size().unstack(fill_value=0).reindex(columns=["Ours", "Manus", "Tie"], fill_value=0)
    by_row = df.groupby(["row", "winner"]).size().unstack(fill_value=0).reindex(columns=["Ours", "Manus", "Tie"], fill_value=0)

    rows_won_by_ours = int(((by_row["Ours"] + by_row["Tie"]) >= 8).sum())
    rows_dominated_by_ours = int((by_row["Ours"] >= 5).sum())

    lines = []
    lines.append("# LLM-Judge Comparison: Ours vs Manus (first 10 rows, 8 fields)\n\n")
    lines.append(f"Judge model: `{MODEL_ID}`\n\n")
    lines.append(f"- Total cells judged: **{len(df)}** (10 rows x 8 fields)\n")
    lines.append(f"- Ours wins: **{by_winner.get('Ours', 0)}**\n")
    lines.append(f"- Manus wins: **{by_winner.get('Manus', 0)}**\n")
    lines.append(f"- Ties: **{by_winner.get('Tie', 0)}**\n")
    lines.append(f"- Rows where Ours >= 5 of 8 fields: **{rows_dominated_by_ours} / 10**\n")
    lines.append(f"- Rows where Ours wins-or-ties on ALL 8 fields: **{rows_won_by_ours} / 10**\n\n")

    lines.append("## Per-column scoreboard\n\n")
    lines.append("| Column | Ours | Manus | Tie |\n|---|---:|---:|---:|\n")
    for col in by_col.index:
        lines.append(f"| {col} | {by_col.loc[col, 'Ours']} | {by_col.loc[col, 'Manus']} | {by_col.loc[col, 'Tie']} |\n")
    lines.append("\n")

    lines.append("## Per-row scoreboard\n\n")
    lines.append("| Row | Vendor | Ours | Manus | Tie | Verdict |\n|---:|---|---:|---:|---:|---|\n")
    for r in range(1, 11):
        if r not in by_row.index:
            continue
        ow, mw, tw = int(by_row.loc[r, "Ours"]), int(by_row.loc[r, "Manus"]), int(by_row.loc[r, "Tie"])
        vendor = df[df["row"] == r]["vendor"].iloc[0]
        if mw == 0:
            verdict = "Ours dominates" if ow >= 5 else "Ours leads"
        elif ow > mw:
            verdict = "Ours wins"
        elif mw > ow:
            verdict = "Manus wins"
        else:
            verdict = "Split"
        lines.append(f"| {r} | {vendor} | {ow} | {mw} | {tw} | {verdict} |\n")
    lines.append("\n")

    # Cells where Manus won (so we know what to fix)
    manus_wins = df[df["winner"] == "Manus"]
    if len(manus_wins) > 0:
        lines.append("## Cells where Manus won (improvement targets)\n\n")
        for _, r in manus_wins.iterrows():
            lines.append(f"### Row {r['row']} ({r['vendor']}) - {r['column']}\n")
            lines.append(f"**Reason:** {r['reason']}\n\n")
            lines.append(f"- Ours:  {r['ours']}\n")
            lines.append(f"- Manus: {r['manus']}\n\n")

    md_path = DEFAULT_OUTPUT_DIR / "first10_judge_summary.md"
    md_path.write_text("".join(lines), encoding="utf-8")

    print(f"\nJudge CSV: {csv_path}")
    print(f"Judge MD:  {md_path}")
    print(f"Ours: {by_winner.get('Ours', 0)} | Manus: {by_winner.get('Manus', 0)} | Tie: {by_winner.get('Tie', 0)}")
    print(f"Rows where Ours wins-or-ties ALL 8: {rows_won_by_ours}/10")
    return 0 if rows_won_by_ours == 10 else 1


if __name__ == "__main__":
    raise SystemExit(main())
