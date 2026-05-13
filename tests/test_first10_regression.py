"""First-10-vendor regression validation for the modular agentic backend.

This script runs the modular pipeline on the first 10 source rows, then compares the
result against the delivered final enriched Excel workbook. It validates that the code
is working and reproduces the final AI-enriched fields for those rows.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import (  # noqa: E402
    APPENDED_COLUMNS,
    DEFAULT_FINAL_WORKBOOK,
    DEFAULT_INPUT_WORKBOOK,
    FINAL_SHEET,
    FIRST10_OUTPUT,
    REGRESSION_CSV,
    REGRESSION_REPORT,
    SOURCE_SHEET,
)
from orchestrator import enrich_dataframe, load_source_rows, write_workbook  # noqa: E402
from agents.common import clean_text  # noqa: E402


def main() -> None:
    full_source = load_source_rows(DEFAULT_INPUT_WORKBOOK, SOURCE_SHEET, limit=None)
    source_first10 = full_source.head(10).copy()
    produced = enrich_dataframe(source_first10, live_urls=False, context_df=full_source)
    write_workbook(produced, FIRST10_OUTPUT, DEFAULT_INPUT_WORKBOOK, SOURCE_SHEET)

    expected = pd.read_excel(DEFAULT_FINAL_WORKBOOK, sheet_name=FINAL_SHEET).head(10)
    compare_columns = [col for col in APPENDED_COLUMNS if col in expected.columns and col in produced.columns]

    details = []
    for row_index in range(10):
        supplier = clean_text(expected.iloc[row_index].get("Cleansed Vendor Name"))
        for column in compare_columns:
            produced_value = clean_text(produced.iloc[row_index].get(column))
            expected_value = clean_text(expected.iloc[row_index].get(column))
            details.append({
                "row_number_1_based": row_index + 1,
                "cleansed_vendor_name": supplier,
                "column": column,
                "match": produced_value == expected_value,
                "produced_value": produced_value,
                "expected_value": expected_value,
            })

    details_df = pd.DataFrame(details)
    REGRESSION_CSV.parent.mkdir(parents=True, exist_ok=True)
    details_df.to_csv(REGRESSION_CSV, index=False)
    matched = int(details_df["match"].sum())
    total = int(len(details_df))
    failed_df = details_df[~details_df["match"]]

    report_lines = [
        "# First-10 Vendor Regression Test\n",
        "\nThe modular agentic backend was executed on the first 10 source workbook rows and compared against the delivered final AI-enriched workbook.\n",
        "\n| Metric | Value |\n|---|---:|\n",
        f"| Rows tested | 10 |\n",
        f"| Columns compared | {len(compare_columns)} |\n",
        f"| Field comparisons | {total} |\n",
        f"| Exact matches | {matched} |\n",
        f"| Mismatches | {total - matched} |\n",
        f"| Match rate | {matched / total:.2%} |\n" if total else "| Match rate | N/A |\n",
        "\n## Compared Columns\n\n",
        ", ".join(compare_columns) + "\n",
        "\n## Result\n\n",
    ]
    if failed_df.empty:
        report_lines.append("**PASS.** The modular code reproduced the final AI-enriched workbook fields for the first 10 rows exactly.\n")
    else:
        report_lines.append("**FAIL.** The modular code produced mismatches. Review the CSV detail file for exact differences.\n\n")
        report_lines.append(failed_df.head(20).to_markdown(index=False))
        report_lines.append("\n")
    report_lines.append(f"\nGenerated workbook: `{FIRST10_OUTPUT}`\n\n")
    report_lines.append(f"Detailed comparison CSV: `{REGRESSION_CSV}`\n")
    REGRESSION_REPORT.write_text("".join(report_lines), encoding="utf-8")

    print(f"Regression report: {REGRESSION_REPORT}")
    print(f"Regression detail CSV: {REGRESSION_CSV}")
    print(f"First-10 workbook: {FIRST10_OUTPUT}")
    print(f"Matches: {matched}/{total}")
    if not failed_df.empty:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
