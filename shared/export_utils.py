from __future__ import annotations

from pathlib import Path

import pandas as pd


def _as_dataframe(rows: list[dict] | pd.DataFrame) -> pd.DataFrame:
    if isinstance(rows, pd.DataFrame):
        return rows
    return pd.DataFrame(rows)


def write_excel_workbook(
    output_path: Path,
    professor_rows: list[dict] | pd.DataFrame,
    coverage_rows: list[dict] | pd.DataFrame,
    crawl_rows: list[dict] | pd.DataFrame,
    domain_rows: list[dict] | pd.DataFrame,
    excluded_rows: list[dict] | pd.DataFrame,
    method_rows: list[dict] | pd.DataFrame,
    summary_rows: list[dict] | pd.DataFrame,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        _as_dataframe(professor_rows).to_excel(writer, index=False, sheet_name="Professor_Emails")
        _as_dataframe(coverage_rows).to_excel(writer, index=False, sheet_name="Coverage_Queue")
        _as_dataframe(crawl_rows).to_excel(writer, index=False, sheet_name="Crawl_Log")
        _as_dataframe(domain_rows).to_excel(writer, index=False, sheet_name="Domains")
        _as_dataframe(excluded_rows).to_excel(writer, index=False, sheet_name="Review_Excluded")
        _as_dataframe(method_rows).to_excel(writer, index=False, sheet_name="Method")
        _as_dataframe(summary_rows).to_excel(writer, index=False, sheet_name="Summary")
