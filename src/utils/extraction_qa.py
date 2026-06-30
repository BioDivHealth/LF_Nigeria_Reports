"""
Extraction QA sidecar helpers.

These helpers write compact JSON provenance next to accepted extraction CSVs.
They avoid Gemini, Supabase, and B2 calls so they can be reused by production
stages and local smoke tests.
"""

import csv
import json
from datetime import datetime, timezone
from pathlib import Path


def _blank(value):
    return value is None or str(value).strip() == ""


def _parse_int(value):
    if _blank(value):
        return None
    try:
        return int(float(str(value).strip()))
    except ValueError:
        return None


def _result_to_dict(result):
    if result is None:
        return None
    return {
        "status": result.status,
        "errors": list(result.errors),
        "warnings": list(result.warnings),
        "row_count": result.row_count,
    }


def find_source_anomalies(rows):
    """Find accepted source-table anomalies worth surfacing in QA output."""
    anomalies = []
    for index, row in enumerate(rows, start=1):
        state = str(row.get("States", "")).strip()
        if not state or state.lower() == "total":
            continue

        confirmed = row.get("Confirmed", "")
        deaths = _parse_int(row.get("Deaths", ""))
        if _blank(confirmed) and deaths is not None and deaths > 0:
            anomalies.append(
                {
                    "row_index": index,
                    "state": state,
                    "type": "blank_confirmed_with_deaths",
                    "confirmed": confirmed,
                    "deaths": deaths,
                }
            )
    return anomalies


def summarize_extracted_rows(rows):
    states = [
        str(row.get("States", "")).strip()
        for row in rows
        if str(row.get("States", "")).strip()
    ]
    non_total_states = [state for state in states if state.lower() != "total"]
    return {
        "row_count": len(rows),
        "state_count": len(non_total_states),
        "has_total_row": any(state.lower() == "total" for state in states),
        "source_anomalies": find_source_anomalies(rows),
    }


def load_json_if_exists(path):
    if not path or not Path(path).exists():
        return None
    with Path(path).open(encoding="utf-8") as infile:
        return json.load(infile)


def read_extracted_csv_rows(csv_path):
    with Path(csv_path).open(newline="", encoding="utf-8") as csvfile:
        return list(csv.DictReader(csvfile))


def default_layout_qa_path_for_enhanced_image(enhanced_image_path):
    if not enhanced_image_path:
        return None
    return Path(enhanced_image_path).with_suffix(".layout_qa.json")


def write_extraction_qa(
    output_path,
    *,
    enhanced_name,
    csv_path,
    year,
    week,
    model_name,
    status,
    accepted_attempt=None,
    max_attempts=None,
    selected_rows=None,
    validation_result=None,
    csv_qa_result=None,
    layout_qa_path=None,
):
    """Write an extraction QA sidecar and return the written payload."""
    rows = selected_rows or []
    row_summary = summarize_extracted_rows(rows)
    validation_payload = None
    if validation_result is not None:
        validation_payload = {
            "status": validation_result.status,
            "errors": list(validation_result.errors),
            "warnings": list(validation_result.warnings),
        }

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "enhanced_name": enhanced_name,
        "csv_path": str(csv_path) if csv_path else None,
        "year": str(year) if year is not None else None,
        "week": str(week) if week is not None else None,
        "model_name": model_name,
        "accepted_attempt": accepted_attempt,
        "max_attempts": max_attempts,
        "row_count": row_summary["row_count"],
        "state_count": row_summary["state_count"],
        "has_total_row": row_summary["has_total_row"],
        "source_anomalies": row_summary["source_anomalies"],
        "validation": validation_payload,
        "csv_qa": _result_to_dict(csv_qa_result),
        "layout_qa": load_json_if_exists(layout_qa_path),
    }

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as outfile:
        json.dump(payload, outfile, indent=2, sort_keys=True)
        outfile.write("\n")
    return payload
