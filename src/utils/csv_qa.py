"""
Pure validation helpers for locally processed extraction CSV files.

These checks are intentionally limited to the CSV shape and basic row hygiene.
They do not call Gemini, Supabase, B2, or any pipeline stage.
"""

import csv
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Optional


CsvQAStatus = Literal["pass", "fail"]

REQUIRED_COLUMNS = ["Week", "Year", "States", "Suspected", "Confirmed", "Probable", "HCW", "Deaths"]
NUMERIC_COLUMNS = ["Suspected", "Confirmed", "Probable", "HCW", "Deaths"]


@dataclass
class CsvQAResult:
    status: CsvQAStatus
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    row_count: int = 0


def _result(errors, warnings, row_count):
    status = "fail" if errors else "pass"
    return CsvQAResult(status=status, errors=errors, warnings=warnings, row_count=row_count)


def _blank(value):
    return value is None or str(value).strip() == ""


def _normalized_state(value):
    return " ".join(str(value).strip().lower().split())


def _normalize_expected_year(value):
    if value is None:
        return None
    text = str(value).strip()
    if len(text) == 2 and text.isdigit():
        return f"20{text}"
    return text


def _matches_expected(value, expected):
    if expected is None:
        return True
    actual_text = str(value).strip()
    expected_text = str(expected).strip()
    try:
        return int(float(actual_text)) == int(float(expected_text))
    except ValueError:
        return actual_text == expected_text


def _is_integer_like(value):
    if _blank(value):
        return True
    text = str(value).strip()
    try:
        number = float(text)
    except ValueError:
        return False
    return math.isfinite(number) and number >= 0 and number.is_integer()


def validate_extracted_rows(rows, expected_year: Optional[str] = None, expected_week: Optional[str] = None):
    """
    Validate already-read extraction CSV rows.

    Args:
        rows: List of dictionaries in the processed CSV schema.
        expected_year: Optional full or two-digit report year.
        expected_week: Optional report week.

    Returns:
        CsvQAResult with pass/fail status, errors, warnings, and row count.
    """
    errors = []
    warnings = []
    expected_year = _normalize_expected_year(expected_year)

    if not rows:
        return _result(["CSV has no data rows."], warnings, 0)

    seen_states = set()
    total_rows = []

    for row_index, row in enumerate(rows, start=2):
        missing_columns = [column for column in REQUIRED_COLUMNS if column not in row]
        if missing_columns:
            errors.append(f"row {row_index}: missing required columns: {', '.join(missing_columns)}")
            continue

        extra_columns = [column for column in row if column not in REQUIRED_COLUMNS and column is not None]
        if extra_columns:
            warnings.append(f"row {row_index}: unexpected columns present: {', '.join(extra_columns)}")

        state = row.get("States")
        if _blank(state):
            errors.append(f"row {row_index}: States is blank.")
            continue

        normalized_state = _normalized_state(state)
        if normalized_state == "total":
            total_rows.append(row_index)
        elif normalized_state in seen_states:
            errors.append(f"row {row_index}: duplicate state '{state}'.")
        else:
            seen_states.add(normalized_state)

        actual_year = _normalize_expected_year(row.get("Year"))
        if not _matches_expected(actual_year, expected_year):
            errors.append(f"row {row_index}: Year '{row.get('Year')}' does not match expected '{expected_year}'.")
        if not _matches_expected(row.get("Week"), expected_week):
            errors.append(f"row {row_index}: Week '{row.get('Week')}' does not match expected '{expected_week}'.")

        for column in NUMERIC_COLUMNS:
            if not _is_integer_like(row.get(column)):
                errors.append(f"row {row_index}: {column} value '{row.get(column)}' is not a non-negative integer or blank.")

    if len(total_rows) > 1:
        errors.append(f"CSV has multiple Total rows: {', '.join(str(row) for row in total_rows)}.")
    elif not total_rows:
        warnings.append("CSV has no Total row.")
    else:
        total_row = rows[total_rows[0] - 2]
        if all(_blank(total_row.get(column)) for column in NUMERIC_COLUMNS):
            errors.append(f"row {total_rows[0]}: Total row has no numeric values.")
        if total_rows[0] != len(rows) + 1:
            warnings.append(f"row {total_rows[0]}: Total row is not the final CSV row.")

    return _result(errors, warnings, len(rows))


def validate_extracted_csv(csv_path, expected_year: Optional[str] = None, expected_week: Optional[str] = None):
    """
    Validate a processed extraction CSV from disk.

    Args:
        csv_path: Path to a processed extraction CSV.
        expected_year: Optional full or two-digit report year.
        expected_week: Optional report week.

    Returns:
        CsvQAResult with pass/fail status, errors, warnings, and row count.
    """
    path = Path(csv_path)
    errors = []
    warnings = []

    try:
        with path.open(newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            fieldnames = reader.fieldnames or []

            if not fieldnames:
                errors.append("CSV has no header row.")
            else:
                missing_columns = [column for column in REQUIRED_COLUMNS if column not in fieldnames]
                extra_columns = [column for column in fieldnames if column not in REQUIRED_COLUMNS]
                if missing_columns:
                    errors.append(f"CSV header is missing required columns: {', '.join(missing_columns)}.")
                if extra_columns:
                    warnings.append(f"CSV header has unexpected columns: {', '.join(extra_columns)}.")

            rows = list(reader)
    except OSError as exc:
        return _result([f"Could not read CSV '{path}': {exc}"], warnings, 0)

    row_result = validate_extracted_rows(rows, expected_year=expected_year, expected_week=expected_week)
    return _result(
        errors + row_result.errors,
        warnings + row_result.warnings,
        row_result.row_count,
    )
