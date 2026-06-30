"""
Extraction validation helpers for comparing repeated LLM table outputs.

This module is intentionally free of database, B2, and Gemini calls so it can be
tested locally and reused by pipeline stages.
"""

from dataclasses import dataclass, field
from typing import Literal, Optional

try:
    from utils.data_validation import (
        filter_comparison_columns,
        normalize_state_names,
        sort_table_rows,
        validate_logical_consistency,
    )
except ImportError:
    from src.utils.data_validation import (
        filter_comparison_columns,
        normalize_state_names,
        sort_table_rows,
        validate_logical_consistency,
    )


ExtractionStatus = Literal["pass", "retry", "fail"]


@dataclass
class ExtractionValidationResult:
    status: ExtractionStatus
    selected_rows: Optional[list[dict]] = None
    normalized_1: list[dict] = field(default_factory=list)
    normalized_2: list[dict] = field(default_factory=list)
    comparison_1: list[dict] = field(default_factory=list)
    comparison_2: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _comparison_rows(rows):
    sorted_rows = sort_table_rows(rows)
    normalized_rows = normalize_state_names(sorted_rows)
    comparison_rows = filter_comparison_columns(normalized_rows)
    return normalized_rows, comparison_rows


def _parse_int(value):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _blank_confirmed_death_anomaly_errors(rows, errors):
    """
    Return allowed errors when every logical error is explained by a source row where
    Confirmed is blank but Deaths is populated.
    """
    if not errors:
        return set()

    allowed_errors = set()
    for i, row in enumerate(rows):
        state = row.get("States", "Unknown")
        state_text = str(state).strip()
        if not state_text or state_text.lower() == "total":
            continue

        confirmed_raw = str(row.get("Confirmed", "")).strip()
        suspected = _parse_int(row.get("Suspected", "0"))
        deaths = _parse_int(row.get("Deaths", "0"))

        if (
            confirmed_raw == ""
            and suspected is not None
            and deaths is not None
            and deaths > 0
            and suspected >= deaths
        ):
            allowed_errors.add(
                f"Logical inconsistency in row {i+1} ({state}): Confirmed (0) < Deaths ({deaths})"
            )

    return set(errors) if allowed_errors and set(errors).issubset(allowed_errors) else set()


def validate_extraction_results(parsed_data, enhanced_name, attempt, max_attempts):
    """
    Validate and compare two parsed Gemini extraction outputs.

    Returns an ExtractionValidationResult with an explicit pass/retry/fail status.
    The function never returns mixed tuple shapes, so callers can handle retry
    decisions without accidental unpacking errors.
    """
    if len(parsed_data) < 2:
        status = "retry" if attempt < max_attempts else "fail"
        return ExtractionValidationResult(
            status=status,
            errors=[f"Expected two parsed outputs for {enhanced_name}, got {len(parsed_data)}."],
        )

    dict_rows_1, dict_rows_2 = parsed_data[:2]

    is_valid_1, validated_rows_1, errors_1 = validate_logical_consistency(dict_rows_1)
    is_valid_2, validated_rows_2, errors_2 = validate_logical_consistency(dict_rows_2)
    source_anomaly_errors_1 = _blank_confirmed_death_anomaly_errors(dict_rows_1, errors_1)
    source_anomaly_errors_2 = _blank_confirmed_death_anomaly_errors(dict_rows_2, errors_2)
    has_source_anomaly_1 = bool(source_anomaly_errors_1)
    has_source_anomaly_2 = bool(source_anomaly_errors_2)
    candidate_valid_1 = is_valid_1 or has_source_anomaly_1
    candidate_valid_2 = is_valid_2 or has_source_anomaly_2

    errors = []
    warnings = []
    if errors_1:
        errors.extend([f"iteration 1: {error}" for error in errors_1 if error not in source_anomaly_errors_1])
    if errors_2:
        errors.extend([f"iteration 2: {error}" for error in errors_2 if error not in source_anomaly_errors_2])
    if has_source_anomaly_1:
        warnings.append(
            f"Iteration 1 has a source-table anomaly for {enhanced_name}: Confirmed is blank while Deaths is populated."
        )
    if has_source_anomaly_2:
        warnings.append(
            f"Iteration 2 has a source-table anomaly for {enhanced_name}: Confirmed is blank while Deaths is populated."
        )

    if not candidate_valid_1 and not candidate_valid_2:
        if attempt < max_attempts:
            return ExtractionValidationResult(
                status="retry",
                errors=errors,
                warnings=warnings + [f"Both iterations have logical inconsistencies for {enhanced_name}."],
            )
        return ExtractionValidationResult(
            status="fail",
            errors=errors,
            warnings=warnings + [f"Both iterations have logical inconsistencies on final attempt for {enhanced_name}."],
        )

    if candidate_valid_1 and not candidate_valid_2:
        normalized_1, comparison_1 = _comparison_rows(dict_rows_1)
        normalized_2, comparison_2 = _comparison_rows(validated_rows_2)
        return ExtractionValidationResult(
            status="pass",
            selected_rows=dict_rows_1,
            normalized_1=normalized_1,
            normalized_2=normalized_2,
            comparison_1=comparison_1,
            comparison_2=comparison_2,
            errors=errors,
            warnings=warnings + ["Using iteration 1 because iteration 2 had logical inconsistencies."],
        )

    if not candidate_valid_1 and candidate_valid_2:
        normalized_1, comparison_1 = _comparison_rows(validated_rows_1)
        normalized_2, comparison_2 = _comparison_rows(dict_rows_2)
        return ExtractionValidationResult(
            status="pass",
            selected_rows=dict_rows_2,
            normalized_1=normalized_1,
            normalized_2=normalized_2,
            comparison_1=comparison_1,
            comparison_2=comparison_2,
            errors=errors,
            warnings=warnings + ["Using iteration 2 because iteration 1 had logical inconsistencies."],
        )

    normalized_1, comparison_1 = _comparison_rows(dict_rows_1)
    normalized_2, comparison_2 = _comparison_rows(dict_rows_2)

    if comparison_1 == comparison_2:
        return ExtractionValidationResult(
            status="pass",
            selected_rows=dict_rows_1,
            normalized_1=normalized_1,
            normalized_2=normalized_2,
            comparison_1=comparison_1,
            comparison_2=comparison_2,
            errors=errors,
            warnings=warnings,
        )

    status = "retry" if attempt < max_attempts else "fail"
    return ExtractionValidationResult(
        status=status,
        normalized_1=normalized_1,
        normalized_2=normalized_2,
        comparison_1=comparison_1,
        comparison_2=comparison_2,
        errors=errors,
        warnings=[f"Outputs differ on comparison columns for {enhanced_name}."],
    )
