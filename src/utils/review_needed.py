"""
Run-local review-needed records for QA-blocked pipeline artifacts.

This module is intentionally small and side-effect light: it appends JSONL
records for existing QA gate decisions and never changes database/storage state.
"""

import json
import logging
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


REVIEW_NEEDED_FILENAME = "review_needed.jsonl"


def default_review_needed_path(base_dir=None):
    project_root = Path(base_dir) if base_dir is not None else Path(__file__).resolve().parents[2]
    return project_root / "data" / "processed" / REVIEW_NEEDED_FILENAME


def _timestamp():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def record_review_needed(
    *,
    stage,
    check_type,
    reason,
    action,
    report_id=None,
    year=None,
    week=None,
    artifact_name=None,
    path=None,
    created_at=None,
):
    """
    Append a single review-needed record.

    Returns the record when it is written, or None if writing failed. Failures
    are logged but do not interrupt the caller's existing pipeline behavior.
    """
    record = {
        "stage": stage,
        "report_id": report_id,
        "year": year,
        "week": week,
        "artifact_name": artifact_name,
        "check_type": check_type,
        "reason": str(reason or "").strip(),
        "action": action,
        "created_at": created_at or _timestamp(),
    }

    output_path = Path(path) if path is not None else default_review_needed_path()
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("a", encoding="utf-8") as outfile:
            outfile.write(json.dumps(record, sort_keys=True))
            outfile.write("\n")
        return record
    except OSError as exc:
        logging.warning(f"Could not write review-needed record to {output_path}: {exc}")
        return None


def summarize_review_needed(path=None, base_dir=None):
    """
    Summarize review-needed JSONL records.

    Missing files return zero counts. Malformed JSONL rows are counted under the
    synthetic ``malformed`` stage/check type so the summary remains visible.
    """
    review_path = Path(path) if path is not None else default_review_needed_path(base_dir)
    summary = {
        "total": 0,
        "by_stage": {},
        "by_check_type": {},
    }
    if not review_path.exists():
        return summary

    by_stage = Counter()
    by_check_type = Counter()
    total = 0

    try:
        with review_path.open(encoding="utf-8") as infile:
            for line in infile:
                if not line.strip():
                    continue
                total += 1
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    by_stage["malformed"] += 1
                    by_check_type["malformed"] += 1
                    continue
                by_stage[str(record.get("stage") or "unknown")] += 1
                by_check_type[str(record.get("check_type") or "unknown")] += 1
    except OSError as exc:
        logging.warning(f"Could not read review-needed summary from {review_path}: {exc}")
        return summary

    summary["total"] = total
    summary["by_stage"] = dict(sorted(by_stage.items()))
    summary["by_check_type"] = dict(sorted(by_check_type.items()))
    return summary
