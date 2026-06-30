"""
Shared QA checks for pipeline status synchronization.

These helpers interpret existing QA sidecars without touching Supabase, B2, or
Gemini. They distinguish a missing sidecar from an explicit failed sidecar so
sync stages can avoid mass-demoting older historical artifacts.
"""

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass
class QAStatusResult:
    ok: bool
    reason: str
    present: bool = True


def _fail(reason, present=True):
    return QAStatusResult(ok=False, reason=reason, present=present)


def _pass(reason):
    return QAStatusResult(ok=True, reason=reason, present=True)


def load_json_sidecar(path):
    """Load a JSON QA sidecar, returning ``(payload, QAStatusResult)``."""
    if not path:
        return None, _fail("QA sidecar path is missing.", present=False)

    sidecar_path = Path(path)
    if not sidecar_path.exists():
        return None, _fail(f"QA sidecar is missing: {sidecar_path}", present=False)

    try:
        with sidecar_path.open(encoding="utf-8") as infile:
            payload = json.load(infile)
    except (OSError, json.JSONDecodeError) as exc:
        return None, _fail(f"Could not read QA sidecar {sidecar_path}: {exc}")

    if not isinstance(payload, dict):
        return None, _fail(f"QA sidecar {sidecar_path} does not contain a JSON object.")

    return payload, _pass(f"Loaded QA sidecar: {sidecar_path}")


def check_layout_qa_payload(payload):
    """Return whether a layout QA payload is acceptable for enhanced status."""
    if not isinstance(payload, dict):
        return _fail("Layout QA payload is not a JSON object.")

    status = payload.get("status")
    confidence = payload.get("confidence")
    selected_page_index = payload.get("selected_page_index")

    if status not in {"pass", "warning"}:
        return _fail(f"Layout QA status is not acceptable: {status!r}.")
    if confidence not in {"high", "medium"}:
        return _fail(f"Layout QA confidence is not acceptable: {confidence!r}.")
    if selected_page_index is None:
        return _fail("Layout QA has no selected_page_index.")

    return _pass("Layout QA passed.")


def check_layout_qa_file(path):
    payload, load_result = load_json_sidecar(path)
    if not load_result.ok:
        return load_result
    return check_layout_qa_payload(payload)


def check_extraction_qa_payload(payload):
    """Return whether an extraction QA payload is acceptable for processed status."""
    if not isinstance(payload, dict):
        return _fail("Extraction QA payload is not a JSON object.")

    status = payload.get("status")
    if status != "pass":
        return _fail(f"Extraction QA status is not acceptable: {status!r}.")

    validation = payload.get("validation")
    if validation is not None:
        if not isinstance(validation, dict):
            return _fail("Extraction QA validation payload is not a JSON object.")
        if validation.get("status") != "pass":
            return _fail(f"Extraction validation status is not acceptable: {validation.get('status')!r}.")

    csv_qa = payload.get("csv_qa")
    if csv_qa is not None:
        if not isinstance(csv_qa, dict):
            return _fail("Extraction QA csv_qa payload is not a JSON object.")
        if csv_qa.get("status") != "pass":
            return _fail(f"CSV QA status is not acceptable: {csv_qa.get('status')!r}.")

    return _pass("Extraction QA passed.")


def check_extraction_qa_file(path):
    payload, load_result = load_json_sidecar(path)
    if not load_result.ok:
        return load_result
    return check_extraction_qa_payload(payload)
