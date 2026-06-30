"""
Report layout helpers for locating Table 3 in Lassa fever PDFs.

The first version is deliberately text-first. It avoids image rendering and
external services so it can run in local smoke tests and production enhancement.
"""

from dataclasses import asdict, dataclass, field
from typing import Literal, Optional

import fitz


LayoutStatus = Literal["pass", "warning", "fail"]
LayoutConfidence = Literal["high", "medium", "low", "none"]

TABLE3_CUE = "Table 3"
WEEKLY_CUE = "Weekly and Cumulative"
CASES_CUE = "suspected and confirmed cases"
TEXT_CUES = [TABLE3_CUE, WEEKLY_CUE, CASES_CUE]


@dataclass
class PageCandidate:
    page_index: int
    page_number: int
    score: int
    confidence: LayoutConfidence
    text_cues: list[str] = field(default_factory=list)

    def to_dict(self):
        return asdict(self)


@dataclass
class Table3PageResult:
    status: LayoutStatus
    selected_page_index: Optional[int]
    selected_page_number: Optional[int]
    confidence: LayoutConfidence
    score: int
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    text_cues: list[str] = field(default_factory=list)
    page_count: int = 0
    candidates: list[dict] = field(default_factory=list)

    def to_dict(self):
        return asdict(self)


def _normalise_text(text):
    return " ".join(str(text or "").split()).lower()


def _normalise_year(value):
    if value is None:
        return None
    text = str(value).strip()
    if text == "2020":
        return "20"
    return text


def _normalise_week(value):
    if value is None:
        return None
    try:
        return str(int(str(value).strip()))
    except ValueError:
        return str(value).strip()


def legacy_table3_page_index(default_page_index=3, year=None, week=None):
    """Return the existing hard-coded page fallback used before layout QA."""
    if _normalise_year(year) == "20" and _normalise_week(week) == "23":
        return 4
    return default_page_index


def _cue_hits(text):
    normalised = _normalise_text(text)
    return [cue for cue in TEXT_CUES if cue.lower() in normalised]


def _confidence_for_hits(hits):
    has_table = TABLE3_CUE in hits
    has_weekly = WEEKLY_CUE in hits
    has_cases = CASES_CUE in hits

    if has_table and has_weekly and has_cases:
        return "high"
    if has_weekly and (has_table or has_cases):
        return "medium"
    if hits:
        return "low"
    return "none"


def _candidate_from_hits(page_index, hits):
    confidence = _confidence_for_hits(hits)
    return PageCandidate(
        page_index=page_index,
        page_number=page_index + 1,
        score=len(hits),
        confidence=confidence,
        text_cues=hits,
    )


def _rank_candidates(candidates):
    confidence_rank = {"high": 3, "medium": 2, "low": 1, "none": 0}
    return sorted(
        candidates,
        key=lambda candidate: (confidence_rank[candidate.confidence], candidate.score, -candidate.page_index),
        reverse=True,
    )


def find_table3_page(pdf_path, default_page_index=3, year=None, week=None):
    """
    Locate the likely Table 3 page using embedded PDF text cues.

    Low-confidence legacy fallback results intentionally return status="fail"
    so callers do not silently enhance a page that was not positively located.
    """
    fallback_page_index = legacy_table3_page_index(default_page_index, year=year, week=week)
    reasons = []
    warnings = []
    candidates = []

    try:
        doc = fitz.open(pdf_path)
    except Exception as exc:
        return Table3PageResult(
            status="fail",
            selected_page_index=None,
            selected_page_number=None,
            confidence="none",
            score=0,
            reasons=[f"Could not open PDF: {exc}"],
            page_count=0,
        )

    try:
        page_count = len(doc)
        for page_index, page in enumerate(doc):
            hits = _cue_hits(page.get_text("text") or "")
            if hits:
                candidates.append(_candidate_from_hits(page_index, hits))

        ranked_candidates = _rank_candidates(candidates)
        acceptable = [
            candidate for candidate in ranked_candidates
            if candidate.confidence in {"high", "medium"}
        ]

        if acceptable:
            selected = acceptable[0]
            status = "pass" if selected.confidence == "high" else "warning"
            reasons.append(
                f"Selected page {selected.page_number} from text cues: {', '.join(selected.text_cues)}."
            )
            if selected.confidence == "medium":
                warnings.append("Only medium-confidence Table 3 title cues were found.")
            return Table3PageResult(
                status=status,
                selected_page_index=selected.page_index,
                selected_page_number=selected.page_number,
                confidence=selected.confidence,
                score=selected.score,
                reasons=reasons,
                warnings=warnings,
                text_cues=selected.text_cues,
                page_count=page_count,
                candidates=[candidate.to_dict() for candidate in ranked_candidates],
            )

        if 0 <= fallback_page_index < page_count:
            warnings.append(
                f"No acceptable Table 3 text candidate found; legacy fallback page index {fallback_page_index} is diagnostic only."
            )
            return Table3PageResult(
                status="fail",
                selected_page_index=fallback_page_index,
                selected_page_number=fallback_page_index + 1,
                confidence="low",
                score=0,
                reasons=["No page had enough Table 3 title cues."],
                warnings=warnings,
                page_count=page_count,
                candidates=[candidate.to_dict() for candidate in ranked_candidates],
            )

        warnings.append(
            f"Legacy fallback page index {fallback_page_index} is outside PDF page range 0-{max(page_count - 1, 0)}."
        )
        return Table3PageResult(
            status="fail",
            selected_page_index=None,
            selected_page_number=None,
            confidence="none",
            score=0,
            reasons=["No page had enough Table 3 title cues."],
            warnings=warnings,
            page_count=page_count,
            candidates=[candidate.to_dict() for candidate in ranked_candidates],
        )
    finally:
        doc.close()
