"""
Helpers for Lassa pipeline artifact names and sidecar paths.

This module preserves the current legacy ``_page3`` naming convention while
giving downstream stages one place to derive artifact names from source
metadata. The helpers are intentionally pure: they do not create, read, or
write files.
"""

from pathlib import Path


def _clean_name(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def legacy_enhanced_name_from_pdf(pdf_name):
    """Return the legacy enhanced PNG name for a source PDF name."""
    clean_pdf_name = _clean_name(pdf_name)
    if not clean_pdf_name:
        return None
    return f"Lines_{Path(clean_pdf_name).stem}_page3.png"


def enhanced_name_for_report(new_name, enhanced_name=None):
    """Return the stored enhanced name, falling back to legacy PDF-derived naming."""
    clean_enhanced_name = _clean_name(enhanced_name)
    if clean_enhanced_name:
        return clean_enhanced_name
    return legacy_enhanced_name_from_pdf(new_name)


def csv_name_for_enhanced(enhanced_name):
    """Return the CSV filename corresponding to an enhanced image filename."""
    clean_enhanced_name = _clean_name(enhanced_name)
    if not clean_enhanced_name:
        return None
    return f"{Path(clean_enhanced_name).stem}.csv"


def csv_name_for_report(new_name, enhanced_name=None):
    """Return the CSV filename for a report, preferring the enhanced artifact name."""
    return csv_name_for_enhanced(enhanced_name_for_report(new_name, enhanced_name))


def layout_qa_name_for_enhanced(enhanced_name):
    """Return the layout QA sidecar filename for an enhanced image filename."""
    clean_enhanced_name = _clean_name(enhanced_name)
    if not clean_enhanced_name:
        return None
    return f"{Path(clean_enhanced_name).stem}.layout_qa.json"


def extraction_qa_name_for_csv(csv_name):
    """Return the extraction QA sidecar filename for a processed CSV filename."""
    clean_csv_name = _clean_name(csv_name)
    if not clean_csv_name:
        return None
    return f"{Path(clean_csv_name).stem}.extraction_qa.json"


def enhanced_image_path(base_dir, year, enhanced_name):
    """Return the local enhanced image path for a year folder and artifact name."""
    clean_enhanced_name = _clean_name(enhanced_name)
    if base_dir is None or year is None or not clean_enhanced_name:
        return None
    return Path(base_dir) / f"PDFs_Lines_{year}" / clean_enhanced_name


def csv_path(base_dir, year, csv_name):
    """Return the local processed CSV path for a year folder and CSV name."""
    clean_csv_name = _clean_name(csv_name)
    if base_dir is None or year is None or not clean_csv_name:
        return None
    return Path(base_dir) / f"CSV_LF_{year}_Sorted" / clean_csv_name


def layout_qa_path_for_enhanced_path(enhanced_path):
    """Return the layout QA sidecar path adjacent to an enhanced image path."""
    if not enhanced_path:
        return None
    return Path(enhanced_path).with_suffix(".layout_qa.json")


def extraction_qa_path_for_csv_path(csv_file_path):
    """Return the extraction QA sidecar path adjacent to a processed CSV path."""
    if not csv_file_path:
        return None
    return Path(csv_file_path).with_suffix(".extraction_qa.json")
