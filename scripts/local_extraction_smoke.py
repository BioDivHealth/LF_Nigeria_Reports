#!/usr/bin/env python3
"""
Local no-side-effect smoke harness for the Lassa table extraction pipeline.

The default paths avoid Supabase, B2, Gemini, production processed folders, and
public exports. Live Gemini extraction is available only with --use-gemini.
"""

import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.csv_qa import REQUIRED_COLUMNS, validate_extracted_csv
from src.utils.extraction_qa import write_extraction_qa
from src.utils.report_layout import find_table3_page
from src.utils.table_enhancement import DEFAULT_PARAMS, enhance_table_lines_from_pdf_hq


TABLE_FIELDNAMES = ["States", "Suspected", "Confirmed", "Probable", "HCW", "Deaths"]
DEFAULT_MODEL = "gemini-3-flash-preview"


def infer_report_metadata(path):
    """
    Infer two-digit year and week from standardized report/artifact names.

    Examples:
      Nigeria_03_May_25_W18.pdf
      Lines_Nigeria_03_May_25_W18_page3.png
    """
    match = re.search(r"Nigeria_\d{2}_[A-Za-z]{3}_(\d{2})_W(\d+)", Path(path).name)
    if not match:
        return {"year": None, "week": None}
    return {"year": match.group(1), "week": match.group(2)}


def default_output_dir():
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path("/tmp") / f"lassa_extraction_smoke_{stamp}"


def ensure_output_dir(path):
    output_dir = Path(path) if path else default_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as outfile:
        json.dump(payload, outfile, indent=2, sort_keys=True)
        outfile.write("\n")


def result_to_dict(result):
    return {
        "status": result.status,
        "errors": result.errors,
        "warnings": result.warnings,
        "row_count": result.row_count,
    }


def inspect_image(path):
    from PIL import Image

    image_path = Path(path)
    with Image.open(image_path) as image:
        image.verify()
        width, height = image.size
        mode = image.mode
    return {"path": str(image_path), "width": width, "height": height, "mode": mode}


def save_rows_to_csv(rows, output_path, year=None, week=None):
    fieldnames = REQUIRED_COLUMNS
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            csv_row = {column: row.get(column, "") for column in TABLE_FIELDNAMES}
            csv_row["Year"] = f"20{year}" if year and len(str(year)) == 2 else (year or "")
            csv_row["Week"] = week or ""
            writer.writerow({column: csv_row.get(column, "") for column in fieldnames})


def run_gemini_extraction(
    image_path,
    output_dir,
    enhanced_name,
    year=None,
    week=None,
    model=DEFAULT_MODEL,
    max_attempts=3,
    layout_qa_path=None,
):
    if not os.environ.get("GOOGLE_API_KEY"):
        return {
            "status": "fail",
            "errors": ["GOOGLE_API_KEY is required when --use-gemini is set."],
            "warnings": [],
        }

    from src.utils.extraction_validation import validate_extraction_results
    from src.utils.gemini_extractor import extract_table_with_gemini, parse_gemini_response

    extraction_dir = output_dir / "gemini"
    extraction_dir.mkdir(parents=True, exist_ok=True)
    attempts = []

    for attempt in range(1, max_attempts + 1):
        parsed_data = []
        responses_ok = 0
        parse_errors = []

        for iteration in range(1, 3):
            success, response = extract_table_with_gemini(str(image_path), model)
            if not success:
                parse_errors.append(f"iteration {iteration}: {response}")
                continue

            responses_ok += 1
            parse_success, parsed = parse_gemini_response(response)
            if not parse_success:
                parse_errors.append(f"iteration {iteration}: {parsed}")
                continue

            parsed_data.append(parsed)
            write_json(extraction_dir / f"attempt_{attempt}_iteration_{iteration}.json", parsed)

        if len(parsed_data) < 2:
            attempts.append(
                {
                    "attempt": attempt,
                    "status": "retry" if attempt < max_attempts else "fail",
                    "responses_ok": responses_ok,
                    "errors": parse_errors,
                    "warnings": [],
                }
            )
            continue

        validation_result = validate_extraction_results(parsed_data, enhanced_name, attempt, max_attempts)
        attempt_summary = {
            "attempt": attempt,
            "status": validation_result.status,
            "errors": validation_result.errors,
            "warnings": validation_result.warnings,
        }
        attempts.append(attempt_summary)

        if validation_result.status == "pass":
            csv_path = output_dir / "extracted" / f"{Path(enhanced_name).stem}.csv"
            save_rows_to_csv(validation_result.selected_rows, csv_path, year=year, week=week)
            csv_qa = validate_extracted_csv(csv_path, expected_year=year, expected_week=week)
            extraction_qa_path = csv_path.with_suffix(".extraction_qa.json")
            extraction_qa = write_extraction_qa(
                extraction_qa_path,
                enhanced_name=enhanced_name,
                csv_path=csv_path,
                year=year,
                week=week,
                model_name=model,
                status=csv_qa.status,
                accepted_attempt=attempt,
                max_attempts=max_attempts,
                selected_rows=validation_result.selected_rows,
                validation_result=validation_result,
                csv_qa_result=csv_qa,
                layout_qa_path=layout_qa_path,
            )
            return {
                "status": csv_qa.status,
                "model": model,
                "max_attempts": max_attempts,
                "attempts": attempts,
                "csv_path": str(csv_path),
                "extraction_qa_path": str(extraction_qa_path),
                "extraction_qa": extraction_qa,
                "csv_qa": result_to_dict(csv_qa),
                "errors": csv_qa.errors,
                "warnings": validation_result.warnings + csv_qa.warnings,
            }

    final_errors = attempts[-1]["errors"] if attempts else ["Gemini extraction did not run."]
    return {
        "status": "fail",
        "model": model,
        "max_attempts": max_attempts,
        "attempts": attempts,
        "errors": final_errors,
        "warnings": [],
    }


def base_summary(command, input_path, output_dir, year=None, week=None):
    return {
        "command": command,
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "year": year,
        "week": week,
        "status": "pass",
        "errors": [],
        "warnings": [],
    }


def write_summary(output_dir, summary):
    summary_path = output_dir / "smoke_summary.json"
    write_json(summary_path, summary)
    return summary_path


def run_pdf(args):
    pdf_path = Path(args.pdf)
    if not pdf_path.is_file():
        return fail_before_output("pdf", pdf_path, args.output_dir, [f"Input PDF does not exist: {pdf_path}"])

    output_dir = ensure_output_dir(args.output_dir)
    metadata = infer_report_metadata(pdf_path)
    year = args.year or metadata["year"]
    week = args.week or metadata["week"]
    summary = base_summary("pdf", pdf_path, output_dir, year=year, week=week)
    if not year or not week:
        summary["warnings"].append("Could not infer year/week from filename; use --year and --week for metadata-aware QA.")

    enhanced_name = f"Lines_{pdf_path.stem}_page3.png"
    enhanced_path = output_dir / "enhanced" / enhanced_name
    enhanced_path.parent.mkdir(parents=True, exist_ok=True)
    layout_result = find_table3_page(pdf_path, default_page_index=DEFAULT_PARAMS["page_number"], year=year, week=week)
    layout_qa = layout_result.to_dict()
    summary["layout_qa"] = layout_qa
    summary["warnings"].extend(layout_result.warnings)
    write_json(output_dir / "layout_qa.json", layout_qa)

    if layout_result.confidence in {"low", "none"} or layout_result.selected_page_index is None:
        summary["status"] = "fail"
        summary["errors"].extend(layout_result.reasons)
        write_summary(output_dir, summary)
        return 1

    try:
        enhancement_params = DEFAULT_PARAMS.copy()
        enhancement_params["page_number"] = layout_result.selected_page_index
        enhance_table_lines_from_pdf_hq(
            str(pdf_path),
            str(enhanced_path),
            **enhancement_params,
            year=year,
            week=week,
        )
        summary["enhanced_path"] = str(enhanced_path)
        summary["image"] = inspect_image(enhanced_path)
    except Exception as exc:
        summary["status"] = "fail"
        summary["errors"].append(f"PDF enhancement failed: {exc}")
        write_summary(output_dir, summary)
        return 1

    if args.use_gemini:
        summary["gemini"] = run_gemini_extraction(
            enhanced_path,
            output_dir,
            enhanced_name,
            year=year,
            week=week,
            model=args.model,
            max_attempts=args.max_attempts,
            layout_qa_path=output_dir / "layout_qa.json",
        )
        if summary["gemini"]["status"] != "pass":
            summary["status"] = "fail"
            summary["errors"].extend(summary["gemini"].get("errors", []))

    write_summary(output_dir, summary)
    return 0 if summary["status"] == "pass" else 1


def run_image(args):
    image_path = Path(args.image)
    if not image_path.is_file():
        return fail_before_output("image", image_path, args.output_dir, [f"Input image does not exist: {image_path}"])

    output_dir = ensure_output_dir(args.output_dir)
    metadata = infer_report_metadata(image_path)
    year = args.year or metadata["year"]
    week = args.week or metadata["week"]
    summary = base_summary("image", image_path, output_dir, year=year, week=week)

    try:
        summary["image"] = inspect_image(image_path)
    except Exception as exc:
        summary["status"] = "fail"
        summary["errors"].append(f"Image inspection failed: {exc}")
        write_summary(output_dir, summary)
        return 1

    if args.use_gemini:
        summary["gemini"] = run_gemini_extraction(
            image_path,
            output_dir,
            image_path.name,
            year=year,
            week=week,
            model=args.model,
            max_attempts=args.max_attempts,
        )
        if summary["gemini"]["status"] != "pass":
            summary["status"] = "fail"
            summary["errors"].extend(summary["gemini"].get("errors", []))

    write_summary(output_dir, summary)
    return 0 if summary["status"] == "pass" else 1


def run_csv(args):
    csv_path = Path(args.csv)
    if not csv_path.is_file():
        return fail_before_output("csv", csv_path, args.output_dir, [f"Input CSV does not exist: {csv_path}"])

    output_dir = ensure_output_dir(args.output_dir)
    metadata = infer_report_metadata(csv_path)
    year = args.year or metadata["year"]
    week = args.week or metadata["week"]
    result = validate_extracted_csv(csv_path, expected_year=year, expected_week=week)
    summary = base_summary("csv", csv_path, output_dir, year=year, week=week)
    summary["csv_qa"] = result_to_dict(result)
    summary["status"] = result.status
    summary["errors"].extend(result.errors)
    summary["warnings"].extend(result.warnings)
    write_summary(output_dir, summary)
    return 0 if result.status == "pass" else 1


def run_csv_scan(args):
    csv_root = Path(args.csv_root)
    if not csv_root.exists():
        return fail_before_output("csv-scan", csv_root, args.output_dir, [f"CSV root does not exist: {csv_root}"])

    output_dir = ensure_output_dir(args.output_dir)
    summary = base_summary("csv-scan", csv_root, output_dir)
    records = []
    for csv_path in sorted(csv_root.rglob("*.csv")):
        metadata = infer_report_metadata(csv_path)
        result = validate_extracted_csv(
            csv_path,
            expected_year=args.year or metadata["year"],
            expected_week=args.week or metadata["week"],
        )
        records.append(
            {
                "path": str(csv_path),
                "year": args.year or metadata["year"],
                "week": args.week or metadata["week"],
                "csv_qa": result_to_dict(result),
            }
        )

    failures = [record for record in records if record["csv_qa"]["status"] != "pass"]
    summary["status"] = "fail" if failures else "pass"
    summary["csv_count"] = len(records)
    summary["failure_count"] = len(failures)
    summary["results"] = records
    if not records:
        summary["warnings"].append("No CSV files found.")
    write_summary(output_dir, summary)
    return 0 if summary["status"] == "pass" else 1


def fail_before_output(command, input_path, output_dir_arg, errors):
    output_dir = ensure_output_dir(output_dir_arg)
    summary = base_summary(command, input_path, output_dir)
    summary["status"] = "fail"
    summary["errors"].extend(errors)
    write_summary(output_dir, summary)
    return 1


def add_common_options(parser):
    parser.add_argument("--output-dir", help="Scratch output directory. Defaults to /tmp/lassa_extraction_smoke_<timestamp>.")
    parser.add_argument("--year", help="Report year, two digits or four digits. Overrides filename inference.")
    parser.add_argument("--week", help="Report week. Overrides filename inference.")


def add_gemini_options(parser):
    parser.add_argument("--use-gemini", action="store_true", help="Opt into live Gemini extraction.")
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"Gemini model for --use-gemini. Default: {DEFAULT_MODEL}.")
    parser.add_argument("--max-attempts", type=int, default=3, help="Maximum Gemini validation attempts. Default: 3.")


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    pdf_parser = subparsers.add_parser("pdf", help="Enhance a local PDF into a scratch output directory.")
    pdf_parser.add_argument("--pdf", required=True, help="Path to a local PDF.")
    add_common_options(pdf_parser)
    add_gemini_options(pdf_parser)
    pdf_parser.set_defaults(func=run_pdf)

    image_parser = subparsers.add_parser("image", help="Inspect an enhanced PNG and optionally run Gemini.")
    image_parser.add_argument("--image", required=True, help="Path to a local enhanced PNG.")
    add_common_options(image_parser)
    add_gemini_options(image_parser)
    image_parser.set_defaults(func=run_image)

    csv_parser = subparsers.add_parser("csv", help="Run CSV QA for one extracted CSV.")
    csv_parser.add_argument("--csv", required=True, help="Path to an extracted CSV.")
    add_common_options(csv_parser)
    csv_parser.set_defaults(func=run_csv)

    scan_parser = subparsers.add_parser("csv-scan", help="Run CSV QA for all CSVs under a root.")
    scan_parser.add_argument("--csv-root", required=True, help="Directory containing extracted CSV files.")
    add_common_options(scan_parser)
    scan_parser.set_defaults(func=run_csv_scan)

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "max_attempts", 1) < 1:
        parser.error("--max-attempts must be at least 1")
    try:
        return args.func(args)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
