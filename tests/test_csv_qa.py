import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.csv_qa import REQUIRED_COLUMNS, validate_extracted_csv, validate_extracted_rows


def load_llm_extraction_module():
    module_path = ROOT / "src" / "04b_LLM_Extraction_Supabase.py"
    spec = importlib.util.spec_from_file_location("llm_extraction_stage_csv_qa", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CsvQATests(unittest.TestCase):
    def valid_rows(self):
        return [
            {
                "Week": "1",
                "Year": "2026",
                "States": "Ondo",
                "Suspected": "51",
                "Confirmed": "3.0",
                "Probable": "",
                "HCW": "",
                "Deaths": "0",
            },
            {
                "Week": "1",
                "Year": "2026",
                "States": "Total",
                "Suspected": "51",
                "Confirmed": "3.0",
                "Probable": "",
                "HCW": "",
                "Deaths": "0",
            },
        ]

    def write_csv(self, path, rows, fieldnames=None):
        with path.open("w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames or REQUIRED_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)

    def test_validate_extracted_rows_accepts_valid_rows(self):
        result = validate_extracted_rows(self.valid_rows(), expected_year="26", expected_week="01")

        self.assertEqual("pass", result.status)
        self.assertEqual([], result.errors)
        self.assertEqual(2, result.row_count)

    def test_validate_extracted_rows_fails_empty_rows(self):
        result = validate_extracted_rows([], expected_year="2026", expected_week="1")

        self.assertEqual("fail", result.status)
        self.assertIn("CSV has no data rows.", result.errors)

    def test_validate_extracted_rows_fails_missing_required_column(self):
        rows = self.valid_rows()
        rows[0].pop("Deaths")

        result = validate_extracted_rows(rows, expected_year="2026", expected_week="1")

        self.assertEqual("fail", result.status)
        self.assertTrue(any("missing required columns: Deaths" in error for error in result.errors))

    def test_validate_extracted_rows_fails_year_week_mismatch(self):
        rows = self.valid_rows()
        rows[0] = {**rows[0], "Year": "2025"}
        rows[1] = {**rows[1], "Week": "2"}

        result = validate_extracted_rows(rows, expected_year="2026", expected_week="1")

        self.assertEqual("fail", result.status)
        self.assertTrue(any("Year '2025'" in error for error in result.errors))
        self.assertTrue(any("Week '2'" in error for error in result.errors))

    def test_validate_extracted_rows_accepts_blank_and_integer_like_numeric_values(self):
        rows = self.valid_rows()
        rows[0] = {**rows[0], "Suspected": "", "Confirmed": "5.0", "Probable": 0, "HCW": 1.0}

        result = validate_extracted_rows(rows, expected_year="2026", expected_week="1")

        self.assertEqual("pass", result.status)

    def test_validate_extracted_rows_fails_bad_numeric_values(self):
        rows = self.valid_rows()
        rows[0] = {**rows[0], "Suspected": "unknown", "Confirmed": "-1", "Probable": "5.5"}

        result = validate_extracted_rows(rows, expected_year="2026", expected_week="1")

        self.assertEqual("fail", result.status)
        self.assertTrue(any("Suspected value 'unknown'" in error for error in result.errors))
        self.assertTrue(any("Confirmed value '-1'" in error for error in result.errors))
        self.assertTrue(any("Probable value '5.5'" in error for error in result.errors))

    def test_validate_extracted_rows_fails_duplicate_state_and_duplicate_total(self):
        rows = self.valid_rows()
        rows.insert(1, {**rows[0], "States": "ondo"})
        rows.append({**rows[-1]})

        result = validate_extracted_rows(rows, expected_year="2026", expected_week="1")

        self.assertEqual("fail", result.status)
        self.assertTrue(any("duplicate state" in error for error in result.errors))
        self.assertTrue(any("multiple Total rows" in error for error in result.errors))

    def test_validate_extracted_rows_warns_when_total_missing(self):
        result = validate_extracted_rows([self.valid_rows()[0]], expected_year="2026", expected_week="1")

        self.assertEqual("pass", result.status)
        self.assertIn("CSV has no Total row.", result.warnings)

    def test_validate_extracted_csv_checks_header_and_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "valid.csv"
            self.write_csv(csv_path, self.valid_rows())

            result = validate_extracted_csv(csv_path, expected_year="2026", expected_week="1")

        self.assertEqual("pass", result.status)
        self.assertEqual(2, result.row_count)

    def test_validate_extracted_csv_fails_missing_header_column(self):
        rows = [
            {key: value for key, value in row.items() if key != "Deaths"}
            for row in self.valid_rows()
        ]
        fieldnames = [column for column in REQUIRED_COLUMNS if column != "Deaths"]

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "missing_deaths.csv"
            self.write_csv(csv_path, rows, fieldnames=fieldnames)

            result = validate_extracted_csv(csv_path, expected_year="2026", expected_week="1")

        self.assertEqual("fail", result.status)
        self.assertTrue(any("missing required columns: Deaths" in error for error in result.errors))

    def test_process_single_report_accepts_valid_existing_csv(self):
        module = load_llm_extraction_module()
        report_metadata = {
            "id": "00000000-0000-0000-0000-000000000000",
            "year": "26",
            "week": "1",
            "enhanced_name": "Lines_Nigeria_01_Jan_26_W1_page3.png",
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            module.CSV_BASE_FOLDER = Path(temp_dir)
            output_dir = Path(temp_dir) / "CSV_LF_26_Sorted"
            output_dir.mkdir(parents=True)
            self.write_csv(output_dir / "Lines_Nigeria_01_Jan_26_W1_page3.csv", self.valid_rows())
            engine = object()

            with patch.object(module, "update_processing_status") as update_mock, \
                patch.object(module, "get_enhanced_image") as image_mock:
                success = module.process_single_report(report_metadata, "gemini-test", engine)
                qa_path = output_dir / "Lines_Nigeria_01_Jan_26_W1_page3.extraction_qa.json"
                qa = json.loads(qa_path.read_text(encoding="utf-8"))

        self.assertTrue(success)
        update_mock.assert_called_once_with(engine, report_metadata["id"])
        image_mock.assert_not_called()
        self.assertEqual("pass", qa["status"])
        self.assertEqual("gemini-test", qa["model_name"])
        self.assertEqual("pass", qa["csv_qa"]["status"])
        self.assertEqual(2, qa["row_count"])

    def test_process_single_report_rejects_invalid_existing_csv(self):
        module = load_llm_extraction_module()
        report_metadata = {
            "id": "00000000-0000-0000-0000-000000000000",
            "year": "26",
            "week": "1",
            "enhanced_name": "Lines_Nigeria_01_Jan_26_W1_page3.png",
        }

        rows = self.valid_rows()
        rows[0] = {**rows[0], "Confirmed": "not a number"}

        with tempfile.TemporaryDirectory() as temp_dir:
            module.CSV_BASE_FOLDER = Path(temp_dir)
            output_dir = Path(temp_dir) / "CSV_LF_26_Sorted"
            output_dir.mkdir(parents=True)
            self.write_csv(output_dir / "Lines_Nigeria_01_Jan_26_W1_page3.csv", rows)

            with patch.object(module, "update_processing_status") as update_mock, \
                patch.object(module, "get_enhanced_image") as image_mock:
                success = module.process_single_report(report_metadata, "gemini-test", object())

        self.assertFalse(success)
        update_mock.assert_not_called()
        image_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
