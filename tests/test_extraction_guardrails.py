import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.prompts.table_extraction_prompt import TABLE_EXTRACTION_PROMPT
from src.utils.data_validation import filter_comparison_columns


def load_llm_extraction_module():
    module_path = ROOT / "src" / "04b_LLM_Extraction_Supabase.py"
    spec = importlib.util.spec_from_file_location("llm_extraction_stage", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ExtractionGuardrailTests(unittest.TestCase):
    def valid_rows(self):
        return [
            {
                "States": "Ondo",
                "Suspected": "51",
                "Confirmed": "3",
                "Probable": "",
                "HCW": "",
                "Deaths": "",
            },
            {
                "States": "Total",
                "Suspected": "51",
                "Confirmed": "3",
                "Probable": "",
                "HCW": "",
                "Deaths": "",
            },
        ]

    def source_anomaly_rows(self):
        return [
            {
                "States": "Edo",
                "Suspected": "20",
                "Confirmed": "",
                "Probable": "",
                "HCW": "",
                "Deaths": "1",
            },
            {
                "States": "Total",
                "Suspected": "20",
                "Confirmed": "",
                "Probable": "",
                "HCW": "",
                "Deaths": "1",
            },
        ]

    def test_filter_comparison_columns_keeps_all_rows(self):
        rows = [
            {"States": "Ondo", "Suspected": "51", "Confirmed": "3", "Deaths": ""},
            {"States": "Total", "Suspected": "51", "Confirmed": "3", "Deaths": ""},
        ]

        filtered = filter_comparison_columns(rows)

        self.assertEqual(rows, filtered)

    def test_get_enhanced_image_downloads_to_derived_year_folder(self):
        module = load_llm_extraction_module()

        with tempfile.TemporaryDirectory() as temp_dir:
            module.ENHANCED_FOLDER = Path(temp_dir) / "PDF"
            enhanced_name = "Lines_Nigeria_24_Jan_26_W4_page3.png"
            expected_path = module.ENHANCED_FOLDER / "PDFs_Lines_26" / enhanced_name

            with patch.object(module, "download_file", return_value=True) as download_mock:
                result = module.get_enhanced_image(enhanced_name, "26")

            self.assertEqual(expected_path, result)
            self.assertTrue(expected_path.parent.exists())
            download_mock.assert_called_once_with(
                f"{module.B2_ENHANCED_PREFIX}PDFs_Lines_26/{enhanced_name}",
                str(expected_path),
            )

    def test_validate_extraction_results_passes_matching_valid_outputs(self):
        from src.utils.extraction_validation import validate_extraction_results

        result = validate_extraction_results(
            [self.valid_rows(), self.valid_rows()],
            enhanced_name="Lines_Nigeria_01_Jan_26_W1_page3.png",
            attempt=1,
            max_attempts=3,
        )

        self.assertEqual("pass", result.status)
        self.assertEqual(self.valid_rows(), result.selected_rows)

    def test_validate_extraction_results_retries_differing_valid_outputs_before_final_attempt(self):
        from src.utils.extraction_validation import validate_extraction_results

        rows_1 = self.valid_rows()
        rows_2 = self.valid_rows()
        rows_2[1] = {**rows_2[1], "Confirmed": "4"}

        result = validate_extraction_results(
            [rows_1, rows_2],
            enhanced_name="Lines_Nigeria_01_Jan_26_W1_page3.png",
            attempt=1,
            max_attempts=3,
        )

        self.assertEqual("retry", result.status)
        self.assertIsNone(result.selected_rows)

    def test_validate_extraction_results_fails_differing_valid_outputs_on_final_attempt(self):
        from src.utils.extraction_validation import validate_extraction_results

        rows_1 = self.valid_rows()
        rows_2 = self.valid_rows()
        rows_2[1] = {**rows_2[1], "Confirmed": "4"}

        result = validate_extraction_results(
            [rows_1, rows_2],
            enhanced_name="Lines_Nigeria_01_Jan_26_W1_page3.png",
            attempt=3,
            max_attempts=3,
        )

        self.assertEqual("fail", result.status)
        self.assertIsNone(result.selected_rows)

    def test_validate_extraction_results_selects_only_valid_output(self):
        from src.utils.extraction_validation import validate_extraction_results

        invalid_rows = self.valid_rows()
        invalid_rows[0] = {**invalid_rows[0], "Suspected": "1", "Confirmed": "3"}
        valid_rows = self.valid_rows()

        result = validate_extraction_results(
            [invalid_rows, valid_rows],
            enhanced_name="Lines_Nigeria_01_Jan_26_W1_page3.png",
            attempt=1,
            max_attempts=3,
        )

        self.assertEqual("pass", result.status)
        self.assertEqual(valid_rows, result.selected_rows)

    def test_validate_extraction_results_allows_matching_source_anomaly(self):
        from src.utils.extraction_validation import validate_extraction_results

        rows = self.source_anomaly_rows()

        result = validate_extraction_results(
            [rows, rows],
            enhanced_name="Lines_Nigeria_24_Jan_26_W4_page3.png",
            attempt=3,
            max_attempts=3,
        )

        self.assertEqual("pass", result.status)
        self.assertEqual(rows, result.selected_rows)
        self.assertEqual("", result.selected_rows[0]["Confirmed"])
        self.assertEqual([], result.errors)
        self.assertTrue(any("source-table anomaly" in warning for warning in result.warnings))

    def test_validate_extraction_results_retries_source_anomaly_when_other_output_imputes_confirmed(self):
        from src.utils.extraction_validation import validate_extraction_results

        blank_rows = self.source_anomaly_rows()
        imputed_rows = self.source_anomaly_rows()
        imputed_rows[0] = {**imputed_rows[0], "Confirmed": "1"}

        result = validate_extraction_results(
            [blank_rows, imputed_rows],
            enhanced_name="Lines_Nigeria_24_Jan_26_W4_page3.png",
            attempt=1,
            max_attempts=3,
        )

        self.assertEqual("retry", result.status)
        self.assertIsNone(result.selected_rows)

    def test_validate_extraction_results_rejects_nonblank_confirmed_less_than_deaths(self):
        from src.utils.extraction_validation import validate_extraction_results

        invalid_rows = self.source_anomaly_rows()
        invalid_rows[0] = {**invalid_rows[0], "Confirmed": "0"}

        result = validate_extraction_results(
            [invalid_rows, invalid_rows],
            enhanced_name="Lines_Nigeria_24_Jan_26_W4_page3.png",
            attempt=3,
            max_attempts=3,
        )

        self.assertEqual("fail", result.status)
        self.assertIsNone(result.selected_rows)

    def test_prompt_maps_report_headers_to_schema_keys(self):
        self.assertIn('report column labelled "HCW*"', TABLE_EXTRACTION_PROMPT)
        self.assertIn('JSON key "HCW"', TABLE_EXTRACTION_PROMPT)
        self.assertIn('report column labelled "Deaths (Confirmed Cases)"', TABLE_EXTRACTION_PROMPT)
        self.assertIn('JSON key "Deaths"', TABLE_EXTRACTION_PROMPT)
        self.assertIn('"States", "Suspected", "Confirmed", "Probable", "HCW", "Deaths"', TABLE_EXTRACTION_PROMPT)

    def test_process_single_report_retries_after_mismatch_and_saves_after_pass(self):
        module = load_llm_extraction_module()
        rows_1 = self.valid_rows()
        rows_2 = self.valid_rows()
        rows_2[1] = {**rows_2[1], "Confirmed": "4"}
        rows_3 = self.valid_rows()
        rows_4 = self.valid_rows()

        with tempfile.TemporaryDirectory() as temp_dir:
            module.CSV_BASE_FOLDER = Path(temp_dir)
            report_metadata = {
                "id": "00000000-0000-0000-0000-000000000000",
                "year": "26",
                "week": "1",
                "enhanced_name": "Lines_Nigeria_01_Jan_26_W1_page3.png",
            }

            with patch.object(module, "get_enhanced_image", return_value=Path(temp_dir) / "image.png"), \
                patch.object(
                    module,
                    "extract_table_with_gemini",
                    side_effect=[
                        (True, "response-1"),
                        (True, "response-2"),
                        (True, "response-3"),
                        (True, "response-4"),
                    ],
                ) as extract_mock, \
                patch.object(
                    module,
                    "parse_gemini_response",
                    side_effect=[
                        (True, rows_1),
                        (True, rows_2),
                        (True, rows_3),
                        (True, rows_4),
                    ],
                ), \
                patch.object(module, "save_extracted_data_to_csv", return_value=True) as save_mock, \
                patch.object(
                    module,
                    "validate_extracted_csv",
                    return_value=SimpleNamespace(status="pass", errors=[], warnings=[], row_count=2),
                ), \
                patch.object(module, "update_processing_status") as update_mock, \
                patch.object(module, "log_extraction_differences") as diff_mock:

                success = module.process_single_report(report_metadata, "gemini-test", object())
                qa_path = Path(temp_dir) / "CSV_LF_26_Sorted" / "Lines_Nigeria_01_Jan_26_W1_page3.extraction_qa.json"
                qa = json.loads(qa_path.read_text(encoding="utf-8"))

        self.assertTrue(success)
        self.assertEqual(4, extract_mock.call_count)
        self.assertEqual(1, save_mock.call_count)
        self.assertEqual(1, update_mock.call_count)
        self.assertEqual(1, diff_mock.call_count)
        self.assertEqual("pass", qa["status"])
        self.assertEqual(2, qa["accepted_attempt"])
        self.assertEqual("gemini-test", qa["model_name"])


if __name__ == "__main__":
    unittest.main()
