import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
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

    def test_filter_comparison_columns_keeps_all_rows(self):
        rows = [
            {"States": "Ondo", "Suspected": "51", "Confirmed": "3", "Deaths": ""},
            {"States": "Total", "Suspected": "51", "Confirmed": "3", "Deaths": ""},
        ]

        filtered = filter_comparison_columns(rows)

        self.assertEqual(rows, filtered)

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
                patch.object(module, "update_processing_status") as update_mock, \
                patch.object(module, "log_extraction_differences") as diff_mock:

                success = module.process_single_report(report_metadata, "gemini-test", object())

        self.assertTrue(success)
        self.assertEqual(4, extract_mock.call_count)
        self.assertEqual(1, save_mock.call_count)
        self.assertEqual(1, update_mock.call_count)
        self.assertEqual(1, diff_mock.call_count)


if __name__ == "__main__":
    unittest.main()
