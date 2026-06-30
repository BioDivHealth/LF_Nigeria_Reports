import json
import tempfile
import unittest
from pathlib import Path

from src.utils.status_qa import (
    check_extraction_qa_file,
    check_extraction_qa_payload,
    check_layout_qa_file,
    check_layout_qa_payload,
)


class StatusQATests(unittest.TestCase):
    def test_layout_pass_and_medium_warning_are_accepted(self):
        self.assertTrue(
            check_layout_qa_payload(
                {"status": "pass", "confidence": "high", "selected_page_index": 3}
            ).ok
        )
        self.assertTrue(
            check_layout_qa_payload(
                {"status": "warning", "confidence": "medium", "selected_page_index": 3}
            ).ok
        )

    def test_layout_low_none_or_fail_are_rejected(self):
        self.assertFalse(
            check_layout_qa_payload(
                {"status": "fail", "confidence": "medium", "selected_page_index": 3}
            ).ok
        )
        self.assertFalse(
            check_layout_qa_payload(
                {"status": "pass", "confidence": "low", "selected_page_index": 3}
            ).ok
        )
        self.assertFalse(
            check_layout_qa_payload(
                {"status": "pass", "confidence": "high", "selected_page_index": None}
            ).ok
        )

    def test_extraction_qa_with_source_anomalies_is_accepted(self):
        result = check_extraction_qa_payload(
            {
                "status": "pass",
                "validation": {"status": "pass", "warnings": ["source anomaly"]},
                "csv_qa": {"status": "pass", "warnings": []},
                "source_anomalies": [
                    {"type": "blank_confirmed_with_deaths", "state": "Edo"}
                ],
            }
        )

        self.assertTrue(result.ok)

    def test_extraction_qa_rejects_failed_nested_statuses(self):
        self.assertFalse(
            check_extraction_qa_payload(
                {"status": "pass", "validation": {"status": "fail"}}
            ).ok
        )
        self.assertFalse(
            check_extraction_qa_payload(
                {"status": "pass", "csv_qa": {"status": "fail"}}
            ).ok
        )

    def test_file_checks_distinguish_missing_from_failed_sidecars(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            missing = check_layout_qa_file(temp_path / "missing.layout_qa.json")
            bad_path = temp_path / "bad.layout_qa.json"
            bad_path.write_text(json.dumps({"status": "fail"}), encoding="utf-8")
            bad = check_layout_qa_file(bad_path)
            good_path = temp_path / "good.extraction_qa.json"
            good_path.write_text(json.dumps({"status": "pass"}), encoding="utf-8")
            good = check_extraction_qa_file(good_path)

        self.assertFalse(missing.ok)
        self.assertFalse(missing.present)
        self.assertFalse(bad.ok)
        self.assertTrue(bad.present)
        self.assertTrue(good.ok)


if __name__ == "__main__":
    unittest.main()
