import json
import tempfile
import unittest
from pathlib import Path

from src.utils.review_needed import (
    default_review_needed_path,
    record_review_needed,
    summarize_review_needed,
)


class ReviewNeededTests(unittest.TestCase):
    def test_appends_valid_jsonl_record_and_creates_parent(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "nested" / "review_needed.jsonl"

            record = record_review_needed(
                stage="SyncProcessed",
                report_id="report-1",
                year="26",
                week="1",
                artifact_name="Lines_Test_page3.csv",
                check_type="csv_qa",
                reason="bad csv",
                action="block_processed_status",
                path=output_path,
                created_at="2026-07-01T00:00:00Z",
            )

            self.assertEqual("SyncProcessed", record["stage"])
            lines = output_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(1, len(lines))
            payload = json.loads(lines[0])
            self.assertEqual("report-1", payload["report_id"])
            self.assertEqual("bad csv", payload["reason"])

    def test_tolerates_missing_optional_metadata(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "review_needed.jsonl"

            record_review_needed(
                stage="LLM_Extraction_Supabase",
                check_type="extraction_validation",
                reason="model mismatch",
                action="skip_extraction",
                path=output_path,
                created_at="2026-07-01T00:00:00Z",
            )

            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertIsNone(payload["report_id"])
            self.assertIsNone(payload["year"])
            self.assertIsNone(payload["week"])
            self.assertIsNone(payload["artifact_name"])

    def test_summarizes_counts_by_stage_and_check_type(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "review_needed.jsonl"
            record_review_needed(
                stage="SyncProcessed",
                check_type="csv_qa",
                reason="bad csv",
                action="block_processed_status",
                path=output_path,
            )
            record_review_needed(
                stage="SyncProcessed",
                check_type="extraction_qa",
                reason="bad qa",
                action="block_processed_status",
                path=output_path,
            )
            record_review_needed(
                stage="CombinedStatus",
                check_type="csv_qa",
                reason="bad csv",
                action="block_combined_status",
                path=output_path,
            )

            summary = summarize_review_needed(output_path)

        self.assertEqual(3, summary["total"])
        self.assertEqual({"CombinedStatus": 1, "SyncProcessed": 2}, summary["by_stage"])
        self.assertEqual({"csv_qa": 2, "extraction_qa": 1}, summary["by_check_type"])

    def test_missing_file_summarizes_as_zero(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            summary = summarize_review_needed(Path(temp_dir) / "missing.jsonl")

        self.assertEqual({"total": 0, "by_stage": {}, "by_check_type": {}}, summary)

    def test_default_path_uses_processed_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            self.assertEqual(
                Path(temp_dir) / "data" / "processed" / "review_needed.jsonl",
                default_review_needed_path(temp_dir),
            )


if __name__ == "__main__":
    unittest.main()
