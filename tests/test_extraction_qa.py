import json
import tempfile
import unittest
from pathlib import Path

from src.utils.extraction_qa import summarize_extracted_rows, write_extraction_qa


class ExtractionQATests(unittest.TestCase):
    def rows_with_anomaly(self):
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

    def test_summarize_extracted_rows_counts_states_total_and_source_anomalies(self):
        summary = summarize_extracted_rows(self.rows_with_anomaly())

        self.assertEqual(2, summary["row_count"])
        self.assertEqual(1, summary["state_count"])
        self.assertTrue(summary["has_total_row"])
        self.assertEqual("blank_confirmed_with_deaths", summary["source_anomalies"][0]["type"])

    def test_write_extraction_qa_includes_layout_qa_and_metadata(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            layout_path = temp_path / "Lines_Test.layout_qa.json"
            qa_path = temp_path / "Lines_Test.extraction_qa.json"
            layout_payload = {"status": "pass", "confidence": "high", "selected_page_index": 3}
            layout_path.write_text(json.dumps(layout_payload), encoding="utf-8")

            payload = write_extraction_qa(
                qa_path,
                enhanced_name="Lines_Test.png",
                csv_path=temp_path / "Lines_Test.csv",
                year="26",
                week="4",
                model_name="gemini-test",
                status="pass",
                accepted_attempt=2,
                max_attempts=3,
                selected_rows=self.rows_with_anomaly(),
                layout_qa_path=layout_path,
            )

            written = json.loads(qa_path.read_text(encoding="utf-8"))

        self.assertEqual(payload, written)
        self.assertEqual("pass", written["status"])
        self.assertEqual("Lines_Test.png", written["enhanced_name"])
        self.assertEqual(2, written["accepted_attempt"])
        self.assertEqual(layout_payload, written["layout_qa"])
        self.assertEqual("blank_confirmed_with_deaths", written["source_anomalies"][0]["type"])


if __name__ == "__main__":
    unittest.main()
