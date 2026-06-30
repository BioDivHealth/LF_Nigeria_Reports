import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

from PIL import Image


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def load_smoke_module():
    module_path = ROOT / "scripts" / "local_extraction_smoke.py"
    spec = importlib.util.spec_from_file_location("local_extraction_smoke", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class LocalExtractionSmokeTests(unittest.TestCase):
    def setUp(self):
        self.smoke = load_smoke_module()

    def read_summary(self, output_dir):
        with (Path(output_dir) / "smoke_summary.json").open(encoding="utf-8") as infile:
            return json.load(infile)

    def write_csv(self, path, confirmed="3"):
        rows = [
            {
                "Week": "18",
                "Year": "2025",
                "States": "Ondo",
                "Suspected": "51",
                "Confirmed": confirmed,
                "Probable": "",
                "HCW": "",
                "Deaths": "0",
            },
            {
                "Week": "18",
                "Year": "2025",
                "States": "Total",
                "Suspected": "51",
                "Confirmed": confirmed,
                "Probable": "",
                "HCW": "",
                "Deaths": "0",
            },
        ]
        with Path(path).open("w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.smoke.REQUIRED_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)

    def write_png(self, path):
        Image.new("RGB", (8, 6), color="white").save(path)

    def layout_result(self, status="pass", confidence="high", selected_page_index=3):
        selected_page_number = selected_page_index + 1 if selected_page_index is not None else None
        payload = {
            "status": status,
            "selected_page_index": selected_page_index,
            "selected_page_number": selected_page_number,
            "confidence": confidence,
            "score": 3 if confidence == "high" else 0,
            "reasons": ["layout reason"],
            "warnings": ["layout warning"] if status == "fail" else [],
            "text_cues": ["Table 3", "Weekly and Cumulative", "suspected and confirmed cases"]
            if confidence == "high"
            else [],
            "page_count": 4,
            "candidates": [],
        }
        return SimpleNamespace(**payload, to_dict=lambda: payload)

    def test_infer_report_metadata_from_pdf_and_enhanced_names(self):
        self.assertEqual(
            {"year": "25", "week": "18"},
            self.smoke.infer_report_metadata("Nigeria_03_May_25_W18.pdf"),
        )
        self.assertEqual(
            {"year": "26", "week": "4"},
            self.smoke.infer_report_metadata("Lines_Nigeria_24_Jan_26_W4_page3.png"),
        )
        self.assertEqual({"year": None, "week": None}, self.smoke.infer_report_metadata("unknown.pdf"))

    def test_csv_command_writes_passing_summary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            csv_path = temp_path / "Lines_Nigeria_03_May_25_W18_page3.csv"
            output_dir = temp_path / "out"
            self.write_csv(csv_path)

            exit_code = self.smoke.main(["csv", "--csv", str(csv_path), "--output-dir", str(output_dir)])

            summary = self.read_summary(output_dir)

        self.assertEqual(0, exit_code)
        self.assertEqual("pass", summary["status"])
        self.assertEqual("25", summary["year"])
        self.assertEqual("18", summary["week"])
        self.assertEqual("pass", summary["csv_qa"]["status"])

    def test_csv_command_writes_failing_summary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            csv_path = temp_path / "Lines_Nigeria_03_May_25_W18_page3.csv"
            output_dir = temp_path / "out"
            self.write_csv(csv_path, confirmed="bad")

            exit_code = self.smoke.main(["csv", "--csv", str(csv_path), "--output-dir", str(output_dir)])

            summary = self.read_summary(output_dir)

        self.assertEqual(1, exit_code)
        self.assertEqual("fail", summary["status"])
        self.assertTrue(any("Confirmed value 'bad'" in error for error in summary["errors"]))

    def test_missing_input_writes_summary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "out"
            missing_path = Path(temp_dir) / "missing.csv"

            exit_code = self.smoke.main(["csv", "--csv", str(missing_path), "--output-dir", str(output_dir)])

            summary = self.read_summary(output_dir)

        self.assertEqual(1, exit_code)
        self.assertEqual("fail", summary["status"])
        self.assertTrue(any("Input CSV does not exist" in error for error in summary["errors"]))

    def test_image_gemini_mode_fails_early_without_api_key(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            image_path = temp_path / "Lines_Nigeria_03_May_25_W18_page3.png"
            output_dir = temp_path / "out"
            self.write_png(image_path)

            with patch.dict("os.environ", {"GOOGLE_API_KEY": ""}, clear=False):
                exit_code = self.smoke.main(
                    [
                        "image",
                        "--image",
                        str(image_path),
                        "--output-dir",
                        str(output_dir),
                        "--use-gemini",
                    ]
                )

            summary = self.read_summary(output_dir)

        self.assertEqual(1, exit_code)
        self.assertEqual("fail", summary["status"])
        self.assertIn("GOOGLE_API_KEY", summary["errors"][0])

    def test_pdf_command_uses_scratch_output_and_mocked_enhancement(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pdf_path = temp_path / "Nigeria_03_May_25_W18.pdf"
            output_dir = temp_path / "out"
            pdf_path.write_bytes(b"%PDF-1.4\n")

            with patch.object(self.smoke, "find_table3_page", return_value=self.layout_result()) as layout_mock, \
                patch.object(self.smoke, "enhance_table_lines_from_pdf_hq", return_value=True) as enhance_mock, \
                patch.object(self.smoke, "inspect_image", return_value={"width": 10, "height": 20, "mode": "RGB"}):
                exit_code = self.smoke.main(["pdf", "--pdf", str(pdf_path), "--output-dir", str(output_dir)])

            summary = self.read_summary(output_dir)
            with (output_dir / "layout_qa.json").open(encoding="utf-8") as infile:
                layout_qa = json.load(infile)

        self.assertEqual(0, exit_code)
        self.assertEqual("pass", summary["status"])
        self.assertEqual("high", summary["layout_qa"]["confidence"])
        self.assertEqual("high", layout_qa["confidence"])
        self.assertTrue(summary["enhanced_path"].endswith("enhanced/Lines_Nigeria_03_May_25_W18_page3.png"))
        layout_mock.assert_called_once()
        enhance_mock.assert_called_once()
        self.assertEqual("25", enhance_mock.call_args.kwargs["year"])
        self.assertEqual("18", enhance_mock.call_args.kwargs["week"])
        self.assertEqual(3, enhance_mock.call_args.kwargs["page_number"])

    def test_pdf_command_fails_on_low_confidence_layout_and_writes_layout_qa(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            pdf_path = temp_path / "Nigeria_03_May_25_W18.pdf"
            output_dir = temp_path / "out"
            pdf_path.write_bytes(b"%PDF-1.4\n")

            low_layout = self.layout_result(status="fail", confidence="low", selected_page_index=3)
            with patch.object(self.smoke, "find_table3_page", return_value=low_layout), \
                patch.object(self.smoke, "enhance_table_lines_from_pdf_hq") as enhance_mock:
                exit_code = self.smoke.main(["pdf", "--pdf", str(pdf_path), "--output-dir", str(output_dir)])

            summary = self.read_summary(output_dir)
            with (output_dir / "layout_qa.json").open(encoding="utf-8") as infile:
                layout_qa = json.load(infile)

        self.assertEqual(1, exit_code)
        self.assertEqual("fail", summary["status"])
        self.assertEqual("low", layout_qa["confidence"])
        self.assertTrue(any("layout reason" in error for error in summary["errors"]))
        enhance_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
