import unittest
from pathlib import Path

from src.utils.artifact_paths import (
    csv_name_for_enhanced,
    csv_name_for_report,
    csv_path,
    enhanced_image_path,
    enhanced_name_for_report,
    extraction_qa_name_for_csv,
    extraction_qa_path_for_csv_path,
    layout_qa_name_for_enhanced,
    layout_qa_path_for_enhanced_path,
    legacy_enhanced_name_from_pdf,
)


class ArtifactPathTests(unittest.TestCase):
    def test_legacy_enhanced_name_from_pdf(self):
        self.assertEqual(
            "Lines_Nigeria_24_Jan_26_W4_page3.png",
            legacy_enhanced_name_from_pdf("Nigeria_24_Jan_26_W4.pdf"),
        )

    def test_legacy_csv_name_when_enhanced_name_missing(self):
        self.assertEqual(
            "Lines_Nigeria_24_Jan_26_W4_page3.csv",
            csv_name_for_report("Nigeria_24_Jan_26_W4.pdf"),
        )

    def test_explicit_enhanced_name_wins(self):
        self.assertEqual(
            "Lines_Nigeria_24_Jan_26_W4_page5.png",
            enhanced_name_for_report(
                "Nigeria_24_Jan_26_W4.pdf",
                enhanced_name="Lines_Nigeria_24_Jan_26_W4_page5.png",
            ),
        )

    def test_csv_name_derives_from_enhanced_stem(self):
        self.assertEqual(
            "Lines_Nigeria_24_Jan_26_W4_page5.csv",
            csv_name_for_enhanced("Lines_Nigeria_24_Jan_26_W4_page5.png"),
        )
        self.assertEqual(
            "Lines_Nigeria_24_Jan_26_W4_page5.csv",
            csv_name_for_report(
                "Nigeria_24_Jan_26_W4.pdf",
                enhanced_name="Lines_Nigeria_24_Jan_26_W4_page5.png",
            ),
        )

    def test_artifact_paths_include_year_folders(self):
        base_dir = Path("/tmp/lassa")
        self.assertEqual(
            base_dir / "PDFs_Lines_26" / "Lines_Nigeria_24_Jan_26_W4_page3.png",
            enhanced_image_path(
                base_dir,
                "26",
                "Lines_Nigeria_24_Jan_26_W4_page3.png",
            ),
        )
        self.assertEqual(
            base_dir / "CSV_LF_26_Sorted" / "Lines_Nigeria_24_Jan_26_W4_page3.csv",
            csv_path(
                base_dir,
                "26",
                "Lines_Nigeria_24_Jan_26_W4_page3.csv",
            ),
        )

    def test_sidecar_paths_are_adjacent_to_artifacts(self):
        enhanced_path = Path("/tmp/enhanced/Lines_Test_page3.png")
        csv_file_path = Path("/tmp/csv/Lines_Test_page3.csv")

        self.assertEqual(
            Path("/tmp/enhanced/Lines_Test_page3.layout_qa.json"),
            layout_qa_path_for_enhanced_path(enhanced_path),
        )
        self.assertEqual(
            Path("/tmp/csv/Lines_Test_page3.extraction_qa.json"),
            extraction_qa_path_for_csv_path(csv_file_path),
        )

    def test_sidecar_names_derive_from_artifact_stems(self):
        self.assertEqual(
            "Lines_Test_page3.layout_qa.json",
            layout_qa_name_for_enhanced("Lines_Test_page3.png"),
        )
        self.assertEqual(
            "Lines_Test_page3.extraction_qa.json",
            extraction_qa_name_for_csv("Lines_Test_page3.csv"),
        )

    def test_blank_or_missing_names_return_none(self):
        self.assertIsNone(legacy_enhanced_name_from_pdf(None))
        self.assertIsNone(legacy_enhanced_name_from_pdf(""))
        self.assertIsNone(enhanced_name_for_report(None))
        self.assertIsNone(csv_name_for_enhanced(""))
        self.assertIsNone(csv_name_for_report(None))
        self.assertIsNone(layout_qa_name_for_enhanced(""))
        self.assertIsNone(extraction_qa_name_for_csv(""))
        self.assertIsNone(enhanced_image_path("/tmp/lassa", "26", ""))
        self.assertIsNone(enhanced_image_path(None, "26", "Lines_Test_page3.png"))
        self.assertIsNone(csv_path("/tmp/lassa", "26", ""))
        self.assertIsNone(csv_path(None, "26", "Lines_Test_page3.csv"))
        self.assertIsNone(layout_qa_path_for_enhanced_path(None))
        self.assertIsNone(extraction_qa_path_for_csv_path(None))


if __name__ == "__main__":
    unittest.main()
