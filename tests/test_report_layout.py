import unittest
from unittest.mock import patch

from src.utils.report_layout import find_table3_page


class FakePage:
    def __init__(self, text):
        self.text = text

    def get_text(self, mode):
        self.mode = mode
        return self.text


class FakeDoc:
    def __init__(self, pages):
        self.pages = [FakePage(text) for text in pages]
        self.closed = False

    def __iter__(self):
        return iter(self.pages)

    def __len__(self):
        return len(self.pages)

    def close(self):
        self.closed = True


class ReportLayoutTests(unittest.TestCase):
    def open_with_pages(self, pages):
        return patch("src.utils.report_layout.fitz.open", return_value=FakeDoc(pages))

    def test_find_table3_page_selects_visual_page_four_from_all_title_cues(self):
        pages = [
            "Highlights mention Table 3 in passing.",
            "Some other page",
            "Another page",
            "Table 3. Weekly and Cumulative number of suspected and confirmed cases for 2025",
        ]

        with self.open_with_pages(pages):
            result = find_table3_page("fake.pdf")

        self.assertEqual("pass", result.status)
        self.assertEqual("high", result.confidence)
        self.assertEqual(3, result.selected_page_index)
        self.assertEqual(4, result.selected_page_number)
        self.assertIn("Table 3", result.text_cues)

    def test_find_table3_page_does_not_select_page_one_table3_narrative_only(self):
        pages = [
            "Highlights mention Table 3 in passing.",
            "Some other page",
            "Another page",
            "Table 3. Weekly and Cumulative number of suspected and confirmed cases for 2025",
        ]

        with self.open_with_pages(pages):
            result = find_table3_page("fake.pdf")

        self.assertNotEqual(0, result.selected_page_index)
        self.assertEqual(3, result.selected_page_index)

    def test_find_table3_page_uses_legacy_2020_week23_fallback_as_failed_diagnostic(self):
        pages = ["No relevant title cues"] * 5

        with self.open_with_pages(pages):
            result = find_table3_page("fake.pdf", year="20", week="23")

        self.assertEqual("fail", result.status)
        self.assertEqual("low", result.confidence)
        self.assertEqual(4, result.selected_page_index)
        self.assertEqual(5, result.selected_page_number)
        self.assertTrue(any("legacy fallback" in warning for warning in result.warnings))

    def test_find_table3_page_fails_when_no_candidate_and_fallback_out_of_range(self):
        pages = ["No relevant title cues"] * 2

        with self.open_with_pages(pages):
            result = find_table3_page("fake.pdf", default_page_index=3)

        self.assertEqual("fail", result.status)
        self.assertEqual("none", result.confidence)
        self.assertIsNone(result.selected_page_index)
        self.assertIsNone(result.selected_page_number)
        self.assertTrue(any("outside PDF page range" in warning for warning in result.warnings))


if __name__ == "__main__":
    unittest.main()
