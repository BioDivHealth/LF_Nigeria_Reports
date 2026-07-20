import unittest
from unittest.mock import patch

import numpy as np

from src.utils.table_enhancement import (
    normalize_hough_lines,
    process_horizontal_lines,
    process_vertical_lines,
)


class TableEnhancementTests(unittest.TestCase):
    def test_normalizes_nested_hough_lines(self):
        lines = np.array([[[1, 2, 3, 4]], [[5, 6, 7, 8]]], dtype=np.int32)

        normalized = normalize_hough_lines(lines)

        np.testing.assert_array_equal(
            normalized,
            np.array([[1, 2, 3, 4], [5, 6, 7, 8]], dtype=np.int32),
        )

    def test_normalizes_flat_hough_lines(self):
        lines = np.array([[1, 2, 3, 4], [5, 6, 7, 8]], dtype=np.int32)

        normalized = normalize_hough_lines(lines)

        np.testing.assert_array_equal(normalized, lines)

    def test_normalizes_missing_and_empty_hough_lines(self):
        self.assertEqual([], normalize_hough_lines(None))
        self.assertEqual([], normalize_hough_lines(np.array([], dtype=np.int32)))

    def test_rejects_malformed_hough_lines(self):
        with self.assertRaisesRegex(ValueError, "Malformed Hough line result with shape"):
            normalize_hough_lines(np.array([1, 2, 3], dtype=np.int32))

    def test_vertical_processing_accepts_flat_hough_output(self):
        lines = np.array([[10, 0, 10, 50], [1, 2, 20, 2]], dtype=np.int32)
        with patch("src.utils.table_enhancement.cv2.HoughLinesP", return_value=lines):
            vertical = process_vertical_lines(np.zeros((2, 2), dtype=np.uint8), 1, 1, 1)

        self.assertEqual([(10, 0, 10, 50)], [tuple(int(value) for value in line) for line in vertical])

    def test_horizontal_processing_returns_normalized_rows(self):
        lines = np.array([[[0, 10, 50, 10]]], dtype=np.int32)
        with patch("src.utils.table_enhancement.cv2.HoughLinesP", return_value=lines):
            horizontal = process_horizontal_lines(np.zeros((2, 2), dtype=np.uint8))

        np.testing.assert_array_equal(horizontal, np.array([[0, 10, 50, 10]], dtype=np.int32))


if __name__ == "__main__":
    unittest.main()
