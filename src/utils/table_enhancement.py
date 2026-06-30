"""
Pure table enhancement helpers for Lassa fever report PDFs.

This module intentionally avoids Supabase, B2, and pipeline status side effects
so local smoke tests and production stages can share the same PDF-to-PNG logic.
"""

import logging

import cv2
import fitz
import numpy as np
from PIL import Image


DEFAULT_PARAMS = {
    "h1": 40,
    "s1": 0,
    "v1": 210,
    "h2": 50,
    "s2": 30,
    "v2": 255,
    "tr1": 1400,
    "linelength1": 79,
    "linegap1": 50,
    "toler1": 10,
    "page_number": 3,
    "dpi": 600,
}


def detect_green_rows(hsv, lower_green, upper_green, pdf_path):
    """Detect green rows in the image and return boundaries."""
    green_mask = cv2.inRange(hsv, lower_green, upper_green)
    h_proj_green = np.sum(green_mask, axis=1)
    green_row_indices = np.where(h_proj_green > 500000)[0]

    if len(green_row_indices) == 0:
        logging.warning(f"No green rows detected in {pdf_path}")
        return 800, 4500
    return green_row_indices[0], green_row_indices[-1]


def process_vertical_lines(thresh_table, tr1, linelength1, linegap1):
    """Find vertical lines using Hough transform."""
    lines = cv2.HoughLinesP(
        thresh_table,
        1,
        np.pi / 180,
        threshold=tr1,
        minLineLength=linelength1,
        maxLineGap=linegap1,
    )
    vertical_lines = []
    if lines is not None:
        for line in lines:
            x1, y1, x2, y2 = line[0]
            if abs(x2 - x1) < 5:
                vertical_lines.append((x1, y1, x2, y2))
    return vertical_lines


def process_horizontal_lines(thresh_table):
    """Find horizontal lines using Hough transform."""
    return cv2.HoughLinesP(
        thresh_table,
        1,
        np.pi / 180,
        threshold=400,
        minLineLength=50,
        maxLineGap=10,
    )


def enhance_table_lines_from_pdf_hq(
    pdf_path,
    output_path,
    tr1,
    linelength1,
    linegap1,
    toler1,
    h1,
    s1,
    v1,
    h2,
    s2,
    v2,
    page_number=3,
    dpi=600,
    year=None,
    week=None,
):
    """
    Enhance vertical column separators and horizontal table lines.

    The current implementation preserves the production crop heuristics. Dynamic
    Table 3 page detection and layout QA will be layered on top later.
    """
    doc = fitz.open(pdf_path)
    try:
        if year == "20" and week == "23":
            page_number = 4
        page = doc[page_number]

        pix = page.get_pixmap(dpi=dpi)
        img_pil = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        img = cv2.cvtColor(np.array(img_pil), cv2.COLOR_RGB2BGR)

        hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
        lower_green = np.array([h1, s1, v1], dtype=np.uint8)
        upper_green = np.array([h2, s2, v2], dtype=np.uint8)
        top_boundary, bottom_boundary = detect_green_rows(hsv, lower_green, upper_green, pdf_path)

        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        table_region = gray[top_boundary:bottom_boundary, :]
        thresh_table = cv2.adaptiveThreshold(
            table_region,
            255,
            cv2.ADAPTIVE_THRESH_MEAN_C,
            cv2.THRESH_BINARY_INV,
            11,
            3,
        )

        vertical_lines = process_vertical_lines(thresh_table, tr1, linelength1, linegap1)
        for x1, y1, x2, y2 in vertical_lines:
            cv2.line(img, (x1, top_boundary - 110), (x2, bottom_boundary + 10), (100, 100, 100), 2)

        lines_h = process_horizontal_lines(thresh_table)
        if lines_h is not None:
            for line in lines_h:
                x1, y1, x2, y2 = line[0]
                if abs(y2 - y1) < 5:
                    y1_global = y1 + top_boundary
                    y2_global = y2 + top_boundary
                    cv2.line(img, (x1, y1_global), (x2, y2_global), (100, 100, 100), 1)

        if year == "20":
            crop_bottom = min(bottom_boundary + 120, img.shape[0])
            crop_top = top_boundary - 390
        else:
            crop_bottom = min(bottom_boundary + 20, img.shape[0])
            crop_top = top_boundary - 360

        width_ratio = 0.59
        if year == "20" and week is not None:
            if int(week) >= 25:
                width_ratio = 0.56
            elif int(week) in [9, 22]:
                width_ratio = 0.60
            elif int(week) in [6]:
                width_ratio = 0.65
            elif int(week) in [7, 8]:
                width_ratio = 0.57

        new_width = int(img.shape[1] * width_ratio)
        new_width2 = int(img.shape[1] * 0.07)
        img_cropped = img[crop_top:crop_bottom, new_width2:new_width]

        output_pil = Image.fromarray(cv2.cvtColor(img_cropped, cv2.COLOR_BGR2RGB))
        output_pil.save(output_path)
        return True
    finally:
        doc.close()
