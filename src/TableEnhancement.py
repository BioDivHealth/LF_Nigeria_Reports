import cv2
import numpy as np
import os
import re

from PIL import Image, ImageColor
import fitz  # PyMuPDF

def hex_to_hsv(hex_color):
    rgb = ImageColor.getcolor(hex_color, "RGB")
    r, g, b = [x / 255.0 for x in rgb]
    hsv = cv2.cvtColor(
        np.uint8([[[b * 255, g * 255, r * 255]]]),
        cv2.COLOR_BGR2HSV
    )[0][0]
    return hsv

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
    page_number=0, 
    dpi=600
):
    """
    Enhances vertical column separators and draws horizontal lines at
    top boundary, bottom boundary, and header bottom. Uses morphological
    filtering to remove small vertical text edges.

    Args:
        pdf_path (str): Path to the PDF.
        output_path (str): Path to save the image (e.g. .png).
        tr1 (int): HoughLinesP threshold.
        linelength1 (int): HoughLinesP minLineLength.
        linegap1 (int): HoughLinesP maxLineGap.
        toler1 (int): Tolerance around the HSV hue for detecting green rows.
        page_number (int): Which PDF page to process (0-indexed).
        dpi (int): Rendering DPI for the PDF page.
        h1 (int): Lower hue value for green detection.
        s1 (int): Lower saturation value for green detection.
        v1 (int): Lower value for green detection.
        h2 (int): Upper hue value for green detection.
        s2 (int): Upper saturation value for green detection.
        v2 (int): Upper value for green detection.
    """
    doc = fitz.open(pdf_path)
    page = doc[page_number]

    # 1. Render the PDF page at high DPI --------------------------------------
    pix = page.get_pixmap(dpi=dpi)
    img_pil = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    # Convert PIL Image to OpenCV BGR
    img = cv2.cvtColor(np.array(img_pil), cv2.COLOR_RGB2BGR)

    height1, width1 = img.shape[:2]
    total_pixels = height1 * width1
    # print("Total pixels =", total_pixels)
    # print("Width =", width1)
    
    # 2. Convert to HSV & detect green rows -----------------------------------
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    target_hsv = np.array([102, 12.66, 92.94], dtype=np.uint8)
    tolerance = toler1
    lower_green = np.array([h1,  s1,  v1], dtype=np.uint8) 
    upper_green = np.array([h2, s2, v2], dtype=np.uint8) 
    green_mask = cv2.inRange(hsv, lower_green, upper_green)
    #green_mask_pil = Image.fromarray(green_mask)
    #green_mask_pil.save("debug_green_mask.png")
    
    # Create an overlay image
    overlay = img.copy()
    # Paint those masked pixels bright green in overlay
    overlay[green_mask > 0] = [0, 255, 0]
    # Blend overlay with original (alpha blending)
    alpha = 0.35
    overlayed_img = cv2.addWeighted(overlay, alpha, img, 1 - alpha, 0)
    overlayed_pil = Image.fromarray(cv2.cvtColor(overlayed_img, cv2.COLOR_BGR2RGB))
    # Save the overlayed image for debugging
    # overlayed_pil.save("debug_green_overlay.png")
    # print("Saved overlay image as debug_green_overlay.png")
    

    h_proj_green = np.sum(green_mask, axis=1)
    green_row_indices = np.where(h_proj_green > 500000)[0]  # Original value is 1000 
    

    if len(green_row_indices) == 0:
        print("No green rows detected.")
        print(pdf_path)
        top_boundary = 800
        bottom_boundary = 4500
    else:
        top_boundary = green_row_indices[0]
        bottom_boundary = green_row_indices[-1] 
    
    # 3. Adaptive Thresholding in the table region ----------------------------
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    table_region = gray[top_boundary:bottom_boundary, :]
    thresh_table = cv2.adaptiveThreshold(
        table_region,
        255,
        cv2.ADAPTIVE_THRESH_MEAN_C,
        cv2.THRESH_BINARY_INV,
        11,
        3
    )

    # 4. Hough Lines to find vertical lines (using the filtered 'cleaned' image)
    lines = cv2.HoughLinesP(
        thresh_table,
        1,
        np.pi / 180,
        threshold=tr1,
        minLineLength=linelength1,
        maxLineGap=linegap1
    )
    vertical_lines = []
    if lines is not None:
        for line in lines:
            x1, y1, x2, y2 = line[0]
            if abs(x2 - x1) < 5:
                vertical_lines.append((x1, top_boundary, x2, top_boundary))

    # 5. Draw vertical lines on the image -------------------------------------
    for x1, y1, x2, y2 in vertical_lines:
        cv2.line(img, (x1, top_boundary-180), (x2, bottom_boundary+95), (100, 100, 100), 2)

    # 6. Also detect horizontal lines in the table_region
    lines_h = cv2.HoughLinesP(
        thresh_table,
        1,                     # rho
        np.pi / 180,           # theta
        threshold=400,         # Hough threshold (tune as needed)
        minLineLength=50,      # minimum length of line (tune as needed)
        maxLineGap=10          # maximum allowed gap (tune as needed)
    )
    if lines_h is not None:
        for line in lines_h:
            x1, y1, x2, y2 = line[0]
            # Check if line is near horizontal
            if abs(y2 - y1) < 5:
                # Shift y-coords into the absolute image coordinate system
                # (you already did something similar with top_boundary for vertical lines)
                y1_global = y1 + top_boundary
                y2_global = y2 + top_boundary

                # Draw the horizontal line on the main image
                cv2.line(img, (x1, y1_global), (x2, y2_global), (100, 100, 100), 1)
    
    # Top boundary (green)
    #height, width = img.shape[:2]
    #cv2.line(img, (0, top_boundary), (width, top_boundary), (0, 255, 0), 2)

    # Bottom boundary (green)
    #cv2.line(img, (0, bottom_boundary), (width, bottom_boundary), (0, 255, 0), 2)

    # Crop the image so that it ends at bottom_boundary + 40
    crop_bottom = bottom_boundary + 130
    crop_bottom = min(crop_bottom, img.shape[0])
    crop_top = top_boundary - 360
    new_width = int(img.shape[1] * 0.59)  # keep left 58% of the image
    new_width2 = int(img.shape[1] * 0.07)  # keep right 5% of the image
    img_cropped = img[crop_top:crop_bottom, new_width2:new_width]
    
    # 7. Convert back to PIL and save
    output_pil = Image.fromarray(cv2.cvtColor(img_cropped, cv2.COLOR_BGR2RGB))
    output_pil.save(output_path)

    #print(f"Saved enhanced table to: {output_path}")

all_pdfs = [f for f in os.listdir("2021/PDFs_2021") if f.endswith(".pdf")]
pdfs_2021 = [f for f in all_pdfs if "_21_W" in f]

sorted_pdfs = sorted(pdfs_2021, key=lambda x: int(re.search(r'_W(\d+)\.pdf$', x).group(1)))

# Limit to the top 3 sorted PDFs if needed
sorted_pdfs = sorted_pdfs[38:40]

for pdf in sorted_pdfs:
    input_pdf = os.path.join("2021/PDFs_2021", pdf)
    output_img = os.path.join("2021/PDFs_Lines_2021", f"Lines_{pdf.replace('.pdf','')}_page3.png")
    enhance_table_lines_from_pdf_hq(input_pdf,
                                    output_img,
                                    h1=40, s1=0, v1=210,
                                    h2=50, s2=30, v2=255,
                                    tr1=1400,
                                    linelength1=79,
                                    linegap1=50,
                                    toler1 = 10,
                                    page_number=3,
                                    dpi=600)