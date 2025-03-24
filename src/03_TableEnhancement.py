import cv2
import numpy as np
import os
import re
import csv
from pathlib import Path
from PIL import Image, ImageColor
import fitz  # PyMuPDF
import logging

# Configure logging
class NewlineLoggingHandler(logging.StreamHandler):
    """Custom logging handler that adds a newline after each log entry."""
    def emit(self, record):
        super().emit(record)
        self.stream.write('\n')
        self.flush()

logging.basicConfig(
    level=logging.INFO, 
    format='%(levelname)s: %(message)s', 
    handlers=[NewlineLoggingHandler()]
)

# Define base paths and constants
BASE_DIR = Path(__file__).parent.parent
CSV_FILE = BASE_DIR / 'data' / 'documentation' / 'website_raw_data.csv'

def hex_to_hsv(hex_color):
    rgb = ImageColor.getcolor(hex_color, "RGB")
    r, g, b = [x / 255.0 for x in rgb]
    hsv = cv2.cvtColor(
        np.uint8([[[b * 255, g * 255, r * 255]]]),
        cv2.COLOR_BGR2HSV
    )[0][0]
    return hsv

def enhance_table_lines_from_pdf_hq(
    pdf_path, output_path,
    tr1, linelength1, linegap1, toler1,
    h1, s1, v1, h2, s2, v2, 
    page_number=3, 
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
    
    # 2. Convert to HSV & detect green rows -----------------------------------
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    target_hsv = np.array([102, 12.66, 92.94], dtype=np.uint8)
    tolerance = toler1
    lower_green = np.array([h1,  s1,  v1], dtype=np.uint8) 
    upper_green = np.array([h2, s2, v2], dtype=np.uint8) 
    green_mask = cv2.inRange(hsv, lower_green, upper_green)
    

    h_proj_green = np.sum(green_mask, axis=1)
    green_row_indices = np.where(h_proj_green > 500000)[0]  # Original value is 1000 
    

    if len(green_row_indices) == 0:
        logging.warning(f"No green rows detected in {pdf_path}")
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
        cv2.line(img, (x1, top_boundary-110), (x2, bottom_boundary+10), (100, 100, 100), 2)

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
 
    # Crop the image so that it ends at bottom_boundary + 40
    crop_bottom = bottom_boundary + 20 #was +130, lets chanhe to just + 20?
    crop_bottom = min(crop_bottom, img.shape[0])
    crop_top = top_boundary - 360
    new_width = int(img.shape[1] * 0.59)  # keep left 58% of the image
    new_width2 = int(img.shape[1] * 0.07)  # keep right 5% of the image
    img_cropped = img[crop_top:crop_bottom, new_width2:new_width]
    
    # 7. Convert back to PIL and save
    output_pil = Image.fromarray(cv2.cvtColor(img_cropped, cv2.COLOR_BGR2RGB))
    output_pil.save(output_path)

def process_reports_from_csv():
    """
    Process Lassa fever reports based on metadata in the CSV file.
    
    Reads website_raw_data.csv, processes PDFs that have been downloaded but not yet enhanced,
    and updates the 'Enhanced' status in the CSV. Processes files from years 2021-2025.
    
    Returns:
        None: Updates are written to the CSV file and enhanced images are saved to appropriate folders
    """
    # Read CSV and get rows
    try:
        with open(CSV_FILE, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            fieldnames = reader.fieldnames
            rows = list(reader)
    except Exception as e:
        logging.error(f"Error reading {CSV_FILE}: {e}")
        return
    
    # Ensure Enhanced column exists
    if 'Enhanced' not in fieldnames:
        fieldnames.append('Enhanced')
        for row in rows:
            if 'Enhanced' not in row:
                row['Enhanced'] = ''
    
    # Ensure Enhanced_name column exists
    if 'Enhanced_name' not in fieldnames:
        fieldnames.append('Enhanced_name')
        for row in rows:
            if 'Enhanced_name' not in row:
                row['Enhanced_name'] = ''
    
    # Keep track of modified rows
    modified = False
    
    # Process only rows with Downloaded='Y' and Enhanced!='Y'
    for row in rows:
        year = row.get('year', '').strip()
        week = row.get('week', '').strip()
        downloaded = row.get('Downloaded', '').strip()
        enhanced = row.get('Enhanced', '').strip()
        new_name = row.get('new_name', '').strip()
        compatible = row.get('Compatible', '').strip()
        
        # Skip if not downloaded or already enhanced or not compatible or not in our year range
        if downloaded != 'Y' or enhanced == 'Y' or compatible == 'N' or not year or not week:
            continue
        
        # Only process years 2021-2025
        if year not in ['21', '22', '23', '24', '25']:
            continue
        
        # Full path to PDF file
        pdf_path = BASE_DIR / 'data' / 'raw' / 'year' / year / new_name
        
        # Skip if file doesn't exist
        if not pdf_path.exists():
            logging.warning(f"PDF file not found: {pdf_path}")
            continue
        
        # Create output directory if it doesn't exist
        full_year = f"20{year}"
        output_dir = BASE_DIR / 'data' / 'processed' / f"PDFs_Lines_{full_year}"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Enhanced image filename
        enhanced_name = f"Lines_{new_name.replace('.pdf', '')}_page3.png"
        output_path = output_dir / enhanced_name
        
        # Skip if already enhanced (file exists)
        if output_path.exists():
            row['Enhanced'] = 'Y'
            row['Enhanced_name'] = enhanced_name
            modified = True
            logging.info(f"Found existing enhanced file: {enhanced_name}")
            continue
        
        try:
            logging.info(f"Enhancing {new_name} (Year: {year}, Week: {week})")
            
            # Parameters for enhance_table_lines_from_pdf_hq vary slightly by year
            params = {
                # Default parameters
                'h1': 40, 's1': 0, 'v1': 210,
                'h2': 50, 's2': 30, 'v2': 255,
                'tr1': 1400,
                'linelength1': 79,
                'linegap1': 50,
                'toler1': 10,
                'page_number': 3,  # This is usually page 3 for most reports
                'dpi': 600
            }
            
            # Call the enhancement function
            enhance_table_lines_from_pdf_hq(
                str(pdf_path),
                str(output_path),
                **params
            )
            
            # Update row if enhancement succeeded
            if output_path.exists():
                row['Enhanced'] = 'Y'
                row['Enhanced_name'] = enhanced_name
                modified = True
                logging.info(f"Successfully enhanced: {enhanced_name}")
            else:
                logging.error(f"Failed to enhance {new_name}")
        
        except Exception as e:
            logging.error(f"Error enhancing {new_name}: {e}")
    
    # Write updated rows back to CSV if any changes were made
    if modified:
        try:
            with open(CSV_FILE, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            logging.info(f"Updated website_raw_data.csv with enhancement status")
        except Exception as e:
            logging.error(f"Error writing to {CSV_FILE}: {e}")

def main():
    """
    Main function to process and enhance Lassa fever report tables.
    Reads the website_raw_data.csv file, processes PDFs that have been downloaded
    but not yet enhanced, and updates the CSV with the enhancement status.
    """
    logging.info("Starting Lassa fever report table enhancement process")
    process_reports_from_csv()
    logging.info("Finished processing reports")

if __name__ == "__main__":
    main()