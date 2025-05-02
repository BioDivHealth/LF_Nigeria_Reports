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

# Default enhancement parameters
DEFAULT_PARAMS = {
    'h1': 40, 's1': 0, 'v1': 210,
    'h2': 50, 's2': 30, 'v2': 255,
    'tr1': 1400,
    'linelength1': 79,
    'linegap1': 50,
    'toler1': 10,
    'page_number': 3,
    'dpi': 600
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
        thresh_table, 1, np.pi/180,
        threshold=tr1, minLineLength=linelength1, maxLineGap=linegap1
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
        thresh_table, 1, np.pi/180,
        threshold=400, minLineLength=50, maxLineGap=10
    )

def enhance_table_lines_from_pdf_hq(
    pdf_path, output_path,
    tr1, linelength1, linegap1, toler1,
    h1, s1, v1, h2, s2, v2, 
    page_number=3, 
    dpi=600, year=None, week=None
):
    """
    Enhances vertical column separators and draws horizontal lines at
    top boundary, bottom boundary, and header bottom.
    """
    doc = fitz.open(pdf_path)
    if year == '20' and week =='23':
        page_number = 4
    page = doc[page_number]

    # 1. Render PDF page at high DPI
    pix = page.get_pixmap(dpi=dpi)
    img_pil = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    img = cv2.cvtColor(np.array(img_pil), cv2.COLOR_RGB2BGR)
    
    # 2. Convert to HSV & detect green rows
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    lower_green = np.array([h1, s1, v1], dtype=np.uint8)
    upper_green = np.array([h2, s2, v2], dtype=np.uint8)
    top_boundary, bottom_boundary = detect_green_rows(hsv, lower_green, upper_green, pdf_path)
    
    # 3. Adaptive Thresholding in table region
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    table_region = gray[top_boundary:bottom_boundary, :]
    thresh_table = cv2.adaptiveThreshold(
        table_region, 255, cv2.ADAPTIVE_THRESH_MEAN_C,
        cv2.THRESH_BINARY_INV, 11, 3
    )

    # 4. Process vertical and horizontal lines
    vertical_lines = process_vertical_lines(thresh_table, tr1, linelength1, linegap1)
    for x1, y1, x2, y2 in vertical_lines:
        cv2.line(img, (x1, top_boundary-110), (x2, bottom_boundary+10), (100, 100, 100), 2)

    lines_h = process_horizontal_lines(thresh_table)
    if lines_h is not None:
        for line in lines_h:
            x1, y1, x2, y2 = line[0]
            if abs(y2 - y1) < 5:
                y1_global = y1 + top_boundary
                y2_global = y2 + top_boundary
                cv2.line(img, (x1, y1_global), (x2, y2_global), (100, 100, 100), 1)
 
    # 5. Crop and save image
    # Determine crop boundaries based on year
    if year == '20':
        crop_bottom = min(bottom_boundary + 120, img.shape[0])
        crop_top = top_boundary - 390
    else:
        crop_bottom = min(bottom_boundary + 20, img.shape[0])
        crop_top = top_boundary - 360
    
    # Calculate width boundaries for cropping based on year and week
    width_ratio = 0.59  # Default ratio
    
    if year == '20':
        if int(week) >= 25:
            width_ratio = 0.56
        elif int(week) in [9, 22]:
            width_ratio = 0.60
        elif int(week) in [7, 8]:
            width_ratio = 0.57
    
    new_width = int(img.shape[1] * width_ratio)
    new_width2 = int(img.shape[1] * 0.07)  # Left margin
    img_cropped = img[crop_top:crop_bottom, new_width2:new_width]
    
    output_pil = Image.fromarray(cv2.cvtColor(img_cropped, cv2.COLOR_BGR2RGB))
    output_pil.save(output_path)

def ensure_column(rows, fieldnames, column_name):
    """Ensure a column exists in the CSV data."""
    if column_name not in fieldnames:
        fieldnames.append(column_name)
        for row in rows:
            if column_name not in row:
                row[column_name] = ''
    return fieldnames

def get_file_paths(row):
    """Generate file paths based on row data."""
    year = row.get('year', '').strip()
    new_name = row.get('new_name', '').strip()
    
    output_dir = BASE_DIR / 'data' / 'processed' / 'PDF' / f"PDFs_Lines_{year}"
    pdf_path = BASE_DIR / 'data' / 'raw' / 'year' / year / new_name
    enhanced_name = f"Lines_{new_name.replace('.pdf', '')}_page3.png"
    output_path = output_dir / enhanced_name
    
    return pdf_path, output_path, output_dir, enhanced_name

def process_reports_from_csv():
    """Process Lassa fever reports based on metadata in the CSV file."""
    try:
        with open(CSV_FILE, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            fieldnames = reader.fieldnames
            rows = list(reader)
    except Exception as e:
        logging.error(f"Error reading {CSV_FILE}: {e}")
        return
    
    # Ensure required columns exist
    fieldnames = ensure_column(rows, fieldnames, 'Enhanced')
    fieldnames = ensure_column(rows, fieldnames, 'Enhanced_name')
    
    # Track if any rows were modified
    modified = False
    
    # Process rows
    for row in rows:
        year = row.get('year', '').strip()
        week = row.get('week', '').strip()
        downloaded = row.get('Downloaded', '').strip()
        enhanced = row.get('Enhanced', '').strip()
        compatible = row.get('Compatible', '').strip()
        
        # Skip if not compatible, missing year/week, or not in target years
        if (compatible == 'N' or not year or not week or 
            year not in ['20', '21', '22', '23', '24', '25']):
            continue
            
        # Get paths
        pdf_path, output_path, output_dir, enhanced_name = get_file_paths(row)
        
        # Check file status vs CSV status
        if not output_path.exists() and enhanced == 'Y':
            row['Enhanced'] = ''
            row['Enhanced_name'] = ''
            enhanced = ''
            modified = True
            logging.info(f"Reset enhanced status for {row.get('new_name')} - file not found")
        elif output_path.exists() and enhanced != 'Y':
            row['Enhanced'] = 'Y'
            row['Enhanced_name'] = enhanced_name
            enhanced = 'Y'
            modified = True
            logging.info(f"Updated enhanced status for {row.get('new_name')} - file exists")
            
        # Skip if not downloaded or already enhanced correctly
        if downloaded != 'Y' or enhanced == 'Y':
            continue
            
        # Skip if PDF doesn't exist
        if not pdf_path.exists():
            logging.warning(f"PDF file not found: {pdf_path}")
            continue
        
        # Create output directory if needed
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Skip if enhanced file already exists
        if output_path.exists():
            continue
        
        try:
            logging.info(f"Enhancing {row.get('new_name')} (Year: {year}, Week: {week})")
            enhance_table_lines_from_pdf_hq(str(pdf_path), str(output_path), **DEFAULT_PARAMS, year=year, week=week)
            
            if output_path.exists():
                row['Enhanced'] = 'Y'
                row['Enhanced_name'] = enhanced_name
                modified = True
                logging.info(f"Successfully enhanced: {enhanced_name}")
            else:
                logging.error(f"Failed to enhance {row.get('new_name')}")
        
        except Exception as e:
            logging.error(f"Error enhancing {row.get('new_name')}: {e}")
    
    # Write updates to CSV if modified
    if modified:
        try:
            with open(CSV_FILE, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            logging.info(f"Updated website_raw_data.csv with enhancement status")
        except Exception as e:
            logging.error(f"Error writing to {CSV_FILE}: {e}")
    else:
        logging.info("No changes were made to the CSV - all files are up to date")

def main():
    """Main function to process and enhance Lassa fever report tables."""
    logging.info("Starting Lassa fever report table enhancement process")
    process_reports_from_csv()
    logging.info("Finished processing reports")

if __name__ == "__main__":
    main()