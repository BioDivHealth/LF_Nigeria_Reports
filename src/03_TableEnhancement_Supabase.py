#!/usr/bin/env python3
"""
03_TableEnhancement_Supabase.py: Lassa Fever Report Table Enhancement

This script enhances tables in Lassa fever reports by improving their visibility
for subsequent extraction. It processes PDFs stored in Backblaze B2 and updates
the enhancement status in a Supabase 'website_data' table.

It performs the following actions:
1. Connects to Supabase and retrieves a list of reports that need enhancement
   (downloaded = 'Y' and enhanced != 'Y')
2. For each report:
   a. Downloads the PDF from B2 to a temporary location
   b. Enhances the table by improving line visibility
   c. Uploads the enhanced image to B2
   d. Updates the enhanced status in Supabase
3. Synchronizes the enhanced status between B2 and Supabase

Dependencies:
    - OpenCV (cv2), PyMuPDF (fitz), PIL, NumPy
    - utils.cloud_storage (for B2 interaction)
    - utils.db_utils (for Supabase interaction)
    - utils.logging_config (for logging)
"""

import cv2
import numpy as np
import os
import re
import tempfile
import time
import logging
from pathlib import Path
from PIL import Image
import fitz  # PyMuPDF
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import Set, List, Tuple, Dict, Optional

# Attempt to import utility functions, supporting both direct and main.py execution
try:
    from utils.cloud_storage import get_b2_file_list
    from utils.cloud_storage import download_file
    from utils.cloud_storage import upload_file
    from utils.cloud_storage import get_b2_report_filenames
    from utils.db_utils import get_db_engine
    from utils.logging_config import configure_logging
    import importlib.util
    # Import the sync_enhanced_status function from 03_SyncEnhancement
    spec = importlib.util.spec_from_file_location("sync_module", "src/03_SyncEnhancement.py")
    sync_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sync_module)
    sync_enhanced_status = sync_module.sync_enhanced_status
except ImportError:
    # This fallback is for when the script is run from the project root as part of main.py
    from src.utils.cloud_storage import get_b2_file_list
    from src.utils.cloud_storage import download_file
    from src.utils.cloud_storage import upload_file
    from src.utils.cloud_storage import get_b2_report_filenames
    from src.utils.db_utils import get_db_engine
    from src.utils.logging_config import configure_logging
    import importlib.util
    # Import the sync_enhanced_status function from 03_SyncEnhancement
    spec = importlib.util.spec_from_file_location("sync_module", "src/03_SyncEnhancement.py")
    sync_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sync_module)
    sync_enhanced_status = sync_module.sync_enhanced_status

# Configure logging
configure_logging()

# --- Configuration -----------------------------------------
SUPABASE_TABLE_NAME = 'website_data'
DATABASE_URL = os.environ.get("DATABASE_URL")

# Define the B2 prefixes
B2_PDF_PREFIX = "lassa-reports/data/processed/PDF/"
B2_RAW_PREFIX = "lassa-reports/data/raw/year/"

COMMON_YEAR_CONDITION = "(year >= 20 OR year >= '20')"
COMPATIBILITY_CONDITION = "(compatible IS NULL OR compatible = 'Y' OR compatible != 'N')"
DOWNLOADED_CONDITION = "downloaded = 'Y'"

# Paths ------------------------------------------------------
BASE_DIR = Path(__file__).parent.parent
RAW_FOLDER = BASE_DIR / 'data' / 'raw' / 'year'
RAW_FOLDER.mkdir(parents=True, exist_ok=True)

# Ensure the prefixes end with a slash
for prefix_var in ["B2_PDF_PREFIX", "B2_RAW_PREFIX"]:
    prefix = locals()[prefix_var]
    if prefix and prefix != '/' and not prefix.endswith('/'):
        locals()[prefix_var] = prefix + '/'

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
# --- End Configuration -------------------------------------

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
    if year in ['20', '21']:
        # For 2020 and 2021 reports
        crop_top = max(0, top_boundary - 110)
        crop_bottom = min(img.shape[0], bottom_boundary + 110)
    else:  # 2022, 2023, etc.
        # For 2022+ reports
        crop_top = max(0, top_boundary - 110)
        crop_bottom = min(img.shape[0], bottom_boundary + 10)
    
    # Crop the image
    cropped_img = img[crop_top:crop_bottom, :]
    
    # Save the enhanced image
    cv2.imwrite(str(output_path), cropped_img)
    
    # Close the PDF document
    doc.close()
    
    return True


def download_pdf_from_b2(b2_filename: str, year: str, temp_dir: Path) -> Optional[Path]:
    """Download a PDF file from B2 to a local temporary directory.
    
    Args:
        b2_filename: The filename of the PDF in B2
        year: The year of the report
        temp_dir: The temporary directory to download to
        
    Returns:
        Path to the downloaded file, or None if download failed
    """
   
    b2_key = f"{B2_RAW_PREFIX}{year}/{b2_filename}"
    local_path = temp_dir / b2_filename
    
    try:
        success = download_file(b2_key, str(local_path))
        if success:
            logging.info(f"Successfully downloaded {b2_filename} from B2")
            return local_path
        else:
            logging.error(f"Failed to download {b2_filename} from B2")
            return None
    except Exception as e:
        logging.error(f"Error downloading {b2_filename} from B2: {e}")
        return None

def get_reports_to_enhance(engine) -> List[Dict]:
    """Query Supabase for reports that need enhancement.
    
    Args:
        engine: SQLAlchemy engine for database connection
        
    Returns:
        List of dictionaries with report data
    """
    with Session(engine) as session:
        try:
            # Query for reports that have been downloaded but not enhanced
            stmt = text(f"""
                SELECT id::text, new_name, year, week, compatible
                FROM \"{SUPABASE_TABLE_NAME}\"
                WHERE {DOWNLOADED_CONDITION}
                AND (enhanced = 'N' OR enhanced IS NULL)
                AND {COMPATIBILITY_CONDITION}
                AND {COMMON_YEAR_CONDITION}
                ORDER BY year DESC, week DESC
            """)
            
            result = session.execute(stmt).fetchall()
            reports = []
            for row in result:
                reports.append({
                    'id': row[0],
                    'new_name': row[1],
                    'year': row[2],
                    'week': row[3],
                    'compatible': row[4]
                })
            
            logging.info(f"Found {len(reports)} reports that need enhancement")
            return reports
        except Exception as e:
            logging.error(f"Error querying Supabase for reports to enhance: {e}")
            return []

def update_enhanced_status(engine, report_id: str, enhanced_name: str, status: str = 'Y'):
    """Update the enhanced status in Supabase.
    
    Args:
        engine: SQLAlchemy engine for database connection
        report_id: ID of the report to update
        enhanced_name: Name of the enhanced image file
        status: Status to set ('Y' or 'N')
    """
    with Session(engine) as session:
        try:
            stmt = text(f"""
                UPDATE \"{SUPABASE_TABLE_NAME}\"
                SET enhanced = :status, enhanced_name = :enhanced_name
                WHERE id = :id::uuid
            """)
            
            session.execute(stmt, {
                'status': status,
                'enhanced_name': enhanced_name,
                'id': report_id
            })
            session.commit()
            logging.info(f"Updated enhanced status for report {report_id} to {status}")
        except Exception as e:
            session.rollback()
            logging.error(f"Error updating enhanced status for report {report_id}: {e}")

def process_reports_from_supabase(engine):
    """Process Lassa fever reports based on metadata in Supabase.
    
    1. Query Supabase for reports that need enhancement
    2. Download each PDF from B2
    3. Enhance the tables
    4. Upload the enhanced images to B2
    5. Update Supabase with the enhanced status
    """
    reports = get_reports_to_enhance(engine)
    if not reports:
        logging.info("No reports to enhance")
        return
    
    # Create a temporary directory for processing
    with tempfile.TemporaryDirectory() as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        temp_output_dir = temp_dir / "enhanced"
        temp_output_dir.mkdir(exist_ok=True)
        
        for report in reports:
            report_id = report['id']
            new_name = report['new_name']
            year = report['year']
            week = report['week']
            
            # Generate enhanced image name
            enhanced_name = f"Lines_{new_name.replace('.pdf', '')}_page3.png"
            
            # Download PDF from B2
            pdf_path = download_pdf_from_b2(new_name, temp_dir)
            if not pdf_path:
                continue
            
            # Path for enhanced output
            output_path = temp_output_dir / enhanced_name
            
            try:
                logging.info(f"Enhancing {new_name} (Year: {year}, Week: {week})")
                enhance_table_lines_from_pdf_hq(
                    str(pdf_path), str(output_path),
                    **DEFAULT_PARAMS, year=year, week=week
                )
                
                if output_path.exists():
                    # Upload enhanced image to B2
                    upload_success = upload_enhanced_image_to_b2(output_path, enhanced_name)
                    
                    if upload_success:
                        # Update Supabase
                        update_enhanced_status(engine, report_id, enhanced_name, 'Y')
                    else:
                        logging.error(f"Failed to upload enhanced image for {new_name}")
                else:
                    logging.error(f"Failed to enhance {new_name}")
            
            except Exception as e:
                logging.error(f"Error enhancing {new_name}: {e}")
                continue
            
            # Clean up the PDF file to save space
            try:
                pdf_path.unlink()
            except Exception as e:
                logging.warning(f"Error removing temporary PDF file {pdf_path}: {e}")

def main():
    """Main function to process and enhance Lassa fever report tables."""
    logging.info("Starting Lassa fever report table enhancement process")
    
    # Connect to Supabase
    engine = get_db_engine(DATABASE_URL)
    if not engine:
        logging.error("Failed to connect to Supabase. Check DATABASE_URL environment variable.")
        return
    
    b2_pdfs = get_b2_report_filenames(B2_RAW_PREFIX, ".pdf")
    
    logging.info(f"File names in B2: {b2_pdfs}")
    # # Get the list of enhanced files from B2
    # b2_enhanced_filenames = get_b2_report_filenames(B2_ENHANCED_PREFIX, ".png")
    
    # # Sync the enhanced status between B2 and Supabase
    # sync_enhanced_status(engine, b2_enhanced_filenames)
    
    # # Process reports that need enhancement
    # process_reports_from_supabase(engine)
    
    reports = get_reports_to_enhance(engine)
    if not reports:
        logging.info("No reports to enhance")
        return
    logging.info(f"Found {len(reports)} reports to enhance")
    logging.info(f"Reports: {reports}")
    
    

    for report in reports:
            report_id = report['id']
            new_name = report['new_name']
            year = report['year']
            week = report['week']
            # Generate enhanced image name
            enhanced_name = f"Lines_{new_name.replace('.pdf', '')}_page3.png"
            if (RAW_FOLDER / str(year) / new_name).exists():
                logging.info(f"Report {new_name} already exists in {RAW_FOLDER / str(year) / new_name}")
            else:
                if new_name in b2_pdfs:
                    logging.info(f"Report {new_name} exists in B2, can be downloaded")
                    download_pdf_from_b2(new_name, str(year), RAW_FOLDER / str(year))
               
                
        
            
    logging.info("Finished processing reports")

if __name__ == "__main__":
    main()
