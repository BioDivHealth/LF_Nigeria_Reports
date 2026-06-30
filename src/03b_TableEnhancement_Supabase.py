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

import os
import json
import time
import logging
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Dict, Optional

# Attempt to import utility functions, supporting both direct and main.py execution
try:
    from utils.artifact_paths import (
        enhanced_image_path,
        enhanced_name_for_report,
        layout_qa_path_for_enhanced_path,
    )
    from utils.cloud_storage import get_b2_file_list
    from utils.cloud_storage import download_file
    from utils.cloud_storage import upload_file
    from utils.cloud_storage import get_b2_report_filenames
    from utils.db_utils import get_db_engine
    from utils.logging_config import configure_logging
    from utils.report_layout import find_table3_page
    from utils.table_enhancement import DEFAULT_PARAMS, enhance_table_lines_from_pdf_hq
    import importlib.util
    # Import the sync_enhanced_status function from 03a_SyncEnhancement.py
    spec = importlib.util.spec_from_file_location("sync_module", "src/03a_SyncEnhancement.py")
except ImportError:
    from src.utils.artifact_paths import (
        enhanced_image_path,
        enhanced_name_for_report,
        layout_qa_path_for_enhanced_path,
    )
    from src.utils.cloud_storage import get_b2_file_list
    from src.utils.cloud_storage import download_file
    from src.utils.cloud_storage import upload_file
    from src.utils.cloud_storage import get_b2_report_filenames
    from src.utils.db_utils import get_db_engine
    from src.utils.logging_config import configure_logging
    from src.utils.report_layout import find_table3_page
    from src.utils.table_enhancement import DEFAULT_PARAMS, enhance_table_lines_from_pdf_hq
    import importlib.util
    # Import the sync_enhanced_status function from 03a_SyncEnhancement.py
    spec = importlib.util.spec_from_file_location("sync_module", "src/03a_SyncEnhancement.py")
    sync_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sync_module)
    sync_enhanced_status = sync_module.sync_enhanced_status
except ImportError:
    # This fallback is for when the script is run from the project root as part of main.py
    from src.utils.artifact_paths import (
        enhanced_image_path,
        enhanced_name_for_report,
        layout_qa_path_for_enhanced_path,
    )
    from src.utils.cloud_storage import get_b2_file_list
    from src.utils.cloud_storage import download_file
    from src.utils.cloud_storage import upload_file
    from src.utils.cloud_storage import get_b2_report_filenames
    from src.utils.db_utils import get_db_engine
    from src.utils.logging_config import configure_logging
    from src.utils.report_layout import find_table3_page
    from src.utils.table_enhancement import DEFAULT_PARAMS, enhance_table_lines_from_pdf_hq
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

# Ensure the prefixes end with a slash
for prefix_var in ["B2_PDF_PREFIX", "B2_RAW_PREFIX"]:
    prefix = locals()[prefix_var]
    if prefix and prefix != '/' and not prefix.endswith('/'):
        locals()[prefix_var] = prefix + '/'

# DEFINE FILTERING CONDITIONS
COMMON_YEAR_CONDITION = "(year >= 20 OR year >= '20')"
COMPATIBILITY_CONDITION = "(compatible IS NULL OR compatible = 'Y' OR compatible != 'N')"
DOWNLOADED_CONDITION = "downloaded = 'Y'"

# Paths ------------------------------------------------------
BASE_DIR = Path(__file__).parent.parent
RAW_FOLDER = BASE_DIR / 'data' / 'raw' / 'year'
ENHANCED_FOLDER = BASE_DIR / 'data' / 'processed' / 'PDF'
ENHANCED_FOLDER.mkdir(parents=True, exist_ok=True)
RAW_FOLDER.mkdir(parents=True, exist_ok=True)

# --- End Configuration -------------------------------------

def write_layout_qa(layout_qa_path: Path, layout_result):
    """Write the layout QA result beside an enhanced image artifact."""
    layout_qa_path.parent.mkdir(parents=True, exist_ok=True)
    with layout_qa_path.open("w", encoding="utf-8") as outfile:
        json.dump(layout_result.to_dict(), outfile, indent=2, sort_keys=True)
        outfile.write("\n")


def enhance_report_pdf(pdf_path: Path, output_path: Path, year, week) -> bool:
    """Run layout QA and then enhance a report PDF when Table 3 is located."""
    layout_result = find_table3_page(
        pdf_path,
        default_page_index=DEFAULT_PARAMS["page_number"],
        year=year,
        week=week,
    )
    layout_qa_path = layout_qa_path_for_enhanced_path(output_path)
    write_layout_qa(layout_qa_path, layout_result)

    logging.info(
        "Layout QA for %s: status=%s confidence=%s selected_page=%s",
        pdf_path.name,
        layout_result.status,
        layout_result.confidence,
        layout_result.selected_page_number,
    )
    for warning in layout_result.warnings:
        logging.warning(f"Layout QA warning for {pdf_path.name}: {warning}")

    if layout_result.status == "fail":
        for reason in layout_result.reasons:
            logging.error(f"Layout QA failed for {pdf_path.name}: {reason}")
        return False

    enhancement_params = DEFAULT_PARAMS.copy()
    enhancement_params["page_number"] = layout_result.selected_page_index
    return enhance_table_lines_from_pdf_hq(
        str(pdf_path),
        str(output_path),
        **enhancement_params,
        year=year,
        week=week,
    )

def download_file_from_b2(b2_key: str, destination: Path) -> Optional[Path]:
    """Download a file from B2 to a local temporary directory.
    
    Args:
        b2_key: The key of the file in B2
        destination: The destination directory to download to
        
    Returns:
        Path to the downloaded file, or None if download failed
    """
    b2_filename = os.path.basename(b2_key)
    logging.info(f"Downloading {b2_filename} from B2 to {destination}")
    try:
        logging.info(f"b2 key is {b2_key}")
        success = download_file(b2_key, str(destination))
        if success:
            logging.info(f"Successfully downloaded {b2_filename} from B2")
            return None
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
                WHERE id = CAST(:id AS uuid)
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

def process_reports_from_supabase():
    """Main function to process and enhance Lassa fever report tables."""
    logging.info("Starting Lassa fever report table enhancement process")
    
    # Connect to Supabase
    engine = get_db_engine(DATABASE_URL)
    if not engine:
        logging.error("Failed to connect to Supabase. Check DATABASE_URL environment variable.")
        return
    
    b2_pdfs = get_b2_report_filenames(B2_RAW_PREFIX, ".pdf")
    
    logging.info(f"File names in B2: {b2_pdfs}")
    
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
            enhanced_name = enhanced_name_for_report(new_name)
            output_path = enhanced_image_path(ENHANCED_FOLDER, year, enhanced_name)
            if not output_path:
                logging.warning(f"Could not derive enhanced artifact path for report {report_id} ({new_name})")
                continue
            output_path.parent.mkdir(parents=True, exist_ok=True)
            if output_path.exists():
                logging.info(f"Enhanced image {enhanced_name} already exists in {output_path}")
                update_enhanced_status(engine, report_id, enhanced_name)
                continue
            if (RAW_FOLDER / str(year) / new_name).exists():
                logging.info(f"Report {new_name} already exists in {RAW_FOLDER / str(year) / new_name}")
                try:
                    logging.info(f"Enhancing {new_name} (Year: {year}, Week: {week})")
                    upload_success = enhance_report_pdf(RAW_FOLDER / str(year) / new_name, output_path, year, week)
                    if upload_success:
                        update_enhanced_status(engine, report_id, enhanced_name)
                        logging.info(f"Successfully enhanced {new_name} (Year: {year}, Week: {week})")
                except Exception as e:
                    logging.error(f"Error enhancing {new_name}: {e}")
                    continue
            elif new_name in b2_pdfs:
                logging.info(f"Report {new_name} exists in B2, can be downloaded")
                b2_key = f"{B2_RAW_PREFIX}{year}/{new_name}"
                download_file_from_b2(b2_key, destination=f"{RAW_FOLDER}/{year}/{new_name}")
                time.sleep(5)
                try:
                    logging.info(f"Enhancing {new_name} (Year: {year}, Week: {week})")
                    upload_success = enhance_report_pdf(RAW_FOLDER / str(year) / new_name, output_path, year, week)
                    if upload_success:
                        update_enhanced_status(engine, report_id, enhanced_name)
                        logging.info(f"Successfully enhanced {new_name} (Year: {year}, Week: {week})")
                except Exception as e:
                    logging.error(f"Error enhancing {new_name}: {e}")
                    continue
            else:
                logging.info(f"Raw report {new_name} does not exist in B2 or locally")  
                continue
            
    logging.info("Finished processing reports")

def main():
    process_reports_from_supabase()

if __name__ == "__main__":
    main()
