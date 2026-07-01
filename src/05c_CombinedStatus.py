#!/usr/bin/env python3
"""
06a_SyncCombiningStatus.py: Lassa Fever Data Combining Status Synchronizer

This script synchronizes the 'combined' status of processed Lassa fever data files
between local storage, Backblaze B2, and the Supabase database. It ensures that:

1. All processed CSVs with 'processed=Y' in the database are marked with 'combined' status
2. Only CSVs that are not yet combined are added to the lassa_data table
3. Missing CSV files are downloaded from B2 when needed

This script helps bridge the gap between data processing and database syncing, making
the pipeline more efficient by avoiding redundant operations and ensuring data integrity.

Usage:
    python src/06a_SyncCombiningStatus.py

Dependencies:
    - sqlalchemy, psycopg2-binary, python-dotenv
    - utils.cloud_storage (for B2 interaction)
    - utils.db_utils (for Supabase interaction)
    - utils.logging_config (for logging)
"""

import os
import sys
import logging
import pandas as pd
from pathlib import Path
from typing import Set, List, Dict, Optional, Tuple
from sqlalchemy import text, update
from sqlalchemy.orm import Session

# Attempt to import utility functions, supporting both direct and main.py execution
try:
    from utils.artifact_paths import csv_name_for_report, extraction_qa_path_for_csv_path
    from utils.csv_qa import validate_extracted_csv
    from utils.db_utils import get_db_engine, get_existing_records
    from utils.logging_config import configure_logging
    from utils.cloud_storage import get_b2_report_filenames, download_file
    from utils.review_needed import record_review_needed
    from utils.status_qa import check_extraction_qa_file
except ImportError:
    from src.utils.artifact_paths import csv_name_for_report, extraction_qa_path_for_csv_path
    from src.utils.csv_qa import validate_extracted_csv
    from src.utils.db_utils import get_db_engine, get_existing_records
    from src.utils.logging_config import configure_logging
    from src.utils.cloud_storage import get_b2_report_filenames, download_file
    from src.utils.review_needed import record_review_needed
    from src.utils.status_qa import check_extraction_qa_file

# Configure logging
configure_logging()

# --- Configuration -----------------------------------------
SUPABASE_TABLE_NAME = 'website_data'
DATA_TABLE_NAME = 'lassa_data'
DATABASE_URL = os.environ.get("DATABASE_URL")
BASE_DIR = Path(__file__).parent.parent
CSV_BASE_FOLDER = BASE_DIR / 'data' / 'processed' / 'CSV'

# Define the B2 prefix where the processed CSV files are stored
B2_REPORTS_PREFIX = "lassa-reports/data/processed/CSV/"
# Ensure the prefix ends with a slash if it's not empty and not just '/'
if B2_REPORTS_PREFIX and B2_REPORTS_PREFIX != '/' and not B2_REPORTS_PREFIX.endswith('/'):
    B2_REPORTS_PREFIX += '/'

# --- Common SQL Conditions ---------------------------------
# Common SQL query conditions to maintain consistency
COMMON_YEAR_CONDITION = "(year >= 20 OR year >= '20')"
COMPATIBILITY_CONDITION = "(compatible IS NULL OR compatible = 'Y' OR compatible != 'N')"
DOWNLOADED_CONDITION = "downloaded = 'Y'"
PROCESSED_CONDITION = "processed = 'Y'"

# --- Functions -----------------------------

def _expected_year(year):
    return f"20{year}" if len(str(year)) == 2 else str(year)


def _record_csv_review(review_context, check_type, reason):
    if not review_context:
        return
    record_review_needed(
        stage=review_context.get("stage", "CombinedStatus"),
        report_id=review_context.get("report_id"),
        year=review_context.get("year"),
        week=review_context.get("week"),
        artifact_name=review_context.get("artifact_name"),
        check_type=check_type,
        reason=reason,
        action=review_context.get("action"),
    )


def csv_artifact_passes_qa(csv_path: Path, year, week, review_context=None) -> bool:
    csv_qa_result = validate_extracted_csv(csv_path, expected_year=_expected_year(year), expected_week=week)
    if csv_qa_result.status != "pass":
        reason = f"CSV QA failed for {csv_path.name}: {'; '.join(csv_qa_result.errors)}"
        logging.info(reason)
        _record_csv_review(review_context, "csv_qa", reason)
        return False

    extraction_qa_path = extraction_qa_path_for_csv_path(csv_path)
    if extraction_qa_path and extraction_qa_path.exists():
        extraction_qa_result = check_extraction_qa_file(extraction_qa_path)
        if not extraction_qa_result.ok:
            reason = f"Extraction QA failed for {csv_path.name}: {extraction_qa_result.reason}"
            logging.info(reason)
            _record_csv_review(review_context, "extraction_qa", reason)
            return False

    return True


def find_local_csv_files() -> Dict[str, Path]:
    """
    Find all CSV files in the local processed directory.
    
    Returns:
        Dictionary mapping CSV filenames to their full paths
    """
    csv_files = {}
    
    # Check if base directory exists
    if not CSV_BASE_FOLDER.exists():
        logging.warning(f"CSV base folder {CSV_BASE_FOLDER} does not exist")
        return csv_files
    
    # Find all CSV files in the sorted directories
    for year_dir in CSV_BASE_FOLDER.glob("CSV_LF_*_Sorted"):
        if year_dir.is_dir():
            for csv_file in year_dir.glob("*.csv"):
                csv_files[csv_file.name] = csv_file
                
    logging.info(f"Found {len(csv_files)} local CSV files")
    return csv_files

def get_report_mapping(engine) -> Dict[str, Tuple[str, str, str, str]]:
    """
    Create a mapping of CSV filenames to report metadata from the database.
    
    Args:
        engine: SQLAlchemy engine for database connection
        
    Returns:
        Dictionary mapping CSV filenames to tuples of (report_id, year, week, combined_status)
    """
    report_map = {}
    
    with Session(engine) as session:
        # Query for reports that have been processed
        query = text(f"""
            SELECT id::text, new_name, enhanced_name, year, week, combined
            FROM "{SUPABASE_TABLE_NAME}" 
            WHERE {PROCESSED_CONDITION}
            AND {COMMON_YEAR_CONDITION}
            AND {COMPATIBILITY_CONDITION}
            AND {DOWNLOADED_CONDITION}
        """)
        
        result = session.execute(query)
        for row in result:
            report_id = row[0]
            pdf_name = row[1]
            enhanced_name = row[2]
            year = row[3]
            week = row[4]
            combined = row[5] or 'N'  # Default to 'N' if NULL
            
            csv_name = csv_name_for_report(pdf_name, enhanced_name)
            if csv_name:
                report_map[csv_name] = (report_id, year, week, combined)
    
    logging.info(f"Retrieved {len(report_map)} report mappings from database")
    return report_map

def update_combined_status(engine, report_ids: List[str], status: str = 'Y'):
    """
    Update the 'combined' status for multiple reports in the database.
    
    Args:
        engine: SQLAlchemy engine for database connection
        report_ids: List of report IDs to update
        status: Status to set ('Y' or 'N')
    """
    if not report_ids:
        return
        
    with Session(engine) as session:
        try:
            update_stmt = text(
                f"UPDATE \"{SUPABASE_TABLE_NAME}\" SET combined = :status "
                f"WHERE id::text = ANY(ARRAY[:ids_list])"
            )
            session.execute(update_stmt, {"status": status, "ids_list": report_ids})
            session.commit()
            logging.info(f"Updated {len(report_ids)} records in Supabase to combined = {status}")
        except Exception as e:
            session.rollback()
            logging.error(f"Error updating combined status: {e}", exc_info=True)

def sync_combining_status(engine):
    """
    Synchronize the 'combined' status between local files, B2, and Supabase.
    
    Args:
        engine: SQLAlchemy engine for database connection
    
    Returns:
        Tuple of:
        - Dict mapping CSV filenames to tuples of (file_path, report_id, year, week) for files to combine
        - List of report IDs that have been newly marked as combined
    """

    # Get local CSV files
    local_csv_files = find_local_csv_files()
    
    # Get report mapping from database
    report_map = get_report_mapping(engine)
    
    # Check which reports are already in lassa_data
    reports_in_lassa_data = get_existing_records(engine, DATA_TABLE_NAME, 'report_id')
    if reports_in_lassa_data:
        logging.info(f"Found {len(reports_in_lassa_data)} reports already in {DATA_TABLE_NAME}")

    # Identify files that need to be marked as 'combined = Y' in the database
    ids_to_mark_combined = []
    
    # Find reports that are in lassa_data but not marked as combined
    for csv_name, (report_id, year, week, combined) in report_map.items():
        logging.debug(f"Checking report {report_id} (CSV: {csv_name})")
        if report_id in reports_in_lassa_data and combined != 'Y':
            if csv_name not in local_csv_files:
                logging.info(f"Report {report_id} is in lassa_data but local CSV {csv_name} is unavailable; not marking combined.")
                continue
            if not csv_artifact_passes_qa(
                local_csv_files[csv_name],
                year,
                week,
                {
                    "stage": "CombinedStatus",
                    "report_id": report_id,
                    "year": year,
                    "week": week,
                    "artifact_name": csv_name,
                    "action": "block_combined_status",
                },
            ):
                logging.info(f"Report {report_id} is in lassa_data but {csv_name} did not pass QA; not marking combined.")
                continue
            ids_to_mark_combined.append(report_id)
            logging.info(f"Report {report_id} (CSV: {csv_name}) is in lassa_data but not marked as combined")
    
    # Update combined status for reports that need it
    if ids_to_mark_combined:
        update_combined_status(engine, ids_to_mark_combined)
    
    return ids_to_mark_combined

def main():
    """
    Main function to orchestrate the combining status synchronization.
    """
    logging.info("Starting Lassa Fever Data Combining Status Synchronizer...")

    # Critical environment variable checks
    if not DATABASE_URL:
        logging.critical("CRITICAL: DATABASE_URL environment variable not set. Exiting.")
        return 1
    
    b2_env_vars_present = (
        os.environ.get('B2_APPLICATION_KEY_ID') and
        os.environ.get('B2_APPLICATION_KEY') and
        os.environ.get('B2_BUCKET_NAME')
    )
    if not b2_env_vars_present:
        logging.critical("CRITICAL: B2 environment variables (B2_APPLICATION_KEY_ID, B2_APPLICATION_KEY, B2_BUCKET_NAME) not fully set. Exiting.")
        return 1
    
    try:
        engine = get_db_engine(DATABASE_URL)
        with engine.connect() as connection:
            logging.info("Successfully connected to Supabase database.")
    except Exception as e:
        logging.critical(f"CRITICAL: Failed to create SQLAlchemy engine or connect to Supabase: {e}", exc_info=True)
        return 1

    # Sync combining status and get files to combine
    ids_marked_combined = sync_combining_status(engine)
    
    # Log results
    logging.info(f"Combining Status Synchronizer finished.")
    logging.info(f"Reports newly marked as combined: {len(ids_marked_combined)}")
    
    return 0

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    sys.exit(main())
