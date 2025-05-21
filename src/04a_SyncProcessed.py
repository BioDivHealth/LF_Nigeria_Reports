#!/usr/bin/env python3
"""
04_SyncProcessed.py: Lassa Fever Report Processing Status Synchronizer

This script synchronizes the 'processed' status of Lassa fever reports
between a Backblaze B2 bucket and a Supabase 'website_data' table.

It performs the following actions:
1.  Fetches a list of CSV filenames from the specified B2 bucket directory.
2.  Connects to the Supabase database.
3.  Compares the B2 file list against the 'website_data' table:
    a.  If a Supabase record has 'processed' as Y but its corresponding CSV is NOT in B2,
        updates 'processed' to N in Supabase.
    b.  If a CSV file IS in B2 but its corresponding Supabase record has
        'processed' as N or NULL, updates 'processed' to Y in Supabase.

This script DOES NOT download or upload any files. It only updates metadata.

Usage:
    python src/04_SyncProcessed.py
    (Ensure B2 and Supabase environment variables are set)

Dependencies:
    - sqlalchemy, psycopg2-binary, python-dotenv
    - utils.cloud_storage (for B2 interaction)
    - utils.db_utils (for Supabase interaction)
    - utils.logging_config (for logging)
"""

import os
import logging
from pathlib import Path
from typing import Set, List, Tuple, Optional

from sqlalchemy import text, update
from sqlalchemy.orm import Session

# Attempt to import utility functions, supporting both direct and main.py execution
try:
    from utils.db_utils import get_db_engine
    from utils.logging_config import configure_logging
    from utils.cloud_storage import get_b2_report_filenames 
except ImportError:
    # This fallback is for when the script is run from the project root as part of main.py
    from src.utils.db_utils import get_db_engine
    from src.utils.logging_config import configure_logging
    from src.utils.cloud_storage import get_b2_report_filenames

# Configure logging
configure_logging()

# --- Configuration -----------------------------------------
SUPABASE_TABLE_NAME = 'website_data'
DATABASE_URL = os.environ.get("DATABASE_URL")

# Define the B2 prefix where the processed CSV files are stored.
# Example: "lassa-reports/data/processed/CSV/CSV_LF_YY_Sorted/"
# This should be the path part *before* the actual filename in B2.
B2_REPORTS_PREFIX = "lassa-reports/data/processed/CSV/"
# Ensure the prefix ends with a slash if it's not empty and not just '/'
if B2_REPORTS_PREFIX and B2_REPORTS_PREFIX != '/' and not B2_REPORTS_PREFIX.endswith('/'):
    B2_REPORTS_PREFIX += '/'

# --- Common SQL Conditions ---------------------------------
# Common SQL query conditions to maintain consistency
COMMON_YEAR_CONDITION = "(year >= 20 OR year >= '20')"
COMPATIBILITY_CONDITION = "(compatible IS NULL OR compatible = 'Y' OR compatible != 'N')"
DOWNLOADED_CONDITION = "downloaded = 'Y'"
# --- Functions -----------------------------

def generate_csv_name(pdf_name: Optional[str]) -> Optional[str]:
    """
    Generate CSV filename from PDF filename.
    
    Args:
        pdf_name: The PDF filename (can be None)
        
    Returns:
        The CSV filename or None if pdf_name is None/empty
    """
    if not pdf_name:
        return None
    return f"Lines_{pdf_name.replace('.pdf', '')}_page3.csv"

def sync_processed_status(engine, b2_filenames: Set[str]):
    """
    Synchronizes the 'processed' status in the Supabase 'website_data' table
    with the list of CSV filenames found in B2.

    Args:
        engine: SQLAlchemy engine for database connection.
        b2_filenames (Set[str]): A set of CSV filenames found in B2.
    """
    
    with Session(engine) as session:
        try:
            # 1. Sync Supabase to B2 (identify files marked processed in DB but NOT in B2)
            logging.info("Step 1: Syncing Supabase -> B2 (marking DB entries as NOT processed if CSV not in B2)...")
            # Using 'processed' as text column with value 'Y' instead of boolean
            query = text(f"""
                SELECT id::text, new_name, enhanced_name
                FROM "{SUPABASE_TABLE_NAME}" 
                WHERE processed = 'Y' 
                AND {COMMON_YEAR_CONDITION}
                AND {COMPATIBILITY_CONDITION}
                AND enhanced = 'Y'
            """)
            
            result = session.execute(query)
            records_marked_processed = [(row[0], row[1], row[2]) for row in result]
            logging.info(f"Found {len(records_marked_processed)} records marked as processed in Supabase")
            
            ids_to_mark_not_processed: List[str] = []
            for row_id_text, pdf_name, enhanced_name in records_marked_processed:
                # Skip records without an enhanced_name
                if not enhanced_name:
                    continue
                # Generate the expected CSV name
                expected_csv_name = generate_csv_name(pdf_name)
                # Check if this file exists in B2
                if expected_csv_name not in b2_filenames:
                    ids_to_mark_not_processed.append(row_id_text)
                    logging.info(f"File '{expected_csv_name}' (ID: {row_id_text}) is marked as 'processed' in DB but not found in B2. Queueing to mark as N.")
            
            if ids_to_mark_not_processed:
                update_false_stmt = text(
                    f"UPDATE \"{SUPABASE_TABLE_NAME}\" SET processed = 'N' "
                    f"WHERE id::text = ANY(ARRAY[:ids_list])"
                )
                session.execute(update_false_stmt, {"ids_list": ids_to_mark_not_processed})
                session.commit()
                logging.info(f"Updated {len(ids_to_mark_not_processed)} records in Supabase to processed = N.")
            else:
                logging.info("No Supabase records needed to be marked as not processed (all marked files found in B2).")
            
            # 2. Sync B2 to Supabase (identify files in B2 but not marked processed in DB)
            logging.info("Step 2: Syncing B2 -> Supabase (marking DB entries as processed if CSV in B2)...")
            query = text(f"""
                SELECT id::text, new_name, enhanced_name
                FROM "{SUPABASE_TABLE_NAME}" 
                WHERE (processed IS NULL OR processed != 'Y') 
                AND {COMMON_YEAR_CONDITION}
                AND {COMPATIBILITY_CONDITION}
                AND enhanced = 'Y'
            """)
            
            result = session.execute(query)
            records_needing_processing = [(row[0], row[1], row[2]) for row in result]
            logging.info(f"Found {len(records_needing_processing)} records needing processing status update")
            ids_to_mark_processed: List[str] = []
            for row_id_text, pdf_name, enhanced_name in records_needing_processing:
                # Skip records without a PDF name
                if not pdf_name or not enhanced_name:
                    continue
                # Generate the expected CSV name
                expected_csv_name = generate_csv_name(pdf_name)
                logging.info(f"Expected CSV name: {expected_csv_name}")
                # Check if this file exists in B2
                if expected_csv_name in b2_filenames:
                    ids_to_mark_processed.append(row_id_text)
                    logging.info(f"File '{expected_csv_name}' (ID: {row_id_text}) is in B2 but not marked as 'processed' in DB. Queueing to mark as Y.")

            if ids_to_mark_processed:
                update_true_stmt = text(
                    f"UPDATE \"{SUPABASE_TABLE_NAME}\" SET processed = 'Y' "
                    f"WHERE id::text = ANY(ARRAY[:ids_list])"
                )
                session.execute(update_true_stmt, {"ids_list": ids_to_mark_processed})
                session.commit()
                logging.info(f"Updated {len(ids_to_mark_processed)} records in Supabase to processed = Y.")
            else:
                logging.info("No Supabase records needed to be marked as processed (all B2 files already marked or not in DB).")

            logging.info("Synchronization complete.")

        except Exception as e:
            session.rollback()
            logging.error(f"Error during Supabase synchronization: {e}", exc_info=True)
            logging.error("Transaction rolled back.")

def main():
    """
    Main function to orchestrate the processed status synchronization.
    """
    logging.info("Starting Lassa Fever Report Processed Status Synchronizer...")

    # Critical environment variable checks
    if not DATABASE_URL:
        logging.critical("CRITICAL: DATABASE_URL environment variable not set. Exiting.")
        return
    
    b2_env_vars_present = (
        os.environ.get('B2_APPLICATION_KEY_ID') and
        os.environ.get('B2_APPLICATION_KEY') and
        os.environ.get('B2_BUCKET_NAME')
    )
    if not b2_env_vars_present:
        logging.critical("CRITICAL: B2 environment variables (B2_APPLICATION_KEY_ID, B2_APPLICATION_KEY, B2_BUCKET_NAME) not fully set. Exiting.")
        return
    
    if not B2_REPORTS_PREFIX:
        logging.warning("B2_REPORTS_PREFIX is not set or is empty. This might lead to incorrect file matching if reports are not at bucket root.")
        # Allow proceeding but with a warning.

    try:
        engine = get_db_engine(DATABASE_URL)
        with engine.connect() as connection: # Test connection
            logging.info("Successfully connected to Supabase database.")
    except Exception as e:
        logging.critical(f"CRITICAL: Failed to create SQLAlchemy engine or connect to Supabase: {e}", exc_info=True)
        return

    b2_report_files = get_b2_report_filenames(B2_REPORTS_PREFIX, ".csv")
    
    # Proceed with sync even if b2_report_files is empty; sync_processed_status handles this.
    sync_processed_status(engine, b2_report_files)
    
    logging.info("Lassa Fever Report Processed Status Synchronizer finished.")

if __name__ == "__main__":
    main()