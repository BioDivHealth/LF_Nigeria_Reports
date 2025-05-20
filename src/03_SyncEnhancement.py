#!/usr/bin/env python3
"""
03_SyncEnhancement.py: Lassa Fever Report Enhancement Status Synchronizer

This script synchronizes the 'enhanced' status of Lassa fever reports
between a Backblaze B2 bucket and a Supabase 'website_data' table.

It performs the following actions:
1.  Fetches a list of PDF filenames from the specified B2 bucket directory.
2.  Connects to the Supabase database.
3.  Compares the B2 file list against the 'website_data' table:
    a.  If a Supabase record has 'enhanced' as Y but its 'new_name' is NOT in B2,
        updates 'enhanced' to N in Supabase.
    b.  If a file's 'new_name' IS in B2 but its corresponding Supabase record has
        'enhanced' as N or NULL, updates 'enhanced' to Y in Supabase.

This script DOES NOT download or upload any files. It only updates metadata.

Usage:
    python src/02_PDF_Download_Supabase.py
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

# Define the B2 prefix where the enhanced images are stored.
# Example: "lassa-reports/data/processed/PDFs_Lines_year/"
# This should be the path part *before* the actual filename in B2.
B2_REPORTS_PREFIX = "lassa-reports/data/processed/PDF/"
# Ensure the prefix ends with a slash if it's not empty and not just '/'
if B2_REPORTS_PREFIX and B2_REPORTS_PREFIX != '/' and not B2_REPORTS_PREFIX.endswith('/'):
    B2_REPORTS_PREFIX += '/'

# --- Common SQL Conditions ---------------------------------
# Common SQL query conditions to maintain consistency
COMMON_YEAR_CONDITION = "(year >= 20 OR year >= '20')"
COMPATIBILITY_CONDITION = "(compatible IS NULL OR compatible = 'Y' OR compatible != 'N')"
DOWNLOADED_CONDITION = "downloaded = 'Y'"
# --- Functions -----------------------------

def generate_enhanced_name(pdf_name: Optional[str]) -> Optional[str]:
    """
    Generate enhanced image filename from PDF filename.
    
    Args:
        pdf_name: The PDF filename (can be None)
        
    Returns:
        The enhanced image filename or None if pdf_name is None/empty
    """
    if not pdf_name:
        return None
    return f"Lines_{pdf_name.replace('.pdf', '')}_page3.png"

def sync_enhanced_status(engine, b2_filenames: Set[str]):
    """
    Synchronizes the 'enhanced' status in the Supabase 'website_data' table
    with the list of enhanced image filenames found in B2.

    Args:
        engine: SQLAlchemy engine for database connection.
        b2_filenames (Set[str]): A set of enhanced image filenames found in B2.
    """
    
    with Session(engine) as session:
        try:
            # 1. Sync Supabase to B2 (identify files marked enhanced in DB but NOT in B2)
            logging.info("Step 1: Syncing Supabase -> B2 (marking DB entries as NOT enhanced if not in B2)...")
            # Using 'enhanced' as text column with value 'Y' instead of boolean
            stmt_select_enhanced_db = text(f"SELECT id::text, enhanced_name, new_name FROM \"{SUPABASE_TABLE_NAME}\" WHERE enhanced = 'Y' AND {DOWNLOADED_CONDITION} AND {COMMON_YEAR_CONDITION} AND {COMPATIBILITY_CONDITION})")
            enhanced_in_db = session.execute(stmt_select_enhanced_db).fetchall()
            
            ids_to_mark_not_enhanced: List[str] = []
            for row_id_text, enhanced_name, new_name in enhanced_in_db:
                # If enhanced_name is empty, generate it from new_name
                if not enhanced_name and new_name:
                    enhanced_name = generate_enhanced_name(new_name)
                    logging.info(f"Generated enhanced_name '{enhanced_name}' for ID: {row_id_text} based on new_name '{new_name}'")
                
                if not enhanced_name or enhanced_name not in b2_filenames:
                    ids_to_mark_not_enhanced.append(row_id_text)
                    logging.info(f"File '{enhanced_name or 'UNKNOWN'}' (ID: {row_id_text}) is 'enhanced' in DB but not in B2. Queueing to mark as N.")

            if ids_to_mark_not_enhanced:
                # Using text() for table name to handle potential quoting needs
                # Using = ANY(ARRAY[:ids_list]::uuid[]) for UUIDs is generally robust
                # Ensure your 'id' column is indeed UUID or adjust cast and array type accordingly
                update_false_stmt = text(
                    f"UPDATE \"{SUPABASE_TABLE_NAME}\" SET enhanced = 'N' "
                    f"WHERE id::text = ANY(ARRAY[:ids_list])"
                )
                session.execute(update_false_stmt, {"ids_list": ids_to_mark_not_enhanced})
                session.commit()
                logging.info(f"Updated {len(ids_to_mark_not_enhanced)} records in Supabase to enhanced = N.")
            else:
                logging.info("No Supabase records needed to be marked as not enhanced (all align with B2 or none were Y).")

            # 2. Sync B2 to Supabase (identify files in B2 but NOT marked enhanced in DB)
            logging.info("Step 2: Syncing B2 -> Supabase (marking DB entries as ENHANCED if present in B2 and not already marked)...")
            if not b2_filenames: 
                 logging.info("B2 filename list is empty, skipping B2 to Supabase sync (no files to mark as enhanced).")
            else:
                logging.info(f"Found {len(b2_filenames)} files in B2 to check")
                logging.info(f"Raw B2 filenames: {b2_filenames}")
                # First, we need to handle records with empty enhanced_name
                # Get all records that need enhancement and have been downloaded
                stmt_select_records_needing_enhancement = text(f"""
                    SELECT id::text, new_name 
                    FROM \"{SUPABASE_TABLE_NAME}\" 
                    WHERE (enhanced = 'N' OR enhanced IS NULL OR enhanced_name IS NULL OR enhanced_name = '') 
                    AND {DOWNLOADED_CONDITION} 
                    AND {COMMON_YEAR_CONDITION}
                    AND {COMPATIBILITY_CONDITION}
                """)
                
                records_needing_enhancement = session.execute(stmt_select_records_needing_enhancement).fetchall()
                logging.info(f"Found {len(records_needing_enhancement)} records needing enhancement")
                ids_to_mark_enhanced: List[str] = []
                for row_id_text, new_name in records_needing_enhancement:
                    # Skip records without a PDF name
                    if not new_name:
                        continue
                    # Generate the expected enhanced_name
                    expected_enhanced_name = generate_enhanced_name(new_name)
                    logging.info(f"Expected enhanced_name: {expected_enhanced_name}")
                    # Check if this file exists in B2
                    if expected_enhanced_name in b2_filenames:
                        ids_to_mark_enhanced.append(row_id_text)
                        logging.info(f"File '{expected_enhanced_name}' (ID: {row_id_text}) is in B2 but not marked as 'enhanced' in DB. Queueing to mark as Y.")
                        
                        # Update the enhanced_name in the database
                        update_name_stmt = text(f"""
                            UPDATE \"{SUPABASE_TABLE_NAME}\" 
                            SET enhanced_name = :enhanced_name 
                            WHERE id = :id
                        """)
                        session.execute(update_name_stmt, {
                            "enhanced_name": expected_enhanced_name,
                            "id": row_id_text
                        })

                if ids_to_mark_enhanced:
                    update_true_stmt = text(
                        f"UPDATE \"{SUPABASE_TABLE_NAME}\" SET enhanced = 'Y' "
                        f"WHERE id::text = ANY(ARRAY[:ids_list])"
                    )
                    session.execute(update_true_stmt, {"ids_list": ids_to_mark_enhanced})
                    session.commit()
                    logging.info(f"Updated {len(ids_to_mark_enhanced)} records in Supabase to enhanced = Y.")
                else:
                    logging.info("No Supabase records needed to be marked as enhanced (all B2 files already marked or not in DB).")

            logging.info("Synchronization complete.")

        except Exception as e:
            session.rollback()
            logging.error(f"Error during Supabase synchronization: {e}", exc_info=True)
            logging.error("Transaction rolled back.")

def main():
    """
    Main function to orchestrate the download status synchronization.
    """
    logging.info("Starting Lassa Fever Report Enhanced Status Synchronizer...")

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

    b2_report_files = get_b2_report_filenames(B2_REPORTS_PREFIX, ".png")
    
    # Proceed with sync even if b2_report_files is empty; sync_download_status handles this.
    sync_enhanced_status(engine, b2_report_files)
    
    logging.info("Lassa Fever Report Enhanced Status Synchronizer finished.")



if __name__ == "__main__":
    main()