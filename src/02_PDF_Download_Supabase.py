#!/usr/bin/env python3
"""
02_PDF_Download_Supabase.py: Lassa Fever Report Download Status Synchronizer

This script synchronizes the 'downloaded' status of Lassa fever reports
between a Backblaze B2 bucket and a Supabase 'website_data' table.

It performs the following actions:
1.  Fetches a list of PDF filenames from the specified B2 bucket directory.
2.  Connects to the Supabase database.
3.  Compares the B2 file list against the 'website_data' table:
    a.  If a Supabase record has 'downloaded' as Y but its 'new_name' is NOT in B2,
        updates 'downloaded' to N in Supabase.
    b.  If a file's 'new_name' IS in B2 but its corresponding Supabase record has
        'downloaded' as N or NULL, updates 'downloaded' to Y in Supabase.

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
from typing import Set, List, Tuple

from sqlalchemy import text, update
from sqlalchemy.orm import Session

# Attempt to import utility functions, supporting both direct and main.py execution
try:
    from utils.cloud_storage import get_b2_file_list
    from utils.db_utils import get_db_engine
    from utils.logging_config import configure_logging
    from utils.cloud_storage import get_b2_report_filenames 
except ImportError:
    # This fallback is for when the script is run from the project root as part of main.py
    from src.utils.cloud_storage import get_b2_file_list
    from src.utils.db_utils import get_db_engine
    from src.utils.logging_config import configure_logging
    from src.utils.cloud_storage import get_b2_report_filenames

# Configure logging
configure_logging()

# --- Configuration -----------------------------------------
SUPABASE_TABLE_NAME = 'website_data'
DATABASE_URL = os.environ.get("DATABASE_URL")

# Define the B2 prefix where the report PDFs are stored.
# Example: "lassa-reports/data/raw/year/"
# This should be the path part *before* the actual filename in B2.
B2_REPORTS_PREFIX = "lassa-reports/data/raw/year/"
# Ensure the prefix ends with a slash if it's not empty and not just '/'
if B2_REPORTS_PREFIX and B2_REPORTS_PREFIX != '/' and not B2_REPORTS_PREFIX.endswith('/'):
    B2_REPORTS_PREFIX += '/'
# --- End Configuration -------------------------------------

def sync_download_status(engine, b2_filenames: Set[str]):
    """
    Synchronizes the 'downloaded' status in the Supabase 'website_data' table
    with the list of filenames found in B2.

    Args:
        engine: SQLAlchemy engine for database connection.
        b2_filenames (Set[str]): A set of 'new_name' filenames found in B2.
    """
    
    with Session(engine) as session:
        try:
            # 1. Sync Supabase to B2 (identify files marked downloaded in DB but NOT in B2)
            logging.info("Step 1: Syncing Supabase -> B2 (marking DB entries as NOT downloaded if not in B2)...")
            # Using 'downloaded' as text column with value 'Y' instead of boolean
            # Only check records where year >= 20
            stmt_select_downloaded_db = text(f"SELECT id::text, new_name FROM \"{SUPABASE_TABLE_NAME}\" WHERE downloaded = 'Y' AND (year >= 20 OR year >= '20') AND compatible != 'N'")
            downloaded_in_db = session.execute(stmt_select_downloaded_db).fetchall()
            
            ids_to_mark_not_downloaded: List[str] = []
            for row_id_text, new_name in downloaded_in_db:
                if new_name not in b2_filenames:
                    ids_to_mark_not_downloaded.append(row_id_text)
                    logging.info(f"File '{new_name}' (ID: {row_id_text}) is 'downloaded' in DB but not in B2. Queueing to mark as N.")

            if ids_to_mark_not_downloaded:
                # Using text() for table name to handle potential quoting needs
                # Using = ANY(ARRAY[:ids_list]::uuid[]) for UUIDs is generally robust
                # Ensure your 'id' column is indeed UUID or adjust cast and array type accordingly
                update_false_stmt = text(
                    f"UPDATE \"{SUPABASE_TABLE_NAME}\" SET downloaded = 'N' "
                    f"WHERE id = ANY(ARRAY[:ids_list]::uuid[])"
                )
                session.execute(update_false_stmt, {"ids_list": ids_to_mark_not_downloaded})
                session.commit()
                logging.info(f"Updated {len(ids_to_mark_not_downloaded)} records in Supabase to downloaded = N.")
            else:
                logging.info("No Supabase records needed to be marked as not downloaded (all align with B2 or none were Y).")

            # 2. Sync B2 to Supabase (identify files in B2 but NOT marked downloaded in DB)
            logging.info("Step 2: Syncing B2 -> Supabase (marking DB entries as DOWNLOADED if present in B2 and not already marked)...")
            if not b2_filenames: 
                 logging.info("B2 filename list is empty, skipping B2 to Supabase sync (no files to mark as downloaded).")
            else:
                # Create a list of SQL-safe string literals for the IN clause
                b2_filenames_sql_list = [f"'{name.replace("'", "''")}'" for name in b2_filenames]
                
                stmt_select_not_downloaded_for_b2_files = text(
                    f"SELECT id::text, new_name FROM \"{SUPABASE_TABLE_NAME}\" "
                    f"WHERE new_name IN ({', '.join(b2_filenames_sql_list)}) "
                    f"AND (downloaded = 'N' OR downloaded IS NULL) "
                    f"AND (year >= 20 OR year >= '20')"
                )
                
                to_mark_downloaded_db = session.execute(stmt_select_not_downloaded_for_b2_files).fetchall()
                
                ids_to_mark_downloaded: List[str] = []
                for row_id_text, new_name in to_mark_downloaded_db:
                    ids_to_mark_downloaded.append(row_id_text)
                    logging.info(f"File '{new_name}' (ID: {row_id_text}) is in B2 but not 'downloaded' in DB. Queueing to mark as Y.")

                if ids_to_mark_downloaded:
                    update_true_stmt = text(
                        f"UPDATE \"{SUPABASE_TABLE_NAME}\" SET downloaded = 'Y' "
                        f"WHERE id = ANY(ARRAY[:ids_list]::uuid[])"
                    )
                    session.execute(update_true_stmt, {"ids_list": ids_to_mark_downloaded})
                    session.commit()
                    logging.info(f"Updated {len(ids_to_mark_downloaded)} records in Supabase to downloaded = Y.")
                else:
                    logging.info("No Supabase records needed to be marked as downloaded (all B2 files already marked or not in DB).")

            logging.info("Synchronization complete.")

        except Exception as e:
            session.rollback()
            logging.error(f"Error during Supabase synchronization: {e}", exc_info=True)
            logging.error("Transaction rolled back.")


def download_pdfs(engine):
    """
    Download PDFs for Supabase records that haven't been downloaded yet.
    
    Queries Supabase for records where:
    - downloaded is 'N' or NULL
    - link is not 'wrong'
    - Recovered is not 'N'
    
    Downloads the PDFs and updates the downloaded status to 'Y' for successful downloads.
    
    Args:
        engine: SQLAlchemy engine for database connection.
    """
    # Define base paths for PDF storage
    BASE_DIR = Path(__file__).parent.parent
    PDF_FOLDER = BASE_DIR / 'data' / 'raw' / 'downloaded'
    PDF_FOLDER.mkdir(parents=True, exist_ok=True)
    DEST_FOLDER = BASE_DIR / 'data' / 'raw' / 'year'
    DEST_FOLDER.mkdir(parents=True, exist_ok=True)
    
    logging.info("Starting PDF download process...")
    
    # Import requests here to avoid importing it if this function is not called
    import requests
    
    with Session(engine) as session:
        try:
            # Query for records that need to be downloaded
            # Only include PDFs where year >= 20
            stmt_select_to_download = text(
                f"SELECT id::text, link, download_name, new_name, year, compatible "
                f"FROM \"{SUPABASE_TABLE_NAME}\" "
                f"WHERE (downloaded = 'N' OR downloaded IS NULL) "
                f"AND (link != 'wrong' AND link IS NOT NULL AND link != '') "
                f"AND (recovered != 'N' OR recovered IS NULL) "
                f"AND (year >= 20 OR year >= '20')"
            )
            
            to_download = session.execute(stmt_select_to_download).fetchall()
            
            if not to_download:
                logging.info("No new PDFs to download.")
                return
                
            logging.info(f"Found {len(to_download)} PDFs to download.")
            
            # Track successfully downloaded files
            downloaded_ids = []
            
            # Process each record
            for row_id, link, download_name, new_name, year, compatible in to_download:
                if not download_name or not link:
                    logging.warning(f"Missing download_name or link for record ID: {row_id}. Skipping.")
                    continue
                    
                # Define file paths
                pdf_path = PDF_FOLDER / download_name
                
                # Only prepare destination path if Compatible is not 'N'
                should_copy_to_year_folder = compatible != 'N'
                dest_path = None
                
                if should_copy_to_year_folder:
                    # Create year folder if needed
                    if year and str(year).isdigit():
                        year_folder = DEST_FOLDER / str(year)
                        year_folder.mkdir(parents=True, exist_ok=True)
                        dest_path = year_folder / new_name
                    else:
                        # If year is invalid, use an 'unknown' folder
                        unknown_folder = DEST_FOLDER / 'unknown'
                        unknown_folder.mkdir(parents=True, exist_ok=True)
                        dest_path = unknown_folder / new_name
                
                try:
                    # Download the file
                    logging.info(f"Downloading {download_name} from {link}...")
                    response = requests.get(link)
                    
                    if response.status_code == 200:
                        # Save to download folder
                        with open(pdf_path, 'wb') as pdf_file:
                            pdf_file.write(response.content)
                            
                        # Copy to year folder with new name only if Compatible is not 'N'
                        if should_copy_to_year_folder and new_name and dest_path:
                            with open(dest_path, 'wb') as dest_file:
                                dest_file.write(response.content)
                                logging.info(f"Copied {download_name} to {dest_path}")
                        elif not should_copy_to_year_folder:
                            logging.info(f"Skipped copying to year folder for {download_name} (Compatible='N')")
                                
                        # Mark as successfully downloaded
                        downloaded_ids.append(row_id)
                        logging.info(f"Successfully downloaded {download_name} to {pdf_path}")
                    else:
                        logging.error(f"Failed to download PDF from {link}: HTTP {response.status_code}")
                except Exception as e:
                    logging.error(f"Error downloading from {link}: {e}")
            
            # Update downloaded status for successful downloads
            if downloaded_ids:
                update_stmt = text(
                    f"UPDATE \"{SUPABASE_TABLE_NAME}\" SET downloaded = 'Y' "
                    f"WHERE id = ANY(ARRAY[:ids_list]::uuid[])"
                )
                session.execute(update_stmt, {"ids_list": downloaded_ids})
                session.commit()
                logging.info(f"Updated {len(downloaded_ids)} records in Supabase to downloaded = 'Y'.")
        
        except Exception as e:
            session.rollback()
            logging.error(f"Error during PDF download process: {e}", exc_info=True)
            logging.error("Transaction rolled back.")


def main():
    """
    Main function to orchestrate the download status synchronization.
    """
    logging.info("Starting Lassa Fever Report Download Status Synchronizer...")

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

    b2_report_files = get_b2_report_filenames(B2_REPORTS_PREFIX, ".pdf")
    
    # Proceed with sync even if b2_report_files is empty; sync_download_status handles this.
    sync_download_status(engine, b2_report_files)
    
    # Download any new PDFs that need to be downloaded
    download_pdfs(engine)

    logging.info("Lassa Fever Report Download Status Synchronizer finished.")



if __name__ == "__main__":
    main()