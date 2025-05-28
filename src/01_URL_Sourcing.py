#!/usr/bin/env python3
"""
URL_Sourcing.py: Nigeria Lassa Fever Report URL Scraper and Metadata Manager (Supabase Version)

This script scrapes the Nigeria Centre for Disease Control (NCDC) website for
Lassa fever outbreak reports, extracts metadata, and manages file status information
by interacting directly with a Supabase 'website_data' table.

The script:
1. Connects to a Supabase PostgreSQL database.
2. Fetches existing report identifiers from the 'website_data' table.
3. Scrapes the NCDC website for Lassa fever reports.
4. Standardizes file naming conventions and extracts metadata.
5. Inserts new, unique report information into the 'website_data' table.
6. (Future steps will include updating records based on a file_status.csv)

Usage:
    python 01_URL_Sourcing.py (ensure DATABASE_URL environment variable is set)

Output:
    - Inserts new report metadata into the Supabase 'website_data' table.
    - Logs processing status and errors.

Dependencies:
    - requests, BeautifulSoup4, pathlib, pandas, SQLAlchemy, psycopg2-binary
"""
import os
import csv
import requests
import certifi
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import logging
import time
import cloudscraper
from pathlib import Path
from bs4 import BeautifulSoup
import pandas as pd
# Handle imports for both standalone execution and execution from main.py
try:
    from utils.data_validation import add_uuid_column, rename_lassa_file
    from utils.db_utils import get_db_engine, push_data_with_upsert, safe_convert_to_int
except ImportError:
    from src.utils.data_validation import add_uuid_column, rename_lassa_file
    from src.utils.db_utils import get_db_engine, push_data_with_upsert, safe_convert_to_int
from sqlalchemy import text

# Import centralized logging configuration
try:
    from utils.logging_config import configure_logging
except ImportError:
    from src.utils.logging_config import configure_logging
# Configure logging
configure_logging()

# --- Supabase Configuration ------------------------------
SUPABASE_TABLE_NAME = 'website_data' 
DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    logging.error("CRITICAL: DATABASE_URL environment variable not set.")
    # Raise an exception instead of calling exit(1) directly
    # This allows the main.py error handling to catch it
    raise EnvironmentError("DATABASE_URL environment variable not set")
try:
    engine = get_db_engine(DATABASE_URL)
    # Test connection
    with engine.connect() as connection:
        logging.info("Successfully connected to Supabase.")
except Exception as e:
    logging.error(f"CRITICAL: Failed to create SQLAlchemy engine or connect to Supabase: {e}")
    exit(1)
# --- End Supabase Configuration ----------------------------

# Define base paths if still needed for file_status.csv or downloaded_dir check by other functions
BASE_DIR = Path(__file__).parent.parent
documentation_dir = BASE_DIR / 'data' / 'documentation'

# Ensure documentation_dir exists if file_status.csv is read from there later
documentation_dir.mkdir(parents=True, exist_ok=True)

# Website
base_url = "https://ncdc.gov.ng"
list_page_url = f"{base_url}/diseases/sitreps/?cat=5&name=An%20update%20of%20Lassa%20fever%20outbreak%20in%20Nigeria"

import logging
import re
def fetch_with_cloudscraper(url: str) -> str:
    """
    Fetch HTML using cloudscraper, which solves Cloudflare IUAM challenges automatically.
    Proxy settings are read from environment variables HTTP_PROXY and HTTPS_PROXY.
    """
    # Build proxies dict if environment variables are set
    proxy = os.getenv('HTTP_PROXY') or os.getenv('HTTPS_PROXY')
    proxies = {'http': proxy, 'https': proxy} if proxy else None

    scraper = cloudscraper.create_scraper()  # returns a CloudScraper instance
    resp = scraper.get(url, proxies=proxies)
    resp.raise_for_status()
    return resp.text

def save_raw_website_data(soup, db_engine):
    """
    Extracts Lassa fever report data from NCDC website HTML soup,
    compares with existing 'new_name' entries in Supabase, and inserts new unique reports.
    """
    try:
        with db_engine.connect() as connection:
            existing_new_names = {row[0] for row in connection.execute(
                text(f"SELECT new_name FROM {SUPABASE_TABLE_NAME} WHERE new_name IS NOT NULL")
            )}
            logging.info(f"Fetched {len(existing_new_names)} existing report 'new_name's from Supabase.")
            existing_downloads = {row[0] for row in connection.execute(
                text(f"SELECT download_name FROM {SUPABASE_TABLE_NAME} WHERE download_name IS NOT NULL")
            )}
            logging.info(f"Fetched {len(existing_downloads)} existing report 'download_name's from Supabase.")
    except Exception as e:
        logging.error(f"Error fetching existing report names from Supabase: {e}")
        logging.warning("Proceeding without knowledge of existing reports. Duplicates might occur if this issue persists.")
    
    table_body = soup.find("tbody")
    if not table_body:
        logging.error("Could not find <tbody> on the page. Cannot parse reports.")
        return

    rows = table_body.find_all("tr")
    if not rows:
        logging.info("No table rows found in <tbody>. No reports to process.")
        return

    new_reports_to_insert = []
    for row_idx, html_row in enumerate(rows):
        cells = html_row.find_all('td')
        if len(cells) >= 3:
            name_cell = cells[1].get_text(strip=True)
            link_tag = cells[2].find('a', href=True)
            if link_tag:
                href = link_tag.get('href', '')
                if href.startswith('/'): # Make URL absolute
                    href = f"{base_url}{href}"
                
                download_name_raw = link_tag.get('download', '')
                if not download_name_raw:
                    logging.warning(f"Row {row_idx+1}: Found link but no 'download' attribute. Link: {href}. Text: {link_tag.get_text(strip=True)}. Skipping.")
                    continue
                
                download_name = download_name_raw.replace(" ", "_") # Original filename for 'download_name'
                name_metadata = rename_lassa_file(download_name_raw) # Pass raw name for parsing
                
                if name_metadata.get('parse_error'):
                    logging.warning(f"Row {row_idx+1}: Skipping report due to parsing error for '{download_name_raw}'.")
                    continue

                current_new_name = name_metadata.get('full_name')

                if current_new_name in existing_new_names:
                    logging.debug(f"Report '{current_new_name}' already exists in Supabase. Skipping.")
                    continue

                if download_name in existing_downloads:
                    logging.debug(f"Download name '{download_name}' already exists in Supabase. Skipping.")
                    continue

                # Prepare data for Supabase, matching 'website_data' table columns
                report_data = {
                    'year': name_metadata.get('year'),           # bigint
                    'week': name_metadata.get('week'),           # bigint
                    'month': name_metadata.get('month'),         # double precision
                    'name': name_cell,                           # text (title from website)
                    'download_name': download_name,              # text (original filename)
                    'new_name': current_new_name,                # text (standardized filename, unique constraint)
                    'link': href,                                # text
                    # Initialize other fields to None or default as per schema
                    'broken_link': None, # Or 'N' / FALSE if preferred default
                    'downloaded': None,
                    'compatible': None,
                    'recovered': None,
                    'processed': None,
                    'enhanced': None,
                    'enhanced_name': None,
                    'combined': None
                }
                new_reports_to_insert.append(report_data)
                existing_new_names.add(current_new_name) # Add to set to avoid duplicates from same scrape batch
        else:
            logging.warning(f"Row {row_idx+1}: Did not find enough cells (expected >=3, got {len(cells)}). Skipping.")

    if new_reports_to_insert:
        try:
            df_new_reports = pd.DataFrame(new_reports_to_insert)
            df_new_reports = add_uuid_column(df_new_reports, id_column='id')
            
            # Use push_data_with_upsert for more robust inserting with conflict handling
            affected_rows = push_data_with_upsert(
                engine=engine,
                df=df_new_reports,
                table_name=SUPABASE_TABLE_NAME,
                conflict_cols=['new_name']
            )
            
            logging.info(f"Successfully inserted/updated {affected_rows} reports in Supabase table '{SUPABASE_TABLE_NAME}'.")
            
            # Print details of inserted/updated rows
            for _, row in df_new_reports.iterrows():
                logging.info(f"  Inserted/Updated: {row['new_name']} (Year: {row['year']}, Week: {row['week']})")
        except Exception as e:
            logging.error(f"Error inserting new reports into Supabase: {e}")
            logging.error("Data for new reports not saved:")
            for rep_data in new_reports_to_insert:
                logging.error(f"  {rep_data.get('new_name')}")
    else:
        logging.info("No new unique reports found on the NCDC website to add to Supabase.")

def process_file_status_update(db_engine):
    """
    Process file_status.csv and update the Supabase 'website_data' table accordingly.
    Handles 'wrong_link', 'missing_row', 'Corrupted', and 'Missing' statuses.
    Uses DataFrames and push_data_with_upsert for consistency instead of direct SQL.
    """
    file_status_path = documentation_dir / 'file_status.csv'

    if not file_status_path.exists():
        logging.info(f"{file_status_path} not found. Skipping status updates from CSV.")
        return

    try:
        with open(file_status_path, 'r', newline='', encoding='utf-8') as fs_file:
            fs_reader = csv.DictReader(fs_file)
            file_status_rows = list(fs_reader)
    except Exception as e:
        logging.error(f"Error reading {file_status_path}: {e}")
        return

    updated_count = 0
    inserted_count = 0
    processed_fs_rows = 0
    
    # Process each row in the file_status.csv
    for fs_row in file_status_rows:
        processed_fs_rows += 1
        try:
            # Extract data from the row
            note = fs_row.get('Notes', '').strip()
            status = fs_row.get('Status', '').strip()
            fs_year_str = fs_row.get('Year', '').strip() # e.g., "2023"
            fs_week_str = fs_row.get('Week', '').strip() # e.g., "1" or "W1"
            fs_month_str = fs_row.get('Month', '').strip() # e.g., "1" or "Jan"
            fs_old_name = fs_row.get('old_name', '').strip()
            fs_new_name = fs_row.get('new_name', '').strip()
            fs_correct_link = fs_row.get('correct_link', '').strip()

            fs_year_int = safe_convert_to_int(fs_year_str, 'Year')
            fs_month_int = safe_convert_to_int(fs_month_str, 'Month')
            fs_week_int = safe_convert_to_int(fs_week_str, 'Week', 'W')
            
            if any(val is None for val in [fs_year_int, fs_week_int] if val != ''):
                continue

            if note == 'wrong_link' and status == 'Found':
                if fs_year_int is None or fs_week_int is None:
                    logging.warning(f"Skipping 'wrong_link' for {fs_row} due to missing year/week.")
                    continue
                
                # First, check if the record exists
                with db_engine.connect() as check_conn:
                    where_clause = "year = :p_year AND week = :p_week"
                    params = {'p_year': fs_year_int, 'p_week': fs_week_int}
                    
                    if fs_old_name:
                        where_clause += " OR download_name = :p_old_name"
                        params['p_old_name'] = fs_old_name
                        
                    check_stmt = text(f"SELECT id, year, week FROM {SUPABASE_TABLE_NAME} WHERE {where_clause} LIMIT 1")
                    record = check_conn.execute(check_stmt, params).fetchone()
                
                if record:
                    # Create update data
                    update_data = {
                        'id': record[0],  # Preserve the existing ID
                        'year': fs_year_int,
                        'week': fs_week_int,
                        'month': fs_month_int,
                        'broken_link': 'Y',
                        'recovered': 'Y'
                    }
                    
                    # Add optional fields
                    if fs_new_name:
                        update_data['new_name'] = fs_new_name
                    if fs_correct_link:
                        update_data['link'] = fs_correct_link
                    
                    # Create DataFrame and use push_data_with_upsert
                    df_update = pd.DataFrame([update_data])
                    
                    # Use push_data_with_upsert for consistent handling
                    rows_affected = push_data_with_upsert(
                        engine=db_engine,
                        df=df_update,
                        table_name=SUPABASE_TABLE_NAME,
                        conflict_cols=['id']
                    )
                    
                    if rows_affected > 0:
                        updated_count += rows_affected
                        logging.debug(f"'wrong_link' status applied for Y{fs_year_str} W{fs_week_str}. Rows affected: {rows_affected}")

            elif note == 'missing_row':
                if fs_year_int is None or fs_week_int is None:
                    logging.warning(f"Skipping 'missing_row' {fs_row} due to missing year/week.")
                    continue
                        
                # Check if record already exists without raw SQL
                with db_engine.connect() as check_conn:
                    check_stmt = text(f"SELECT 1 FROM {SUPABASE_TABLE_NAME} WHERE year = :p_year AND week = :p_week LIMIT 1")
                    exists_result = check_conn.execute(check_stmt, {'p_year': fs_year_int, 'p_week': fs_week_int}).fetchone()
                    
                if not exists_result:
                   
                    # Create a standardized new_name if one wasn't provided
                    if not fs_new_name:
                        fs_new_name = f"Nigeria_XX_XXX_{str(fs_year_int)[-2:]}_W{str(fs_week_int).zfill(2)}_recovered.pdf"

                    # Prepare insert data
                    insert_data = {
                        'year': fs_year_int,
                        'week': fs_week_int,
                        'month': fs_month_int,
                        'name': f"An update of Lassa fever outbreak in Nigeria for Week {fs_week_int}", # Generic name
                        'download_name': fs_old_name if fs_old_name else 'unknown_original_source.pdf',
                        'new_name': fs_new_name,
                        'link': fs_correct_link if fs_correct_link else None,
                        'broken_link': 'N',
                        'recovered': 'Y'
                    }
                    
                    # Create DataFrame and use push_data_with_upsert for consistent handling
                    df_insert = pd.DataFrame([insert_data])
                    # Add UUID using the shared utility
                    df_insert = add_uuid_column(df_insert, id_column='id')
                    
                    # Use push_data_with_upsert for better handling and consistency
                    # Use 'new_name' as conflict column since it has a unique constraint
                    rows_affected = push_data_with_upsert(
                        engine=db_engine,
                        df=df_insert,
                        table_name=SUPABASE_TABLE_NAME,
                        conflict_cols=['new_name']
                    )
                    
                    if rows_affected > 0:
                        inserted_count += rows_affected
                        logging.debug(f"'missing_row' inserted for Y{fs_year_str} W{fs_week_str}.")

            elif status == 'Corrupted' or status == 'Missing':
                if fs_year_int is None or fs_week_int is None:
                    logging.warning(f"Skipping '{status}' for {fs_row} due to missing year/week.")
                    continue
                    
                # First, check if the record exists
                with db_engine.connect() as check_conn:
                    check_stmt = text(f"SELECT id, year, week FROM {SUPABASE_TABLE_NAME} WHERE year = :p_year AND week = :p_week LIMIT 1")
                    record = check_conn.execute(check_stmt, {'p_year': fs_year_int, 'p_week': fs_week_int}).fetchone()
                
                if record:
                    # Create update data based on status
                    update_data = {'id': record[0]}  # Preserve the existing ID
                    
                    if status == 'Corrupted':
                        update_data['compatible'] = 'N'
                    elif status == 'Missing':
                        update_data['broken_link'] = 'Y'
                        update_data['recovered'] = 'N'
                        update_data['downloaded'] = 'N'
                    
                    # Create DataFrame and use push_data_with_upsert
                    df_update = pd.DataFrame([update_data])
                    
                    # Use push_data_with_upsert for consistent handling
                    rows_affected = push_data_with_upsert(
                        engine=db_engine,
                        df=df_update,
                        table_name=SUPABASE_TABLE_NAME,
                        conflict_cols=['id']
                    )
                    
                    if rows_affected > 0:
                        updated_count += rows_affected
                        logging.debug(f"'{status}' status applied for Y{fs_year_str} W{fs_week_str}. Rows affected: {rows_affected}")
            
        except Exception as e_row:
            logging.error(f"Error processing row in file_status.csv: {fs_row}. Error: {e_row}")

    logging.info(f"Processed {processed_fs_rows} rows from file_status.csv.")
    if updated_count > 0:
        logging.info(f"Updated {updated_count} records in Supabase based on file_status.csv.")
    if inserted_count > 0:
        logging.info(f"Inserted {inserted_count} new records into Supabase based on file_status.csv.")
    if updated_count == 0 and inserted_count == 0 and processed_fs_rows > 0:
        logging.info("No records in Supabase were changed based on file_status.csv (either no matches or data was already consistent).")

def main():
    """
    Main entry point for the script.
    Fetches NCDC website, scrapes report data, and saves/updates to Supabase.
    Then processes file_status.csv to further update Supabase records.
    """
    logging.info(f"Starting 01_URL_Sourcing script...")
    logging.info(f"Attempting to fetch NCDC list page: {list_page_url}")
    try:
        # Using cloudscraper to bypass Cloudflare protection
        logging.info("Fetching NCDC page with cloudscraper...")
        
        try:
            html_content = fetch_with_cloudscraper(list_page_url)
            logging.info("Successfully fetched page using cloudscraper")
        except Exception as e:
            logging.error(f"Failed to fetch page with cloudscraper: {e}")
            raise requests.exceptions.RequestException(f"Cloudscraper fetch failed: {e}")

        soup_content = BeautifulSoup(html_content, "html.parser")
        logging.info("Successfully parsed NCDC page content.")

        # Save the HTML content for debugging in case of future issues
        try:
            debug_dir = BASE_DIR / 'data' / 'debug'
            debug_dir.mkdir(parents=True, exist_ok=True)
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            debug_file = debug_dir / f"ncdc_page_{timestamp}.html"
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logging.info(f"Saved debug HTML content to {debug_file}")
        except Exception as e:
            logging.warning(f"Failed to save debug HTML: {e}")
            # Continue with processing even if debug save fails

        save_raw_website_data(soup_content, engine) # Scrape and save new entries
        process_file_status_update(engine)      # Update based on file_status.csv

    except requests.exceptions.Timeout:
        logging.error(f"Timeout while trying to fetch NCDC page: {list_page_url}")
    except requests.exceptions.RequestException as e_req:
        logging.error(f"Failed to fetch NCDC page due to network error: {e_req}")
    except Exception as e_main:
        logging.error(f"An unexpected error occurred in main execution: {e_main}", exc_info=True)
    finally:
        logging.info("01_URL_Sourcing script finished.")

if __name__ == "__main__":
    main()