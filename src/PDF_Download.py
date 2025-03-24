#!/usr/bin/env python3
"""
PDF_Download.py: Nigeria Lassa Fever Report Downloader and Organizer

This script downloads Lassa fever outbreak reports from the Nigeria Centre for Disease Control (NCDC)
and organizes them by year. It works in conjunction with URL_Sourcing.py which scrapes the URLs and
metadata for these reports.

The script:
1. Reads metadata from website_raw_data.csv containing report URLs and filenames
2. Downloads PDF reports that haven't been downloaded yet
3. Updates the CSV to track download status
4. Organizes downloaded PDFs into folders by year for easier access
5. Uses standardized naming conventions for files

Usage:
    python PDF_Download.py

Output:
    - Downloads PDF files to data/raw/downloaded/
    - Organizes compatible PDFs to data/raw/year/{YEAR}/
    - Updates website_raw_data.csv with download status

Dependencies:
    - requests: For HTTP requests
    - pathlib: For file path management
    - csv: For CSV file operations
    - shutil: For file operations
"""
import os
import re
import csv
import shutil
import requests
import logging
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup, Comment

class NewlineLoggingHandler(logging.StreamHandler):
    """Custom logging handler that adds a newline after each log entry."""
    def emit(self, record):
        super().emit(record)
        self.stream.write('\n')
        self.flush()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s', handlers=[NewlineLoggingHandler()])

# Define base paths and constants
BASE_DIR = Path(__file__).parent.parent
CSV_FILE = BASE_DIR / 'data' / 'documentation' / 'website_raw_data.csv'
PDF_FOLDER = BASE_DIR / 'data' / 'raw' / 'downloaded'
PDF_FOLDER.mkdir(parents=True, exist_ok=True)
DEST_FOLDER = BASE_DIR / 'data' / 'raw' / 'year'

def get_fieldnames():
    """
    Dynamically retrieve column headers from the CSV file.
    
    Returns:
        list: List of fieldnames (column headers) from the CSV file
    """
    with open(CSV_FILE, 'r', newline='') as csvfile:
        return csvfile.readline().strip().split(',')

FIELDNAMES = get_fieldnames()

def download_pdf(pdf_url, download_path):
    """
    Download a PDF file from a given URL and save it to the specified path.
    
    Args:
        pdf_url (str): URL of the PDF to download
        download_path (Path): Destination path where the PDF will be saved
        
    Returns:
        bool: True if download was successful, False otherwise
    """
    logging.info(f"Downloading {pdf_url} -> {download_path.name}")
    try:
        response = requests.get(pdf_url)
        response.raise_for_status()
        download_path.write_bytes(response.content)
        return True
    except Exception as e:
        logging.error(f"Failed to download {pdf_url}: {e}")
        return False


def download_lassa_pdfs():
    """
    Download Lassa fever PDFs based on metadata in the CSV file.
    
    Reads website_raw_data.csv, downloads PDFs that haven't been downloaded yet,
    and updates the 'Downloaded' status in the CSV. Skips files with invalid links
    or those marked as not recoverable.
    
    Returns:
        None: Updates are written to the CSV file and files are downloaded to PDF_FOLDER
    """
    # Read CSV and get dynamic fieldnames
    with open(CSV_FILE, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        fieldnames = reader.fieldnames
        rows = list(reader)

    # Ensure PDF_FOLDER exists
    PDF_FOLDER.mkdir(parents=True, exist_ok=True)

    # Track if any files were downloaded
    files_downloaded = False

    # Process each row
    for row in rows:
        link = row.get('link', '').strip()
        file_name = row.get('download_name')
        pdf_path = PDF_FOLDER / file_name
        # If PDF file does not exist and 'Downloaded' is 'Y', reset 'Downloaded' to ''
        if not pdf_path.exists() and row.get('Downloaded', '').strip() == 'Y':
            row['Downloaded'] = ''
        # Only process if link is not 'wrong' and not empty and not already downloaded
        if link and link.lower() != 'wrong' and row.get('Downloaded', '').strip() != 'Y' and row.get('Recovered', '').strip() != 'N':
            try:
                response = requests.get(link)
                if response.status_code == 200:
                    # Use the file name from the 'download_name' column (assumed to be the intended 'old_name')
                    if file_name:
                        with open(pdf_path, 'wb') as pdf_file:
                            pdf_file.write(response.content)
                        row['Downloaded'] = 'Y'
                        logging.info(f"Downloaded {file_name}")
                        files_downloaded = True
                    else:
                        logging.warning("No download_name provided for row:", row)
                else:
                    logging.error(f"Failed to download PDF from {link}: HTTP {response.status_code}")
            except Exception as e:
                logging.error(f"Error downloading from {link}: {e}")

    # If no files were downloaded, log that information
    if not files_downloaded:
        logging.info("No new files to download")

    # Write updated rows back to CSV
    with open(CSV_FILE, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def organize_pdfs_by_year():
    """
    Organize downloaded PDFs into year-based folders.
    
    Copies PDFs from the download folder to year-specific folders based on the 'year' field
    in the CSV. Only processes files marked as 'Downloaded' and not marked as incompatible.
    Files are renamed according to the standardized naming convention in 'new_name'.
    
    Returns:
        None: Files are copied to year-specific folders under DEST_FOLDER
    """
    # Read CSV to obtain rows
    with open(CSV_FILE, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)
    # Sort rows by year and week in descending order
    rows.sort(key=lambda x: (
        int(x.get('year', '0') or '0'),
        int(x.get('week', '0') or '0')
    ), reverse=True)

    # Process each row
    for row in rows:
        # Only process if Compatible is not 'N' and PDF was downloaded
        if row.get('Downloaded', '').strip() == 'Y' and row.get('Compatible', '').strip() != 'N':
            year = row.get('year', 'unknown').strip()
            file_name_old = row.get('download_name')
            file_name_new = row.get('new_name')
            if file_name_old:
                src_file = PDF_FOLDER / file_name_old
                # Destination folder for the specific year
                year_folder = DEST_FOLDER / year
                year_folder.mkdir(parents=True, exist_ok=True)
                dest_file = year_folder / file_name_new
                if src_file.exists() and not dest_file.exists():
                    shutil.copy(src_file, dest_file)
                    logging.info(f"Copied {file_name_old} to {year_folder}")
                elif not src_file.exists():
                    logging.warning(f"Source file does not exist: {src_file}")
            else:
                logging.warning(f"No download_name provided for row: {row}")


def main():
    """
    Main function to execute the PDF download and organization process.
    
    First downloads any missing PDFs, then organizes them by year.
    
    Returns:
        None
    """
    download_lassa_pdfs()
    organize_pdfs_by_year()

if __name__ == "__main__":
    main()