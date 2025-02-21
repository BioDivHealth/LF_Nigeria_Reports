#!/usr/bin/env python3
import os
import re
import csv
import shutil
import requests
import logging
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup, Comment

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Define base paths and constants
BASE_DIR = Path(__file__).parent.parent
CSV_FILE = BASE_DIR / 'data' / 'documentation' / 'website_raw_data.csv'
PDF_FOLDER = BASE_DIR / 'data' / 'raw' / 'downloaded'
PDF_FOLDER.mkdir(parents=True, exist_ok=True)
DEST_FOLDER = BASE_DIR / 'data' / 'raw' / 'year'

# Dynamically generate fieldnames from CSV header

def get_fieldnames():
    with open(CSV_FILE, 'r', newline='') as csvfile:
        return csvfile.readline().strip().split(',')

FIELDNAMES = get_fieldnames()

def load_csv_records(csv_path, fieldnames):
    if not os.path.exists(csv_path):
        ensure_csv(csv_path, fieldnames)
        return []
    with open(csv_path, 'r', newline='') as csvfile:
        return list(csv.DictReader(csvfile))

def download_pdf(pdf_url, download_path):
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
    """Dynamically loads CSV header for fieldnames, downloads PDFs where link is valid, saves files using download_name, and updates CSV if download is successful."""
    # Read CSV and get dynamic fieldnames
    with open(CSV_FILE, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        fieldnames = reader.fieldnames
        rows = list(reader)

    # Ensure PDF_FOLDER exists
    PDF_FOLDER.mkdir(parents=True, exist_ok=True)

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
                        print(f"Downloaded {file_name}")
                    else:
                        print("No download_name provided for row:", row)
                else:
                    print(f"Failed to download PDF from {link}: HTTP {response.status_code}")
            except Exception as e:
                print(f"Error downloading from {link}: {e}")

    # Write updated rows back to CSV
    with open(CSV_FILE, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def organize_pdfs_by_year():
    """Copies downloaded PDFs into yearly folders if the CSV row's 'Compatible' field is not 'N'."""
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
                if src_file.exists():
                    shutil.copy(src_file, dest_file)
                    print(f"Copied {file_name_old} to {year_folder}")
                else:
                    print(f"Source file does not exist: {src_file}")
            else:
                print("No download_name provided for row:", row)


def main():
    download_lassa_pdfs()
    organize_pdfs_by_year()

if __name__ == "__main__":
    main()