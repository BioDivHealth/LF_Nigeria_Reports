#!/usr/bin/env python3
"""
URL_Sourcing.py: Nigeria Lassa Fever Report URL Scraper and Metadata Manager

This script scrapes the Nigeria Centre for Disease Control (NCDC) website for 
Lassa fever outbreak reports, extracts metadata, and manages file status information.

The script:
1. Scrapes the NCDC website for Lassa fever reports
2. Standardizes file naming conventions
3. Extracts and organizes metadata (year, week, etc.) from report names
4. Updates a central CSV database of report information
5. Cross-references with file_status.csv to manage broken links, missing files, etc.
6. Tracks download status and file compatibility

Usage:
    python URL_Sourcing.py

Output:
    - Updates website_raw_data.csv with report metadata
    - Logs processing status and errors

Dependencies:
    - requests: For HTTP requests
    - BeautifulSoup4: For HTML parsing
    - pathlib: For file path management
    - csv: For CSV file operations
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

# Configure logging
class NewlineLoggingHandler(logging.StreamHandler):
    """Custom logging handler that adds a newline after each log entry."""
    def emit(self, record):
        super().emit(record)
        self.stream.write('\n')
        self.flush()

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s', handlers=[NewlineLoggingHandler()])

# Define base paths and constants
BASE_DIR = Path(__file__).parent.parent
CSV_FILE = BASE_DIR / 'data' / 'documentation' / 'website_raw_data.csv'
FIELDNAMES = ['year','week','month', 'name', 'download_name','new_name', 'link', 'Broken_Link', 'Downloaded', 'Compatible', 'Recovered', 'Processed']

# Create directories if they don't exist
documentation_dir = BASE_DIR / 'data' / 'documentation'
downloaded_dir = BASE_DIR / 'data' / 'raw' / 'downloaded'
documentation_dir.mkdir(parents=True, exist_ok=True)
downloaded_dir.mkdir(parents=True, exist_ok=True)

# Website 
base_url = "https://ncdc.gov.ng"
list_page_url = f"{base_url}/diseases/sitreps/?cat=5&name=An%20update%20of%20Lassa%20fever%20outbreak%20in%20Nigeria"
logging.info(f"Fetching list page: {list_page_url}")
response = requests.get(list_page_url)
response.raise_for_status()
soup = BeautifulSoup(response.text, "html.parser")

# Define function to rename Lassa fever report filenames and extract metadata
def rename_lassa_file(old_name):
    """
    Standardize Lassa fever report filenames and extract metadata.
    
    Takes original filenames from the NCDC website and converts them to a standardized format:
    Nigeria_DD_MMM_YY_WXX.pdf (e.g., Nigeria_01_Jan_22_W01.pdf)
    
    Args:
        old_name (str): Original filename from the NCDC website
        
    Returns:
        dict: Contains standardized filename and extracted metadata:
            - full_name: The standardized filename
            - month_name: Three-letter month abbreviation (Jan, Feb, etc.)
            - year: Two-digit year
            - month: Two-digit month number
            - week: Week number
            - day: Two-digit day of month
    """
    month_map = {
        "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
        "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
        "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec",
    }
    old_name = old_name.replace(" ", "_")
    parts = old_name.split("_")
    if len(parts) < 9:
        return {'full_name': old_name}
    date_str = parts[8]
    week_str = parts[9].replace(".pdf", "") if parts[9].endswith(".pdf") else ""
    if len(date_str) != 6:
        return {'full_name': old_name}
    dd, mm, yy = date_str[:2], date_str[2:4], date_str[4:]
    month_name = month_map.get(mm, "???")
    full_name = f"Nigeria_{dd}_{month_name}_{yy}_W{week_str}.pdf"
    return {
        'full_name': full_name,
        'month_name': month_name, 
        'year': yy,
        'month': mm,
        'week': week_str,
        'day': dd,
    }

def save_raw_website_data(soup):
    """
    Extract Lassa fever report data from the NCDC website and save to CSV.
    
    Parses the HTML table from the NCDC situation reports page, extracts relevant
    metadata about each report, and saves it to website_raw_data.csv. Handles both
    creating a new CSV file and updating an existing one with new records.
    
    Args:
        soup (BeautifulSoup): BeautifulSoup object containing the parsed HTML from the NCDC website
        
    Returns:
        None: Results are written to the CSV_FILE and logged
    """
    raw_data_file = CSV_FILE
    raw_fieldnames = FIELDNAMES

    table_body = soup.find("tbody")
    if not table_body:
        logging.error("Could not find <tbody> on the page.")
        return

    rows = table_body.find_all("tr")
    if not rows:
        logging.error("No table rows found in <tbody>.")
        return

    new_rows = []
    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= 3:
            name_cell = cells[1].get_text(strip=True)
            link_tag = cells[2].find('a', href=True)
            if link_tag:
                href = link_tag.get('href', '')
                if href.startswith('/'):
                    href = f"https://ncdc.gov.ng{href}"
                download_name = link_tag.get('download', '')
                download_name = download_name.replace(" ", "_")
                new_name = rename_lassa_file(download_name)
                new_rows.append({
                    'year': new_name.get('year', ''),
                    'week': new_name.get('week', ''),
                    'month': new_name.get('month', ''),
                    'name': name_cell,
                    'download_name': download_name,
                    'new_name': new_name.get('full_name', download_name),
                    'link': href,
                    'Broken_Link': '',
                    'Downloaded': '',
                    'Compatible': '',
                    'Recovered': '',
                    'Processed': ''
                })

    if raw_data_file.exists():
        existing_combinations = set()
        existing_download_names = set()
        with open(raw_data_file, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for existing_row in reader:
                existing_combinations.add((existing_row.get('year', '').strip(), existing_row.get('week', '').strip()))
                existing_download_names.add(existing_row.get('download_name', '').strip())
        rows_to_append = [
            row for row in new_rows
            if (row.get('year', '').strip(), row.get('week', '').strip()) not in existing_combinations 
               and row.get('download_name', '').strip() not in existing_download_names
        ]
        if rows_to_append:
            with open(raw_data_file, 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=raw_fieldnames)
                writer.writerows(rows_to_append)
            logging.info(f"Appended {len(rows_to_append)} new rows to {raw_data_file}")
        else:
            logging.info("No new records to append.")
    else:
        with open(raw_data_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=raw_fieldnames)
            writer.writeheader()
            writer.writerows(new_rows)
        logging.info(f"Created file and saved raw website data to {raw_data_file}")

def process_file_status_update():
    """
    Process file_status.csv and update website_raw_data.csv accordingly.

    - For file_status rows where Notes == "wrong_link":
        Update matching rows in website_raw_data.csv (matched by Year and Week) by setting:
            'link' to "wrong" and 'Recovered' to "Y".

    - For file_status rows where Notes == "missing_row":
        If no row in website_raw_data.csv matches the Year and Week, append a new row with:
            'year': last two digits of Year from file_status,
            'week': Week from file_status,
            'name': "An update of Lassa fever outbreak in Nigeria for Week {Week}",
            'download_name': "none",
            'new_name': new_name from file_status,
            and other fields empty.
    """
    documentation_dir = BASE_DIR / 'data' / 'documentation'
    file_status_path = documentation_dir / 'file_status.csv'
    website_data_path = documentation_dir / 'website_raw_data.csv'

    # Load file_status.csv
    try:
        with open(file_status_path, 'r', newline='') as fs_file:
            fs_reader = csv.DictReader(fs_file)
            file_status_rows = list(fs_reader)
    except Exception as e:
        logging.error(f"Error reading {file_status_path}: {e}")
        return

    # Load website_raw_data.csv
    try:
        with open(website_data_path, 'r', newline='') as ws_file:
            ws_reader = csv.DictReader(ws_file)
            website_fieldnames = ws_reader.fieldnames
            website_rows = list(ws_reader)
    except Exception as e:
        logging.error(f"Error reading {website_data_path}: {e}")
        return

    rows_updated = []
    # Process file_status rows for both wrong_link and missing_row in one loop
    for fs in file_status_rows:
        note = fs.get('Notes', '').strip()
        status = fs.get('Status', '').strip()
        fs_year = fs.get('Year', '').strip()
        fs_week = fs.get('Week', '').strip()
        fs_year_last2 = fs_year[-2:] if len(fs_year) >= 2 else fs_year
        fs_old_name = fs.get('old_name', '').strip()
        fs_new_name = fs.get('new_name', '').strip()
        fs_correct_link = fs.get('correct_link', '').strip()

        if note == 'wrong_link' and status == 'Found':
            for row in website_rows:
                row_year = row.get('year', '').strip()
                row_week = row.get('week', '').strip()
                row_download_name = row.get('download_name', '').strip()

                if row_year == fs_year_last2 and row_week == fs_week or (row_download_name == fs_old_name): # This should take care of 2022/2023 instance of Week 53 isntead of W1
                    row['new_name'] = fs_new_name
                    row['Broken_Link'] = 'Y'
                    row['Recovered'] = 'Y'
                    row['year'] = fs_year_last2
                    row['week'] = fs_week
                    row['link'] = fs_correct_link
                    rows_updated.append(row)
        elif note == 'missing_row':
            exists = any(row.get('year', '').strip() == fs_year_last2 and row.get('week', '').strip() == fs_week for row in website_rows)
            if not exists:
                new_row = { key: '' for key in website_fieldnames }
                new_row['year'] = fs_year_last2
                new_row['week'] = fs_week
                new_row['name'] = f"An update of Lassa fever outbreak in Nigeria for Week {fs_week}"
                new_row['download_name'] =  fs.get('old_name', '').strip()
                new_row['link'] = fs.get('correct_link', '').strip()
                new_row['new_name'] = fs.get('new_name', '').strip()
                website_rows.append(new_row)
                rows_updated.append(new_row)
        elif status == 'Corrupted':
            for row in website_rows:
                row_year = row.get('year', '').strip()
                row_week = row.get('week', '').strip()
                if row_year == fs_year_last2 and row_week == fs_week:
                    row['Compatible'] = 'N'
                    rows_updated.append(row)
        elif status == 'Missing':
            for row in website_rows:
                row_year = row.get('year', '').strip()
                row_week = row.get('week', '').strip()
                if row_year == fs_year_last2 and row_week == fs_week:
                    row['Broken_Link'] = 'Y'
                    row['Recovered'] = 'N'
                    rows_updated.append(row)

    # New code: update 'Downloaded' field based on files available in the downloaded folder
    downloaded_dir = BASE_DIR / 'data' / 'raw' / 'downloaded'
    if downloaded_dir.exists():
        downloaded_files = {f.name for f in downloaded_dir.iterdir() if f.is_file()}
        for row in website_rows:
            if row.get('download_name', '').strip() in downloaded_files:
                row['Downloaded'] = 'Y'
                rows_updated.append(row)

    if rows_updated:
        with open(website_data_path, 'w', newline='') as ws_file:
            writer = csv.DictWriter(ws_file, fieldnames=website_fieldnames)
            writer.writeheader()
            writer.writerows(website_rows)
        logging.info(f"Updated {len(rows_updated)} rows in {website_data_path} based on file_status.csv.")
    else:
        logging.info("No rows updated in website_raw_data.csv based on file_status.csv.")

def main():
    """
    Main entry point for the script.
    
    Executes the process to save raw website data by scraping the NCDC website.
    The scraping process extracts Lassa fever report data and saves it to CSV.
    """
    save_raw_website_data(soup)

if __name__ == "__main__":
    main()
    process_file_status_update()