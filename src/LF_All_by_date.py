#!/usr/bin/env python3
import os
import re
import csv
import shutil
import requests
from bs4 import BeautifulSoup, Comment
from datetime import datetime
from pathlib import Path

# Define base paths
BASE_DIR = Path(__file__).parent.parent
CSV_FILE = BASE_DIR / 'data' / 'documentation' / 'downloaded_reports.csv'
PDF_FOLDER = BASE_DIR / 'data' / 'raw' / 'PDFs_Sourced'

# Ensure the PDFs_Sourced folder exists
PDF_FOLDER.mkdir(parents=True, exist_ok=True)

FULL_FIELDNAMES = ['date_text', 'pdf_url', 'download_name', 'new_name', 'month_name', 'year', 'week', 'month', 'day']

def ensure_csv_file(csv_path):
    if not os.path.exists(csv_path):
        with open(csv_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=FULL_FIELDNAMES)
            writer.writeheader()
        print(f"Created CSV file with headers at {csv_path}")

def load_downloaded_records(csv_path):
    full_fieldnames = ['date_text', 'pdf_url', 'download_name', 'new_name', 'month_name', 'year', 'week', 'month', 'day']
    if not os.path.exists(csv_path):
        # Create the CSV file with all headers if it doesn't exist
        with open(csv_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=full_fieldnames)
            writer.writeheader()
        return []
    records = []
    with open(csv_path, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            records.append(row)
    return records

def append_record(csv_path, report):
    full_fieldnames = ['date_text', 'pdf_url', 'download_name', 'new_name', 'month_name', 'year', 'week', 'month', 'day']
    file_exists = os.path.exists(csv_path)
    with open(csv_path, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=full_fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(report)

def download_pdf(pdf_url, download_name):
    download_name = download_name.replace(" ", "_")  # Replace spaces with underscores
    print(f"Downloading {pdf_url} -> {download_name}")
    try:
        pdf_response = requests.get(pdf_url)
        pdf_response.raise_for_status()
        with open(download_name, 'wb') as f:
            f.write(pdf_response.content)
        return True
    except Exception as e:
        print(f"Failed to download {pdf_url}: {e}")
        return False

def get_lassa_report_links():
    base_url = "https://ncdc.gov.ng"
    list_page_url = "https://ncdc.gov.ng/diseases/sitreps/?cat=5&name=An%20update%20of%20Lassa%20fever%20outbreak%20in%20Nigeria"
    
    print(f"Fetching list page: {list_page_url}")
    response = requests.get(list_page_url)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, "html.parser")
    table_body = soup.find("tbody")
    if not table_body:
        print("Could not find <tbody> on the page.")
        return []
    rows = table_body.find_all("tr")
    if not rows:
        print("No table rows found in <tbody>.")
        return []
        
    reports = []
    current_date = datetime.now()
    
    # Define full field names
    full_fieldnames = ['date_text', 'pdf_url', 'download_name', 'new_name', 'month_name', 'year', 'week', 'month', 'day']
    
    # Load existing records once
    existing_records = load_downloaded_records(str(CSV_FILE))
    downloaded_keys = { tuple(r.get(field, "") for field in full_fieldnames) for r in existing_records }
    
    for row in rows:
        date_text = None
        comments = row.find_all(string=lambda text: isinstance(text, Comment))
        for comment in comments:
            date_search = re.search(r'(\d{1,2}\s+\w+\s+\d{4})', comment)
            if date_search:
                date_text = date_search.group(1)
                break
        if not date_text:
            continue
        try:
            report_date = datetime.strptime(date_text, "%d %B %Y")
        except ValueError as ve:
            print(f"Failed to parse date '{date_text}': {ve}")
            continue
        if report_date > current_date:
            continue
        
        link_tag = row.find("a", href=True)
        if not link_tag:
            continue
        pdf_url = link_tag["href"]
        if pdf_url.startswith("/"):
            pdf_url = base_url + pdf_url
        download_name = link_tag.get("download")
        if not download_name:
            download_name = pdf_url.split("/")[-1]
        download_name = download_name.replace(" ", "_")
        
        new_name_info = rename_lassa_file(download_name)

        # Create new record tuple
        new_record = (
            date_text,
            pdf_url,
            download_name,
            new_name_info['full_name'],
            new_name_info['month_name'],
            new_name_info['year'],
            new_name_info['week'],
            new_name_info['month'],
            new_name_info['day']
        )

        if new_record in downloaded_keys:
            continue
        
        reports.append({
            "date_text": date_text,
            "pdf_url": pdf_url,
            "download_name": download_name,
            "new_name": new_name_info['full_name'],
            "month_name": new_name_info['month_name'],
            "year": new_name_info['year'],
            "week": new_name_info['week'],
            "month": new_name_info['month'],
            "day": new_name_info['day']
        })
    return reports

def download_lassa_pdfs(reports):
    for report in reports:
        local_pdf_path = PDF_FOLDER / report["download_name"]
        if not local_pdf_path.exists():
            if download_pdf(report["pdf_url"], str(local_pdf_path)):
                append_record(str(CSV_FILE), report)
                print(f"Downloaded and recorded report: {report['date_text']}")
            else:
                print("Download failed; not recording the report.")
        else:
            print(f"PDF {local_pdf_path} already exists locally.")
            append_record(str(CSV_FILE), report)
            print(f"Recorded existing report: {report['date_text']}")

def rename_lassa_file(old_name):
    """
    Converts 'An_update_of_Lassa_fever_outbreak_in_Nigeria_041124_45.pdf'
    to 'Nigeria_04_Nov_24_W45.pdf', extracting day=04, month=11 => 'Nov', year=24,
    and the week number 45.
    
    Args:
        old_name (str): Original filename to be converted
    
    Returns:
        str: New filename format, or original name if conversion fails
    """
    # For mapping month number to short name
    month_map = {
        "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
        "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
        "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec",
    }

    # Replace spaces with underscores
    old_name = old_name.replace(" ", "_")
    
    # Split on underscores
    parts = old_name.split("_")
    
    if len(parts) < 9:
        return old_name
    
    # Extract date and week parts
    date_str = parts[8]  # "041124"
    week_str_pdf = parts[9]  # "45.pdf"
    
    # Remove ".pdf" from week string
    if week_str_pdf.endswith(".pdf"):
        week_str = week_str_pdf.replace(".pdf", "")
    else:
        return old_name
    
    # Validate date string
    if len(date_str) != 6:
        return old_name
        
    dd = date_str[0:2]   # "04"
    mm = date_str[2:4]   # "11"
    yy = date_str[4:6]   # "24"
    
    # Convert month number to name
    month_name = month_map.get(mm, "???")
    
    full_name = f"Nigeria_{dd}_{month_name}_{yy}_W{week_str}.pdf"

    # Build new name
    return {
        'full_name': full_name,
        'month_name': month_name, 
        'year': yy,
        'month': mm,
        'week': week_str,
        'day': dd,
    }

def get_most_recent_lassa_pdf():
    # ...existing code...
    reports = get_lassa_report_links()
    download_lassa_pdfs(reports)
    #rename_lassa_files(PDF_FOLDER)  # Optionally re-enable renaming

def load_reports_csv():
    """Loads report data from downloaded_reports.csv and returns a list of report dictionaries."""
    if not CSV_FILE.exists():
        print("No downloaded_reports.csv file found.")
        return []
    reports = []
    with open(CSV_FILE, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            reports.append(row)
    return reports

def organize_pdfs_by_year():
    # Load reports from downloaded_reports.csv
    reports = load_reports_csv()
    for report in reports:
        # Use the 'year' field directly, prefixing with '20' if needed
        year = report['year']
        if len(year) == 2:
            year = '20' + year

        # Create year directory if it doesn't exist
        year_dir = BASE_DIR / 'data' / 'raw' / 'year' / year
        year_dir.mkdir(parents=True, exist_ok=True)

        # Source PDF file using the original download_name
        source_pdf = PDF_FOLDER / report['download_name']
        
        # Destination path using the new_name
        dest_path = year_dir / report['new_name']

        # If the file doesn't exist in the destination and exists in source, copy it
        if not dest_path.exists() and source_pdf.exists():
            shutil.copy2(source_pdf, dest_path)
            print(f"Copied {source_pdf.name} to {dest_path}")

if __name__ == "__main__":
    ensure_csv_file(str(CSV_FILE))
    get_most_recent_lassa_pdf()
    organize_pdfs_by_year()

#abc = get_lassa_report_links()