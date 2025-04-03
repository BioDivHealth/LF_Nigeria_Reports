#!/usr/bin/env python3
import os
import re
import csv
import requests
from bs4 import BeautifulSoup, Comment
from datetime import datetime

CSV_FILE = 'downloaded_reports.csv'
PDF_FOLDER = 'PDFs_Sourced'
if not os.path.exists(PDF_FOLDER):
    os.makedirs(PDF_FOLDER)


def load_downloaded_records(csv_path):
    records = set()
    if os.path.exists(csv_path):
        with open(csv_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                records.add(row['date'])
    return records


def append_record(csv_path, date_str, pdf_url):
    file_exists = os.path.exists(csv_path)
    with open(csv_path, 'a', newline='') as csvfile:
        fieldnames = ['date', 'pdf_url']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({'date': date_str, 'pdf_url': pdf_url})


def download_pdf(pdf_url, download_name):
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


def get_most_recent_lassa_pdf():
    # Base URL of the NCDC site
    base_url = "https://ncdc.gov.ng"
    # URL that lists the Lassa fever situation reports
    list_page_url = "https://ncdc.gov.ng/diseases/sitreps/?cat=5&name=An%20update%20of%20Lassa%20fever%20outbreak%20in%20Nigeria"
    
    print(f"Fetching list page: {list_page_url}")
    response = requests.get(list_page_url)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, "html.parser")

    table_body = soup.find("tbody")
    if not table_body:
        print("Could not find <tbody> on the page.")
        return

    rows = table_body.find_all("tr")
    if not rows:
        print("No table rows found in <tbody>.")
        return

    most_recent_info = None
    most_recent_date = None

    for row in rows:
        # Extract the commented date if available
        date_text = None
        comments = row.find_all(string=lambda text: isinstance(text, Comment))
        for comment in comments:
            # Look for a date pattern, e.g. '08 February 2025'
            date_search = re.search(r'(\d{1,2}\s+\w+\s+\d{4})', comment)
            if date_search:
                date_text = date_search.group(1)
                break

        if not date_text:
            # If no commented date found, skip this row
            continue

        try:
            report_date = datetime.strptime(date_text, "%d %B %Y")
        except ValueError as ve:
            print(f"Failed to parse date '{date_text}': {ve}")
            continue

        # Extract the download link: assume it's in the last cell with <a>
        link_tag = row.find("a", href=True)
        if not link_tag:
            continue

        pdf_url = link_tag["href"]
        if pdf_url.startswith("/"):
            pdf_url = base_url + pdf_url

        # New: Get download name from the link or fallback to URL.
        download_name = link_tag.get("download")
        if not download_name:
            download_name = pdf_url.split("/")[-1]
        download_name = download_name.replace(" ", "_")
        
        # Extract description from second cell if possible
        cells = row.find_all("td")
        description = cells[1].get_text(strip=True) if len(cells) > 1 else "No description"

        # Check if this report is the most recent so far and not in the future
        current_date = datetime.now()
        if report_date > current_date:
            # Ignore reports dated in the future
            continue

        if (most_recent_date is None) or (report_date > most_recent_date):
            most_recent_date = report_date
            most_recent_info = {
                "date": report_date,
                "date_str": date_text,
                "description": description,
                "url": pdf_url,
                "download_name": download_name   # New field added
            }

    if most_recent_info:
        print("Most recent Lassa fever report:")
        print(f"Date: {most_recent_info['date_str']}, Description: {most_recent_info['description']}")
        print("Download URL:", most_recent_info["url"])
        
        # Check CSV database
        records = load_downloaded_records(CSV_FILE)
        if most_recent_info['date_str'] in records:
            print(f"Report for {most_recent_info['date_str']} already processed.")
        else:
            local_pdf_path = os.path.join(PDF_FOLDER, most_recent_info['download_name'])  # Updated path
            if not os.path.exists(local_pdf_path):
                if download_pdf(most_recent_info['url'], local_pdf_path):
                    append_record(CSV_FILE, most_recent_info['date_str'], most_recent_info['url'])
                    print(f"Downloaded and recorded new report: {most_recent_info['date_str']}")
                else:
                    print("Download failed; not recording the report.")
            else:
                print(f"PDF {local_pdf_path} already exists locally.")
                append_record(CSV_FILE, most_recent_info['date_str'], most_recent_info['url'])
                print(f"Recorded existing report: {most_recent_info['date_str']}")
    else:
        print("No valid report found.")


if __name__ == "__main__":
    get_most_recent_lassa_pdf()
