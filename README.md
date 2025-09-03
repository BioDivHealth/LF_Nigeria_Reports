# Lassa Fever Reports Scraping Pipeline

A Python-based data processing pipeline for scraping, processing, and analyzing Lassa fever situation reports from the Nigeria Centre for Disease Control (NCDC).

> ### Newest up-to-date dataset is available [here 🔗](exports/lassa_data_latest.csv)
---
## Project Overview

This pipeline automates the end-to-end processing of weekly Lassa fever reports:
- Scrapes the NCDC website for report listings and extracts metadata.
- Downloads raw PDF reports and organizes them by year.
- Enhances table images in PDFs for accurate data extraction.
- Uses Google Gemini AI to extract structured case data (Suspected, Confirmed, Probable, HCW, Deaths) at state and week granularity.
- Validates logical consistency (Suspected ≥ Confirmed ≥ Deaths) with retry and correction logic.
- Combines per-year CSV datasets into a unified master CSV for time-series analysis.

**Data sources:**
- Raw PDF situation reports from NCDC
- Intermediate enhanced table images
- Yearly extracted CSVs
- Final combined master CSV

## Repository Structure
```text
Lassa_Reports_Scraping/
├── README.md                # This file
├── main.py                  # Orchestrates the full pipeline
├── requirements.txt         # Python dependencies
├── .env                     # Environment variables (API keys)
├── data/
│   ├── raw/                 # Raw downloaded PDFs (organized by year)
│   ├── processed/           # Processed data and images
│   │   ├── PDF/             # Enhanced table images for each year
│   │   ├── CSV/             # Extracted and sorted CSV data by year
│   └── documentation/       # Metadata and status tracking CSVs
├── exports/                 # Exported final datasets (CSV, README)
├── src/                     # Core pipeline scripts
│   ├── 01_URL_Sourcing.py               # Scrape report URLs and metadata, update Supabase
│   ├── 02_PDF_Download_Supabase.py      # Sync/download PDFs, update download status in Supabase
│   ├── 03a_SyncEnhancement.py           # Sync enhanced status between B2 and Supabase
│   ├── 03b_TableEnhancement_Supabase.py # Enhance table images, upload to B2, update DB
│   ├── 04a_SyncProcessed.py             # Sync processed (CSV) status between B2 and Supabase
│   ├── 04b_LLM_Extraction_Supabase.py   # Extract tables from images using Gemini AI, save as CSV, update DB
│   ├── 05a_SyncCombiningStatus.py       # Sync 'combined' status for CSVs between local, B2, and Supabase
│   ├── 05b_PushToDB.py                  # Push processed CSVs to main DB table (lassa_data)
│   ├── 05c_CombinedStatus.py            # Ensure DB 'combined' status matches data table
│   ├── 05d_CleanStates.py               # Standardize state names in lassa_data
│   ├── 06_CloudSync.py                  # Upload all pipeline artifacts to B2 cloud storage
│   ├── 07_ExportData.py                 # Export final data to CSV, upload to Supabase Storage
│   └── utils/                           # Utility modules (logging, cloud, db, validation, etc.)
└── notebooks/                           # Jupyter notebooks and experiments
```

---

### Pipeline Script Overview

| Script Name                       | Description |
|-----------------------------------|-------------|
| 01_URL_Sourcing.py                | Scrape NCDC website for Lassa fever reports, extract metadata, update Supabase |
| 02_PDF_Download_Supabase.py       | Sync/download PDFs from B2, update download status in Supabase |
| 03a_SyncEnhancement.py            | Sync 'enhanced' status for images between B2 and Supabase |
| 03b_TableEnhancement_Supabase.py  | Enhance table images from PDFs, upload to B2, update DB |
| 04a_SyncProcessed.py              | Sync 'processed' (CSV) status between B2 and Supabase |
| 04b_LLM_Extraction_Supabase.py    | Extract tables from enhanced images using Gemini AI, validate, save as CSV, update DB |
| 05a_SyncCombiningStatus.py        | Sync 'combined' status for CSVs between local, B2, and Supabase |
| 05b_PushToDB.py                   | Push processed CSVs to the main DB table (lassa_data) |
| 05c_CombinedStatus.py             | Ensure DB 'combined' status matches actual data table |
| 05d_CleanStates.py                | Standardize and clean state names in lassa_data |
| 06_CloudSync.py                   | Upload all pipeline artifacts (PDFs, images, CSVs) to B2 cloud storage |
| 07_ExportData.py                  | Export final, cleaned data to CSV and Supabase Storage |

---



## Setup

1. Clone the repository and create a virtual environment:
   ```bash
   git clone <repo_url>
   cd Lassa_Reports_Scraping
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Create a `.env` file in the project root with your API keys, e.g.:
   ```bash
   GOOGLE_GENAI_API_KEY=<your_key>
   B2_APPLICATION_KEY_ID=<your_key>
   B2_APPLICATION_KEY=<your_key>
   B2_BUCKET_NAME=<your_key>
   DATABASE_URL=<your_key>
   SUPABASE_URL=<your_key>
   SUPABASE_KEY=<your_key>
   ```

## Usage

### Run the Full Pipeline

```bash
python main.py
```

This executes the following steps in order:

1. **URL Sourcing** (`src/01_URL_Sourcing.py`)
2. **PDF Download** (`src/02_PDF_Download_Supabase.py`)
3. **SyncEnhancement** (`src/03a_SyncEnhancement.py`)
4. **Table Enhancement** (`src/03b_TableEnhancement_Supabase.py`)
5. **SyncProcessed** (`src/04a_SyncProcessed.py`)
6. **LLM Extraction** (`src/04b_LLM_Extraction_Supabase.py`)
7. **SyncCombiningStatus** (`src/05a_SyncCombiningStatus.py`)
8. **PushToDB** (`src/05b_PushToDB.py`)
9. **CombinedStatus** (`src/05c_CombinedStatus.py`)
10. **State Cleaning** (`src/05d_CleanStates.py`)
11. **CloudSync** (`src/06_CloudSync.py`)
12. **ExportData** (`src/07_ExportData.py`)

## Data Flow

1. **Raw PDFs**: `data/raw/`, `PDFs_Sourced/`
2. **Enhanced Images**: `data/processed/PDFs_Lines_{year}/`
3. **Extracted CSV**: `data/processed/CSV_LF_{year}_Sorted/`
4. **Combined Master CSV**: `data/processed/combined_lassa_data_{years}.csv`
5. **Metadata**: `data/documentation/website_raw_data.csv` and status CSVs

## Data Access

### Download Latest Data

The pipeline automatically exports the latest Lassa fever case data to CSV files. You can access this data in two ways:

1. **Direct Download from GitHub**: 
   - Navigate to the [exports directory](exports/lassa_data_latest.csv) in the repository
   - Download `lassa_data_latest.csv` for the most recent data

2. **Supabase Storage**:
   - The data is also available through Supabase Storage
   - Direct download link: [click here](https://csoccwksnrjkwkfpzqxx.supabase.co/storage/v1/object/sign/lassa-data/data/exports/lassa_data_latest.csv?token=eyJraWQiOiJzdG9yYWdlLXVybC1zaWduaW5nLWtleV81YThiMWNkNi1hM2RlLTQxZDUtODBhOC0zMGU2M2EwNzFkMTIiLCJhbGciOiJIUzI1NiJ9.eyJ1cmwiOiJsYXNzYS1kYXRhL2RhdGEvZXhwb3J0cy9sYXNzYV9kYXRhX2xhdGVzdC5jc3YiLCJpYXQiOjE3NDg4ODE1NjAsImV4cCI6MTc4MDQxNzU2MH0.bdBA9U2WZmg3QgX98FZf-ZwWBk9llP2wKGrnJhXKx9w)

### Data Format

Each CSV file contains the following columns:
- `year`: Year of the report
- `week`: Epidemiological week number
- `states`: Nigerian state name
- `suspected`: Number of suspected cases
- `confirmed`: Number of confirmed cases
- `probable`: Number of probable cases
- `hcw`: Number of healthcare worker cases
- `deaths`: Number of deaths

### License and data attribution

- **License (code and derived CSVs)**: CC0 1.0 Universal (Public Domain Dedication). See `LICENSE`.
- **Source data attribution**: The raw situation report PDFs and underlying figures are published by the **Nigeria Centre for Disease Control (NCDC)** and made publicly available. This repository automates retrieval and produces a cleaned, combined dataset for convenience; we do not claim ownership over NCDC materials.

### How to cite

If you use this repository or the dataset, please cite it. A machine-readable citation file is provided in `CITATION.cff`. Example citations:

- **Software**: Trebski, A. (2025). Lassa Fever NCDC Reports Sourcing Pipeline. GitHub. https://github.com/BioDivHealth/LF_Nigeria_Reports
- **Dataset**: Trebski, A. (2025). Lassa Fever weekly NCDC repors dataset. https://github.com/BioDivHealth/LF_Nigeria_Reports/blob/main/exports/lassa_data_latest.csv.
