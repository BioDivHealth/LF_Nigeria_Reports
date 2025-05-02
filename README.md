# Lassa Fever Reports Scraping Pipeline

A Python-based data processing pipeline for scraping, processing, and analyzing Lassa fever situation reports from the Nigeria Centre for Disease Control (NCDC).

## Project Overview

This pipeline automates the end-to-end processing of weekly Lassa fever reports:
- Scrapes the NCDC website for report listings and extracts metadata.
- Downloads raw PDF reports and organizes them by year.
- Enhances table images in PDFs for accurate data extraction.
- Uses Google Gemini AI to extract structured case data (Suspected, Confirmed, Probable, HCW, Deaths) at state and week granularity.
- Validates logical consistency (Suspected ≥ Confirmed ≥ Deaths) with retry and correction logic.
- Combines per-year CSV datasets into a unified master CSV for time-series analysis.
- Provides an interactive Dash dashboard for exploring case trends by state, week, and year.

**Data sources:**
- Raw PDF situation reports from NCDC
- Intermediate enhanced table images
- Yearly extracted CSVs
- Final combined master CSV

## Repository Structure
```TeX
└── Lassa_Reports_Scraping/
    ├── README.md                # This file
    ├── main.py                  # Orchestrates the pipeline
    ├── requirements.txt         # Python dependencies
    ├── .env                     # Environment variables (API keys)
    ├── config/                # Optional configuration files
    ├── data/
    │   ├── raw/               # Raw downloaded PDFs
    │   ├── processed/         # Processed data and images
    │   │   ├── PDFs_Lines_{year}/    # Enhanced table images for each year
    │   │   ├── CSV_LF_{year}_Sorted/ # Extracted and sorted CSV data
    │   │   └── combined_lassa_data_{years}.csv  # Combined master CSV
    │   └── documentation/       # CSV metadata tracking file statuses
    ├── logs/                    # Log files generated during execution
    ├── PDFs_Sourced/            # Temporary storage for sourced PDFs
    ├── reports/                 # Additional analysis reports
    ├── src/                     # Core scripts and modules
    │   ├── 00_Update_Status.py      # Update processing status
    │   │   - Scans processed directories and updates metadata CSV with current status.
    │   │   - Tracks missing outputs and resets statuses for reprocessing.
    │   ├── 01_URL_Sourcing.py       # Scrape report URLs and metadata
    │   │   - Scrapes NCDC website for Lassa fever reports and extracts metadata.
    │   │   - Standardizes filenames and updates `data/documentation/website_raw_data.csv`.
    │   │   - Maintains download and processing status flags.
    │   ├── 02_PDF_Download.py       # Download PDF reports
    │   │   - Reads metadata CSV and downloads new PDF reports.
    │   │   - Organizes PDFs by year under `data/raw/` and `PDFs_Sourced/`.
    │   │   - Updates download status in metadata CSV.
    │   ├── 03_TableEnhancement.py   # Enhance table visibility
    │   │   - Renders PDF pages at high resolution.
    │   │   - Detects table boundaries via color markers.
    │   │   - Enhances lines, crops table images, and saves to `data/processed/PDFs_Lines_{year}/`.
    │   ├── 04_TableExtractionSorting.py  # Extract & sort table data using AI
    │   │   - Uses Google Gemini AI to extract tables from enhanced images.
    │   │   - Validates logical consistency (Suspected ≥ Confirmed ≥ Deaths).
    │   │   - Retries on inconsistencies, logs issues, and sorts outputs into `data/processed/CSV_LF_{year}_Sorted/`.
    │   ├── 05_CombineData.py         # Combine yearly data into master CSV
    │   │   - Loads per-year CSVs, concatenates, converts types, and sorts by Year & Week.
    │   │   - Outputs combined master CSV to `data/processed/combined_lassa_data_{years}.csv`.
    │   ├── Dashboard.py             # Interactive Dash dashboard
    │   │   - Builds bar and line charts with Year/Week dropdown controls and callbacks.
    │   ├── standardize_columns.py   # Column standardization utilities
    │   ├── debug.py                 # Debugging utilities
    │   ├── prompts/                 # AI prompt templates
    │   │   ├── table_extraction_prompt.py  # Template for AI table extraction prompt
    │   │   └── ...
    │   └── utils/                   # Utility modules
    │       ├── logging_config.py    # Configures NewlineLoggingHandler and suppresses AFC logs
    │       └── ...
    └── **notebooks/**              # Jupyter notebooks and experiments
```


## Setup

1. Clone the repository and create a virtual environment:
   ```bash
   git clone <repo_url>
   cd Lassa_Reports_Scraping
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Create a `.env` file in the project root with your API keys:
   ```bash
   GOOGLE_GENAI_API_KEY=<your_key>
   ```

## Usage

### Run the Full Pipeline

```bash
python main.py
```

This executes the following steps in order:

1. **URL Sourcing** (`src/01_URL_Sourcing.py`)
2. **PDF Download** (`src/02_PDF_Download.py`)
3. **Table Enhancement** (`src/03_TableEnhancement.py`)
4. **Table Extraction & Sorting** (`src/04_TableExtractionSorting.py`)
5. **Data Combination** (`src/05_CombineData.py`)

### Run Individual Steps

You can run any script independently:
```bash
python src/01_URL_Sourcing.py
```

### Update Processing Status

```bash
python src/00_Update_Status.py
```

### Dashboard

Launch the interactive dashboard:
```bash
python src/Dashboard.py
```
Access it at `http://127.0.0.1:8050`.

## Data Flow

1. **Raw PDFs**: `data/raw/`, `PDFs_Sourced/`
2. **Enhanced Images**: `data/processed/PDFs_Lines_{year}/`
3. **Extracted CSV**: `data/processed/CSV_LF_{year}_Sorted/`
4. **Combined Master CSV**: `data/processed/combined_lassa_data_{years}.csv`
5. **Metadata**: `data/documentation/website_raw_data.csv` and status CSVs

## Dependencies

- Python 3.8+
- requests>=2.31.0
- beautifulsoup4>=4.12.2
- opencv-python>=4.8.0
- numpy>=1.24.0
- Pillow>=10.0.0
- PyMuPDF>=1.22.5
- python-dotenv>=1.0.0
- google-genai>=0.3.0
- pydantic>=2.4.0
- pandas>=1.4.0
- mistralai>=0.1.0
- plotly>=5.0.0
- dash>=2.0.0

(See `requirements.txt` for exact versions.)
