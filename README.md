# Lassa Reports Scraping

This repository contains tools and scripts for scraping, processing, and analyzing Lassa fever situation reports from the Nigeria Centre for Disease Control (NCDC).

## Repository Structure

- **config/**  
  Configuration files and settings used across the project.

- **data/**  
  - **documentation/**  
    CSV files tracking report metadata, download status, and processing status.
  - **processed/**  
    - **PDFs_Lines_2021/**, **PDFs_Lines_2022/**, **PDFs_Lines_2023/**, **PDFs_Lines_2024/**, **PDFs_Lines_2025/**  
      Enhanced table images extracted from PDFs, organized by year.
    - **CSV_LF_21_Sorted/**, **CSV_LF_22_Sorted/**, **CSV_LF_23_Sorted/**, **CSV_LF_24_Sorted/**, **CSV_LF_25_Sorted/**  
      Sorted and processed CSV data extracted from tables.
  - **raw/**  
    - **downloaded/**  
      Raw downloaded PDF files from NCDC website.
    - **year/**  
      PDFs organized by year with standardized filenames.

- **misc/**  
  Miscellaneous resources:
  - **clean/**  
    Cleaned data files for testing and development.
  - **debug_green_mask.png**, **debug_green_overlay.png**  
    Debug images used for table enhancement development.
  - **PDFs/**, **PDFs_Lines_Test/**  
    Test PDF files and image outputs.

- **reports/**  
  Additional analysis reports and documentation.

## Processing Pipeline

The project implements a complete data pipeline for Lassa fever reports:

1. **URL Sourcing** (`01_URL_Sourcing.py`)  
   Scrapes the NCDC website for Lassa fever reports, extracts metadata, and standardizes filenames. Updates `website_raw_data.csv` with report metadata and maintains file status tracking.

2. **PDF Download** (`02_PDF_Download.py`)  
   Downloads PDF reports based on URLs in the metadata CSV, updates download status, and organizes files into year-based folders with standardized naming.

3. **Table Enhancement** (`03_TableEnhancement.py`)  
   Processes downloaded PDFs to enhance table visibility:
   - Renders PDF pages at high DPI
   - Detects table boundaries using green row markers
   - Enhances vertical and horizontal lines
   - Crops and saves processed images to year-specific folders

4. **Status Updates** (`00_Update_Status.py`)  
   Tracks processing status by scanning the processed directories and updating metadata CSV to reflect current state of enhancement.

5. **Table Extraction and Sorting** (`04_TableExtractionSorting.py`)  
   Extracts data from enhanced table images and converts it to structured CSV format.

## Requirements

The project requires several Python packages:
- requests
- beautifulsoup4
- opencv-python
- numpy
- Pillow
- PyMuPDF
- python-dotenv
- pandas
- pydantic
