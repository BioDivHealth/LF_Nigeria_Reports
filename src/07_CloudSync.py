# src/07_CloudSync.py
"""
Synchronize all Lassa Fever Report pipeline artifacts to cloud storage.

This script uploads all PDFs, enhanced images, and CSV files to cloud storage.
It should be run after the entire pipeline completes to preserve all artifacts.
"""

import logging
from pathlib import Path
import os
from dotenv import load_dotenv
from utils.logging_config import configure_logging

try:
    # For standalone execution
    from utils.cloud_storage import upload_directory
except ImportError:
    # When called from main.py
    from src.utils.cloud_storage import upload_directory

# Check if B2 credentials are set
load_dotenv()
configure_logging()
required_vars = ['B2_APPLICATION_KEY_ID', 'B2_APPLICATION_KEY', 'B2_BUCKET_NAME']
missing = [var for var in required_vars if not os.environ.get(var)]
if missing:
    logging.error(f"Missing required environment variables: {', '.join(missing)}")
    logging.error("Please set these in your .env file")
    

def main():
    """
    Upload all pipeline artifacts to cloud storage.
    """
    base_dir = Path(__file__).parent.parent
    
    # 1. Upload raw PDFs
    logging.info("Uploading raw PDFs to cloud storage...")
    data_dir = base_dir / "data"
    if data_dir.exists():
        # Upload raw PDFs (only years 20-25)
        raw_dir = data_dir / "raw"
        if raw_dir.exists():
            logging.info(f"Uploading raw PDFs from {raw_dir} (only years 20-25)...")
            
            # Get all year directories
            year_dirs = [d for d in raw_dir.glob('year/*') if d.is_dir()]
            
            # Filter for years 20-25
            target_years = [f"{y}" for y in range(20, 26)]  # 20, 21, 22, 23, 24, 25
            filtered_dirs = [d for d in year_dirs if d.name in target_years]
            
            if not filtered_dirs:
                logging.warning(f"No year directories found matching years 20-25 in {raw_dir}/year/")
            
            # Upload each filtered year directory separately
            for year_dir in filtered_dirs:
                year_name = year_dir.name
                logging.info(f"Processing year {year_name} from {year_dir}...")
                results = upload_directory(year_dir, f"lassa-reports/data/raw/year/{year_name}", skip_if_exists=skip_existing)
                logging.info(f"Year {year_name}: {results['success']} uploaded, {results['skipped']} skipped, {results['failed']} failed")
 #   
 #  # 2. Upload PDFs_Sourced directory
 #   sourced_pdf_dir = base_dir / "PDFs_Sourced"
 #   if sourced_pdf_dir.exists():
 #       upload_directory(
 #           sourced_pdf_dir,
 #           b2_prefix="lassa-reports/PDFs_Sourced",
 #           file_extensions=[".pdf"]
 #       )
    
    # 3. Upload enhanced table images
    logging.info("Uploading enhanced table images to cloud storage...")
    processed_dir = base_dir / "data" / "processed"
    if processed_dir.exists():
        # Find all PDFs_Lines_YYYY directories
        for pdf_lines_dir in processed_dir.glob("PDFs_Lines_*"):
            if pdf_lines_dir.is_dir():
                year = pdf_lines_dir.name.replace("PDFs_Lines_", "")
                upload_directory(
                    pdf_lines_dir,
                    b2_prefix=f"lassa-reports/data/processed/{pdf_lines_dir.name}",
                    file_extensions=[".png", ".jpg", ".jpeg"]
                )
    
        # 4. Upload individual CSV files
        logging.info("Uploading individual CSV files to cloud storage...")
        for csv_dir in processed_dir.glob("CSV_LF_*_Sorted"):
            if csv_dir.is_dir():
                upload_directory(
                    csv_dir,
                    b2_prefix=f"lassa-reports/data/processed/{csv_dir.name}",
                    file_extensions=[".csv"]
                )
    
        # 5. Upload combined CSV file
        logging.info("Uploading combined CSV file to cloud storage...")
        for combined_csv in processed_dir.glob("combined_lassa_data_*.csv"):
            upload_directory(
                processed_dir,
                b2_prefix="lassa-reports/data/processed",
                file_extensions=[".csv"]
            )
    
    # 6. Upload documentation files
    doc_dir = base_dir / "data" / "documentation"
    if doc_dir.exists():
        upload_directory(
            doc_dir,
            b2_prefix="lassa-reports/data/documentation",
            file_extensions=[".csv"]
        )
    
    logging.info("Cloud sync completed successfully")

if __name__ == "__main__":
    main()