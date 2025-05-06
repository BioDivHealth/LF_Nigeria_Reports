# src/07_CloudSync.py
"""
Synchronize all Lassa Fever Report pipeline artifacts to cloud storage.

This script uploads all PDFs, enhanced images, and CSV files to cloud storage.
It should be run after the entire pipeline completes to preserve all artifacts.
"""

import logging
from pathlib import Path

try:
    # For standalone execution
    from utils.cloud_storage import upload_directory
except ImportError:
    # When called from main.py
    from src.utils.cloud_storage import upload_directory

def main():
    """
    Upload all pipeline artifacts to cloud storage.
    """
    base_dir = Path(__file__).parent.parent
    
    # 1. Upload raw PDFs
    logging.info("Uploading raw PDFs to cloud storage...")
    raw_pdf_dir = base_dir / "data" / "raw"
    if raw_pdf_dir.exists():
        upload_directory(
            raw_pdf_dir,
            s3_prefix="lassa-reports/data/raw",
            file_extensions=[".pdf"]
        )
    
    # 2. Upload PDFs_Sourced directory
    sourced_pdf_dir = base_dir / "PDFs_Sourced"
    if sourced_pdf_dir.exists():
        upload_directory(
            sourced_pdf_dir,
            s3_prefix="lassa-reports/PDFs_Sourced",
            file_extensions=[".pdf"]
        )
    
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
                    s3_prefix=f"lassa-reports/data/processed/{pdf_lines_dir.name}",
                    file_extensions=[".png", ".jpg", ".jpeg"]
                )
    
        # 4. Upload individual CSV files
        logging.info("Uploading individual CSV files to cloud storage...")
        for csv_dir in processed_dir.glob("CSV_LF_*_Sorted"):
            if csv_dir.is_dir():
                upload_directory(
                    csv_dir,
                    s3_prefix=f"lassa-reports/data/processed/{csv_dir.name}",
                    file_extensions=[".csv"]
                )
    
        # 5. Upload combined CSV file
        logging.info("Uploading combined CSV file to cloud storage...")
        for combined_csv in processed_dir.glob("combined_lassa_data_*.csv"):
            upload_directory(
                processed_dir,
                s3_prefix="lassa-reports/data/processed",
                file_extensions=[".csv"]
            )
    
    # 6. Upload documentation files
    doc_dir = base_dir / "data" / "documentation"
    if doc_dir.exists():
        upload_directory(
            doc_dir,
            s3_prefix="lassa-reports/data/documentation",
            file_extensions=[".csv"]
        )
    
    logging.info("Cloud sync completed successfully")

if __name__ == "__main__":
    main()