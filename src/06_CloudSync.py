#!/usr/bin/env python3
# src/07_CloudSync.py
"""
Synchronize all Lassa Fever Report pipeline artifacts to cloud storage.

This script uploads all PDFs, enhanced images, and CSV files to cloud storage.
It should be run after the entire pipeline completes to preserve all artifacts.

Usage:
    python src/07_CloudSync.py
    (Ensure B2 environment variables are set)

Dependencies:
    - utils.cloud_storage (for B2 interaction)
    - utils.logging_config (for logging)
    - dotenv (for environment variables)

Returns:
    0 on success, 1 on failure
"""

import logging
import sys
import time
from pathlib import Path
import os
from dotenv import load_dotenv

# Attempt to import utility functions, supporting both direct and main.py execution
try:
    # For standalone execution
    from utils.cloud_storage import upload_directory
    from utils.logging_config import configure_logging
except ImportError:
    # When called from main.py
    from src.utils.cloud_storage import upload_directory
    from src.utils.logging_config import configure_logging

# Configure logging
configure_logging()

# --- Configuration -----------------------------------------
# Check if B2 credentials are set
load_dotenv()
required_vars = ['B2_APPLICATION_KEY_ID', 'B2_APPLICATION_KEY', 'B2_BUCKET_NAME']
missing = [var for var in required_vars if not os.environ.get(var)]
if missing:
    logging.error(f"Missing required environment variables: {', '.join(missing)}")
    logging.error("Please set these in your .env file")

# Define the B2 prefix where files are stored
B2_PREFIX = "lassa-reports"
# Ensure the prefix ends with a slash if it's not empty and not just '/'
if B2_PREFIX and B2_PREFIX != '/' and not B2_PREFIX.endswith('/'):
    B2_PREFIX += '/'


def main():
    """
    Upload all pipeline artifacts to cloud storage.
    """
    start_time = time.time()
    logging.info("Starting cloud synchronization process...")
    
    try:
        base_dir = Path(__file__).parent.parent
        logging.info(f"Base directory: {base_dir}")
        
        # Default to skip existing files unless explicitly overridden
        skip_existing = True
        
        # Track overall statistics
        total_stats = {"success": 0, "skipped": 0, "failed": 0, "total": 0}
    
        # 1. Upload raw PDFs
        try:
            logging.info("Uploading raw PDFs to cloud storage...")
            data_dir = base_dir / "data"
            if not data_dir.exists():
                logging.error(f"Data directory not found at {data_dir}")
            else:
                # Upload raw PDFs (years 20–26+; any two-digit year folder under raw/year/)
                raw_dir = data_dir / "raw"
                if not raw_dir.exists():
                    logging.error(f"Raw directory not found at {raw_dir}")
                else:
                    logging.info(f"Uploading raw PDFs from {raw_dir} (years 20–26+)...")
                    
                    # Get all year directories
                    year_dirs = [d for d in raw_dir.glob('year/*') if d.is_dir()]
                    
                    if not year_dirs:
                        logging.warning(f"No year directories found in {raw_dir}/year/")
                    else:
                        # Include years 20–26 and beyond (two-digit: 20, 21, ..., 99)
                        target_years = [f"{y}" for y in range(20, 100)]
                        filtered_dirs = [d for d in year_dirs if d.name in target_years]
                        
                        if not filtered_dirs:
                            logging.warning(f"No year directories found matching years 20+ in {raw_dir}/year/")
                        
                        # Upload each filtered year directory separately
                        for year_dir in filtered_dirs:
                            try:
                                year_name = year_dir.name
                                logging.info(f"Processing year {year_name} from {year_dir}...")
                                results = upload_directory(
                                    year_dir, 
                                    f"{B2_PREFIX}data/raw/year/{year_name}", 
                                    skip_if_exists=skip_existing
                                )
                                logging.info(f"Year {year_name}: {results['success']} uploaded, {results['skipped']} skipped, {results['failed']} failed")
                                
                                # Update total stats
                                for key in ['success', 'skipped', 'failed', 'total']:
                                    total_stats[key] += results.get(key, 0)
                            except Exception as e:
                                logging.error(f"Error processing year {year_dir.name}: {str(e)}", exc_info=True)
        except Exception as e:
            logging.error(f"Error in raw PDF upload section: {str(e)}", exc_info=True)
    
        # 2. Upload enhanced table images
        try:
            logging.info("Uploading enhanced table images to cloud storage...")
            processed_dir = base_dir / "data" / "processed"
            if not processed_dir.exists():
                logging.error(f"Processed directory not found at {processed_dir}")
            else:
                logging.info(f"Processing enhanced table images from {processed_dir}...")
                
                # Find all PDFs_Lines_YYYY directories in the PDF subdirectory
                pdf_dir = processed_dir / "PDF"
                if not pdf_dir.exists():
                    logging.error(f"PDF directory not found at {pdf_dir}")
                else:
                    pdf_dirs = list(pdf_dir.glob("PDFs_Lines_*"))
                    if not pdf_dirs:
                        logging.warning(f"No PDFs_Lines_* directories found in {pdf_dir}")
                    
                    for pdf_lines_dir in pdf_dirs:
                        try:
                            if pdf_lines_dir.is_dir():
                                year = pdf_lines_dir.name.replace("PDFs_Lines_", "")
                                logging.info(f"Uploading enhanced images for year {year}...")
                                results = upload_directory(
                                    pdf_lines_dir,
                                    b2_prefix=f"{B2_PREFIX}data/processed/PDF/{pdf_lines_dir.name}",
                                    file_extensions=[".png", ".jpg", ".jpeg"],
                                    skip_if_exists=skip_existing
                                )
                                logging.info(f"Enhanced images for year {year}: {results['success']} uploaded, {results['skipped']} skipped, {results['failed']} failed")
                                
                                # Update total stats
                                for key in ['success', 'skipped', 'failed', 'total']:
                                    total_stats[key] += results.get(key, 0)
                        except Exception as e:
                            logging.error(f"Error processing PDF directory {pdf_lines_dir.name}: {str(e)}", exc_info=True)
        except Exception as e:
            logging.error(f"Error in enhanced table images upload section: {str(e)}", exc_info=True)
    
        # 3. Upload individual CSV files
        try:
            logging.info("Uploading individual CSV files to cloud storage...")
            csv_dir = processed_dir / "CSV"
            if not csv_dir.exists():
                logging.error(f"CSV directory not found at {csv_dir}")
            else:
                csv_dirs = list(csv_dir.glob("CSV_LF_*_Sorted"))
                if not csv_dirs:
                    logging.warning(f"No CSV_LF_*_Sorted directories found in {csv_dir}")
                
                for year_csv_dir in csv_dirs:
                    try:
                        if year_csv_dir.is_dir():
                            year = year_csv_dir.name.replace("CSV_LF_", "").replace("_Sorted", "")
                            logging.info(f"Uploading CSV files for year {year}...")
                            results = upload_directory(
                                year_csv_dir,
                                b2_prefix=f"{B2_PREFIX}data/processed/CSV/{year_csv_dir.name}",
                                file_extensions=[".csv"],
                                skip_if_exists=skip_existing
                            )
                            logging.info(f"CSV files for year {year}: {results['success']} uploaded, {results['skipped']} skipped, {results['failed']} failed")
                            
                            # Update total stats
                            for key in ['success', 'skipped', 'failed', 'total']:
                                total_stats[key] += results.get(key, 0)
                    except Exception as e:
                        logging.error(f"Error processing CSV directory {year_csv_dir.name}: {str(e)}", exc_info=True)
        except Exception as e:
            logging.error(f"Error in CSV files upload section: {str(e)}", exc_info=True)
        
        # Calculate and log total time and statistics
        total_time = time.time() - start_time
        logging.info(f"Cloud sync completed in {total_time:.2f} seconds")
        logging.info(f"Total statistics: {total_stats['success']} uploaded, {total_stats['skipped']} skipped, {total_stats['failed']} failed, {total_stats['total']} total")
        return 0
    
    except Exception as e:
        logging.error(f"Unexpected error during cloud synchronization: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())