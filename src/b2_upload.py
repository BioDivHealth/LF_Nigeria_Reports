# src/one_time_upload.py
"""
One-time script to upload all existing data to Backblaze B2.
"""
import os
import logging
from dotenv import load_dotenv
from pathlib import Path
from utils.cloud_storage import upload_directory
from utils.logging_config import configure_logging

def main():
    # Load environment variables from .env file
    load_dotenv()
    configure_logging()
    
    # Check if B2 credentials are set
    required_vars = ['B2_APPLICATION_KEY_ID', 'B2_APPLICATION_KEY', 'B2_BUCKET_NAME']
    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        logging.error(f"Missing required environment variables: {', '.join(missing)}")
        logging.error("Please set these in your .env file")
        return
    
    base_dir = Path(__file__).parent.parent
    
    logging.info("Starting one-time upload of all existing data to Backblaze B2...")
    
    # Upload all data directories
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
                logging.info(f"Uploading year {year_name} from {year_dir}...")
                upload_directory(year_dir, f"lassa-reports/data/raw/year/{year_name}")
        
        # Upload processed data (only CSV and PDF folders)
        processed_dir = data_dir / "processed"
        if processed_dir.exists():
            logging.info(f"Uploading processed data from {processed_dir} (only CSV and PDF folders)...")
            
            # Define target folders to upload
            target_folders = ["CSV", "PDF"]
            
            # Upload each target folder separately
            for folder_name in target_folders:
                folder_path = processed_dir / folder_name
                if folder_path.exists() and folder_path.is_dir():
                    logging.info(f"Uploading {folder_name} folder from {folder_path}...")
                    upload_directory(folder_path, f"lassa-reports/data/processed/{folder_name}")
                else:
                    logging.warning(f"Folder {folder_name} not found in {processed_dir}")

        
        # Upload documentation
        doc_dir = data_dir / "documentation"
        if doc_dir.exists():
            logging.info(f"Uploading documentation from {doc_dir}...")
            upload_directory(doc_dir, "lassa-reports/data/documentation")
    
    logging.info("One-time upload completed!")

if __name__ == "__main__":
    main()