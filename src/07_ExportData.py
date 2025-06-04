#!/usr/bin/env python3
"""
07_ExportData.py: Export Lassa Fever Data to CSV and Supabase Storage

This script exports the Lassa fever case data from the Supabase database to a CSV file
and uploads it to Supabase Storage for easy access by collaborators.

The script:
1. Connects to the Supabase database
2. Retrieves all data from the lassa_data table
3. Exports the data to a CSV file in the exports directory
4. Creates both a latest version and a timestamped version for historical tracking
5. Uploads the CSV files to a public Supabase Storage bucket

Dependencies:
    - pandas: For data manipulation and CSV export
    - sqlalchemy: For database connection
    - requests: For Supabase Storage API operations
    - dotenv: For loading environment variables

Environment Variables:
    - DATABASE_URL: Supabase PostgreSQL connection string
    - SUPABASE_URL: Supabase project URL
    - SUPABASE_KEY: Supabase service role key (for storage operations)

Usage:
    python src/07_ExportData.py
"""

import os
import sys
import logging
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text
from urllib.parse import urlparse
from dotenv import load_dotenv

# Add the project root directory to Python path to fix import issues
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables from .env file
#load_dotenv(project_root / ".env")

# Import centralized logging configuration
try:
    from utils.logging_config import configure_logging
except ImportError:
    from src.utils.logging_config import configure_logging

# Configure logging
configure_logging()

def connect_to_database():
    """
    Connect to the Supabase PostgreSQL database.
    
    Returns:
        sqlalchemy.engine.Engine: Database connection engine
    """
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        logging.error("DATABASE_URL environment variable not set")
        sys.exit(1)
    
    try:
        engine = create_engine(database_url)
        logging.info("Connected to database successfully")
        return engine
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        sys.exit(1)

def export_data_to_csv(engine, output_dir):
    """
    Export data from the lassa_data table to CSV files.
    
    Args:
        engine (sqlalchemy.engine.Engine): Database connection engine
        output_dir (Path): Directory to save CSV files
    
    Returns:
        tuple: Paths to the latest and timestamped CSV files and the dataframe
    """
    try:
        # Query to select all data from lassa_data table
        query = text("""
            SELECT 
                year, 
                month,
                week, 
                states, 
                suspected, 
                confirmed, 
                probable, 
                hcw, 
                deaths 
            FROM 
                lassa_data 
            ORDER BY 
                year DESC, 
                month DESC,
                week DESC, 
                states
        """)
        
        # Execute query and load into DataFrame
        df = pd.read_sql(query, engine)
        
        # Create output directory if it doesn't exist
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Define file paths
        latest_path = output_dir / "lassa_data_latest.csv"
        timestamp = datetime.now().strftime("%Y%m%d")
        timestamped_path = output_dir / f"lassa_data_{timestamp}.csv"
        
        # Export to CSV files
        df.to_csv(latest_path, index=False)
        df.to_csv(timestamped_path, index=False)
        
        logging.info(f"Exported {len(df)} records to {latest_path}")
        logging.info(f"Created timestamped backup at {timestamped_path}")
        
        # Also create a README file in the exports directory
        readme_path = output_dir / "README.md"
        with open(readme_path, 'w') as f:
            f.write(f"""# Lassa Fever Case Data Exports

This directory contains exported CSV files of Lassa fever case data from Nigeria.

## Files

- `lassa_data_latest.csv`: Always contains the most recent data export
- `lassa_data_{timestamp}.csv`: Timestamped version of the current export
- Additional timestamped files: Historical exports

## Data Format

Each CSV file contains the following columns:
- `year`: Year of the report
- `week`: Epidemiological week number
- `states`: Nigerian state name
- `suspected`: Number of suspected cases
- `confirmed`: Number of confirmed cases
- `probable`: Number of probable cases
- `hcw`: Number of healthcare worker cases
- `deaths`: Number of deaths

Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
""")
        
        logging.info(f"Created README file at {readme_path}")
        
        return latest_path, timestamped_path, df
    except Exception as e:
        logging.error(f"Failed to export data to CSV: {e}")
        sys.exit(1)

def upload_to_supabase_storage(file_paths, df):
    """
    Upload CSV files to Supabase Storage.
    
    Args:
        file_paths (tuple): Paths to the CSV files to upload
        df (pandas.DataFrame): DataFrame containing the data
    
    Returns:
        dict: Dictionary with public URLs for the uploaded files
    """
    # Get Supabase credentials from environment variables
    supabase_url = os.environ.get('SUPABASE_URL')
    supabase_key = os.environ.get('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        logging.error("Supabase credentials not set in environment variables")
        logging.warning("Skipping upload to Supabase Storage")
        return None
    
    try:
        # Extract project reference from URL
        parsed_url = urlparse(supabase_url)
        project_ref = parsed_url.netloc.split('.')[0]
        
        # Define bucket name
        bucket_name = 'lassa-data'
        
        # Create bucket if it doesn't exist
        headers = {
            'Authorization': f'Bearer {supabase_key}',
            'Content-Type': 'application/json'
        }
        
        # Check if bucket exists
        bucket_url = f"{supabase_url}/storage/v1/bucket/{bucket_name}"
        response = requests.get(bucket_url, headers=headers)
        
        if response.status_code == 404:
            # Create bucket
            create_bucket_url = f"{supabase_url}/storage/v1/bucket"
            bucket_data = {
                'id': bucket_name,
                'name': bucket_name,
                'public': True
            }
            response = requests.post(create_bucket_url, headers=headers, json=bucket_data)
            
            if response.status_code == 200 or response.status_code == 201:
                logging.info(f"Created new public bucket: {bucket_name}")
            else:
                logging.error(f"Failed to create bucket: {response.text}")
                return None
        elif response.status_code == 200:
            logging.info(f"Found existing bucket: {bucket_name}")
        else:
            logging.error(f"Error checking bucket: {response.text}")
            return None
        
        # Upload files
        latest_path, timestamped_path, _ = file_paths
        upload_url = f"{supabase_url}/storage/v1/object/{bucket_name}"
        
        # Upload latest CSV
        latest_file_path = "data/exports/lassa_data_latest.csv"
        
        # First check if file exists and delete it if it does
        check_file_url = f"{upload_url}/{latest_file_path}"
        response = requests.head(check_file_url, headers=headers)
        
        if response.status_code == 200:
            # File exists, delete it first
            # Use only the Authorization header for delete requests
            delete_headers = {
                'Authorization': f'Bearer {supabase_key}'
            }
            delete_response = requests.delete(check_file_url, headers=delete_headers)
            if delete_response.status_code in [200, 204]:
                logging.info(f"Deleted existing {latest_path.name} from Supabase Storage")
            else:
                logging.warning(f"Failed to delete existing file: {delete_response.text}")
        
        # Now upload the file
        with open(latest_path, 'rb') as f:
            files = {'file': (latest_file_path, f, 'text/csv')}
            headers_upload = {
                'Authorization': f'Bearer {supabase_key}'
            }
            response = requests.post(
                check_file_url, 
                headers=headers_upload, 
                files=files
            )
            
            if response.status_code == 200:
                logging.info(f"Uploaded {latest_path.name} to Supabase Storage")
            else:
                logging.error(f"Failed to upload {latest_path.name}: {response.text}")
        
        # Upload timestamped CSV
        timestamp_file_path = f"data/exports/{timestamped_path.name}"
        
        # Check if timestamped file exists and delete it if it does
        check_timestamp_url = f"{upload_url}/{timestamp_file_path}"
        response = requests.head(check_timestamp_url, headers=headers)
        
        if response.status_code == 200:
            # File exists, delete it first
            # Use only the Authorization header for delete requests
            delete_headers = {
                'Authorization': f'Bearer {supabase_key}'
            }
            delete_response = requests.delete(check_timestamp_url, headers=delete_headers)
            if delete_response.status_code in [200, 204]:
                logging.info(f"Deleted existing {timestamped_path.name} from Supabase Storage")
            else:
                logging.warning(f"Failed to delete existing file: {delete_response.text}")
        
        # Now upload the timestamped file
        with open(timestamped_path, 'rb') as f:
            files = {'file': (timestamp_file_path, f, 'text/csv')}
            response = requests.post(
                check_timestamp_url, 
                headers=headers_upload, 
                files=files
            )
            
            if response.status_code == 200:
                logging.info(f"Uploaded {timestamped_path.name} to Supabase Storage")
            else:
                logging.error(f"Failed to upload {timestamped_path.name}: {response.text}")
        
        # Generate public URLs
        latest_url = f"{supabase_url}/storage/v1/object/public/{bucket_name}/{latest_file_path}"
        timestamped_url = f"{supabase_url}/storage/v1/object/public/{bucket_name}/{timestamp_file_path}"
        
        logging.info(f"Files uploaded successfully to Supabase Storage")
        logging.info(f"Public URL for latest data: {latest_url}")
        
        return {
            'latest_url': latest_url,
            'timestamped_url': timestamped_url
        }
    except Exception as e:
        logging.error(f"Failed to upload to Supabase Storage: {e}")
        logging.warning("Continuing without uploading to Supabase Storage")
        return None

def main():
    """
    Main function to export data to CSV and upload to Supabase Storage.
    """
    logging.info("Starting Lassa fever data export process")
    
    # Connect to database
    engine = connect_to_database()
    
    # Define output directory
    output_dir = project_root / "exports"
    
    # Export data to CSV
    file_paths = export_data_to_csv(engine, output_dir)
    
    # Upload to Supabase Storage
    storage_urls = upload_to_supabase_storage(file_paths, file_paths[2])
    
    logging.info("Data export completed successfully")
    logging.info(f"Latest data available at: {file_paths[0]}")
    if storage_urls:
        logging.info(f"Public URL for Supabase Storage: {storage_urls['latest_url']}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
