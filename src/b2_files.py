#!/usr/bin/env python3
"""
B2 Files and Supabase Sync Utility

This script checks for inconsistencies between Backblaze B2 storage and Supabase database.
It identifies records in Supabase that are not marked as enhanced but actually have 
their enhanced files in B2.
"""
import os
import logging

from utils.cloud_storage import get_b2_api
from utils.cloud_storage import get_b2_file_list
from utils.db_utils import get_db_engine
from utils.logging_config import configure_logging

from dotenv import load_dotenv
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Set
from sqlalchemy import text

try:
    # For standalone execution
    from utils.cloud_storage import upload_directory
except ImportError:
    # When called from main.py
    from src.utils.cloud_storage import upload_directory

# Configure logging
configure_logging()

# Initialize environment variables
load_dotenv()

# Supabase configuration
SUPABASE_TABLE_NAME = 'website_data' 
DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    logging.error("CRITICAL: DATABASE_URL environment variable not set. Exiting.")
    exit(1)
try:
    engine = get_db_engine(DATABASE_URL)
    # Test connection
    with engine.connect() as connection:
        logging.info("Successfully connected to Supabase.")
except Exception as e:
    logging.error(f"CRITICAL: Failed to create SQLAlchemy engine or connect to Supabase: {e}")
    exit(1)

def get_website_data() -> List[Dict]:
    """
    Get all records from website_data table in Supabase.
    
    Returns:
        List[Dict]: List of website_data records
    """
    logging.info(f"Retrieving website_data from Supabase...")
    
    try:
        with engine.connect() as connection:
            query = text(f"SELECT * FROM {SUPABASE_TABLE_NAME}")
            result = connection.execute(query)
            records = [dict(row._mapping) for row in result]
            
            logging.info(f"Retrieved {len(records)} records from Supabase")
            return records
    except Exception as e:
        logging.error(f"Error fetching website_data from Supabase: {e}")
        return []

def check_enhanced_status(b2_files: Set[str], website_records: List[Dict]) -> List[Dict]:
    """
    Check which records that don't have enhanced='Y' actually have enhanced files in B2.
    
    Args:
        b2_files: Set of all file paths in B2
        website_records: List of records from website_data table
        
    Returns:
        List[Dict]: Records that have enhanced files in B2 but not marked as enhanced
    """
    logging.info("Checking enhanced status for records...")
    
    # Filter records where enhanced is not 'Y'
    not_enhanced_records = [r for r in website_records if r.get('enhanced') != 'Y']
    logging.info(f"Found {len(not_enhanced_records)} records not marked as enhanced")
    
    # Records that actually have enhanced files in B2
    mismatched_records = []
    
    for record in not_enhanced_records:
        new_name = record.get('new_name', '').strip()
        if not new_name:
            continue
            
        # Create the expected enhanced file name
        enhanced_name = f"Lines_{new_name.replace('.pdf', '')}_page3.png"
        
        # Construct the expected B2 path
        year = record.get('year', '')  # Extract year from filename
        b2_path = f"lassa-reports/data/processed/PDF/PDFs_Lines_{year}/{enhanced_name}"
        
        # Check if the file exists in B2
        if b2_path in b2_files:
            record['expected_enhanced_name'] = enhanced_name
            record['b2_path'] = b2_path
            mismatched_records.append(record)
    
    logging.info(f"Found {len(mismatched_records)} records with enhanced files in B2 but not marked as enhanced")
    return mismatched_records

def main():
    """Main function to run the B2 and Supabase sync check."""
    # 1. Get all files from B2
    b2_files = get_b2_file_list()
    
    # 2. Get all records from website_data
    website_records = get_website_data()
    
    # 3. Check which records have enhanced files in B2 but not marked as enhanced
    mismatched_records = check_enhanced_status(b2_files, website_records)
    
    # 4. Print results
    if mismatched_records:
        logging.info("\nRecords with enhanced files in B2 but not marked as enhanced:")
        for i, record in enumerate(mismatched_records, 1):
            logging.info(f"{i}. {record.get('new_name')} - Enhanced file: {record.get('expected_enhanced_name')}")
        
        # Optionally, we could update these records in Supabase
        # update_enhanced_status(mismatched_records)
    else:
        logging.info("No mismatches found: All records with enhanced files in B2 are properly marked as enhanced.")
    
    return mismatched_records

if __name__ == "__main__":
    mismatched_records = main()