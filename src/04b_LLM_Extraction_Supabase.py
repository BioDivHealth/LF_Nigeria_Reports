"""
04_LLM_Extraction_Supabase.py: Lassa Fever Report Table Extraction with Supabase

This script extracts tabular data from enhanced PDF images using the Google Gemini API,
validates the data for logical consistency, and saves the results as CSV files.
It reads metadata from a Supabase database and updates processing status there.

Dependencies:
    - utils/gemini_extractor.py: For Gemini API interaction
    - utils/data_validation.py: For data validation and transformation
    - utils/cloud_storage.py: For B2 interaction when needed
    - utils/db_utils.py: For Supabase interaction

Inputs:
    - Enhanced PDF images (locally or from B2)
    - Metadata from Supabase 'website_data' table
    
Outputs:
    - CSV files with extracted table data in data/processed/CSV_LF_YY_Sorted directories
    - Updated 'processed' status in Supabase 'website_data' table
"""

import os
import logging
from pathlib import Path
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.orm import Session

# Import centralized logging configuration
try:
    from utils.logging_config import configure_logging
except ImportError:
    from src.utils.logging_config import configure_logging

# Configure logging
configure_logging()

# Import utility modules
try:
    # Try direct import first (when run as a standalone script)
    from utils.gemini_extractor import (
        extract_table_with_gemini, parse_gemini_response,
        log_extraction_differences, save_extracted_data_to_csv)
    from utils.data_validation import (
        validate_logical_consistency, sort_table_rows,
        normalize_state_names, filter_comparison_columns)
    from utils.cloud_storage import download_file, get_b2_report_filenames
    from utils.db_utils import get_db_engine
except ImportError:
    # Fall back to relative import (when run from main.py)
    from src.utils.gemini_extractor import (
        extract_table_with_gemini, parse_gemini_response,
        log_extraction_differences, save_extracted_data_to_csv)
    from src.utils.data_validation import (
        validate_logical_consistency, sort_table_rows,
        normalize_state_names, filter_comparison_columns)
    from src.utils.cloud_storage import download_file, get_b2_report_filenames
    from src.utils.db_utils import get_db_engine

# Set third-party loggers to higher levels
for logger_name in ["google", "google.genai", "google.api_core", "httpx", "httpcore"]: 
    logging.getLogger(logger_name).setLevel(logging.ERROR)    

# Define base paths and constants
BASE_DIR = Path(__file__).parent.parent
ENHANCED_FOLDER = BASE_DIR / 'data' / 'processed' / 'PDF'
CSV_BASE_FOLDER = BASE_DIR / 'data' / 'processed' / 'CSV'

# Supabase configuration
SUPABASE_TABLE_NAME = 'website_data'
DATABASE_URL = os.environ.get("DATABASE_URL")

fieldnames_table = [
                "States", "Suspected", "Confirmed",
                "Probable", "HCW", "Deaths"
            ]

# B2 configuration - only for downloading when needed
B2_ENHANCED_PREFIX = "lassa-reports/data/processed/PDF/"

def get_reports_to_process(engine):
    """
    Query Supabase for reports that need table extraction.
    
    Args:
        engine: SQLAlchemy engine for database connection
        
    Returns:
        List of dictionaries with report data
    """
    reports = []
    
    with Session(engine) as session:
        try:
            # Query reports that have been enhanced but not processed
            stmt = text(f"""
                SELECT 
                    id, year, week, new_name, enhanced_name
                FROM 
                    "{SUPABASE_TABLE_NAME}"
                WHERE 
                    enhanced = 'Y'
                    AND (processed IS NULL OR processed != 'Y')
                    AND (year >= 20 OR year >= '20')
                    AND (compatible IS NULL OR compatible = 'Y' OR compatible != 'N')
                    AND (downloaded = 'Y')
                ORDER BY
                    year, week
            """)
            
            result = session.execute(stmt)
            
            for row in result:
                report_data = {
                    'id': row.id,
                    'year': row.year,
                    'week': row.week,
                    'new_name': row.new_name,
                    'enhanced_name': row.enhanced_name
                }
                reports.append(report_data)
                
            logging.info(f"Found {len(reports)} reports needing extraction")
            
        except Exception as e:
            logging.error(f"Error querying reports to process: {e}")
    
    return reports


def get_enhanced_image(enhanced_name, year):
    """
    Check if enhanced image exists locally, otherwise download from B2.
    
    Args:
        enhanced_name: Name of the enhanced image
        year: Year of the report
        
    Returns:
        Path to the enhanced image or None if not available
    """
    if not enhanced_name:
        return None
        
    # Check if image exists locally first
    year_folder = ENHANCED_FOLDER / f"PDFs_Lines_{year}"
    local_path = year_folder / enhanced_name
    
    if local_path.exists():
        logging.info(f"Found enhanced image locally: {local_path}")
        return local_path
        
    # Image not found locally, try to download from B2
    logging.info(f"Enhanced image not found locally, attempting to download from B2: {enhanced_name}")
        
    # Create directory if it doesn't exist
    year_folder.mkdir(parents=True, exist_ok=True)
    
    # B2 key for enhanced image
    b2_key = f"{B2_ENHANCED_PREFIX}PDFs_Lines_{year}/{enhanced_name}"
    
    try:
        # Download file from B2
        success = download_file(b2_key, str(local_path))
        
        if success:
            logging.info(f"Successfully downloaded {enhanced_name} from B2")
            return local_path
        else:
            logging.error(f"Failed to download {enhanced_name} from B2")
            return None
            
    except Exception as e:
        logging.error(f"Error downloading enhanced image {enhanced_name}: {e}")
        return None

def validate_extraction_results(parsed_data, enhanced_name, attempt, max_attempts):
    """
    Validate the logical consistency of extraction results and determine which data to use.
    
    Args:
        parsed_data: List of parsed data from multiple extraction attempts
        enhanced_name: Name of the enhanced image being processed
        attempt: Current attempt number
        max_attempts: Maximum number of attempts allowed
        
    Returns:
        tuple: (should_continue, final_data)
            - should_continue: True if we should continue to the next attempt
            - final_data: The validated data to use, or None if should_continue is True
    """
    if len(parsed_data) < 2:
        return True, None
        
    dict_rows_1, dict_rows_2 = parsed_data
            
    # Validate logical consistency in both iterations
    is_valid_1, validated_rows_1, errors_1 = validate_logical_consistency(dict_rows_1)
    is_valid_2, validated_rows_2, errors_2 = validate_logical_consistency(dict_rows_2)
            
    # Log any inconsistencies found
    if not is_valid_1:
        logging.warning(f"Logical inconsistencies found in iteration 1 for {enhanced_name}:")
        for error in errors_1:
            logging.warning(f"  - {error}")
            
    if not is_valid_2:
        logging.warning(f"Logical inconsistencies found in iteration 2 for {enhanced_name}:")
        for error in errors_2:
            logging.warning(f"  - {error}")
            
    # If both iterations have inconsistencies and we haven't reached max attempts,
    # try again with a new extraction
    if not is_valid_1 and not is_valid_2 and attempt < max_attempts:
        logging.warning(f"Both iterations have logical inconsistencies. Retrying extraction (attempt {attempt}/{max_attempts})")
        attempt += 1
        return True, None
            
    # If only one iteration has inconsistencies, use the consistent one
    if is_valid_1 and not is_valid_2:
        logging.info(f"Using iteration 1 data (iteration 2 had inconsistencies)")
        dict_rows_2 = validated_rows_1  # Use the valid data for both
    elif not is_valid_1 and is_valid_2:
        logging.info(f"Using iteration 2 data (iteration 1 had inconsistencies)")
        dict_rows_1 = validated_rows_2  # Use the valid data for both
    elif not is_valid_1 and not is_valid_2:
        # Both have inconsistencies and we've reached max attempts, use the validated (fixed) data
        logging.warning(f"Both iterations have inconsistencies after {attempt}/{max_attempts} attempts. Using validated data.")
        dict_rows_1 = validated_rows_1
        dict_rows_2 = validated_rows_2
            
    # Create sorted versions for comparison
    sorted_dict_rows_1 = sort_table_rows(dict_rows_1)
    sorted_dict_rows_2 = sort_table_rows(dict_rows_2)
            
    # Normalize state names for comparison
    normalized_1 = normalize_state_names(sorted_dict_rows_1)
    normalized_2 = normalize_state_names(sorted_dict_rows_2)
            
    # Filter out irrelevant columns (HCW and Probable) for comparison
    comparison_1 = filter_comparison_columns(normalized_1)
    comparison_2 = filter_comparison_columns(normalized_2)
    
    return dict_rows_1, dict_rows_2,normalized_1, normalized_2, comparison_1, comparison_2

def update_processing_status(engine, report_id, status='Y'):
    """
    Update the processed status in Supabase.
    
    Args:
        engine: SQLAlchemy engine for database connection
        report_id: ID of the report to update
        status: Status to set ('Y' or 'N')
    """
    with Session(engine) as session:
        try:
            stmt = text(f"""
                UPDATE "{SUPABASE_TABLE_NAME}"
                SET processed = :status
                WHERE id = CAST(:id AS uuid)
            """)
            
            session.execute(stmt, {
                'status': status,
                'id': report_id
            })
            session.commit()
            logging.info(f"Updated processed status for report {report_id} to {status}")
        except Exception as e:
            session.rollback()
            logging.error(f"Error updating processed status for report {report_id}: {e}")


def process_single_report(report_metadata, model_name, engine):
    """
    Process a single Lassa fever report.
    
    Args:
        report_metadata (dict): Dictionary containing report metadata
        model_name (str): Name of the Gemini model to use
        engine: SQLAlchemy engine for database connection
        
    Returns:
        bool: True if processing was successful, False otherwise
    """
    # Extract metadata
    report_id = report_metadata['id']
    year = report_metadata['year']
    week = report_metadata['week']
    enhanced_name = report_metadata['enhanced_name']
    
    # Skip if no enhanced_name is provided
    if not enhanced_name:
        logging.warning(f"No enhanced image name for Year {year}, Week {week}")
        return False
    
    # Output folder for CSV files
    output_dir = CSV_BASE_FOLDER / f"CSV_LF_{year}_Sorted"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Output CSV filename
    base_filename = os.path.splitext(enhanced_name)[0]
    output_path = output_dir / f"{base_filename}.csv"
    
    # Check if output file already exists
    if output_path.exists():
        logging.info(f"Found existing processed file: {base_filename}.csv")
        # Update status in Supabase if file exists locally
        update_processing_status(engine, report_id)
        return True
    
    # Get enhanced image - checks locally first, then downloads from B2 if needed
    input_path = get_enhanced_image(enhanced_name, year)
    if not input_path:
        logging.warning(f"Enhanced image not available: {enhanced_name}")
        return False
    
    logging.info(f"Processing {enhanced_name} (Year: {year}, Week: {week})")
    
    try:
        # Process the image with retry logic
        max_attempts = 3  # Maximum number of attempts to get matching outputs
        attempt = 1
        
        while attempt <= max_attempts:
            # Extract table data twice for validation
            responses = []
            for i in range(2):
                success, response = extract_table_with_gemini(
                    image_path=str(input_path),
                    model_name=model_name
                )
                if success:
                    responses.append(response)
                else:
                    logging.warning(f"Failed to extract data: {response}")
            
            if len(responses) < 2:
                logging.warning(f"Failed to get two valid responses. Attempt {attempt}/{max_attempts}")
                attempt += 1
                continue
            
            # Parse responses
            parsed_data = []
            for i, response in enumerate(responses):
                success, data = parse_gemini_response(response)
                if success:
                    parsed_data.append(data)
                else:
                    logging.warning(f"Failed to parse response {i+1}: {data}")
            
            if len(parsed_data) < 2:
                logging.warning(f"Failed to parse two valid responses. Attempt {attempt}/{max_attempts}")
                attempt += 1
                continue
            
            # Validate the extraction results
            dict_rows_1, dict_rows_2, normalized_1, normalized_2, comparison_1, comparison_2 = validate_extraction_results(
                parsed_data, enhanced_name, attempt, max_attempts
            )
            
            # Compare only the relevant columns
            if comparison_1 == comparison_2:
                logging.info(f"Both outputs are identical on relevant columns (States, Suspected, Confirmed, Deaths) on attempt {attempt}. Saving CSV.")
                
                # Save the data to CSV with Year and Week
                if save_extracted_data_to_csv(dict_rows_1, output_path, fieldnames_table, year=year, week=week):
                    logging.info(f"Successfully processed: {base_filename}.csv")
                    update_processing_status(engine, report_id)
                    return True
                else:
                    logging.error(f"Error writing CSV for image {enhanced_name}")
                    return False
            else:
                logging.warning(f"Outputs differ on relevant columns (States, Suspected, Confirmed, Deaths) between iterations for image: {enhanced_name} (attempt {attempt}/{max_attempts})")
                
                # Record differences to a text file in the output directory
                diff_file = output_dir / "differing_outputs.txt"
                log_extraction_differences(diff_file, enhanced_name, attempt, max_attempts, normalized_1, normalized_2)
                
                # Increment the attempt counter and try again if we haven't reached the maximum
                attempt += 1
                
                # If this was the last attempt and outputs still differ, log a final message
                if attempt > max_attempts:
                    logging.error(f"Failed to get matching outputs after {max_attempts} attempts for image: {enhanced_name}")
        
            return True
        
        # If we've exhausted all attempts without success
        logging.error(f"Failed to extract consistent data after {max_attempts} attempts for {enhanced_name}")
        return False
        
    except Exception as e:
        logging.error(f"Error processing {enhanced_name}: {e}")
        return False

def process_reports_from_supabase(model_name="gemini-2.0-flash"):
    """
    Process Lassa fever reports based on metadata from Supabase.
    
    Args:
        model_name (str): Name of the Gemini model to use for processing
        
    Returns:
        None: Processes reports and updates Supabase
    """
    logging.info("Starting LLM extraction process from Supabase data")
    
    # Critical environment variable checks
    if not DATABASE_URL:
        logging.error("CRITICAL: DATABASE_URL environment variable not set. Exiting.")
        return
    
    try:
        engine = get_db_engine(DATABASE_URL)
        with engine.connect() as connection:
            logging.info("Successfully connected to Supabase database.")
    except Exception as e:
        logging.error(f"CRITICAL: Failed to create SQLAlchemy engine or connect to Supabase: {e}")
        return
    
    # Get reports that need processing
    reports = get_reports_to_process(engine)
    if not reports:
        logging.info("No reports to process")
        return
    
    # Process each report
    processed_count = 0
    for report in reports:
        success = process_single_report(report, model_name, engine)
        if success:
            processed_count += 1
    
    logging.info(f"Finished processing reports. Successfully processed: {processed_count}/{len(reports)}")

def main():
    """
    Main function to execute the table extraction and sorting process.
    
    Processes enhanced images based on Supabase data, extracts table data,
    and updates the processing status in Supabase.
    
    Returns:
        None
    """
    logging.info("Starting Lassa fever report table extraction and sorting process")
    process_reports_from_supabase(model_name="gemini-3-flash-preview")
    logging.info("Finished LLM extraction process")

if __name__ == "__main__":
    main()
