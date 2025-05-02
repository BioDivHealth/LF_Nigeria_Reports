"""
Table extraction and sorting module for Lassa fever reports.

This module is the fourth stage in the Lassa Reports Scraping pipeline.
It extracts tabular data from enhanced PDF images using the Google Gemini API,
validates the data for logical consistency, and saves the results as CSV files.

Dependencies:
    - utils/gemini_extractor.py: For Gemini API interaction
    - utils/data_validation.py: For data validation and transformation
    - website_raw_data.csv: Contains metadata about reports to process

Inputs:
    - Enhanced PDF images in data/processed/PDFs_Lines_YYYY directories
    
Outputs:
    - CSV files with extracted table data in data/processed/CSV_LF_YY_Sorted directories
    - Updated 'Processed' status in website_raw_data.csv
"""

import os
import csv
import logging
from pathlib import Path

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
except ImportError:
    # Fall back to relative import (when run from main.py)
    from src.utils.gemini_extractor import (
        extract_table_with_gemini, parse_gemini_response,
        log_extraction_differences, save_extracted_data_to_csv)
    from src.utils.data_validation import (
        validate_logical_consistency, sort_table_rows,
        normalize_state_names, filter_comparison_columns)

# Set third-party loggers to higher levels
for logger_name in ["google", "google.genai", "google.api_core", "httpx", "httpcore"]: 
    logging.getLogger(logger_name).setLevel(logging.ERROR)    

# Define base paths and constants
BASE_DIR = Path(__file__).parent.parent
CSV_FILE = BASE_DIR / 'data' / 'documentation' / 'website_raw_data.csv'

def process_single_report(report_metadata, model_name, output_dir):
    """
    Process a single Lassa fever report.
    
    Args:
        report_metadata (dict): Dictionary containing report metadata
        model_name (str): Name of the Gemini model to use
        output_dir (Path): Directory to save the output CSV
        
    Returns:
        bool: True if processing was successful, False otherwise
    """
    # Extract metadata
    year = report_metadata.get('year', '').strip()
    week = report_metadata.get('week', '').strip()
    enhanced_name = report_metadata.get('Enhanced_name', '').strip()
    
    # Skip if no enhanced_name is provided
    if not enhanced_name:
        logging.warning(f"No enhanced image name for Year {year}, Week {week}")
        return False
    
    # Input folder with enhanced images
    input_dir = BASE_DIR / 'data' / 'processed' / f"PDFs_Lines_{year}"
    input_path = input_dir / enhanced_name
    
    # Check if input file exists
    if not input_path.exists():
        logging.warning(f"Enhanced image not found: {input_path}")
        return False
    
    # Output CSV filename
    base_filename = os.path.splitext(enhanced_name)[0]
    output_path = output_dir / f"{base_filename}.csv"
    
    # Check if output file already exists
    if output_path.exists():
        logging.info(f"Found existing processed file: {base_filename}.csv")
        return True
    
    logging.info(f"Processing {enhanced_name} (Year: {year}, Week: {week})")
    
    try:
        # Process the image with retry logic
        max_attempts = 3  # Maximum number of attempts to get matching outputs
        attempt = 1
        
        while attempt <= max_attempts:
            # Extract table data twice for validation
            responses = []
            for i in range(2):
                success, response = extract_table_with_gemini(input_path, model_name)
                if not success:
                    logging.error(f"Error during API call for image {enhanced_name} on iteration {i+1}, attempt {attempt}: {response}")
                    break
                responses.append(response)
            
            if len(responses) != 2:
                logging.error(f"Skipping image due to API call errors: {enhanced_name}")
                break  # Exit the retry loop if we couldn't get two responses
            
            # Parse the responses
            parsed_results = []
            for i, response in enumerate(responses):
                success, result = parse_gemini_response(response)
                if not success:
                    logging.error(f"Error parsing response for image {enhanced_name}, iteration {i+1}, attempt {attempt}: {result}")
                    break
                parsed_results.append(result)
            
            if len(parsed_results) != 2:
                break  # Exit the retry loop if we couldn't parse both responses
            
            dict_rows_1, dict_rows_2 = parsed_results
            
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
                continue
            
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
            
            fieldnames_table = [
                "States", "Suspected", "Confirmed",
                "Probable", "HCW", "Deaths"
            ]
            
            # Compare only the relevant columns
            if comparison_1 == comparison_2:
                logging.info(f"Both outputs are identical on relevant columns (States, Suspected, Confirmed, Deaths) on attempt {attempt}. Saving CSV.")
                
                # Save the data to CSV with Year and Week
                if save_extracted_data_to_csv(dict_rows_1, output_path, fieldnames_table, year=year, week=week):
                    logging.info(f"Successfully processed: {base_filename}.csv")
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
        
        return False  # If we got here, we didn't successfully process the report
    except Exception as e:
        logging.error(f"Error processing {enhanced_name}: {e}")
        return False


def process_reports_from_csv(model_name="gemini-2.0-flash"):
    """
    Process Lassa fever reports based on metadata in the CSV file.
    Reads website_raw_data.csv, processes enhanced images that haven't been processed yet, and updates the 'Processed' status in the CSV.
    
    Args:
        model_name (str): Name of the Gemini model to use for processing
        
    Returns:
        None: Updates are written to the CSV file and processed data saved as CSV files
    """
    # Read CSV and get rows
    try:
        with open(CSV_FILE, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            fieldnames = reader.fieldnames
            rows = list(reader)
    except Exception as e:
        logging.error(f"Error reading {CSV_FILE}: {e}")
        return
    
    # Ensure Processed column exists
    if 'Processed' not in fieldnames:
        fieldnames.append('Processed')
        for row in rows:
            if 'Processed' not in row:
                row['Processed'] = ''
    
    # Keep track of modified rows
    modified = False
    
    # Process only rows with Enhanced='Y' and Processed!='Y'
    for row in rows:
        year = row.get('year', '').strip()
        week = row.get('week', '').strip()
        enhanced = row.get('Enhanced', '').strip()
        processed = row.get('Processed', '').strip()
        compatible = row.get('Compatible', '').strip()
        
        # Skip if not enhanced or not compatible or not in our year range
        if enhanced != 'Y' or compatible == 'N' or not year or not week:
            continue
        
        # Only process years 2021-2025
        if year not in ['20', '21', '22', '23', '24', '25']:
            continue
        
        # Output folder for CSV files
        output_dir = BASE_DIR / 'data' / 'processed' / f"CSV_LF_{year}_Sorted"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Output CSV filename
        enhanced_name = row.get('Enhanced_name', '').strip()
        if not enhanced_name:
            logging.warning(f"No enhanced image name for Year {year}, Week {week}")
            continue
            
        base_filename = os.path.splitext(enhanced_name)[0]
        output_path = output_dir / f"{base_filename}.csv"
        
        # Handle file status logic
        # Case 1: Output file exists - mark as processed if not already
        if output_path.exists():
            if processed != 'Y':
                row['Processed'] = 'Y'
                modified = True
                logging.info(f"Found existing processed file: {base_filename}.csv")
            continue  # Skip further processing since file already exists
            
        # Case 2: Marked as processed but file doesn't exist - reset status
        if processed == 'Y':
            row['Processed'] = ''
            processed = ''
            modified = True
            logging.info(f"Reset processing status for {base_filename}.csv - file not found")
            # Continue processing to regenerate the file
        
        # Process the report
        success = process_single_report(row, model_name, output_dir)
        
        # Update processing status if successful
        if success:
            row['Processed'] = 'Y'
            modified = True
    
    # Write updated rows back to CSV if any changes were made
    if modified:
        try:
            with open(CSV_FILE, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            logging.info(f"Updated website_raw_data.csv with processing status")
        except Exception as e:
            logging.error(f"Error writing to {CSV_FILE}: {e}")


def main():
    """
    Main function to execute the table extraction and sorting process.
    
    Processes enhanced images from the website_raw_data.csv file that haven't been processed yet,
    and updates the CSV with the processing status.
    
    Returns:
        None
    """
    logging.info("Starting Lassa fever report table extraction and sorting process")
    process_reports_from_csv(model_name="gemini-2.0-flash")
    logging.info("Finished processing reports")


if __name__ == "__main__":
    main()
