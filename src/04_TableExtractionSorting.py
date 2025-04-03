import os
import csv
import copy
import logging
from pathlib import Path
from dotenv import load_dotenv
from google import genai
from PIL import Image
from pydantic import BaseModel, Field

# Configure logging with AFC suppression
class NewlineLoggingHandler(logging.StreamHandler):
    """Custom logging handler that adds a newline after each log entry and filters AFC logs."""
    def filter(self, record):
        return 'afc' not in record.getMessage().lower()
        
    def emit(self, record):
        super().emit(record)
        self.stream.write('\n')
        self.flush()

logging.basicConfig(
    level=logging.INFO, 
    format='%(levelname)s: %(message)s', 
    handlers=[NewlineLoggingHandler()]
)

# Set third-party loggers to higher levels
# ADD 'google.api_core' or other potential sources
for logger_name in ["google", "google.genai", "google.api_core", "httpx", "httpcore"]: 
    logging.getLogger(logger_name).setLevel(logging.ERROR)    

# Define base paths and constants
BASE_DIR = Path(__file__).parent.parent
CSV_FILE = BASE_DIR / 'data' / 'documentation' / 'website_raw_data.csv'

# Initialize the Gemini client with your API key
load_dotenv()
client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

# Define the Pydantic model for one row of the table.
class TableRow(BaseModel):
    States: str = Field(..., alias="States")
    Suspected: str = Field(..., alias="Suspected")
    Confirmed: str = Field(..., alias="Confirmed")
    Probable: str = Field(..., alias="Probable")
    HCW: str = Field(..., alias="HCW*")
    Deaths: str = Field(..., alias="Deaths (Confirmed Cases)")

try:
    from prompts.table_extraction_prompt import TABLE_EXTRACTION_PROMPT
except ImportError:
    from src.prompts.table_extraction_prompt import TABLE_EXTRACTION_PROMPT

# Use the imported prompt
prompt_template = TABLE_EXTRACTION_PROMPT

def sort_table_rows(table_rows):
    """
    Sort the extracted table rows alphabetically by the 'States' field (case-insensitive),
    excluding rows where 'States' is blank, and preserving the 'Total' row at the end.
    Additionally, remove rows (except for the 'Total' row) where all keys apart from 'States' are blank.
    """
    # Identify the Total row (assumes the Total row has a "States" value equal to "Total")
    total_rows = [row for row in table_rows if row.get("States", "").strip().lower() == "total"]

    # For the rest, filter out rows where 'States' is blank or only whitespace, skip "Total",
    # and remove rows where every key except 'States' is blank.
    non_total_rows = [
        row for row in table_rows 
        if row.get("States", "").strip() and row.get("States", "").strip().lower() != "total"
        and any(str(row.get(k, "")).strip() for k in row if k != "States")
    ]
    
    # Sort non-total rows alphabetically by the 'States' field (case-insensitive)
    sorted_rows = sorted(non_total_rows, key=lambda x: x["States"].strip().lower())
    
    # Append the Total row at the end if it exists (even if other fields are blank)
    if total_rows:
        sorted_rows.extend(total_rows)
    return sorted_rows

def normalize_state_names(rows):
    """
    Convert all state names to lowercase and replace hyphens with spaces.
    This function returns a new list of dictionaries without modifying the originals.
    """
    new_rows = []
    for row in rows:
        new_row = row.copy()
        if "States" in new_row and new_row["States"]:
            new_row["States"] = new_row["States"].lower().replace("-", " ")
        new_rows.append(new_row)
    return new_rows

def filter_comparison_columns(rows):
    """
    Filter out columns that are not relevant for comparison (HCW and Probable).
    Only keep States, Suspected, Confirmed, and Deaths for comparison purposes.
    Returns a new list of dictionaries without modifying the originals.
    """
    relevant_columns = ["States", "Suspected", "Confirmed", "Deaths (Confirmed Cases)"]
    filtered_rows = []
    
    for row in rows:
        filtered_row = {}
        for column in relevant_columns:
            if column in row:
                filtered_row[column] = row[column]
        filtered_rows.append(filtered_row)
    
    return filtered_rows

def validate_logical_consistency(rows):
    """
    Validate logical consistency of extracted data based on these rules:
    1. Suspected >= Confirmed
    2. Suspected >= Deaths
    3. All values must be non-negative integers
    
    Returns:
        tuple: (is_valid, validated_rows, error_messages)
            - is_valid: Boolean indicating if all rows are valid
            - validated_rows: List of rows with inconsistencies fixed or flagged
            - error_messages: List of error messages describing inconsistencies
    """
    validated_rows = []
    error_messages = []
    is_valid = True
    
    for row in rows:
        row_copy = row.copy()
        state_name = row_copy.get("States", "Unknown")
        
        # Convert values to integers, defaulting to 0 for empty or non-numeric values
        try:
            suspected = int(row_copy.get("Suspected", "0") or "0")
        except ValueError:
            suspected = 0
            error_messages.append(f"Non-numeric Suspected value for {state_name}: '{row_copy.get('Suspected', '')}'")
            is_valid = False
            
        try:
            confirmed = int(row_copy.get("Confirmed", "0") or "0")
        except ValueError:
            confirmed = 0
            error_messages.append(f"Non-numeric Confirmed value for {state_name}: '{row_copy.get('Confirmed', '')}'")
            is_valid = False
            
        try:
            deaths = int(row_copy.get("Deaths (Confirmed Cases)", "0") or "0")
        except ValueError:
            deaths = 0
            error_messages.append(f"Non-numeric Deaths value for {state_name}: '{row_copy.get('Deaths (Confirmed Cases)', '')}'")
            is_valid = False
        
        # Check logical consistency
        if suspected < confirmed:
            error_messages.append(f"Logical inconsistency for {state_name}: Suspected ({suspected}) < Confirmed ({confirmed})")
            is_valid = False
            # Fix: Set Confirmed to match Suspected if Suspected is non-zero, otherwise zero both
            if suspected > 0:
                row_copy["Confirmed"] = str(suspected)
            else:
                row_copy["Suspected"] = "0"
                row_copy["Confirmed"] = "0"
        
        if suspected < deaths:
            error_messages.append(f"Logical inconsistency for {state_name}: Suspected ({suspected}) < Deaths ({deaths})")
            is_valid = False
            # Fix: Set Deaths to match Suspected if Suspected is non-zero, otherwise zero both
            if suspected > 0:
                row_copy["Deaths (Confirmed Cases)"] = str(suspected)
            else:
                row_copy["Suspected"] = "0"
                row_copy["Deaths (Confirmed Cases)"] = "0"
        
        # Add a flag to indicate this row had inconsistencies
        if row_copy != row:
            row_copy["_had_inconsistencies"] = True
        
        validated_rows.append(row_copy)
    
    return is_valid, validated_rows, error_messages

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
        enhanced_name = row.get('Enhanced_name', '').strip()
        compatible = row.get('Compatible', '').strip()
        
        # Skip if not enhanced or not compatible or not in our year range
        if enhanced != 'Y' or compatible == 'N' or not year or not week:
            continue
        
        # Only process years 2021-2025
        if year not in ['21', '22', '23', '24', '25']:
            continue
        
        # Skip if no enhanced_name is provided
        if not enhanced_name:
            logging.warning(f"No enhanced image name for Year {year}, Week {week}")
            continue
        
        full_year = f"20{year}"
        
        # Input folder with enhanced images
        input_dir = BASE_DIR / 'data' / 'processed' / f"PDFs_Lines_{full_year}"
        input_path = input_dir / enhanced_name
        
        # Output folder for CSV files
        output_dir = BASE_DIR / 'data' / 'processed' / f"CSV_LF_{year}_Sorted"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Output CSV filename
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
        
        # Case 3: Input file doesn't exist - can't process
        if not input_path.exists():
            logging.warning(f"Enhanced image not found: {input_path}")
            continue

        # At this point we have:
        # - No output file exists
        # - Input file exists
        # - Status is not 'Y' (either reset or was never processed)
        # So we proceed with processing
        
        logging.info(f"Processing {enhanced_name} (Year: {year}, Week: {week})")
        
        try:
            # Open the image
            image = Image.open(input_path)
            
            # Process the image with retry logic
            max_attempts = 3  # Maximum number of attempts to get matching outputs
            attempt = 1
            
            while attempt <= max_attempts:
                responses = []
                for i in range(2):
                    try:
                        response = client.models.generate_content(
                            model=model_name,
                            contents=[prompt_template, image],
                            config={
                                "response_mime_type": "application/json",
                                "response_schema": list[TableRow],
                            }
                        )
                        responses.append(response)
                    except Exception as e:
                        logging.error(f"Error during API call for image {enhanced_name} on iteration {i+1}, attempt {attempt}: {e}")
                        break  # Skip to next image if there's an error
                
                if len(responses) != 2:
                    logging.error(f"Skipping image due to API call errors: {enhanced_name}")
                    break  # Exit the retry loop if we couldn't get two responses
                
                # Parse the responses into lists of TableRow objects
                try:
                    table_rows_1 = responses[0].parsed  # List[TableRow]
                    table_rows_2 = responses[1].parsed  # List[TableRow]
                except Exception as e:
                    logging.error(f"Error parsing responses for image {enhanced_name}, attempt {attempt}: {e}")
                    break  # Exit the retry loop if we couldn't parse the responses
                
                # Convert both lists into lists of dictionaries
                dict_rows_1 = [row.model_dump(by_alias=True) for row in table_rows_1]
                dict_rows_2 = [row.model_dump(by_alias=True) for row in table_rows_2]
                
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
                
                # Create sorted versions for comparison by filtering out blank 'States' rows,
                # removing rows where all keys (except 'States') are blank, and sorting alphabetically (excluding 'Total' row)
                sorted_dict_rows_1 = sort_table_rows(dict_rows_1)
                sorted_dict_rows_2 = sort_table_rows(dict_rows_2)
                
                # Normalize state names for comparison
                normalized_1 = normalize_state_names(copy.deepcopy(sorted_dict_rows_1))
                normalized_2 = normalize_state_names(copy.deepcopy(sorted_dict_rows_2))
                
                # Filter out irrelevant columns (HCW and Probable) for comparison
                comparison_1 = filter_comparison_columns(normalized_1)
                comparison_2 = filter_comparison_columns(normalized_2)
                
                fieldnames_table = [
                    "States", "Suspected", "Confirmed",
                    "Probable", "HCW*", "Deaths (Confirmed Cases)"
                ]
                
                # Compare only the relevant columns
                if comparison_1 == comparison_2:
                    logging.info(f"Both outputs are identical on relevant columns (States, Suspected, Confirmed, Deaths) on attempt {attempt}. Saving CSV.")
                    # At the stage of writing CSV, filter out any rows where all columns apart from 'States' are empty strings
                    filtered_rows = [
                        row for row in dict_rows_1
                        if row.get("States", "").strip() and (
                            row.get("States", "").strip().lower() == "total" or
                            any(str(row.get(k, "")).strip() for k in row if k != "States" and not k.startswith("_"))
                        )
                    ]
                    
                    # Write the filtered data to CSV
                    try:
                        with open(output_path, mode="w", newline="", encoding="utf-8") as csvfile:
                            writer = csv.DictWriter(csvfile, fieldnames=fieldnames_table)
                            writer.writeheader()
                            for table_row in filtered_rows:
                                # Remove any internal fields before writing
                                csv_row = {k: v for k, v in table_row.items() if not k.startswith("_")}
                                writer.writerow(csv_row)
                        
                        # Update row if CSV writing succeeded
                        row['Processed'] = 'Y'
                        modified = True
                        logging.info(f"Successfully processed: {base_filename}.csv")
                    except Exception as e:
                        logging.error(f"Error writing CSV for image {enhanced_name}: {e}")
                    
                    # Exit the retry loop since we got matching outputs
                    break
                else:
                    logging.warning(f"Outputs differ on relevant columns (States, Suspected, Confirmed, Deaths) between iterations for image: {enhanced_name} (attempt {attempt}/{max_attempts})")
                    
                    # Record differences to a text file in the output directory
                    diff_file = output_dir / "differing_outputs.txt"
                    try:
                        with open(diff_file, "a", encoding="utf-8") as f:
                            f.write(f"Differences in {enhanced_name} (attempt {attempt}/{max_attempts}):\n")
                            # Identify differing rows
                            min_len = min(len(normalized_1), len(normalized_2))
                            for i in range(min_len):
                                if normalized_1[i] != normalized_2[i]:
                                    f.write(f"  Row {i+1}:\n")
                                    f.write(f"    Iteration 1: {normalized_1[i]}\n")
                                    f.write(f"    Iteration 2: {normalized_2[i]}\n")
                            # Check for any extra rows
                            if len(normalized_1) > min_len:
                                f.write("  Additional rows in iteration 1:\n")
                                for i in range(min_len, len(normalized_1)):
                                    f.write(f"    Row {i+1}: {normalized_1[i]}\n")
                            if len(normalized_2) > min_len:
                                f.write("  Additional rows in iteration 2:\n")
                                for i in range(min_len, len(normalized_2)):
                                    f.write(f"    Row {i+1}: {normalized_2[i]}\n")
                            f.write("\n" + "-" * 80 + "\n")
                    except Exception as e:
                        logging.error(f"Error writing differences file: {e}")
                    
                    # Increment the attempt counter and try again if we haven't reached the maximum
                    attempt += 1
                    
                    # If this was the last attempt and outputs still differ, log a final message
                    if attempt > max_attempts:
                        logging.error(f"Failed to get matching outputs after {max_attempts} attempts for image: {enhanced_name}")
            
        except Exception as e:
            logging.error(f"Error processing {enhanced_name}: {e}")
    
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
