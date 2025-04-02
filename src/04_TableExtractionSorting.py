import os
import glob
import csv
import copy
import logging
from pathlib import Path
from dotenv import load_dotenv
from google import genai
from PIL import Image
from pydantic import BaseModel, Field

# Configure logging
class NewlineLoggingHandler(logging.StreamHandler):
    """Custom logging handler that adds a newline after each log entry."""
    def emit(self, record):
        super().emit(record)
        self.stream.write('\n')
        self.flush()

logging.basicConfig(
    level=logging.INFO, 
    format='%(levelname)s: %(message)s', 
    handlers=[NewlineLoggingHandler()]
)

# Suppress specific warnings and logs from the Google GenAI library
logging.getLogger("google.genai").setLevel(logging.ERROR)
# Suppress HTTP request and AFC logs
logging.getLogger("httpx").setLevel(logging.ERROR)
logging.getLogger("httpcore").setLevel(logging.ERROR)
# Suppress any loggers with "afc" in the name
for logger_name in logging.root.manager.loggerDict:
    if "afc" in logger_name.lower() or "http" in logger_name.lower():
        logging.getLogger(logger_name).setLevel(logging.ERROR)

# Define base paths and constants
BASE_DIR = Path(__file__).parent.parent
CSV_FILE = BASE_DIR / 'data' / 'documentation' / 'website_raw_data.csv'

# Load the API key from the .env file
load_dotenv()
api_key = os.getenv("GOOGLE_API_KEY")
if not api_key:
    raise ValueError("GOOGLE_API_KEY is not set in your .env file. Please add it and restart the notebook.")

# Initialize the Gemini client with your API key
client = genai.Client(api_key=api_key)

# Define the Pydantic model for one row of the table.
class TableRow(BaseModel):
    States: str = Field(..., alias="States")
    Suspected: str = Field(..., alias="Suspected")
    Confirmed: str = Field(..., alias="Confirmed")
    # Trend: str = Field(..., alias="Trend")  # Ignored column
    Probable: str = Field(..., alias="Probable")
    HCW: str = Field(..., alias="HCW*")
    Deaths: str = Field(..., alias="Deaths (Confirmed Cases)")

# Define the prompt with instructions to extract JSON formatted output.
prompt_template = """
The provided image contains a table with weekly Lassa Fever case data across States in Nigeria. Your task is to extract the data from the table.
The table has the following columns in this exact left-to-right order:
1. States
2. Suspected
3. Confirmed
4. Trend (ignore this column - it is not needed)
5. Probable
6. HCW*
7. Deaths (Confirmed Cases)

Extract the values located under each column headers (States, Suspected, Confirmed, Probable, HCW*, Deaths (Confirmed Cases)) and return the results in JSON format. Do not hallucinate any values if a cell is empty. 
Ignore the "Trend" column.
Return a JSON list of objects, where each object corresponds to one row of the table.

Each object must have the following keys (exactly in this order):
"States", "Suspected", "Confirmed", "Probable", "HCW*", "Deaths (Confirmed Cases)".

"States" corresponds to the states of Nigeria: Ondo, Edo, Bauchi, Taraba, Benue, Ebonyi, Kogi, Kaduna, Plateau, Enugu, Cross River, Rivers, Delta, Nasarawa, Anambra, Gombe, Niger, Imo, Jigawa, Bayelsa, Adamawa, Fct, Katsina, Kano, Oyo, Lagos, Ogun, Yobe, Sokoto, Kebbi, Zamfara, Akwa Ibom, Ekiti, Kwara, Borno, Osun, Abia. These are the correct names, sometimes there may be a typo in the image.
You should include ONLY the names of the States that you see in the image.
You can only use these names of states, but order may often differ. Not all states have to be included in an image. You need to write the names of States in the order in which they appear in the image you see.

Include one object per State you see in the image, and the last object should correspond to the "Total" row.
Ensure that all keys are present in every object, even if some values are blank.
Output the JSON in valid format.
"""

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

def process_reports_from_csv(model_name="gemini-2.0-flash"):
    """
    Process Lassa fever reports based on metadata in the CSV file.
    
    Reads website_raw_data.csv, processes enhanced images that haven't been processed yet,
    and updates the 'Processed' status in the CSV.
    
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
            
            # Process the image twice for validation
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
                    logging.error(f"Error during API call for image {enhanced_name} on iteration {i+1}: {e}")
                    break  # Skip to next image if there's an error
            
            if len(responses) != 2:
                logging.error(f"Skipping image due to API call errors: {enhanced_name}")
                continue
            
            # Parse the responses into lists of TableRow objects
            try:
                table_rows_1 = responses[0].parsed  # List[TableRow]
                table_rows_2 = responses[1].parsed  # List[TableRow]
            except Exception as e:
                logging.error(f"Error parsing responses for image {enhanced_name}: {e}")
                continue
            
            # Convert both lists into lists of dictionaries
            dict_rows_1 = [row.model_dump(by_alias=True) for row in table_rows_1]
            dict_rows_2 = [row.model_dump(by_alias=True) for row in table_rows_2]
            
            # Create sorted versions for comparison by filtering out blank 'States' rows,
            # removing rows where all keys (except 'States') are blank, and sorting alphabetically (excluding 'Total' row)
            sorted_dict_rows_1 = sort_table_rows(dict_rows_1)
            sorted_dict_rows_2 = sort_table_rows(dict_rows_2)
            
            # Normalize state names for comparison
            normalized_1 = normalize_state_names(copy.deepcopy(sorted_dict_rows_1))
            normalized_2 = normalize_state_names(copy.deepcopy(sorted_dict_rows_2))
            
            fieldnames_table = [
                "States",
                "Suspected",
                "Confirmed",
                "Probable",
                "HCW*",
                "Deaths (Confirmed Cases)"
            ]
            
            # Compare the sorted outputs
            if normalized_1 == normalized_2:
                logging.info("Both outputs are identical. Saving CSV.")
                # At the stage of writing CSV, filter out any rows where all columns apart from 'States' are empty strings
                filtered_rows = [
                    row for row in dict_rows_1
                    if row.get("States", "").strip() and (
                        row.get("States", "").strip().lower() == "total" or
                        any(str(row.get(k, "")).strip() for k in row if k != "States")
                    )
                ]
                
                # Write the filtered data to CSV
                try:
                    with open(output_path, mode="w", newline="", encoding="utf-8") as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames_table)
                        writer.writeheader()
                        for table_row in filtered_rows:
                            writer.writerow(table_row)
                    
                    # Update row if CSV writing succeeded
                    row['Processed'] = 'Y'
                    modified = True
                    logging.info(f"Successfully processed: {base_filename}.csv")
                except Exception as e:
                    logging.error(f"Error writing CSV for image {enhanced_name}: {e}")
            else:
                logging.warning(f"Outputs differ between iterations for image: {enhanced_name}")
                # Record differences to a text file in the output directory
                diff_file = output_dir / "differing_outputs.txt"
                try:
                    with open(diff_file, "a", encoding="utf-8") as f:
                        f.write(f"Differences in {enhanced_name}:\n")
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
