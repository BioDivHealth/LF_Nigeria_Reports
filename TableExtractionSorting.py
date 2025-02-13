import os
import glob
import csv
import copy
from dotenv import load_dotenv
from google import genai
from PIL import Image
from pydantic import BaseModel, Field

# Load the API key from the .env file
load_dotenv()
api_key = os.getenv("GOOGLE_API_KEY")
if not api_key:
    raise ValueError("GEMINI_API_KEY is not set in your .env file. Please add it and restart the notebook.")

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

"States" corresponds to the states of Nigeria: Ondo, Edo, Bauchi, Taraba, Benue, Ebonyi, Kogi, Kaduna, Plateau, Enugu, Cross River, Rivers, Delta, Nasarawa, Anambra, Gombe, Niger, Imo, Jigawa, Bayelsa, Adamawa, Fct, Katsina, Kano, Oyo, Lagos, Ogun, Yobe, Sokoto, Kebbi, Zamfara, Akwa Ibom, Ekiti, Kwara, Borno, Osun, Abia.
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

def process_images(input_folder: str, output_folder: str, model_name: str):
    """
    Process multiple PNG images in a folder and extract tabular data using an LLM.
    This function processes PNG images containing tabular data (Lassa fever reports), runs them through a Gemini AI model twice for validation, 
    and saves the extracted data to CSV files when the two processing attempts yield identical results.
    
    Args:
        input_folder (str): Path to the folder containing PNG images to process.
        output_folder (str): Path to the folder where CSV files will be saved.
        model_name (str): Name of the LLM to use for processing.
    """
    
    non_identical_files = []  # List to store filenames with non-identical outputs.
    # Get list of PNG images from the input_folder.
    image_paths = glob.glob(os.path.join(input_folder, "*.png"))
    if not image_paths:
        print(f"No PNG images found in the folder '{input_folder}'. Please check the folder name and path.")
        return

    fieldnames = [
        "States",
        "Suspected",
        "Confirmed",
        # "Trend",  # Ignored column
        "Probable",
        "HCW*",
        "Deaths (Confirmed Cases)"
    ]
    
    for image_path in image_paths:
        base_filename = os.path.splitext(os.path.basename(image_path))[0]
        csv_filename = os.path.join(output_folder, f"{base_filename}.csv")
        # Check if the CSV file already exists.
        if os.path.exists(csv_filename):
            print(f"CSV file already exists for {image_path}. Skipping.")
            continue
        
        print("Processing image:", image_path)
        # Open the image using Pillow.
        try:
            image = Image.open(image_path)
        except Exception as e:
            print(f"Error opening image {image_path}: {e}")
            continue

        # Process the image twice.
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
                print(f"Error during API call for image {image_path} on iteration {i+1}: {e}")
                responses = []
                break  # Skip to next image if there's an error.

        if len(responses) != 2:
            print("Skipping image due to API call errors.")
            continue

        # Parse the responses into lists of TableRow objects.
        try:
            table_rows_1 = responses[0].parsed  # List[TableRow]
            table_rows_2 = responses[1].parsed  # List[TableRow]
        except Exception as e:
            print(f"Error parsing responses for image {image_path}: {e}")
            continue

        # Convert both lists into lists of dictionaries.
        dict_rows_1 = [row.dict(by_alias=True) for row in table_rows_1]
        dict_rows_2 = [row.dict(by_alias=True) for row in table_rows_2]

        # Create sorted versions for comparison by filtering out blank 'States' rows,
        # removing rows where all keys (except 'States') are blank, and sorting alphabetically (excluding 'Total' row).
        sorted_dict_rows_1 = sort_table_rows(dict_rows_1)
        sorted_dict_rows_2 = sort_table_rows(dict_rows_2)
        
        # Normalize the 'States' field on copies so that original dict_rows_1 remain intact.
        sorted_dict_rows_1 = normalize_state_names(copy.deepcopy(sorted_dict_rows_1))
        sorted_dict_rows_2 = normalize_state_names(copy.deepcopy(sorted_dict_rows_2))

        # Compare the sorted outputs.
        if sorted_dict_rows_1 == sorted_dict_rows_2:
            print("Both sorted outputs are identical. Saving CSV using original output order.")
            # At the stage of writing CSV, filter out any rows where all columns apart from 'States' are empty strings.
            filtered_rows = [
                row for row in dict_rows_1
                if any(str(row.get(k, "")).strip() for k in row if k != "States")
            ]
            # Write the filtered data to CSV.
            try:
                with open(csv_filename, mode="w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    for row in filtered_rows:
                        writer.writerow(row)
                print(f"CSV file saved as: {csv_filename}")
            except Exception as e:
                print(f"Error writing CSV for image {image_path}: {e}")
        else:
            print("Outputs differ between iterations for image:", image_path)
            non_identical_files.append(os.path.basename(image_path))  # Record the filename.
            # Identify differing rows in the sorted outputs.
            min_len = min(len(sorted_dict_rows_1), len(sorted_dict_rows_2))
            differences_found = False
            
            for i in range(min_len):
                if sorted_dict_rows_1[i] != sorted_dict_rows_2[i]:
                    differences_found = True
                    print(f"Difference in row {i+1}:")
                    print("Iteration 1:", sorted_dict_rows_1[i])
                    print("Iteration 2:", sorted_dict_rows_2[i])
            
            # Check for any extra rows.
            if len(sorted_dict_rows_1) > min_len:
                differences_found = True
                print("Additional rows in iteration 1:")
                for i in range(min_len, len(sorted_dict_rows_1)):
                    print(f"Row {i+1}:", sorted_dict_rows_1[i])
            if len(sorted_dict_rows_2) > min_len:
                differences_found = True
                print("Additional rows in iteration 2:")
                for i in range(min_len, len(sorted_dict_rows_2)):
                    print(f"Row {i+1}:", sorted_dict_rows_2[i])
            
            if not differences_found:
                print("No individual row differences were found, despite overall inequality.")
            
            print("Skipping CSV saving for this image due to inconsistency.")
        
        print("-" * 80)
    
    # After processing all images, save non-identical filenames to a text file if any exist.
    if non_identical_files:
        non_identical_filepath = os.path.join(output_folder, "non_identical_files.txt")
        try:
            with open(non_identical_filepath, "w", encoding="utf-8") as f:
                for filename in non_identical_files:
                    f.write(filename + "\n")
            print(f"Non-identical file names saved in: {non_identical_filepath}")
        except Exception as e:
            print(f"Error writing non-identical filenames file: {e}")

# Optionally, call the function with desired folders and model.
process_images("2021/PDFs_Lines_2021", 
               "2021/CSV_LF_21_Sorted", 
               model_name="gemini-2.0-flash")

# Models available:
# "gemini-2.0-flash"
# "gemini-2.0-pro-exp-02-05"
# "gemini-2.0-flash-thinking-exp"
# "gemini-2.0-flash-lite-preview-02-05"
# "gemini-1.5-pro"
