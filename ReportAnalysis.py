import os
import glob
import csv
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
    Trend: str = Field(..., alias="Trend")
    Probable: str = Field(..., alias="Probable")
    HCW: str = Field(..., alias="HCW*")
    Deaths: str = Field(..., alias="Deaths (Confirmed Cases)")

# Define the prompt with instructions to extract JSON formatted output.
prompt_template = """
The provided image contains a table with a section labeled "Current Week". Your task is to extract the data from this section only.
The "Current Week" section has the following columns in this exact left-to-right order:
1. States
2. Suspected
3. Confirmed
4. Trend
5. Probable
6. HCW*
7. Deaths (Confirmed Cases)

Extract the values located under each column header and return the results in JSON format.
Return a JSON list of objects, where each object corresponds to one row of the table.

Each object must have the following keys (exactly in this order):
"States", "Suspected", "Confirmed", "Trend", "Probable", "HCW*", "Deaths (Confirmed Cases)".

"States" corresponds to the states of Nigeria: Ondo, Edo, Bauchi, Taraba, Benue, Ebonyi, Kogi, Kaduna, Plateau, Enugu, Cross River, Rivers, Delta, Nasarawa, Anambra, Gombe, Niger, Imo, Jigawa, Bayelsa, Adamawa, Fct, Katsina, Kano, Oyo, Lagos, Ogun, Yobe, Sokoto, Kebbi, Zamfara, Akwa Ibom, Ekiti, Kwara, Borno, Osun, Abia. The last row should correspond to the "Total" for all states.
You should include every state in the "States" column, even if other columns in that row are blank.
If a row is fully blank and has no value in "States" column, you should omit it.

"Trend" column may contain one of two types of triangles: ▲ (Up, red triangle) or ▼ (Down, green triangle). You should input "Up" for red triangle ▲ or "Down" for green triangle ▼.

Include one object per state, and the last object should correspond to the "Total" row.
Ensure that all keys are present in every object, even if some values are blank.
Output the JSON in valid format.
"""

def process_images(input_folder: str, output_folder: str):
    # Get list of PNG images from the input_folder
    image_paths = glob.glob(os.path.join(input_folder, "*.png"))
    if not image_paths:
        print(f"No PNG images found in the folder '{input_folder}'. Please check the folder name and path.")
        return

    fieldnames = [
        "States",
        "Suspected",
        "Confirmed",
        "Trend",
        "Probable",
        "HCW*",
        "Deaths (Confirmed Cases)"
    ]
    
    for image_path in image_paths:
        base_filename = os.path.splitext(os.path.basename(image_path))[0]
        csv_filename = os.path.join(output_folder, f"{base_filename}.csv")
        # Check if the CSV file already exists
        if os.path.exists(csv_filename):
            print(f"CSV file already exists for {image_path}. Skipping.")
            continue
        
        print("Processing image:", image_path)
        # Open the image using Pillow
        try:
            image = Image.open(image_path)
        except Exception as e:
            print(f"Error opening image {image_path}: {e}")
            continue

        # Process the image twice
        responses = []
        for i in range(2):
            try:
                response = client.models.generate_content(
                    model="gemini-2.0-flash",
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
                break  # Skip to next image if there's an error

        if len(responses) != 2:
            print("Skipping image due to API call errors.")
            continue

        # Parse the responses into lists of TableRow objects
        try:
            table_rows_1 = responses[0].parsed  # List[TableRow]
            table_rows_2 = responses[1].parsed  # List[TableRow]
        except Exception as e:
            print(f"Error parsing responses for image {image_path}: {e}")
            continue

        # Convert both lists into lists of dictionaries
        dict_rows_1 = [row.dict(by_alias=True) for row in table_rows_1]
        dict_rows_2 = [row.dict(by_alias=True) for row in table_rows_2]

        # Compare the two outputs
        if dict_rows_1 == dict_rows_2:
            print("Both outputs are identical. Saving CSV.")
            # Write the extracted data to CSV
            try:
                with open(csv_filename, mode="w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    for row in dict_rows_1:
                        writer.writerow(row)
                print(f"CSV file saved as: {csv_filename}")
            except Exception as e:
                print(f"Error writing CSV for image {image_path}: {e}")
        else:
            print("Outputs differ between iterations for image:", image_path)
            # Identify differing rows
            min_len = min(len(dict_rows_1), len(dict_rows_2))
            differences_found = False
            
            for i in range(min_len):
                if dict_rows_1[i] != dict_rows_2[i]:
                    differences_found = True
                    print(f"Difference in row {i+1}:")
                    print("Iteration 1:", dict_rows_1[i])
                    print("Iteration 2:", dict_rows_2[i])
            
            # Check for any extra rows
            if len(dict_rows_1) > min_len:
                differences_found = True
                print("Additional rows in iteration 1:")
                for i in range(min_len, len(dict_rows_1)):
                    print(f"Row {i+1}:", dict_rows_1[i])
            if len(dict_rows_2) > min_len:
                differences_found = True
                print("Additional rows in iteration 2:")
                for i in range(min_len, len(dict_rows_2)):
                    print(f"Row {i+1}:", dict_rows_2[i])
            
            if not differences_found:
                print("No individual row differences were found, despite overall inequality.")
            
            print("Skipping CSV saving for this image due to inconsistency.")
        
        print("-" * 80)

# Optionally, call the function with desired folders
process_images("PDFs_Lines", "CSV_LF")