"""
Gemini API interaction utilities for table extraction from PDF images.

This module provides functions to interact with the Google Gemini API
for extracting tabular data from images of Lassa fever reports.
"""

import logging
import os
from pathlib import Path
from dotenv import load_dotenv
from google import genai
from PIL import Image
from pydantic import BaseModel, Field
from google.genai import types

# Utility modules should use logging but not configure it - configuration is done in main scripts

# Initialize the Gemini client with API key
load_dotenv()
client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

# Define the Pydantic model for one row of the table
class TableRow(BaseModel):
    """Pydantic model representing one row of the Lassa fever table."""
    States: str = Field(..., alias="States")
    Suspected: str = Field(..., alias="Suspected")
    Confirmed: str = Field(..., alias="Confirmed")
    Probable: str = Field(..., alias="Probable")
    HCW: str = Field(..., alias="HCW")
    Deaths: str = Field(..., alias="Deaths")

# Import the prompt template with appropriate error handling
try:
    from prompts.table_extraction_prompt import TABLE_EXTRACTION_PROMPT
except ImportError:
    from src.prompts.table_extraction_prompt import TABLE_EXTRACTION_PROMPT

# Use the imported prompt
prompt_template = TABLE_EXTRACTION_PROMPT


def extract_table_with_gemini(image_path, model_name):
    """
    Extract table data from an image using the Gemini API.
    
    Args:
        image_path (Path): Path to the image file
        model_name (str): Name of the Gemini model to use
        
    Returns:
        tuple: (success, response) where success is a boolean and response is either the API response or an error message
    """
    try:
        # Open the image
        image = Image.open(image_path)
        
        # Call the Gemini API
        if model_name == "gemini-2.0-flash":
            response = client.models.generate_content(
                model=model_name,
                contents=[prompt_template, image],
                config={
                    "response_mime_type": "application/json",
                    "response_schema": list[TableRow],
                }
            )
        elif model_name == "gemini-2.5-flash-preview-04-17":
            response = client.models.generate_content(
                model=model_name,
                contents=[prompt_template, image],
                config = types.GenerateContentConfig(
                    thinking_config = types.ThinkingConfig(
                        thinking_budget=0,
                    ),
                    response_mime_type="application/json",
                    response_schema=list[TableRow],
                )
            )
        return True, response
    except Exception as e:
        return False, str(e)


def parse_gemini_response(response):
    """
    Parse the Gemini API response into a list of dictionaries.

    Args:
        response: The Gemini API response object

    Returns:
        tuple: (success, result) where success is a boolean and result is either a list of dictionaries or an error message
    """
    try:
        if response is None:
            return False, "Gemini API response is None."
        table_rows = getattr(response, "parsed", None)
        if table_rows is None:
            return False, "Gemini API response has no 'parsed' data."
        dict_rows = [row.model_dump(by_alias=True) for row in table_rows]
        return True, dict_rows
    except Exception as e:
        return False, f"Exception during parsing: {str(e)}"


def log_extraction_differences(diff_file, enhanced_name, attempt, max_attempts, normalized_1, normalized_2):
    """
    Log differences between two extraction iterations to a text file.
    
    Args:
        diff_file (Path): Path to the differences log file
        enhanced_name (str): Name of the enhanced image being processed
        attempt (int): Current attempt number
        max_attempts (int): Maximum number of attempts
        normalized_1 (list): First set of normalized rows
        normalized_2 (list): Second set of normalized rows
        
    Returns:
        bool: True if logging was successful, False otherwise
    """
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
        return True
    except Exception as e:
        logging.error(f"Error writing differences file: {e}")
        return False


def save_extracted_data_to_csv(data, output_path, fieldnames, year=None, week=None):
    """
    Save extracted table data to a CSV file.
    
    Args:
        data (list): List of dictionaries containing table data
        output_path (Path): Path to save the CSV file
        fieldnames (list): List of column names for the CSV
        year (str, optional): Year of the report (YY format)
        week (str, optional): Week number of the report
        
    Returns:
        bool: True if saving was successful, False otherwise
    """
    import csv
    try:
        # Filter out rows where all columns apart from 'States' are empty strings
        filtered_rows = [
            row for row in data
            if row.get("States", "").strip() and (
                row.get("States", "").strip().lower() == "total" or
                any(str(row.get(k, "")).strip() for k in row if k != "States" and not k.startswith("_"))
            )
        ]
        
        # Write the filtered data to CSV
        with open(output_path, mode="w", newline="", encoding="utf-8") as csvfile:
            # Add Year and Week to fieldnames if provided
            if year is not None and 'Year' not in fieldnames:
                fieldnames = ['Year'] + fieldnames
            if week is not None and 'Week' not in fieldnames:
                fieldnames = ['Week'] + fieldnames
                
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for table_row in filtered_rows:
                # Remove any internal fields and add Year/Week
                csv_row = {k: v for k, v in table_row.items() if not k.startswith("_")}
                if year is not None:
                    csv_row['Year'] = f"20{year}"
                if week is not None:
                    csv_row['Week'] = week
                writer.writerow(csv_row)
        return True
    except Exception as e:
        logging.error(f"Error writing CSV: {e}")
        return False