"""
Data validation and transformation utilities for Lassa fever report data.

This module provides functions to validate, normalize, and transform
tabular data extracted from Lassa fever reports.
"""

import copy
import logging
import uuid
import pandas as pd


def add_uuid_column(df, id_column='id'):
    """
    Add (and/or populate) a UUID column.

    Ensures every row has a non‑null UUID. Existing values are kept;
    new values are generated only for rows where the column is missing,
    null, or empty.
    
    The column is properly typed as a PostgreSQL UUID for better performance and type safety.
    """
    import sqlalchemy.dialects.postgresql as pg
    
    if id_column not in df.columns:
        # Create the column first with proper UUID type
        df[id_column] = pd.Series(dtype='object')

    # Identify rows that still lack a UUID
    mask = df[id_column].isna() | (df[id_column] == '')
    
    # Generate UUID objects, not strings
    if mask.sum() > 0:
        df.loc[mask, id_column] = [uuid.uuid4() for _ in range(mask.sum())]
    
    # If there are string UUIDs already in the dataframe, convert them to UUID objects
    str_mask = df[id_column].apply(lambda x: isinstance(x, str))
    if str_mask.any():
        df.loc[str_mask, id_column] = df.loc[str_mask, id_column].apply(lambda x: uuid.UUID(x))
    
    return df


def sort_table_rows(table_rows):
    """
    Sort the extracted table rows alphabetically by the 'States' field (case-insensitive),
    excluding rows where 'States' is blank, and preserving the 'Total' row at the end.
    Additionally, remove rows (except for the 'Total' row) where all keys apart from 'States' are blank.
    
    Args:
        table_rows (list): List of dictionaries representing table rows
        
    Returns:
        list: Sorted list of dictionaries
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
    
    Args:
        rows (list): List of dictionaries representing table rows
        
    Returns:
        list: List of dictionaries with normalized state names
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
    
    Args:
        rows (list): List of dictionaries representing table rows
        
    Returns:
        list: List of dictionaries with only relevant columns
    """
    relevant_columns = ["States", "Suspected", "Confirmed", "Deaths"]
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
    2. Confirmed >= Deaths
    3. All values must be non-negative integers
    
    Args:
        rows (list): List of dictionaries representing table rows
        
    Returns:
        tuple: (
            is_valid (bool): True if all rows are valid, False otherwise
            validated_rows (list): List of rows with inconsistencies fixed or flagged
            error_messages (list): List of error messages describing inconsistencies
        )
    """
    is_valid = True
    error_messages = []
    validated_rows = copy.deepcopy(rows)
    
    for i, row in enumerate(validated_rows):
        # Skip rows without a state name or the "Total" row for validation
        if not row.get("States", "").strip() or row.get("States", "").strip().lower() == "total":
            continue
        
        # Extract values, defaulting to 0 for missing or non-numeric values
        try:
            suspected = int(row.get("Suspected", "0").strip() or "0")
        except ValueError:
            suspected = 0
            error_messages.append(f"Non-numeric Suspected value in row {i+1} ({row.get('States', 'Unknown')}): {row.get('Suspected', '')}")
            is_valid = False
            
        try:
            confirmed = int(row.get("Confirmed", "0").strip() or "0")
        except ValueError:
            confirmed = 0
            error_messages.append(f"Non-numeric Confirmed value in row {i+1} ({row.get('States', 'Unknown')}): {row.get('Confirmed', '')}")
            is_valid = False
            
        try:
            deaths = int(row.get("Deaths", "0").strip() or "0")
        except ValueError:
            deaths = 0
            error_messages.append(f"Non-numeric Deaths value in row {i+1} ({row.get('States', 'Unknown')}): {row.get('Deaths', '')}")
            is_valid = False
        
        # Check for negative values
        if suspected < 0 or confirmed < 0 or deaths < 0:
            error_messages.append(f"Negative values found in row {i+1} ({row.get('States', 'Unknown')})")
            is_valid = False
            
            # Fix negative values
            suspected = max(0, suspected)
            confirmed = max(0, confirmed)
            deaths = max(0, deaths)
            
            # Update the row with fixed values
            validated_rows[i]["Suspected"] = str(suspected)
            validated_rows[i]["Confirmed"] = str(confirmed)
            validated_rows[i]["Deaths"] = str(deaths)
        
        # Check rule: Suspected >= Confirmed
        if suspected < confirmed:
            error_messages.append(f"Logical inconsistency in row {i+1} ({row.get('States', 'Unknown')}): Suspected ({suspected}) < Confirmed ({confirmed})")
            is_valid = False
            
            # Fix: Set Suspected = Confirmed
            validated_rows[i]["Suspected"] = str(confirmed)
        
        # Check rule: Confirmed >= Deaths
        if confirmed < deaths:
            error_messages.append(f"Logical inconsistency in row {i+1} ({row.get('States', 'Unknown')}): Confirmed ({confirmed}) < Deaths ({deaths})")
            is_valid = False
            
            # Fix: Set Confirmed = Deaths
            validated_rows[i]["Confirmed"] = str(deaths)
    
    return is_valid, validated_rows, error_messages

def rename_lassa_file(old_name):
    """
    Standardize Lassa fever report filenames and extract metadata.
    Converts original NCDC filenames to a standardized format and extracts date/week info.
    Returns a dictionary with parsed info or an error flag.
    """
    month_map = {
        "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
        "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
        "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec",
    }
    original_filename_for_logging = old_name
    old_name = old_name.replace(" ", "_")
    parts = old_name.split("_")

    if len(parts) < 9:
        logging.warning(f"Could not parse filename: {original_filename_for_logging}, too few parts.")
        return {'full_name': original_filename_for_logging, 'parse_error': True}

    date_str = parts[8]
    week_str_raw = parts[9].replace(".pdf", "") if parts[9].endswith(".pdf") else ""
    
    # Ensure week_str is just the number, remove 'W' if present
    week_str = week_str_raw.upper().lstrip('W')

    if len(date_str) != 6:
        logging.warning(f"Could not parse date string from filename: {original_filename_for_logging}, date_str: {date_str}")
        return {'full_name': original_filename_for_logging, 'parse_error': True}

    dd_str, mm_str, yy_str = date_str[:2], date_str[2:4], date_str[4:]
    month_name = month_map.get(mm_str, "???")
    
    try:
        # Always use last two digits for year (e.g., '2025' -> 25, '2021' -> 21)
        year_int = int(yy_str)
        week_int = int(week_str) if week_str.isdigit() else None
        month_int = int(mm_str) if mm_str.isdigit() else None
        day_int = int(dd_str) if dd_str.isdigit() else None
    except ValueError as e:
        logging.warning(f"Could not convert parts of {original_filename_for_logging} to int (yy:{yy_str}, w:{week_str}, m:{mm_str}, d:{dd_str}). Error: {e}")
        return {'full_name': original_filename_for_logging, 'parse_error': True}

    # Standardized filename
    # Just use the week number without leading zeros (W1, W2, etc.)
    week_display = str(week_int) if week_int is not None else 'XX'
    full_name = f"Nigeria_{dd_str}_{month_name}_{yy_str}_W{week_display}.pdf"

    return {
        'full_name': full_name,          # Standardized name for 'new_name' column
        'month_name': month_name,        # For reference, not a direct DB column usually
        'year': year_int,                # For 'year' column (bigint, last two digits only)
        'month': month_int,              # For 'month' column (int, no leading zero)
        'week': week_int,                # For 'week' column (bigint)
        'day': day_int,                  # For reference, not typically in 'website_data'
        'parse_error': False
    }
