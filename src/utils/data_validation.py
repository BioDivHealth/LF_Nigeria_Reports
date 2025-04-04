"""
Data validation and transformation utilities for Lassa fever report data.

This module provides functions to validate, normalize, and transform
tabular data extracted from Lassa fever reports.
"""

import copy
import logging


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
            deaths = int(row.get("Deaths (Confirmed Cases)", "0").strip() or "0")
        except ValueError:
            deaths = 0
            error_messages.append(f"Non-numeric Deaths value in row {i+1} ({row.get('States', 'Unknown')}): {row.get('Deaths (Confirmed Cases)', '')}")
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
            validated_rows[i]["Deaths (Confirmed Cases)"] = str(deaths)
        
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
