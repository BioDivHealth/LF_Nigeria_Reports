#!/usr/bin/env python
"""
Clean state names in Lassa fever data.

This script identifies and corrects misspelled or inconsistently capitalized
state names in the Supabase lassa_data table. It uses a canonical list of 
Nigerian states as reference and applies fuzzy matching for ambiguous cases.

Usage:
    python 05c_CleanStates.py [--dry-run]
"""

import os
import sys
import logging
import argparse
import pandas as pd
from sqlalchemy import create_engine, text
from fuzzywuzzy import process
from pathlib import Path
import time

# Handle imports for both standalone execution and execution from main.py
try:
    from utils.db_utils import get_db_engine
except ImportError:
    from src.utils.db_utils import get_db_engine

# Define canonical list of Nigerian states (official names)
CANONICAL_STATES = [
    "Abia", "Adamawa", "Akwa Ibom", "Anambra", "Bauchi", "Bayelsa", "Benue", 
    "Borno", "Cross River", "Delta", "Ebonyi", "Edo", "Ekiti", "Enugu", 
    "FCT", "Gombe", "Imo", "Jigawa", "Kaduna", "Kano", "Katsina", "Kebbi", 
    "Kogi", "Kwara", "Lagos", "Nasarawa", "Niger", "Ogun", "Ondo", "Osun", 
    "Oyo", "Plateau", "Rivers", "Sokoto", "Taraba", "Yobe", "Zamfara"
]

# Additional special cases that should be preserved
SPECIAL_CASES = ["Total", "Cameroon"]

# Define mapping for common misspellings and capitalization issues
MANUAL_MAPPINGS = {
    "Plateu": "Plateau",
    "Fct": "FCT",
    # Add any other known manual mappings here
}

def clean_state_names(engine, dry_run=False):
    """
    Clean state names in the lassa_data table.
    
    Args:
        engine: SQLAlchemy engine
        dry_run (bool): If True, only show changes without applying them
        
    Returns:
        int: Number of rows updated
    """
    logger = logging.getLogger(__name__)
    
    # Fetch all unique state names from the database
    query = text("SELECT DISTINCT states FROM lassa_data")
    with engine.connect() as conn:
        result = conn.execute(query)
        existing_states = [row[0] for row in result if row[0] is not None]
    
    logger.info(f"Found {len(existing_states)} unique state names in the database")
    
    # Create mapping dictionary
    state_mapping = {}
    
    # First apply manual mappings
    for state in existing_states:
        if state in MANUAL_MAPPINGS:
            state_mapping[state] = MANUAL_MAPPINGS[state]
        elif state in CANONICAL_STATES or state in SPECIAL_CASES:
            # Keep correct states as they are
            state_mapping[state] = state
        else:
            # Use fuzzy matching for other cases
            allowed_states = CANONICAL_STATES + SPECIAL_CASES
            best_match, score = process.extractOne(state, allowed_states)
            
            if score >= 80:  # High confidence threshold
                state_mapping[state] = best_match
                logger.info(f"Fuzzy matching: '{state}' → '{best_match}' (score: {score})")
            else:
                # Keep as is if match confidence is low
                state_mapping[state] = state
                logger.warning(f"Low confidence match for '{state}' → '{best_match}' (score: {score}), keeping original")
    
    # Display mapping for verification
    logger.info("State name mapping:")
    changes = 0
    for original, corrected in state_mapping.items():
        if original != corrected:
            changes += 1
            logger.info(f"  '{original}' → '{corrected}'")
    
    if changes == 0:
        logger.info("No state name changes needed")
        return 0
        
    if dry_run:
        logger.info(f"Dry run: {changes} state names would be updated")
        return changes
    
    # Apply updates to the database
    updated_rows = 0
    deleted_rows = 0
    with engine.begin() as conn:  # Use transaction
        for original, corrected in state_mapping.items():
            if original != corrected:
                # 1) Remove duplicates that would violate the unique constraint
                delete_query = text("""
                    DELETE FROM lassa_data a
                    USING lassa_data b
                    WHERE a.states = :original
                      AND b.states = :corrected
                      AND a.full_year = b.full_year
                      AND a.week = b.week
                      AND a.id <> b.id
                """)
                delete_result = conn.execute(delete_query, {"original": original, "corrected": corrected})
                deleted_rows += delete_result.rowcount
                if delete_result.rowcount:
                    logger.info(f"Deleted {delete_result.rowcount} duplicate rows for '{original}' → '{corrected}'")

                # 2) Update remaining rows only where no corrected row exists
                update_query = text("""
                    UPDATE lassa_data a
                    SET states = :corrected
                    WHERE a.states = :original
                      AND NOT EXISTS (
                          SELECT 1 FROM lassa_data b
                          WHERE b.states = :corrected
                            AND b.full_year = a.full_year
                            AND b.week = a.week
                      )
                """)
                update_result = conn.execute(update_query, {"original": original, "corrected": corrected})
                updated_rows += update_result.rowcount
                logger.info(f"Updated {update_result.rowcount} rows: '{original}' → '{corrected}'")
    
    if deleted_rows:
        logger.info(f"Total duplicate rows deleted: {deleted_rows}")
    logger.info(f"Total rows updated: {updated_rows}")
    return updated_rows

def main():
    """
    Main function to clean state names in the database.
    """
    parser = argparse.ArgumentParser(description="Clean state names in Lassa fever data")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without applying them")
    args = parser.parse_args()
    
    logger = logging.getLogger(__name__)
    start_time = time.time()
    
    if "DATABASE_URL" not in os.environ:
        logger.error("DATABASE_URL environment variable not set")
        return 1
    
    # Create engine with the DATABASE_URL
    engine = get_db_engine(os.environ["DATABASE_URL"])
    
    # Clean state names
    updated_rows = clean_state_names(engine, dry_run=args.dry_run)
    
    logger.info(f"State name cleaning completed in {time.time() - start_time:.2f} seconds")
    logger.info(f"Total rows updated: {updated_rows}")
    
    return 0

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    main()
