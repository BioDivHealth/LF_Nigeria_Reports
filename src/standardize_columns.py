"""
Standardize column names in all extracted Lassa fever CSV files.

This script scans all CSV files in data/processed/CSV_LF_*_Sorted/ and ensures
that each file uses the standard column order and names:
    Week,Year,States,Suspected,Confirmed,Probable,HCW,Deaths

- Handles common variations like 'HCW*', 'HCW.', 'Deaths (Confirmed Cases)', etc.
- Overwrites original files (or you can modify to output to a new location)
- Logs actions and errors for traceability
"""

import pandas as pd
from pathlib import Path
import logging

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# --- Column Standardization ---
STANDARD_COLS = ["Week", "Year", "States", "Suspected", "Confirmed", "Probable", "HCW", "Deaths"]
COLUMN_VARIANTS = {
    "Week": ["Week", "week"],
    "Year": ["Year", "year"],
    "States": ["States", "State", "state"],
    "Suspected": ["Suspected", "suspected"],
    "Confirmed": ["Confirmed", "confirmed"],
    "Probable": ["Probable", "probable"],
    "HCW": ["HCW", "HCW*", "HCW.", "HCW_Confirmed"],
    "Deaths": ["Deaths", "Deaths (Confirmed Cases)", "Deaths..Confirmed.Cases.", "deaths"]
}

# --- Main Function ---
def standardize_all_csvs(base_dir: Path):
    """
    Standardize columns in all CSV files under data/processed/CSV_LF_*_Sorted/
    """
    pattern = base_dir / "CSV_LF_*_Sorted" / "*.csv"
    files = list(base_dir.glob("CSV_LF_*_Sorted/*.csv"))
    logger.info(f"Found {len(files)} CSV files to process.")

    for file_path in files:
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Processing {file_path.relative_to(base_dir.parent)}")

            # Build rename map for this file
            rename_map = {}
            for std_col, variants in COLUMN_VARIANTS.items():
                for var in variants:
                    if var in df.columns:
                        rename_map[var] = std_col
                        break
            df.rename(columns=rename_map, inplace=True)

            # Add missing columns as empty
            for col in STANDARD_COLS:
                if col not in df.columns:
                    df[col] = ""

            # Reorder columns
            df = df[STANDARD_COLS]

            # Write back to the same file (or change to a new file if desired)
            df.to_csv(file_path, index=False)
            logger.info(f"Standardized columns in {file_path.name}")
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")

if __name__ == "__main__":
    BASE_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"
    standardize_all_csvs(BASE_DIR)
