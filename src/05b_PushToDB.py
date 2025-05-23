# src/06_PushToDB.py
"""
Sync Lassa data to online Postgres tables using upsert strategy and UUIDs.

This script reads individual CSV files with Lassa data and pushes them to Supabase/Postgres
tables using an upsert strategy (INSERT ... ON CONFLICT UPDATE) to avoid duplicates.
Each row in the lassa_data table is linked to its corresponding report in the website_data
table via a report_id foreign key.

UUIDs are used as primary keys, either client-generated or server-generated.

"""
import os
import sys
import logging
import uuid
import numpy as np # For np.nan
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from pathlib import Path
import time
from sqlalchemy.exc import SQLAlchemyError, NoSuchTableError
# Handle imports for both standalone execution and execution from main.py
try:
    from utils.db_utils import push_data_with_upsert
    from utils.data_validation import add_uuid_column
except ImportError:
    from src.utils.db_utils import push_data_with_upsert
    from src.utils.data_validation import add_uuid_column

# Define base directory
BASE_DIR = Path(__file__).parent.parent

# def add_uuid_column(df, id_column='id'):
#     """
#     Add (and/or populate) a UUID column.

#     Ensures every row has a non‑null UUID. Existing values are kept;
#     new values are generated only for rows where the column is missing,
#     null, or empty.
#     """
#     if id_column not in df.columns:
#         # Create the column first so we can assign into it
#         df[id_column] = pd.Series(dtype='string')

#     # Identify rows that still lack a UUID
#     mask = df[id_column].isna() | (df[id_column] == '')
#     df.loc[mask, id_column] = [str(uuid.uuid4()) for _ in range(mask.sum())]
#     return df

def load_and_normalize_csv(csv_path):
    """
    Load CSV into a DataFrame and normalize column names to lowercase.
    """
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.lower()
    return df

def push_lassa_data_individually(engine):
    """
    Push individual Lassa data CSVs to the database with upsert strategy.
    Each row will be linked to its corresponding report in website_data.
    
    Args:
        engine: SQLAlchemy engine
        
    Returns:
        int: Number of rows affected
    """
    logger = logging.getLogger(__name__)
    
    # Get a list of processed CSV files
    csv_dir = Path(BASE_DIR) / "data" / "processed" / "CSV"
    if not csv_dir.exists():
        logger.error(f"CSV directory {csv_dir} not found")
        return 0
        
    # Create a map of filename to report_id from website_data
    report_map = {}
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, new_name 
            FROM website_data 
            WHERE processed = 'Y'
        """))
        for row in result:
            report_id = row[0]
            filename = row[1]
            if filename:
                # Generate the expected CSV name pattern
                csv_name = f"Lines_{filename.replace('.pdf', '')}_page3.csv"
                report_map[csv_name] = report_id
    
    # Process each CSV file
    total_affected_rows = 0
    
    # Find all CSV files in the processed directory and subdirectories
    csv_files = []
    for year_dir in csv_dir.glob("CSV_LF_*_Sorted"):
        if year_dir.is_dir():
            csv_files.extend(year_dir.glob("*.csv"))
    
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    for csv_file in csv_files:
        csv_basename = csv_file.name
        
        # Check if we have a matching report_id
        if csv_basename not in report_map:
            logger.warning(f"No matching report found for {csv_basename}, skipping")
            continue
            
        report_id = report_map[csv_basename]
        logger.info(f"Processing {csv_basename} with report_id {report_id}")
        
        # Load and process the CSV
        df = load_and_normalize_csv(csv_file)
        
        # Add the report_id column to link to website_data
        df['report_id'] = report_id
        
        # Add UUID column if not present
        df = add_uuid_column(df, id_column='id')
        
        # Convert numeric columns to appropriate types before table creation or upsert
        # Process year column - create both year (2-digit) and full_year (4-digit) versions
        if 'year' in df.columns:
            # Convert to numeric first
            df['year'] = pd.to_numeric(df['year'], errors='coerce')
            
            # Create full_year column (preserve original 4-digit year)
            df['full_year'] = df['year'].copy()
            
            # Convert year to 2-digit format (e.g., 2024 -> 24)
            # Using modulo is more reliable as it handles all numeric types
            df['year'] = df['year'].apply(lambda x: int(x) % 100 if pd.notna(x) else x).astype('Int64')
            
            # Ensure full_year is also properly typed
            df['full_year'] = df['full_year'].astype('Int64')
            
        if 'week' in df.columns:
            df['week'] = pd.to_numeric(df['week'], errors='coerce').astype('Int64') # Use pandas nullable integer

        # Other numeric columns to float (nullable)
        for col in ['suspected', 'confirmed', 'probable', 'hcw', 'deaths']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('float')
        
        # Define conflict columns for upsert - must match the unique constraint
        conflict_cols = ['full_year', 'week', 'states']
        
        # Ensure conflict_cols exist in DataFrame
        missing_cols = [col for col in conflict_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Conflict columns {missing_cols} not found in DataFrame for {csv_basename}. Skipping.")
            continue
        
        # Check if table exists and create it if needed
        with engine.connect() as conn:
            table_exists = conn.execute(text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'lassa_data')")).scalar()
            
            # Import PostgreSQL data types
            from sqlalchemy.dialects.postgresql import UUID, INTEGER, FLOAT, TEXT
            
            # If table exists, check if full_year column exists and add it if needed
            if table_exists:
                logger.info("Table lassa_data exists, checking for full_year column")
                full_year_exists = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_name = 'lassa_data' AND column_name = 'full_year'
                    )
                """)).scalar()
                
                if not full_year_exists:
                    logger.info("Adding full_year column to lassa_data table")
                    try:
                        # Use a transaction for the ALTER TABLE operation
                        with engine.begin() as trans_conn:
                            trans_conn.execute(text('ALTER TABLE "lassa_data" ADD COLUMN "full_year" INTEGER'))
                        logger.info("Successfully added full_year column")
                    except SQLAlchemyError as e:
                        logger.error(f"Error adding full_year column: {e}")
            # If table doesn't exist, create it with proper columns
            else:
                logger.info("Creating lassa_data table with UUID column and report_id foreign key")
                
                # Create the table directly using SQLAlchemy's Table and Column objects
                # This avoids pandas dtype issues with UUID
                from sqlalchemy import Table, Column, MetaData, String, create_engine
                from sqlalchemy.schema import CreateTable
                
                # Create a metadata instance
                metadata_obj = MetaData()
                
                # Define the table with all columns
                lassa_table = Table(
                    'lassa_data', metadata_obj,
                    Column('id', UUID(as_uuid=True), primary_key=True),
                    Column('report_id', UUID(as_uuid=True)),
                    Column('year', INTEGER()),
                    Column('full_year', INTEGER()),
                    Column('week', INTEGER()),
                    Column('suspected', FLOAT()),
                    Column('confirmed', FLOAT()),
                    Column('probable', FLOAT()),
                    Column('hcw', FLOAT()),
                    Column('deaths', FLOAT()),
                    # Add any other columns from the DataFrame that aren't in dtype_map
                    Column('states', String(255)),  # Assuming states is a string column
                )
                
                # Add any other columns from the DataFrame that aren't already defined
                existing_cols = set([col.name for col in lassa_table.columns])
                for col_name in df.columns:
                    if col_name not in existing_cols:
                        # Basic type inference for other columns
                        if pd.api.types.is_numeric_dtype(df[col_name]):
                            # If it contains only integers
                            if df[col_name].dropna().apply(lambda x: x.is_integer() if pd.notna(x) else True).all():
                                lassa_table.append_column(Column(col_name, INTEGER()))
                            else:
                                lassa_table.append_column(Column(col_name, FLOAT()))
                        elif pd.api.types.is_datetime64_any_dtype(df[col_name]):
                            from sqlalchemy import DateTime
                            lassa_table.append_column(Column(col_name, DateTime()))
                        else:
                            lassa_table.append_column(Column(col_name, String(255)))
                
                # Create the table
                with engine.begin() as conn:
                    conn.execute(CreateTable(lassa_table))
                
                # Add foreign key constraint
                # Primary key is already defined in the table creation
                with engine.begin() as conn_trans: # Use a transaction for DDL
                    conn_trans.execute(text('ALTER TABLE "lassa_data" ADD CONSTRAINT fk_report_id FOREIGN KEY (report_id) REFERENCES website_data(id);'))
                    
                    # Add unique constraint for conflict columns
                    unique_constraint = f"ALTER TABLE \"lassa_data\" ADD CONSTRAINT uc_lassa_data_unique UNIQUE (full_year, week, states);"
                    try:
                        conn_trans.execute(text(unique_constraint))
                        logger.info("Added unique constraint on (full_year, week, states)")
                    except SQLAlchemyError as e:
                        logger.warning(f"Could not add unique constraint: {e}")
        
        # Push data to database
        affected_rows = push_data_with_upsert(engine, df, "lassa_data", conflict_cols)
        total_affected_rows += affected_rows
        logger.info(f"Processed {csv_basename}: {affected_rows} rows affected")
    
    return total_affected_rows


def main():
    """
    Main function to sync all data sources to the database.
    """
    logger = logging.getLogger(__name__)
    start_time = time.time()
    
    if "DATABASE_URL" not in os.environ:
        logger.error("DATABASE_URL environment variable not set")
        return 1
    
    # Create engine with the DATABASE_URL
    engine = create_engine(os.environ["DATABASE_URL"])
     
    # Push Lassa data individually, linking to website_data
    lassa_rows = push_lassa_data_individually(engine)
    
    logger.info(f"Database sync completed in {time.time() - start_time:.2f} seconds")
    if lassa_rows is not None:
        logger.info(f"Total affected rows: Lassa data {lassa_rows}")
    
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