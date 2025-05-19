# src/06_PushToDB.py
"""
Sync Lassa data to online Postgres tables using upsert strategy and UUIDs.

This script reads CSV files with Lassa data and pushes them to Supabase/Postgres
tables using an upsert strategy (INSERT ... ON CONFLICT UPDATE) to avoid duplicates.
UUIDs are used as primary keys, either client-generated or server-generated.

Env vars needed:
    DATABASE_URL = "postgresql+psycopg2://user:pass@host:port/dbname"
"""
import os
import sys
import logging
import uuid
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from pathlib import Path
import time
from sqlalchemy.exc import SQLAlchemyError, NoSuchTableError

def add_uuid_column(df, id_column='id'):
    """
    Add (and/or populate) a UUID column.

    Ensures every row has a non‑null UUID. Existing values are kept;
    new values are generated only for rows where the column is missing,
    null, or empty.
    """
    if id_column not in df.columns:
        # Create the column first so we can assign into it
        df[id_column] = pd.Series(dtype='string')

    # Identify rows that still lack a UUID
    mask = df[id_column].isna() | (df[id_column] == '')
    df.loc[mask, id_column] = [str(uuid.uuid4()) for _ in range(mask.sum())]
    return df

def load_and_normalize_csv(csv_path):
    """
    Load CSV into a DataFrame and normalize column names to lowercase.
    """
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.lower()
    return df

def push_data_with_upsert(engine, df, table_name, conflict_cols, batch_size=500):
    """
    Generic function to push DataFrame to a table with upsert logic.
    Uses SQLAlchemy Table reflection and ON CONFLICT DO UPDATE.
    """
    logger = logging.getLogger(__name__)
    metadata = MetaData()
    # Prepare unique constraint SQL
    constraint_name = f"uc_{table_name}_{'_'.join(conflict_cols)}"
    unique_cols_sql = ', '.join([f'"{c}"' for c in conflict_cols])
    alter_sql = f'ALTER TABLE "{table_name}" ADD CONSTRAINT "{constraint_name}" UNIQUE ({unique_cols_sql});'
    try:
        # Reflect existing table and ensure unique constraint
        table = Table(table_name, metadata, autoload_with=engine)
        with engine.begin() as conn:
            try:
                conn.execute(text(alter_sql))
            except SQLAlchemyError:
                pass
    except NoSuchTableError:
        # Create empty table and add unique constraint
        # Check if 'id' column exists in the DataFrame
        if 'id' in df.columns:
            # Create table with id as primary key
            with engine.begin() as conn:
                # First create the table without primary key
                df.head(0).to_sql(table_name, engine, index=False, if_exists='replace')
                # Then add primary key constraint
                conn.execute(text(f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ("id");'))
        else:
            # Create table without primary key
            df.head(0).to_sql(table_name, engine, index=False)
            
        # Add unique constraint
        with engine.begin() as conn:
            try:
                conn.execute(text(alter_sql))
            except SQLAlchemyError as e:
                logger.warning(f"Could not add unique constraint: {e}")
                
        metadata.reflect(bind=engine, only=[table_name])
        table = metadata.tables[table_name]
    records = df.to_dict(orient='records')
    stmt = insert(table).values(records)
    # Never overwrite the primary‑key UUID during an upsert
    update_cols = {
        col: getattr(stmt.excluded, col)
        for col in df.columns
        if col not in conflict_cols + ['id']
    }
    stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=update_cols)
    with engine.begin() as conn:
        result = conn.execute(stmt)
    return result.rowcount

def push_lassa_data(engine, csv_path=None):
    """
    Push combined Lassa data to the database with upsert strategy.
    
    Args:
        engine: SQLAlchemy engine
        csv_path (Path, optional): Path to the CSV file, defaults to latest
    """
    logger = logging.getLogger(__name__)
    if csv_path is None:
        try:
            csv_path = max(Path("data/documentation").glob("combined_lassa_data_*.csv"))
        except ValueError:
            logger.error("No combined_lassa_data CSV file found in data/documentation/")
            return 0
    
    logger.info(f"Processing Lassa data from {csv_path}")
    df = load_and_normalize_csv(csv_path)
    
    # Add UUID column and ensure it's a proper UUID object, not a string
    df = add_uuid_column(df, id_column='id')
    
    # Define conflict columns for upsert: year, week, and states
    conflict_cols = ['year', 'week', 'states']
    
    # Ensure conflict_cols exist in DataFrame
    for col in conflict_cols:
        if col not in df.columns:
            logger.error(f"Conflict column '{col}' not found in Lassa data DataFrame. Aborting.")
            return 0
    
    # Explicitly define column types for PostgreSQL
    from sqlalchemy.dialects.postgresql import UUID
    dtype_map = {'id': UUID(as_uuid=True)}
            
    # Check if table exists
    with engine.connect() as conn:
        table_exists = conn.execute(text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'lassa_reports')")).scalar()
        
        # If table doesn't exist, create it with proper UUID column
        if not table_exists:
            logger.info("Creating lassa_reports table with UUID column")
            # Create table with explicit UUID type
            df.head(0).to_sql("lassa_reports", engine, if_exists='replace', index=False, dtype=dtype_map)
            
            # Add primary key constraint
            with engine.begin() as conn:
                conn.execute(text(f'ALTER TABLE "lassa_reports" ADD PRIMARY KEY ("id");'))
    
    return push_data_with_upsert(engine, df, "lassa_reports", conflict_cols)

def push_website_data(engine, csv_path=None):
    """
    Push website raw data to the database with upsert strategy.
    Uses 'Link' as the unique key for conflict resolution.
    
    Args:
        engine: SQLAlchemy engine
        csv_path (Path, optional): Path to the CSV file
    """
    logger = logging.getLogger(__name__)
    
    if csv_path is None:
        # Only look in data/documentation
        doc_path = Path("data/documentation/website_raw_data.csv")
        if doc_path.exists():
            csv_path = doc_path
        else:
            logger.warning(f"Website data file not found in {doc_path}")
            return 0
    
    logger.info(f"Processing website data from {csv_path}")
    df = load_and_normalize_csv(csv_path)
    
    # Add UUID if missing and ensure it's a proper UUID object, not a string
    df = add_uuid_column(df, id_column='id')
    
    # Ensure all numeric columns are of appropriate types to avoid PostgreSQL errors
    # Convert year and week to integers, month to float
    if 'year' in df.columns:
        df['year'] = df['year'].astype('int32')  # Use int32 instead of int64
    if 'week' in df.columns:
        df['week'] = df['week'].astype('int32')  # Use int32 instead of int64
    if 'month' in df.columns:
        df['month'] = df['month'].astype('float32')  # Use float32 instead of float64
    
    # Define unique columns for upsert
    unique_columns = ['new_name']
    
    # Explicitly define column types for PostgreSQL
    from sqlalchemy.dialects.postgresql import UUID, INTEGER, FLOAT
    dtype_map = {
        'id': UUID(as_uuid=True),
        'year': INTEGER(),
        'week': INTEGER(),
        'month': FLOAT()
    }
    
    # Check if table exists
    with engine.connect() as conn:
        table_exists = conn.execute(text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'website_data')")).scalar()
        
        # If table doesn't exist, create it with proper UUID column
        if not table_exists:
            logger.info("Creating website_data table with UUID column")
            # Create table with explicit UUID type
            df.head(0).to_sql("website_data", engine, if_exists='replace', index=False, dtype=dtype_map)
            
            # Add primary key constraint
            with engine.begin() as conn:
                conn.execute(text('ALTER TABLE "website_data" ADD PRIMARY KEY ("id");'))
                logger.info("Added primary key constraint to website_data table")
    
    # Check for primary key constraint on existing table
    has_pk = False
    with engine.connect() as conn:
        try:
            # Check if primary key exists
            result = conn.execute(text("""
                SELECT count(*) FROM pg_constraint
                WHERE conrelid = 'website_data'::regclass
                AND contype = 'p'
            """))
            has_pk = result.scalar() > 0
        except SQLAlchemyError:
            # Table might not exist yet
            pass
        
        if not has_pk:
            try:
                conn.execute(text('ALTER TABLE "website_data" ADD PRIMARY KEY ("id");'))
                logger.info("Added primary key constraint to website_data table")
            except SQLAlchemyError as e:
                logger.warning(f"Could not add primary key: {e}")
                

    affected_rows = push_data_with_upsert(
        engine,  # Pass engine directly
        df,
        "website_data",
        conflict_cols=unique_columns
    )
    
    logger.info(f"Website data: Processed {len(df)} rows, affected {affected_rows} rows")
    return affected_rows

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
    
    # Ensure both tables have UUID columns
    from utils.db_utils import ensure_uuid_columns
    ensure_uuid_columns(engine, ['lassa_reports', 'website_data'])
    
    # Push Lassa data
    lassa_rows = push_lassa_data(engine)
    
    # Push website data
    website_rows = push_website_data(engine)
    
    logger.info(f"Database sync completed in {time.time() - start_time:.2f} seconds")
    logger.info(f"Total affected rows: {lassa_rows + website_rows}")
    
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