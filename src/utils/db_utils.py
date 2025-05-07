"""
Database utilities for the Lassa Reports Scraping project.

This module provides functions for database operations,
including connecting to databases and performing upserts.
"""

import logging
import pandas as pd
import uuid
from sqlalchemy import create_engine, text, MetaData, Table, Column
from sqlalchemy import String, Integer, Float, Boolean
from sqlalchemy.dialects.postgresql import insert, UUID
from sqlalchemy.exc import SQLAlchemyError, NoSuchTableError

def get_db_engine(database_url):
    """
    Create and return a SQLAlchemy engine from a database URL.
    
    Args:
        database_url (str): Database connection URL
        
    Returns:
        Engine: SQLAlchemy engine object
    """
    return create_engine(database_url)

def push_data_with_upsert(engine, df, table_name, conflict_cols, batch_size=500):
    """
    Generic function to push DataFrame to a table with upsert logic.
    Uses SQLAlchemy Table reflection and ON CONFLICT DO UPDATE.
    
    Ensures proper UUID type handling for PostgreSQL by:
    1. Converting string UUIDs to actual UUID objects
    2. Creating tables with proper UUID column types
    3. Setting up correct type mapping for inserts
    
    Args:
        engine: SQLAlchemy engine
        df: Pandas DataFrame to insert/update
        table_name (str): Name of the target table
        conflict_cols (list): Columns to use for conflict detection
        batch_size (int, optional): Batch size for processing large DataFrames
        
    Returns:
        int: Number of affected rows
    """
    # Force UUID type for id column in PostgreSQL
    from sqlalchemy.dialects.postgresql import UUID as pg_UUID
    logger = logging.getLogger(__name__)
    metadata = MetaData()
    
    # Ensure proper UUID type for id column if it exists
    if 'id' in df.columns:
        # Convert string UUIDs to actual UUID objects
        str_mask = df['id'].apply(lambda x: isinstance(x, str))
        if str_mask.any():
            df.loc[str_mask, 'id'] = df.loc[str_mask, 'id'].apply(lambda x: uuid.UUID(x))
    
    # Define column types mapping
    dtype_map = {}
    if 'id' in df.columns:
        dtype_map['id'] = UUID(as_uuid=True)
    
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
        # Create new table with proper column types
        logger.info(f"Creating new table {table_name} with UUID column type")
        
        # Create table with explicit column types
        with engine.begin() as conn:
            # Create table with properly typed columns
            df.head(0).to_sql(table_name, engine, index=False, if_exists='replace', dtype=dtype_map)
            
            # Add primary key constraint if id column exists
            if 'id' in df.columns:
                conn.execute(text(f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ("id");'))
            
            # Add unique constraint
            try:
                conn.execute(text(alter_sql))
            except SQLAlchemyError as e:
                logger.warning(f"Could not add unique constraint: {e}")
        
        # Reflect the newly created table
        metadata.reflect(bind=engine, only=[table_name])
        table = metadata.tables[table_name]
    
    # For existing tables, check if we need to alter the id column type
    else:
        with engine.begin() as conn:
            # Check current column type
            check_type_sql = text(f"""SELECT data_type FROM information_schema.columns 
                                WHERE table_name = '{table_name}' AND column_name = 'id'""")
            result = conn.execute(check_type_sql)
            column_type = result.fetchone()
            
            # If id column exists but is not UUID type, alter it
            if column_type and column_type[0] != 'uuid' and 'id' in df.columns:
                logger.info(f"Converting {table_name}.id column from {column_type[0]} to UUID type")
                try:
                    # First try to directly alter the column type
                    alter_type_sql = text(f"ALTER TABLE \"{table_name}\" ALTER COLUMN \"id\" TYPE uuid USING \"id\"::uuid;")
                    conn.execute(alter_type_sql)
                except SQLAlchemyError as e:
                    logger.warning(f"Could not convert id column to UUID: {e}")
    
    # Convert DataFrame to records
    records = df.to_dict(orient='records')
    
    # Prepare upsert statement
    try:
        stmt = insert(table).values(records)
        
        # Never overwrite the primary‑key UUID during an upsert
        update_cols = {
            col: getattr(stmt.excluded, col)
            for col in df.columns
            if col not in conflict_cols + ['id']
        }
        
        # Execute upsert with conflict handling
        stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=update_cols)
        with engine.begin() as conn:
            result = conn.execute(stmt)
        
        return result.rowcount
    except Exception as e:
        logger.error(f"Error during upsert operation: {e}")
        
        # If we get a numeric error, try to convert problematic columns to text
        if "out of range" in str(e).lower():
            logger.info("Attempting to fix numeric range issues by converting numeric columns to text")
            
            # Convert numeric columns to text to avoid range issues
            for col in df.select_dtypes(include=['int64', 'float64']).columns:
                df[col] = df[col].astype(str)
            
            # Try again with string-converted values
            records = df.to_dict(orient='records')
            stmt = insert(table).values(records)
            
            # Never overwrite the primary‑key UUID during an upsert
            update_cols = {
                col: getattr(stmt.excluded, col)
                for col in df.columns
                if col not in conflict_cols + ['id']
            }
            
            # Execute upsert with conflict handling
            stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=update_cols)
            with engine.begin() as conn:
                result = conn.execute(stmt)
            
            return result.rowcount
        else:
            # Re-raise the exception if it's not a numeric range issue
            raise

def ensure_uuid_columns(engine, table_names):
    """
    Directly ensure that 'id' columns in specified tables are UUID type.
    This function will alter existing tables if needed.
    
    Args:
        engine: SQLAlchemy engine
        table_names (list): List of table names to check and update
    """
    logger = logging.getLogger(__name__)
    
    with engine.begin() as conn:
        for table_name in table_names:
            # Check if table exists
            check_table_sql = text(f"""SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            )""")
            table_exists = conn.execute(check_table_sql).scalar()
            
            if not table_exists:
                logger.info(f"Table {table_name} does not exist yet, will be created with UUID type")
                continue
            
            # Check current column type
            check_type_sql = text(f"""SELECT data_type FROM information_schema.columns 
                            WHERE table_name = '{table_name}' AND column_name = 'id'""")
            result = conn.execute(check_type_sql)
            column_type = result.fetchone()
            
            if column_type and column_type[0] != 'uuid':
                logger.info(f"Converting {table_name}.id column from {column_type[0]} to UUID type")
                try:
                    # Alter the column type to UUID
                    alter_type_sql = text(f"""ALTER TABLE "{table_name}" 
                                        ALTER COLUMN "id" TYPE uuid USING "id"::uuid;""")
                    conn.execute(alter_type_sql)
                    logger.info(f"Successfully converted {table_name}.id to UUID type")
                except Exception as e:
                    logger.error(f"Failed to convert {table_name}.id to UUID: {e}")


def get_existing_records(engine, table_name, id_column="id", where_clause=None):
    """
    Fetch existing records from a table.
    
    Args:
        engine: SQLAlchemy engine
        table_name (str): Name of the table
        id_column (str): Column to return (typically a unique identifier)
        where_clause (str, optional): SQL WHERE clause to filter results
        
    Returns:
        set: Set of existing IDs/values
    """
    query = f"SELECT {id_column} FROM {table_name}"
    if where_clause:
        query += f" WHERE {where_clause}"
        
    with engine.connect() as connection:
        result = connection.execute(text(query))
        return set(row[0] for row in result if row[0] is not None)

def safe_convert_to_int(value_str, field_name, strip_prefix=None):
    if not value_str:
        return None
    try:
        if strip_prefix and value_str.upper().startswith(strip_prefix):
            value_str = value_str.upper().lstrip(strip_prefix)
        return int(value_str)
    except ValueError:
        logging.warning(f"Skipping file_status row due to invalid {field_name} format '{value_str}': {fs_row}")
        return None