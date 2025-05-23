#!/usr/bin/env python3
"""
main.py: Orchestrates the Lassa Fever Report Processing Pipeline

This script serves as the main entry point for the entire Lassa Fever report processing pipeline.
It sequentially executes each step of the pipeline in the correct order:

1. URL_Sourcing: Scrapes NCDC website for Lassa fever reports and extracts metadata
2. PDF_Download_Supabase: Downloads PDF reports and organizes them by year
3. SyncEnhancement: Syncs enhancement status between local and cloud storage
4. TableEnhancement_Supabase: Processes PDFs to enhance table visibility for extraction
5. SyncProcessed: Syncs processed files status between local and cloud storage
6. LLM_Extraction_Supabase: Extracts table data using AI and sorts it into CSV files
7. SyncCombiningStatus: Syncs combining status between local and cloud storage
8. PushToDB: Pushes extracted data to the database
9. CloudSync: Syncs all data to cloud storage

Usage:
    python main.py

This script is designed to be the single entry point for the entire pipeline,
making it easier to containerize and deploy the application.
"""

import sys
import time
import logging
import importlib.util
import os
from pathlib import Path

# Add the project root directory to Python path to fix import issues
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


# Import centralized logging configuration
try:
    from utils.logging_config import configure_logging
except ImportError:
    from src.utils.logging_config import configure_logging

# Configure logging
configure_logging()

# Load environment variables from .env file
def load_env_file(env_path):
    """Load environment variables from .env file"""
    if not env_path.exists():
        logging.warning(f".env file not found at {env_path}")
        return
    
    logging.info(f"Loading environment variables from {env_path}")
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, value = line.split('=', 1)
            # Remove quotes if present
            value = value.strip('"\'')
            os.environ[key] = value
            logging.debug(f"Loaded environment variable: {key}")

# Load environment variables
env_path = project_root / '.env'
load_env_file(env_path)

def import_module_from_file(module_name, file_path):
    """
    Import a module from a file path.
    
    Args:
        module_name (str): Name to give the imported module
        file_path (str): Path to the Python file to import
        
    Returns:
        module: The imported module object
    """
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

def run_pipeline():
    """
    Run the complete Lassa Fever report processing pipeline.
    
    Executes each step of the pipeline in sequence, with appropriate logging.
    Continues to the next step if a script fails due to missing environment variables.
    """
    # Define base directory and script paths
    base_dir = Path(__file__).parent
    scripts = [
        {"name": "URL_Sourcing", "path": base_dir / "src" / "01_URL_Sourcing.py"},
        {"name": "PDF_Download_Supabase", "path": base_dir / "src" / "02_PDF_Download_Supabase.py"},
        {"name": "SyncEnhancement", "path": base_dir / "src" / "03a_SyncEnhancement.py"},
        {"name": "TableEnhancement_Supabase", "path": base_dir / "src" / "03b_TableEnhancement_Supabase.py"},
        {"name": "SyncProcessed", "path": base_dir / "src" / "04a_SyncProcessed.py"},
        {"name": "LLM_Extraction_Supabase", "path": base_dir / "src" / "04b_LLM_Extraction_Supabase.py"},
        {"name": "SyncCombiningStatus", "path": base_dir / "src" / "05a_SyncCombiningStatus.py"},
        {"name": "PushToDB", "path": base_dir / "src" / "05b_PushToDB.py"},
        {"name": "CloudSync", "path": base_dir / "src" / "06_CloudSync.py"}
    ]
    
    # Track overall success of the pipeline
    pipeline_success = True
    completed_steps = 0
    
    # Execute each script in sequence
    for i, script in enumerate(scripts):
        script_name = script["name"]
        script_path = script["path"]
        
        if not script_path.exists():
            logging.error(f"Script not found: {script_path}")
            pipeline_success = False
            continue
        
        logging.info(f"Starting step {i+1}/{len(scripts)}: {script_name}")
        start_time = time.time()
        
        try:
            # Import and run the module
            module = import_module_from_file(f"lassa_{script_name.lower()}", script_path)
            
            # Check if the module has a main function
            if hasattr(module, "main"):
                try:
                    module.main()
                except SystemExit as e:
                    # If the script exits with code 1, log it and continue with the next script
                    # This handles cases where environment variables are missing
                    if e.code == 1:
                        logging.warning(f"{script_name} exited with code 1. This may be due to missing environment variables. Continuing with next step.")
                        pipeline_success = False
                        continue
                    else:
                        # For other exit codes, re-raise the exception
                        raise
            else:
                logging.warning(f"No main() function found in {script_name}, attempting to execute module directly")
            
            # Special case for URL_Sourcing which has a separate process_file_status_update function
            if script_name == "URL_Sourcing" and hasattr(module, "process_file_status_update"):
                try:
                    logging.info(f"Running process_file_status_update for {script_name}")
                    # Get the engine from the module if it exists
                    if hasattr(module, "engine"):
                        module.process_file_status_update(module.engine)
                    else:
                        logging.warning(f"Skipping process_file_status_update for {script_name}: No engine found in module")
                except SystemExit as e:
                    if e.code == 1:
                        logging.warning(f"process_file_status_update for {script_name} exited with code 1. Continuing with next step.")
                        pipeline_success = False
                        continue
                    else:
                        raise
                except Exception as e:
                    logging.error(f"Error in process_file_status_update for {script_name}: {e}")
                    pipeline_success = False
                    continue
            
            elapsed_time = time.time() - start_time
            logging.info(f"Completed {script_name} in {elapsed_time:.2f} seconds")
            completed_steps += 1
            
        except Exception as e:
            logging.error(f"Error executing {script_name}: {e}", exc_info=True)
            pipeline_success = False
            # Continue with the next script instead of exiting
            continue
    
    if pipeline_success:
        logging.info("Pipeline completed successfully!")
    else:
        logging.warning(f"Pipeline completed with errors. {completed_steps}/{len(scripts)} steps completed successfully.")
    
    return pipeline_success

if __name__ == "__main__":
    logging.info("Starting Lassa Fever report processing pipeline")
    success = run_pipeline()
    logging.info("Pipeline execution finished")
    
    # Exit with appropriate code based on pipeline success
    sys.exit(0 if success else 1)
