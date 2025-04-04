#!/usr/bin/env python3
"""
main.py: Orchestrates the Lassa Fever Report Processing Pipeline

This script serves as the main entry point for the entire Lassa Fever report processing pipeline.
It sequentially executes each step of the pipeline in the correct order:

1. URL_Sourcing: Scrapes NCDC website for Lassa fever reports and extracts metadata
2. PDF_Download: Downloads PDF reports and organizes them by year
3. TableEnhancement: Processes PDFs to enhance table visibility for extraction
4. TableExtractionSorting: Extracts table data using AI and sorts it into CSV files

Usage:
    python main.py

This script is designed to be the single entry point for the entire pipeline,
making it easier to containerize and deploy the application.
"""

import sys
import time
import logging
import importlib.util
from pathlib import Path

# Replace the current logging setup in main.py with:
from src.utils.logging_config import configure_logging
configure_logging()

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
    """
    # Define base directory and script paths
    base_dir = Path(__file__).parent
    scripts = [
        {"name": "URL_Sourcing", "path": base_dir / "src" / "01_URL_Sourcing.py"},
        {"name": "PDF_Download", "path": base_dir / "src" / "02_PDF_Download.py"},
        {"name": "TableEnhancement", "path": base_dir / "src" / "03_TableEnhancement.py"},
        {"name": "TableExtractionSorting", "path": base_dir / "src" / "04_TableExtractionSorting.py"}
    ]
    
    # Execute each script in sequence
    for i, script in enumerate(scripts):
        script_name = script["name"]
        script_path = script["path"]
        
        if not script_path.exists():
            logging.error(f"Script not found: {script_path}")
            sys.exit(1)
        
        logging.info(f"Starting step {i+1}/{len(scripts)}: {script_name}")
        start_time = time.time()
        
        try:
            # Import and run the module
            module = import_module_from_file(f"lassa_{script_name.lower()}", script_path)
            
            # Run the main function
            if hasattr(module, "main"):
                module.main()
            else:
                logging.warning(f"No main() function found in {script_name}, attempting to execute module directly")
            
            # Special case for URL_Sourcing which has a separate process_file_status_update function
            if script_name == "URL_Sourcing" and hasattr(module, "process_file_status_update"):
                logging.info(f"Running process_file_status_update for {script_name}")
                module.process_file_status_update()
            
            elapsed_time = time.time() - start_time
            logging.info(f"Completed {script_name} in {elapsed_time:.2f} seconds")
            
        except Exception as e:
            logging.error(f"Error executing {script_name}: {e}", exc_info=True)
            sys.exit(1)
    
    logging.info("Pipeline completed successfully!")

if __name__ == "__main__":
    logging.info("Starting Lassa Fever report processing pipeline")
    run_pipeline()
    logging.info("Pipeline execution finished")
