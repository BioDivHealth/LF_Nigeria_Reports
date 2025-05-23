"""
Logging configuration for the Lassa Reports Scraping project.

This module provides a centralized logging configuration to be used across
all scripts in the project. It includes a custom handler that suppresses
AFC-related logs from the Google Gemini API.
"""

import logging

import sys
class NewlineLoggingHandler(logging.StreamHandler):
    """Custom logging handler that adds a newline after each log entry and filters AFC logs."""
    def __init__(self):
        super().__init__(sys.stderr)  # Send logs to stderr

    def filter(self, record):
        return 'afc' not in record.getMessage().lower()
        
    def emit(self, record):
        super().emit(record)
        self.stream.write('\n')
        self.flush()

def configure_logging():
    """
    Configure logging with the NewlineLoggingHandler and set appropriate log levels.
    
    This function sets up the root logger with INFO level and suppresses logs from
    third-party libraries related to the Google Gemini API.
    """
    # Only configure if not already configured
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO, 
            format='%(levelname)s: %(message)s', 
            handlers=[NewlineLoggingHandler()]
        )

        # Set third-party loggers to higher levels to suppress AFC logs
        for logger_name in ["google", "google.genai", "google.api_core", "httpx", "httpcore"]: 
            logging.getLogger(logger_name).setLevel(logging.ERROR)
