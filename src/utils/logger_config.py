import logging
import sys
from pathlib import Path

def setup_pipeline_logger(name="FintechPipeline", log_file="pipeline.log"):
    """Sets up a logger that outputs to both console and a file."""
    
    logger = logging.getLogger(name)
    
    # If the logger already has handlers, don't add more (prevents duplicate logs in notebooks)
    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.DEBUG)
    
    # Create formatters
    file_formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')

    # File Handler (Captures everything down to DEBUG)
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)

    # Console Handler (Clean output for the notebook, usually INFO and above)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger