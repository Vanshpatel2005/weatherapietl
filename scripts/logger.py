"""
Logging configuration for the Weather ETL Pipeline.
Provides centralized logging setup for all modules.
"""

import logging
import os
from datetime import datetime
from config.config import Config


def setup_logger(name: str = __name__) -> logging.Logger:
    """
    Set up and return a configured logger instance.
    
    Args:
        name: Name of the logger (usually __name__)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, Config.LOG_LEVEL.upper()))
    
    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger
    
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(Config.LOG_FILE_PATH)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    # File handler
    file_handler = logging.FileHandler(Config.LOG_FILE_PATH)
    file_handler.setLevel(getattr(logging, Config.LOG_LEVEL.upper()))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, Config.LOG_LEVEL.upper()))
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def log_pipeline_start(logger: logging.Logger, pipeline_name: str):
    """Log the start of a pipeline execution."""
    logger.info("=" * 60)
    logger.info(f"Starting {pipeline_name}")
    logger.info(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)


def log_pipeline_end(logger: logging.Logger, pipeline_name: str, status: str = "SUCCESS"):
    """Log the end of a pipeline execution."""
    logger.info("=" * 60)
    logger.info(f"Finished {pipeline_name}")
    logger.info(f"Status: {status}")
    logger.info(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)


def log_error(logger: logging.Logger, error: Exception, context: str = ""):
    """Log an error with context information."""
    logger.error(f"Error in {context}: {str(error)}", exc_info=True)
