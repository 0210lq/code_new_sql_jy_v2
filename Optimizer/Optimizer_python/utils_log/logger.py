import logging
import os
from datetime import datetime
import sys

def setup_logger(name, log_level=logging.INFO):
    """
    Set up a logger with both file and console handlers
    
    Args:
        name (str): Name of the logger
        log_level (int): Logging level (default: logging.INFO)
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Create file handler
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'log', 'processing_log')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    current_date = datetime.now().strftime('%Y%m%d')
    log_file = os.path.join(log_dir, f'Optimizer_log_{current_date}.log')
    
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(file_formatter)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def setup_check_logger(name, log_level=logging.INFO):
    """
    Set up a logger specifically for check results
    
    Args:
        name (str): Name of the logger
        log_level (int): Logging level (default: logging.INFO)
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Create formatters
    check_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Create check log directory
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'log', 'check')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Create check log file handler with date-based filename
    current_date = datetime.now().strftime('%Y%m%d')
    check_log_file = os.path.join(log_dir, f'opt_check_{current_date}.log')
    check_handler = logging.FileHandler(check_log_file, encoding='utf-8')
    check_handler.setLevel(log_level)
    check_handler.setFormatter(check_formatter)
    
    # Add handler to logger
    logger.addHandler(check_handler)
    
    return logger

# Create default logger instance
default_logger = setup_logger('Optimizer') 