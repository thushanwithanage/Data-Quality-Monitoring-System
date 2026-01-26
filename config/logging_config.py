import logging
import os
from datetime import datetime

def setup_logger(log_path: str, log_name_prefix: str = "dq_metrics") -> logging.Logger:
    os.makedirs(log_path, exist_ok=True)

    # Log file named with current date
    current_date = datetime.now().strftime("%Y_%m_%d")
    log_file = os.path.join(log_path, f"{log_name_prefix}_{current_date}.log")

    # Create logger
    logger = logging.getLogger("dq_pipeline")
    logger.setLevel(logging.INFO)

    # File handler
    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Avoid adding multiple handlers if function called multiple times
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger