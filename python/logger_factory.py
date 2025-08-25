import logging
import os
import sys
from protocols.app_logger import AppLogger

def create_logger(name : str, level=logging.INFO) -> AppLogger:
     # Ensure log folder exists
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)  # <--- creates folder if missing

     # Convert level if passed as string
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)

    logging.getLogger("kafka").setLevel(logging.WARNING) # Suppress Kafka logs
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False  # Prevent double logging

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    # INFO/DEBUG/WARNING → stdout + file
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)  # capture debug and above
    stdout_handler.setFormatter(formatter)

    info_file_handler = logging.FileHandler("logs/app.log", mode='a')
    info_file_handler.setLevel(logging.DEBUG)
    info_file_handler.setFormatter(formatter)

    # ERROR → stderr + file
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.ERROR)
    stderr_handler.setFormatter(formatter)

    error_file_handler = logging.FileHandler("logs/app.err", mode='a')
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)
    logger.addHandler(info_file_handler)
    logger.addHandler(stderr_handler)
    logger.addHandler(error_file_handler)
    os.system("cls")
    return logger