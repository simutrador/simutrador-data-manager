import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Set up the logger
logger = logging.getLogger("main_logger")
logger.setLevel(logging.DEBUG)

# Check if handlers are already added to avoid duplicate logs
if not logger.hasHandlers():
    # Console handler for debugging
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"),
    )

    # Rotating file handler for error logs
    log_dir = Path(__file__).resolve().parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        log_dir / "error.log",
        maxBytes=5000,
        backupCount=2,
    )
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
    )

    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
