import logging
import sys

def setup_logger(name: str) -> logging.Logger:
    """Configures a standardized logger that writes to both console and a file."""
    logger = logging.getLogger(name)
    
    # Prevent duplicate logs if setup_logger is called multiple times
    if logger.hasHandlers():
        return logger
        
    logger.setLevel(logging.INFO)

    # 1. Define a clean, industrial log layout format
    # Includes: Timestamp | Log Level | File/Component Name | Message
    formatter = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 2. Console Handler (Outputs to terminal)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 3. File Handler (Appends to a physical log file)
    file_handler = logging.FileHandler("app.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
