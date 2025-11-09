import os
import logging
from datetime import datetime


class KeywordFilter(logging.Filter):
    def __init__(self, keywords):
        if isinstance(keywords, str):
            keywords = [keywords]
        self.keywords = keywords

    def filter(self, record):
        return any(keyword in record.getMessage() for keyword in self.keywords)

def setup_logging(log_name="pipeline", log_dir="logs", to_console=False, level=logging.INFO, console_filter_keywords=None):
    """
    Initializes logging for a specific module (e.g., training, inference).

    Args:
        log_name (str): Prefix for the log filename.
        log_dir (str): Directory to store logs.
        to_console (bool): Whether to also log to console.
        level (int): Logging level.
        console_filter_keywords (list or str): Only show console logs containing these keyword(s).

    Returns:
        str: Path to the created log file.
    """
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(log_dir, f"{log_name}_{timestamp}.log")

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        filename=log_file,
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    if to_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))

        # Add multi-keyword filter if specified
        if console_filter_keywords:
            console_handler.addFilter(KeywordFilter(console_filter_keywords))

        logging.getLogger().addHandler(console_handler)

        logging.info(f"Logging initialized: {log_file}")

    return log_file

