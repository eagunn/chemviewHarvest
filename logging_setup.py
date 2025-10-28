import logging
from logging import handlers
import sys

DEFAULT_LOG_PATH = "harvestSection5.log"
FORMAT = "%(asctime)s.%(msecs)03d %(levelname)-5s [%(name)s:%(lineno)d] %(message)s"
DATEFMT = "%Y-%m-%d %H:%M:%S"


def initialize_logging(level=logging.INFO, log_path: str = DEFAULT_LOG_PATH, console: bool = False):
    """Initialize root logging for the process.

    - Writes to `log_path` (overwrites file each run).
    - Uses a timestamp-first formatter with milliseconds.
    """
    root = logging.getLogger()
    # Remove any existing handlers so we can control where logs go
    for h in list(root.handlers):
        root.removeHandler(h)

    root.setLevel(level)

    formatter = logging.Formatter(FORMAT, datefmt=DATEFMT)

    # File handler (overwrite each run)
    file_handler = logging.FileHandler(log_path, mode='w', encoding='utf-8')
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    return root
