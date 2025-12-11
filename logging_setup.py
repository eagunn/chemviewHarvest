import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

DEFAULT_LOG_PATH = "logs/harvestSection5.log"
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

    # Ensure the parent directory for the log file exists
    try:
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        # best-effort; if mkdir fails, let the handler raise the error
        pass

    # File handler (rolling logs with max size 10 MB and 10 backups)
    file_handler = RotatingFileHandler(log_path, maxBytes=10 * 1024 * 1024, backupCount=10, encoding='utf-8')
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    return root
