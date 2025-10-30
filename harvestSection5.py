# script to harvest html and PDF files related to the TSCA Section 5
# orders from the EPA ChemView website.

import argparse
import logging
from dataclasses import dataclass
from logging_setup import initialize_logging
from harvest_framework import run_harvest
from drive_section5_download import drive_section5_download
from file_types import FileTypes

# module logger (will be configured by logging_setup.initialize_logging)
logger = logging.getLogger(__name__)

@dataclass
class Config:
    input_file: str = "input_files/chemviewS5export20251019.csv"
    db_path: str = "chemview_harvest.db"
    headless: bool = False  # headless false means the browser will be displayed
    debug_out: str = "debug_artifacts"
    archive_root: str = "chemview_archive"
    max_downloads: int = None  # if set, limit number of download attempts (not rows)

# Initialize CONFIG with concrete type so static analyzers see its attributes
CONFIG: Config = Config()


def initialize_config(argv):
    """
    Build the Config object from command-line args.

    This keeps the same CLI and defaults as before. The function was kept in
    this thin wrapper so comments and CLI help remain colocated with this
    script. The heavy lifting (loop, DB, browser reuse) is delegated to the
    shared `harvest_framework` module.
    """
    parser = argparse.ArgumentParser(description="Section 5 harvest script")
    parser.add_argument("--headless", action="store_true", help="Run headless (placeholder)")
    parser.add_argument("--input-file", type=str, help="CSV input file name")
    parser.add_argument("--download-dir", type=str, help="Download directory")
    parser.add_argument("--db-path", type=str, help="Path to SQLite DB")
    parser.add_argument("--debug-out", type=str, help="Debug artifacts directory")
    parser.add_argument("--archive-root", type=str, help="Archive root directory")
    parser.add_argument("--max-downloads", dest='max_downloads', type=int, help="Maximum number of download attempts to perform")
    args = parser.parse_args(argv)

    global CONFIG
    CONFIG = Config(
        input_file=args.input_file if args.input_file is not None else Config.input_file,
        db_path=args.db_path if args.db_path is not None else Config.db_path,
        headless=args.headless if args.headless else Config.headless,
        debug_out=args.debug_out if args.debug_out is not None else Config.debug_out,
        archive_root=args.archive_root if args.archive_root is not None else Config.archive_root,
        max_downloads=args.max_downloads if args.max_downloads is not None else Config.max_downloads
    )
    logging.info(f"Configuration initialized: {CONFIG}")


def main(argv=None):
    """Entry point for the Section 5 harvest wrapper.

    This file is intentionally a thin wrapper that preserves the original
    script's CLI and comments while delegating the primary work to the
    framework module. That keeps your original documentation in place and
    consolidates the shared behavior.
    """
    initialize_config(argv)
    # Configure centralized logging for the process (keeps prior behavior)
    initialize_logging()

    logger.info("Starting Section 5 harvest via framework")

    # Delegate to the shared run_harvest implementation, providing the
    # Section 5 download driver and the policy names for file types.
    rc = run_harvest(CONFIG, drive_section5_download, FileTypes)

    logger.info("harvestSection5 finished with return code %s", rc)
    return rc


if __name__ == "__main__":
    # Keep the top-level invocation the same so the script can still be run
    # directly and so your original top-level comment block remains useful.
    main()
