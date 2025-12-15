# script to harvest html and PDF files related to Substantial Risk
# reports from the EPA ChemView website.

import argparse
import logging
from dataclasses import dataclass
from logging_setup import initialize_logging
from harvest_framework import run_harvest
from drive_substantial_risk_download import drive_substantial_risk_download
from file_types import FileTypes

# module logger (will be configured by logging_setup.initialize_logging)
logger = logging.getLogger(__name__)

@dataclass
class Config:
    input_file: str = "input_files/substantialRiskReports20251215.csv"
    #input_file: str = "input_files/srExportTest1.csv"
    archive_root: str = "H:/openSource/dataPreservation/chemView/harvest/chemview_archive_substantial_risk"
    db_path: str = "chemview_harvest.db"
    headless: bool = False  # headless false means the browser will be displayed
    debug_out: str = "debug_artifacts"
    max_downloads: int = None  # if set, limit number of downloads made (not rows)
    start_row: int = None  # if set, skip rows up to this row number
    stop_file: str = "harvest.stop"  # optional stop-file; when present the harvest stops gracefully
    retry_interval_hours: float = 12.0  # hours to wait after a failure before retrying
    data_type: str = "substantialRiskReports"  # which data/report type this run targets

# Initialize CONFIG with concrete type so static analyzers see its attributes
CONFIG: Config = Config()


def initialize_config(argv):
    """
    Build the Config object from defaults and any runtime arguments given
    """
    parser = argparse.ArgumentParser(description="Substantial Risk harvest script")
    parser.add_argument("--headless", action="store_true", help="Run headless (placeholder)")
    parser.add_argument("--input-file", type=str, help="CSV input file name")
    parser.add_argument("--download-dir", type=str, help="Download directory")
    parser.add_argument("--db-path", type=str, help="Path to SQLite DB")
    parser.add_argument("--debug-out", type=str, help="Debug artifacts directory")
    parser.add_argument("--archive-root", type=str, help="Archive root directory")
    parser.add_argument("--max-downloads", dest='max_downloads', type=int, help="Maximum number of download attempts to perform")
    parser.add_argument("--start-row", type=int, help="Start processing from this row number (1-based index)")
    parser.add_argument("--stop-file", dest='stop_file', type=str, help="Path to stop file (when present, harvest stops)")
    parser.add_argument("--retry-interval-hours", dest='retry_interval_hours', type=float, help="Hours to wait after a failure before retrying (default 12.0)")
    parser.add_argument("--data-type", dest='data_type', type=str, help="Data/report type name (default: premanufactureNotices)")
    args = parser.parse_args(argv)

    global CONFIG
    CONFIG = Config(
        input_file=args.input_file if args.input_file is not None else Config.input_file,
        db_path=args.db_path if args.db_path is not None else Config.db_path,
        headless=args.headless if args.headless else Config.headless,
        debug_out=args.debug_out if args.debug_out is not None else Config.debug_out,
        archive_root=args.archive_root if args.archive_root is not None else Config.archive_root,
        max_downloads=args.max_downloads if args.max_downloads is not None else Config.max_downloads,
        start_row=args.start_row if args.start_row is not None else None,
        stop_file=args.stop_file if args.stop_file is not None else Config.stop_file,
        retry_interval_hours=args.retry_interval_hours if args.retry_interval_hours is not None else Config.retry_interval_hours,
        data_type=args.data_type if args.data_type is not None else Config.data_type,
    )
    logging.info(f"Configuration initialized: {CONFIG}")


def main(argv=None):
    """Entry point for the Substantial Risk harvest wrapper.

    This file is an intentionally thin wrapper around the standard
    harvest framework, invoking our own specialized download driver.
    """
    initialize_config(argv)
    # Configure centralized logging for the process
    initialize_logging(log_path="./logs/harvestSubstantialRisk.log", level=logging.DEBUG)

    logger.info("Starting Substantial Risk harvest via framework")

    # Delegate to the shared run_harvest implementation, providing the
    # Substantial Risk download driver and the policy names for file types.
    rc = run_harvest(CONFIG, drive_substantial_risk_download, FileTypes)

    logger.info("harvestsubstantialRisk finished with return code %s", rc)
    return rc


if __name__ == "__main__":
    # Keep the top-level invocation the same so the script can still be run
    # directly and so your original top-level comment block remains useful.
    main()
