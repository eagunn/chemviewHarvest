# script to harvest html and PDF files related to the TSCA Section 5
# orders from the EPA ChemView website.

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
import time
from urllib.parse import urlparse, parse_qs, urlencode
from HarvestDB import HarvestDB
from drive_section5_download import drive_section5_download
import logging
from logging_setup import initialize_logging
from typing import Optional

# Global variables used by many functions will be initialized in main()
DB: Optional[HarvestDB] = None

# module logger (will be configured by logging_setup.initialize_logging)
logger = logging.getLogger(__name__)

@dataclass
class Config:
    input_file: str = "input_files/s5ExportTest2.csv"
    db_path: str = "chemview_harvest.db"
    headless: bool = False  # headless false means the browser will be displayed
    debug_out: str = "debug_artifacts"
    archive_root: str = "chemview_archive"
    max_rows: int = None  # if set, limit number of processed (non-blank) rows

@dataclass
class FileTypes:
    section5_html: str = "section5_html"
    section5_pdf: str = "section5_pdf"

# Initialize CONFIG with concrete type so static analyzers see its attributes
CONFIG: Config = Config()

def open_chemview_export_file():
    """Open the local CSV export and return a file handle.

    Looks for the named file in the current working directory.
    Returns an open file object on
    success, or None on failure (and prints an error message).
    """
    script_dir = Path(__file__).resolve().parent
    csv_path = script_dir / CONFIG.input_file
    try:
        # Use 'utf-8-sig' to transparently handle BOM if present
        fh = csv_path.open("r", encoding="utf-8-sig")
    except Exception as e:
        logger.error("Error: could not open %s: %s", csv_path, e)
        return None, None
    logger.info("Opened export file: %s", csv_path)
    first_line = fh.readline()
    logger.debug("First line preview: %s", (first_line.strip() if first_line else "(empty)"))
    header_fields = [h.strip() for h in first_line.split(',')] if first_line else []
    return fh, header_fields

def fixup_url(url, cas_val):
    """
    If the URL's ch= field is empty, insert cas_val as its value.
    Otherwise, return the URL unchanged.
    """
    if not url or not cas_val:
        logger.debug("fixup_url: missing url or cas_val (url=%s, cas_val=%s)", url, cas_val)
        return url

    new_url = url
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        ch_val = qs.get('ch', [''])[0]
        if not ch_val:
            qs['ch'] = [cas_val]
            new_query = urlencode(qs, doseq=True)
            new_url = parsed._replace(query=new_query).geturl()
            logger.info("FixedURL for chem id %s to be %s", cas_val, new_url)
    except Exception as e:
        logger.debug("fixup_url: exception %s for url=%s, cas_val=%s", e, url, cas_val)

    return new_url

def initialize_config(argv):
    """
    Build and return a Config object from argv via argparse args, using defaults as needed.
    """
    parser = argparse.ArgumentParser(description="Section 5 harvest script")
    parser.add_argument("--headless", action="store_true", help="Run headless (placeholder)")
    parser.add_argument("--input-file", type=str, help="CSV input file name")
    parser.add_argument("--download-dir", type=str, help="Download directory")
    parser.add_argument("--db-path", type=str, help="Path to SQLite DB")
    parser.add_argument("--debug-out", type=str, help="Debug artifacts directory")
    parser.add_argument("--archive-root", type=str, help="Archive root directory")
    parser.add_argument("--max-rows", type=int, help="Maximum number of non-blank rows to process")
    args = parser.parse_args(argv)

    global CONFIG
    CONFIG = Config(
        input_file=args.input_file if args.input_file is not None else Config.input_file,
        db_path=args.db_path if args.db_path is not None else Config.db_path,
        headless=args.headless if args.headless else Config.headless,
        debug_out=args.debug_out if args.debug_out is not None else Config.debug_out,
        archive_root=args.archive_root if args.archive_root is not None else Config.archive_root,
        max_rows=args.max_rows if args.max_rows is not None else Config.max_rows
    )
    logging.info(f"Configuration initialized: {CONFIG}")
    return

def do_need_download(db, cas_val, file_type):
    """
    Returns True if a download is needed for the given cas_val and file_type.
    - If no record is found, return True.
    - If record exists but last_success_datetime is null, return True.
    - Otherwise, return False.
    **** TEMPORARY LOGIC ****
    - If there is no last_success_datetime but there is a last_failure_datetime, do NOT retry; log skip and return False.
    **** TEMPORARY LOGIC **** -- change this later when we're ready to write cooldown logic below
    TODO:
     - implement cooldown logic to avoid re-downloading too frequently after failures.
     - implement refresh logic, passing in a either a refresh interval or a
       refresh-if-older-than datetime.
    """
    do_download = False
    record = db.get_harvest_status(cas_val, file_type)
    if record:
        logger.debug("DB record for id %s / %s: %s", cas_val, file_type, record)
        # Extract success/failure timestamps if present
        last_success = record.get('last_success_datetime')
        last_failure = record.get('last_failure_datetime')
        # If there's no recorded success
        if not last_success:
            # If there's a prior failure, do not retry -- just log and skip
            if not last_failure:
                logger.debug("Record for id %s / %s exists but no success or failure recorded: %s", cas_val, file_type, record)
                do_download = True
            else:
                logger.info("****Skipping retry FOR NOW for id %s / %s, previous failure at %s", cas_val, file_type, last_failure)
                do_download = False

    else:
        logger.debug("No record found for id: %s / %s", cas_val, file_type)
        do_download = True

    logger.debug("Will attempt download: %s", do_download)
    return do_download

def main(argv=None):
    initialize_config(argv)
    # initialize centralized logging for the process before any logging calls
    initialize_logging()
    initialize_db_access()
    # ensure static analyzers know DB is initialized
    assert DB is not None

    logger.info("harvestSection5: ready")
    logger.debug("Parsed arguments -> %s", CONFIG)

    Path(CONFIG.debug_out).mkdir(parents=True, exist_ok=True)
    Path(CONFIG.archive_root).mkdir(parents=True, exist_ok=True)

    fh, header_fields = open_chemview_export_file()
    if fh is None:
        logger.error("Failed to open chemview export file. Exiting with error.")
        return 1

    if header_fields is None:
        logger.error("Error: CSV header could not be read. Exiting with error code 2.")
        return 2

    total_rows = 0
    html_success_count = 0
    pdf_success_count = 0
    total_download_time = 0.0
    download_calls = 0
    try:
        reader = csv.DictReader(fh, fieldnames=header_fields)
        first_field = header_fields[0]
        last_field = header_fields[-1]
        for row in reader:
            if not row or all(not (v and v.strip()) for v in row.values()):
                continue  # Skip blank or all-empty lines

            # If a max_rows limit is configured, stop after that many processed rows
            if CONFIG.max_rows is not None and total_rows >= CONFIG.max_rows:
                logger.info("Reached configured max_rows=%s; stopping processing.", CONFIG.max_rows)
                break

            logger.debug("--- starting processing of next row ---")
            total_rows += 1
            cas_val = (row.get(first_field) or '').strip() if first_field else ''
            url = (row.get(last_field) or '').strip()
            if not url or not cas_val:
                logger.warning("missing url or cas_val (url=%s, cas_val=%s), skipping this entry", url, cas_val)
                continue

            # Use do_need_download to decide if we need to download
            need_html_download = do_need_download(DB, cas_val, FileTypes.section5_html)
            need_pdf_download = do_need_download(DB, cas_val, FileTypes.section5_pdf)
            if not need_html_download and not need_pdf_download:
                logger.info("Skipping download for cas=%s; files already downloaded.", cas_val)
                continue
            else:
                logger.info("At least one download needed for cas=%s: html (%s), pdf(%s)", cas_val, need_html_download, need_pdf_download)

            # some URLs in the export have an empty ch= field; fix those up
            url = fixup_url(url, cas_val)
            cas_dir = None
            if cas_val:
                cas_clean = str(cas_val).strip()
                cas_dir = Path(CONFIG.archive_root) / f"CAS-{cas_clean}"
                cas_dir.mkdir(parents=True, exist_ok=True)
            #TODO need to get actual file paths back from the drive function
            # Time the download operation
            start_time = time.perf_counter()
            result = drive_section5_download(url, cas_dir, need_html_download, need_pdf_download, debug_out=Path(CONFIG.debug_out), headless=CONFIG.headless)
            end_time = time.perf_counter()
            elapsed = end_time - start_time
            total_download_time += elapsed
            download_calls += 1
            logger.info("Download elapsed for cas=%s: %.3f seconds", cas_val, elapsed)
            html_result = result['html']
            pdf_result = result['pdf']
            if need_html_download:
                if html_result['success']:
                    DB.log_success(cas_val, FileTypes.section5_html, html_result['path'])
                    html_success_count += 1
                else:
                    DB.log_failure(cas_val, FileTypes.section5_html)
                    if html_result['error']:
                        logger.warning("HTML error for cas=%s: %s", cas_val, html_result['error'])
            if need_pdf_download:
                if pdf_result['success']:
                    DB.log_success(cas_val, FileTypes.section5_pdf, pdf_result['path'])
                    pdf_success_count += 1
                else:
                    DB.log_failure(cas_val, FileTypes.section5_pdf)
                    if pdf_result['error']:
                        logger.warning("PDF error for cas=%s: %s", cas_val, pdf_result['error'])
            # Heartbeat message always goes to console so user can tell that code is still making progress
            print(f"Row {total_rows} processed: cas={cas_val}, html_ok={html_result['success']}, pdf_ok={pdf_result['success']}")
    finally:
        fh.close()
        logger.debug("Closed export file handle.")
    try:
        logger.info("Summary statistics:")
        logger.info("Total rows read: %d", total_rows)
        logger.info("HTML captures succeeded: %d", html_success_count)
        logger.info("PDF downloads succeeded: %d", pdf_success_count)
        # Log timing information
        logger.info("Total download time (seconds): %.3f", total_download_time)
        if download_calls:
            avg = total_download_time / download_calls
            logger.info("Average download time (seconds) over %d calls: %.3f", download_calls, avg)
        else:
            logger.info("No download calls were made; average download time N/A")
    except Exception:
        pass


def initialize_db_access() -> None:
    global DB
    # Initialize the database connection
    DB = HarvestDB(CONFIG.db_path)
    return


if __name__ == "__main__":
    logger.info("Starting harvest section 5.")
    main()
