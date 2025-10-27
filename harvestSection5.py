# script to harvest html and PDF files related to the TSCA Section 5
# orders from the EPA ChemView website.

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
import random
import sys
from urllib.parse import urlparse, parse_qs, urlencode

# Global log file handle; will be opened in main()
LOG_FILE = None
CONFIG = None

@dataclass
class Config:
    input_file: str = "s5ExportTest2.csv"
    download_dir: str = "downloads"
    db_path: str = "chemview_harvest.db"
    headless: bool = False
    debug_out: str = "debug_artifacts"
    archive_root: str = "chemview_archive"

def open_chemview_export_file(config: Config):
    """Open the local CSV export and return a file handle.

    Looks for the named file in the current working directory.
    Returns an open file object on
    success, or None on failure (and prints an error message).
    """
    script_dir = Path(__file__).resolve().parent
    csv_path = script_dir / config.input_file
    try:
        # Use 'utf-8-sig' to transparently handle BOM if present
        fh = csv_path.open("r", encoding="utf-8-sig")
    except Exception as e:
        print(f"Error: could not open {csv_path!s}: {e}", file=LOG_FILE)
        return None, None
    print(f"Opened export file: {csv_path}", file=LOG_FILE)
    first_line = fh.readline()
    print("First line preview:", first_line.strip() if first_line else "(empty)", file=LOG_FILE)
    header_fields = [h.strip() for h in first_line.split(',')] if first_line else []
    return fh, header_fields

def drive_file_download(url, cas_dir=None, debug_out=None, headless=True):
    """
    Stub for drive_file_download. Returns random True/False for html_ok and pdf_ok.
    """
    return (random.choice([True, False]), random.choice([True, False]))

def fixup_url(url, cas_val):
    """
    If the URL's ch= field is empty, insert cas_val as its value.
    Otherwise, return the URL unchanged.
    """
    if not url or not cas_val:
        print(f"fixup_url: missing url or cas_val (url={url}, cas_val={cas_val})", file=LOG_FILE)
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
            print(f"FixedURL for chem id {cas_val} to be {new_url}", file=LOG_FILE)
    except Exception as e:
        print(f"fixup_url: exception {e} for url={url}, cas_val={cas_val}", file=LOG_FILE)

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
    args = parser.parse_args(argv)

    global CONFIG
    CONFIG = Config(
        input_file=args.input_file if args.input_file is not None else Config.input_file,
        download_dir=args.download_dir if args.download_dir is not None else Config.download_dir,
        db_path=args.db_path if args.db_path is not None else Config.db_path,
        headless=args.headless if args.headless else Config.headless,
        debug_out=args.debug_out if args.debug_out is not None else Config.debug_out,
        archive_root=args.archive_root if args.archive_root is not None else Config.archive_root
    )
    #print("Configuration initialized:", CONFIG, file=LOG_FILE)
    return

def main(argv=None):

    initialize_config(argv)

    global LOG_FILE
    try:
        LOG_FILE = Path("harvestSection5.log").open("w", encoding="utf-8")
    except Exception as e:
        LOG_FILE = sys.stdout
        print(f"Warning: could not open log file harvestSection5.log: {e}; logging to stdout", file=LOG_FILE)

    print("harvestSection5: ready", file=LOG_FILE)
    print("Parsed arguments ->", CONFIG, file=LOG_FILE)

    Path(CONFIG.debug_out).mkdir(parents=True, exist_ok=True)
    Path(CONFIG.archive_root).mkdir(parents=True, exist_ok=True)

    fh, header_fields = open_chemview_export_file(CONFIG)
    if fh is None:
        print("Failed to open chemview export file. Exiting with error.", file=LOG_FILE)
        return 1

    if header_fields is None:
        print("Error: CSV header could not be read. Exiting with error code 2.", file=LOG_FILE)
        return 2

    total_rows = 0
    html_success_count = 0
    pdf_success_count = 0
    try:
        reader = csv.DictReader(fh, fieldnames=header_fields)
        first_field = header_fields[0]
        last_field = header_fields[-1]
        for row in reader:
            if not row or all(not (v and v.strip()) for v in row.values()):
                continue  # Skip blank or all-empty lines
            print("\n***", file=LOG_FILE)  # Separator for each row processing
            total_rows += 1
            url = (row.get(last_field) or '').strip()
            cas_val = (row.get(first_field) or '').strip() if first_field else ''
            url = fixup_url(url, cas_val) if 'fixup_url' in globals() else url
            if not url:
                print("Skipping empty URL entry", file=LOG_FILE)
                continue
            cas_dir = None
            if cas_val:
                cas_clean = str(cas_val).strip()
                cas_dir = Path(CONFIG.archive_root) / f"CAS-{cas_clean}"
                cas_dir.mkdir(parents=True, exist_ok=True)
            html_ok, pdf_ok = drive_file_download(url, cas_dir=cas_dir, debug_out=Path(CONFIG.debug_out), headless=CONFIG.headless)
            if html_ok:
                html_success_count += 1
            if pdf_ok:
                pdf_success_count += 1
            print(f"Row {total_rows} processed: cas={cas_val}, html_ok={html_ok}, pdf_ok={pdf_ok}")
    finally:
        fh.close()
        print("Closed export file handle.", file=LOG_FILE)
    try:
        print("\nSummary statistics:", file=LOG_FILE)
        print(f"Total rows read: {total_rows}", file=LOG_FILE)
        print(f"Section 5 HTML captures succeeded: {html_success_count}", file=LOG_FILE)
        print(f"PDF downloads succeeded: {pdf_success_count}", file=LOG_FILE)
    except Exception:
        pass

if __name__ == "__main__":
    main()
