"""
Create a CSV of rows from the input export that have failures recorded in the DB
for the file_type 'substantial_risk_html'.

Usage:
    python substRiskFailures.py \
        --db chemview_harvest.db \
        --input input_files/chemviewSubstRisksExport20251029.csv \
        --output substRiskFailures.csv

If the chemical-id column name cannot be autodetected from the CSV header, pass
--id-column to specify it.

The output CSV preserves the header row and writes rows ordered by chemical id
(as returned by the DB query with ORDER BY chemical_id).
"""

import argparse
import csv
from file_types import FileTypes
import logging
import sqlite3
from pathlib import Path
from typing import Dict, List
from datetime import datetime

logger = logging.getLogger(__name__)

DEFAULT_DB = 'chemview_harvest.db'
DEFAULT_INPUT = 'input_files/chemviewSubstRisksExport20251029.csv'
DEFAULT_OUTPUT = None
FAIL_FILE_TYPE = FileTypes.substantial_risk_html


def normalize_id(s: str) -> str:
    if s is None:
        return ''
    return s.strip()


def get_failing_ids(db_path: Path) -> List[str]:
    """Return ordered list of chemical_id strings from DB that have failures recorded.
    Ordered alphabetically by chemical_id via SQL ORDER BY.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        sql = (
            "SELECT DISTINCT chemical_id FROM harvest_log "
            "WHERE file_type = ? AND last_failure_datetime IS NOT NULL "
            "ORDER BY chemical_id COLLATE NOCASE ASC"
        )
        cur.execute(sql, (FAIL_FILE_TYPE,))
        rows = cur.fetchall()
        ids = [normalize_id(r[0]) for r in rows if r and r[0] is not None]
        logger.info(f"Found {len(ids)} chemical ids with recorded failures in DB")
        return ids
    finally:
        conn.close()


def find_id_column(header: List[str]) -> str:
    """Return the CSV header column to use for chemical id.
    We expect the column is named 'CAS Number'. Check for exact match first,
    then case-insensitive; raise ValueError if not found.
    """
    DEFAULT_ID_COL = 'CAS Number'

    # Prefer exact match first
    for h in header:
        if h == DEFAULT_ID_COL:
            return h
    # Case-insensitive fallback
    for h in header:
        if h.strip().lower() == DEFAULT_ID_COL.lower():
            return h
    raise ValueError(f"Expected id column '{DEFAULT_ID_COL}' not found in CSV header; consider reformatting input CSV")


def main(argv=None):
    # runtime-computed default output filename with today's date
    today = datetime.now().strftime('%Y%m%d')
    default_output = f"input_files/substantialRiskFailuresAsOf{today}.csv"

    parser = argparse.ArgumentParser(description="Extract rows with substantial risk failures")
    parser.add_argument('--db', default=DEFAULT_DB, help='Path to sqlite DB (default: %(default)s)')
    parser.add_argument('--input', default=DEFAULT_INPUT, help='Input CSV file (default: %(default)s)')
    parser.add_argument('--output', default=default_output, help='Output CSV file (default: %(default)s)')
    parser.add_argument('--ignore-case', action='store_true', help='Match chemical ids case-insensitively')
    args = parser.parse_args(argv)

    # Write logging to a file for later inspection; overwrite on each run
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s [%(name)s:%(lineno)d] %(message)s',
        filename=str(Path('substRiskFailures.log')),
        filemode='w',
        encoding='utf-8'
    )

    db_path = Path(args.db)
    input_path = Path(args.input)
    output_path = Path(args.output)

    if not db_path.exists():
        logger.error("DB file not found: %s", db_path)
        return 2
    if not input_path.exists():
        logger.error("Input CSV not found: %s", input_path)
        return 3

    failing_ids = get_failing_ids(db_path)
    failing_set = set([fid.lower() if args.ignore_case else fid for fid in failing_ids])
    logger.info("Found %d chemical ids with recorded failures in DB", len(failing_ids))

    # Read CSV and collect only matching rows
    matches: Dict[str, List[List[str]]] = {}
    header = None
    # Use 'utf-8-sig' to automatically handle files that include a UTF-8 BOM
    with open(input_path, 'r', encoding='utf-8-sig', newline='') as fh:
        rdr = csv.reader(fh)
        try:
            header = next(rdr)
        except StopIteration:
            logger.error("Input CSV is empty")
            return 4
        logger.debug("CSV Header: %s", header)
        try:
            id_col = find_id_column(header)
        except ValueError as e:
            logger.error(str(e))
            return 5

        id_idx = header.index(id_col)
        logger.info("Using CSV id column: '%s' (index %d)", id_col, id_idx)

        row_count = 0
        matched_count = 0
        for row in rdr:
            row_count += 1
            if id_idx >= len(row):
                continue
            raw_id = normalize_id(row[id_idx])
            key = raw_id.lower() if args.ignore_case else raw_id
            if key in failing_set:
                matches.setdefault(key, []).append(row)
                matched_count += 1

    logger.info("Scanned %d rows; matched %d rows for failing ids", row_count, matched_count)

    # Write output in the order of failing_ids (already ordered by SQL)
    out_count = 0
    with open(output_path, 'w', encoding='utf-8', newline='') as ofh:
        w = csv.writer(ofh)
        w.writerow(header)
        for fid in failing_ids:
            key = fid.lower() if args.ignore_case else fid
            rows = matches.get(key)
            if not rows:
                logger.warning("No CSV row found for failing chemical_id: %s", fid)
                continue
            for r in rows:
                w.writerow(r)
                out_count += 1

    logger.info("Wrote %d rows to %s", out_count, output_path)
    # Console summary for quick human inspection
    try:
        print(f"Input file: {input_path}")
        print(f"Output file: {output_path}")
        print(f"Failures found in DB: {len(failing_ids)}")
        print(f"Failures matched in CSV and written: {out_count}")
    except Exception:
        # Printing is best-effort; don't fail the script if stdout is closed
        logger.debug("Unable to print console summary")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
