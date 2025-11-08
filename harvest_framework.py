import csv
import logging
import time
from pathlib import Path
from typing import Callable, Any
from urllib.parse import urlparse, parse_qs, urlencode
from HarvestDB import HarvestDB

logger = logging.getLogger(__name__)

def open_chemview_export_file(input_file: str):
    script_dir = Path(__file__).resolve().parent
    csv_path = script_dir / input_file
    try:
        fh = csv_path.open("r", encoding="utf-8-sig")
    except Exception as e:
        logger.error("Error: could not open %s: %s", csv_path, e)
        return None, None
    logger.info("Opened export file: %s", csv_path)
    first_line = fh.readline()
    logger.debug("First line preview: %s", (first_line.strip() if first_line else "(empty)"))
    header_fields = [h.strip() for h in first_line.split(',')] if first_line else []
    return fh, header_fields


def fixup_url(url: str, cas_val: str) -> str:
    if not url or not cas_val:
        logger.debug("fixup_url: missing url or cas_val (url=%s, cas_val=%s)", url, cas_val)
        return url

    new_url = url
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        ch_vals = qs.get('ch')
        ch_val = ch_vals[0] if ch_vals and len(ch_vals) > 0 else ''
        if not ch_val:
            qs['ch'] = [cas_val]
            new_query = urlencode(qs, doseq=True)
            new_url = parsed._replace(query=new_query).geturl()
            logger.info("FixedURL for chem id %s to be %s", cas_val, new_url)
    except Exception as e:
        logger.debug("fixup_url: exception %s for url=%s, cas_val=%s", e, url, cas_val)

    return new_url


def run_harvest(config: Any, drive_func: Callable[..., dict], file_types: Any):
    """Run the harvesting loop using the provided drive function.
    - config: object with attributes input_file, db_path, headless, debug_out, archive_root, max_downloads
    - drive_func: callable that implements report-specific download logic and DB writes
    - file_types: object with attributes for file type names (e.g., section5_html, section5_pdf)
    - Note: it is the responsibility of the caller to initialize logging.
    """

    Path(config.debug_out).mkdir(parents=True, exist_ok=True)
    Path(config.archive_root).mkdir(parents=True, exist_ok=True)

    # NOTE: Most DB interactions are handled inside driver modules now.
    # But we open the DB here and pass its handle to the driver.
    db = None
    if not config.db_path:
        msg = "No db_path provided"
        logger.error(msg)
        return 3
    try:
        db = HarvestDB(config.db_path)
    except Exception as e:
        msg = f"Failed to open DB at {config.db_path}: {e}"
        logger.exception(msg)
        return 3

    # Attempt to start a single playwright browser for reuse. If Playwright
    # isn't available, drive_func is expected to create its own browser per call.
    p = None
    browser = None
    page = None
    try:
        from playwright.sync_api import sync_playwright
        p = sync_playwright().start()
        browser = p.chromium.launch(headless=config.headless)
        page = browser.new_page()
        try:
            page.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})
        except Exception:
            logger.warning("Failed to set extra_http_headers")
        logger.info("Launched Playwright browser for reuse (headless=%s)", config.headless)
    except Exception as e:
        logger.warning("Playwright not available for reuse: %s; will let download create browsers per-call", e)

    fh, header_fields = open_chemview_export_file(config.input_file)
    if fh is None:
        logger.error("Failed to open chemview export file. Exiting with error.")
        return 1
    if header_fields is None:
        logger.error("Error: CSV header could not be read. Exiting with error code 2.")
        return 2

    # Determine stop-file path (default 'harvest.stop' in CWD, can be overridden by config.stop_file)
    stop_file_name = getattr(config, "stop_file", "harvest.stop")
    stop_path = Path(stop_file_name)
    if not stop_path.is_absolute():
        stop_path = Path.cwd() / stop_path
    logger.info("Will watch for stop file: %s", stop_path)

    logger.debug("Chemview CSV file opened and we have header fields")
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
            # Check for external stop signal before processing each row
            try:
                if stop_path.exists():
                    logger.info("Stop file detected at %s; terminating harvest loop gracefully.", stop_path)
                    print("Stop file detected; terminating harvest loop.")
                    break
            except Exception as e:
                logger.warning("Failed to check stop file %s: %s", stop_path, e)

            if not row or all(not (v and v.strip()) for v in row.values()):
                continue

            total_rows += 1
            # Skip rows until the start_row is reached
            if config.start_row is not None and total_rows < config.start_row:
                #logger.debug("Skipping row %d due to start_row=%d", total_rows, config.start_row)
                continue

            # Stop if we've reached the configured number of actual download attempts
            if config.max_downloads is not None and download_calls >= config.max_downloads:
                logger.info("Reached configured max_downloads=%s; stopping processing.", config.max_downloads)
                break

            logger.debug("--- starting processing of row %d ---", total_rows)
            cas_val = (row.get(first_field) or '').strip() if first_field else ''
            url = (row.get(last_field) or '').strip()
            if not url or not cas_val:
                logger.warning("missing url or cas_val (url=%s, cas_val=%s), skipping this entry", url, cas_val)
                continue

            # Let the driver decide whether a download is needed and perform any DB updates.
            url = fixup_url(url, cas_val)
            cas_dir = None
            if cas_val:
                cas_clean = str(cas_val).strip()
                cas_dir = Path(config.archive_root) / f"CAS-{cas_clean}"
                cas_dir.mkdir(parents=True, exist_ok=True)

            start_time = time.perf_counter()
            logger.debug(f"about to call driver for cas={cas_val}, url={url}")
            result = drive_func(
                url,
                cas_val,
                cas_dir,
                debug_out=Path(config.debug_out),
                headless=config.headless,
                browser=browser,
                page=page,
                db=db,
                file_types=file_types,
            )
            end_time = time.perf_counter()
            elapsed = end_time - start_time

            # If the driver attempted a download, count it towards configured max_downloads and timing
            attempted = bool(result and result.get('attempted'))
            if attempted:
                total_download_time += elapsed
                download_calls += 1
                logger.info("Processing time elapsed for cas=%s: %.3f seconds", cas_val, elapsed)

            # Aggregate success counts based on driver's reported results
            html_result = (result.get('html') if result else {}) or {}
            pdf_result = (result.get('pdf') if result else {}) or {}

            if html_result.get('success'):
                html_success_count += 1
            if pdf_result.get('success'):
                pdf_success_count += 1

            # Log errors reported by driver
            if html_result.get('error'):
                logger.warning("HTML error for cas=%s: %s", cas_val, html_result.get('error'))
            if pdf_result.get('error'):
                logger.warning("PDF error for cas=%s: %s", cas_val, pdf_result.get('error'))

            # Heartbeat to console (keep this printed to console as before)
            print(f"Row {total_rows}: cas={cas_val}, html_ok={html_result.get('success')}, pdf_ok={pdf_result.get('success')}, (processed {download_calls} of {config.max_downloads} so far)")

    finally:
        fh.close()
        logger.debug("Closed export file handle.")
        try:
            if page is not None:
                try:
                    page.close()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            if browser is not None:
                try:
                    browser.close()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            if p is not None:
                try:
                    p.stop()
                except Exception:
                    pass
        except Exception:
            pass

    try:
        logger.info("Summary statistics:")
        logger.info("Total rows read: %d", total_rows)
        logger.info("HTML captures succeeded: %d", html_success_count)
        logger.info("PDF downloads succeeded: %d", pdf_success_count)
        logger.info("Total processing time (seconds): %.3f", total_download_time)
        if download_calls:
            avg = total_download_time / download_calls
            logger.info("Average processing time (seconds) over %d calls: %.3f", download_calls, avg)
        else:
            logger.info("No download calls were made; average download time N/A")
    except Exception:
        pass

    return 0
