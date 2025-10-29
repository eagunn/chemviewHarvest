import csv
import logging
import time
from pathlib import Path
from typing import Callable, Any
from urllib.parse import urlparse, parse_qs, urlencode
from HarvestDB import HarvestDB
from logging_setup import initialize_logging

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


def do_need_download(db: HarvestDB, cas_val: str, file_type: str) -> bool:
    do_download = False
    record = db.get_harvest_status(cas_val, file_type)
    if record:
        logger.debug("DB record for id %s / %s: %s", cas_val, file_type, record)
        last_success = record.get('last_success_datetime')
        last_failure = record.get('last_failure_datetime')
        if not last_success:
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


def run_harvest(config: Any, drive_func: Callable[..., dict], file_types: Any):
    """Run the harvesting loop using the provided drive function.

    - config: object with attributes input_file, db_path, headless, debug_out, archive_root, max_downloads
    - drive_func: callable with signature matching drive_section5_download
    - file_types: object with attributes for file type names (e.g., section5_html, section5_pdf)
    """
    # Ensure logging is initialized (caller may already have done this)
    try:
        initialize_logging()
    except Exception:
        pass

    Path(config.debug_out).mkdir(parents=True, exist_ok=True)
    Path(config.archive_root).mkdir(parents=True, exist_ok=True)

    db = HarvestDB(config.db_path)

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
                continue

            if config.max_downloads is not None and download_calls >= config.max_downloads:
                logger.info("Reached configured max_downloads=%s; stopping processing.", config.max_downloads)
                break

            total_rows += 1
            logger.debug("--- starting processing of row %d ---", total_rows)
            cas_val = (row.get(first_field) or '').strip() if first_field else ''
            url = (row.get(last_field) or '').strip()
            if not url or not cas_val:
                logger.warning("missing url or cas_val (url=%s, cas_val=%s), skipping this entry", url, cas_val)
                continue

            need_html_download = do_need_download(db, cas_val, file_types.section5_html)
            need_pdf_download = do_need_download(db, cas_val, file_types.section5_pdf)
            if not need_html_download and not need_pdf_download:
                logger.info("Skipping download for cas=%s; files already downloaded.", cas_val)
                continue
            else:
                logger.info("At least one download needed for cas=%s: html (%s), pdf(%s)", cas_val, need_html_download, need_pdf_download)

            url = fixup_url(url, cas_val)
            cas_dir = None
            if cas_val:
                cas_clean = str(cas_val).strip()
                cas_dir = Path(config.archive_root) / f"CAS-{cas_clean}"
                cas_dir.mkdir(parents=True, exist_ok=True)

            start_time = time.perf_counter()
            result = drive_func(
                url,
                cas_dir,
                need_html_download,
                need_pdf_download,
                debug_out=Path(config.debug_out),
                headless=config.headless,
                LOG_FILE=None,
                browser=browser,
                page=page,
            )
            end_time = time.perf_counter()
            elapsed = end_time - start_time
            total_download_time += elapsed
            download_calls += 1
            logger.info("Download elapsed for cas=%s: %.3f seconds", cas_val, elapsed)

            html_result = result.get('html', {})
            pdf_result = result.get('pdf', {})

            if need_html_download:
                if html_result.get('success'):
                    local_path = html_result.get('local_file_path')
                    nav_via = html_result.get('navigate_via')
                    db.log_success(cas_val, file_types.section5_html, local_path, nav_via)
                    html_success_count += 1
                else:
                    nav_via = html_result.get('navigate_via')
                    db.log_failure(cas_val, file_types.section5_html, nav_via)
                    if html_result.get('error'):
                        logger.warning("HTML error for cas=%s: %s", cas_val, html_result.get('error'))

            if need_pdf_download:
                if pdf_result.get('success'):
                    local_path = pdf_result.get('local_file_path')
                    nav_via = pdf_result.get('navigate_via')
                    db.log_success(cas_val, file_types.section5_pdf, local_path, nav_via)
                    pdf_success_count += 1
                else:
                    nav_via = pdf_result.get('navigate_via')
                    db.log_failure(cas_val, file_types.section5_pdf, nav_via)
                    if pdf_result.get('error'):
                        logger.warning("PDF error for cas=%s: %s", cas_val, pdf_result.get('error'))

            # Heartbeat to console
            print(f"Row {total_rows} processed: cas={cas_val}, html_ok={html_result.get('success')}, pdf_ok={pdf_result.get('success')}")

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
        logger.info("Total download time (seconds): %.3f", total_download_time)
        if download_calls:
            avg = total_download_time / download_calls
            logger.info("Average download time (seconds) over %d calls: %.3f", download_calls, avg)
        else:
            logger.info("No download calls were made; average download time N/A")
    except Exception:
        pass

    return 0

