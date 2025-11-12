import requests
import html as html_lib
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import logging
from typing import Dict, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
from datetime import datetime, timedelta
import atexit
import re

logger = logging.getLogger(__name__)

# Module-level accumulator for download plans so we can write one JSON per N CAS entries
PDF_PLAN_ACCUM: Dict[str, Any] = {'folder': 'chemview_archive_ncn', 'subfolderList': [], 'downloadList': []}
PDF_PLAN_ACCUM_CAS_SET: set = set()
PDF_PLAN_ACCUM_CAS_SINCE_WRITE: int = 0
PDF_PLAN_WRITE_BATCH_SIZE: int = 25
PDF_PLAN_OUT_DIR: Path = Path('pdfDownloadsToDo')
PDF_PLAN_OUT_DIR.mkdir(parents=True, exist_ok=True)


def _need_download_from_db(db, cas_val: str, file_type: str, retry_interval_hours: float = 12.0) -> bool:
    """
    Policy:
    - If no record: return True
    - If record and last_success_datetime is not null: return False (do not retry if any success)
    - If record and last_success_datetime is null:
        - If last_failure_datetime is null: return True
        - If last_failure_datetime is less than `retry_interval_hours` ago: return False
        - If last_failure_datetime is more than `retry_interval_hours` ago: return True

    retry_interval_hours: number of hours to wait after a failure before retrying (default 12.0)
    """
    record = None
    do_need_download = False
    try:
        record = db.get_harvest_status(cas_val, file_type)
    except Exception:
        logger.exception("DB read failed when checking need for %s / %s", cas_val, file_type)
        # If we can't read the db, we don't want to be doing downloads.
        # We'll return the false set above

    if record:
        last_success = record.get('last_success_datetime')
        last_failure = record.get('last_failure_datetime')
        # If any success is recorded, do not retry
        if last_success:
            logger.debug("Found prior success for %s / %s; no download needed", cas_val, file_type)
        else:
            # If no success, check failure interval
            if last_failure:
                now = datetime.now()
                try:
                    last_failure_dt = datetime.fromisoformat(str(last_failure))
                except Exception:
                    logger.exception("Failed to parse last_failure_datetime for %s / %s", cas_val, file_type)
                    # conservative: do not retry if we can't parse the stored timestamp
                    return False
                if now - last_failure_dt > timedelta(hours=retry_interval_hours):
                    do_need_download = True
                    logger.debug("Found old-enough prior failure for %s / %s (threshold=%sh)", cas_val, file_type, retry_interval_hours)
                else:
                    logger.debug("Found too-new prior failure for %s / %s; no download needed (threshold=%sh)", cas_val, file_type, retry_interval_hours)
            else:
                # no success, no failure -> need download
                do_need_download = True
    else:
        # no record -> need download
        do_need_download = True
    return do_need_download


# --- helpers for building and saving a per-run JSON download plan ---

def _ensure_cas_entry(plan: Dict[str, Any], cas_folder_name: str) -> Dict[str, Any]:
    """Return or create a cas entry dict inside plan['subfolderList']."""
    for entry in plan.get('subfolderList', []):
        if entry.get('folder') == cas_folder_name:
            return entry
    new_entry = {'folder': cas_folder_name, 'subfolderList': [], 'downloadList': []}
    plan.setdefault('subfolderList', []).append(new_entry)
    return new_entry


def _ensure_reports_subfolder(cas_entry: Dict[str, Any], reports_name: str = 'substantialRiskReports') -> Dict[str, Any]:
    """Return or create the reports subfolder dict inside a cas_entry."""
    for sf in cas_entry.get('subfolderList', []):
        if sf.get('folder') == reports_name:
            return sf
    new_sf = {'folder': reports_name, 'subfolderList': [], 'downloadList': []}
    cas_entry.setdefault('subfolderList', []).append(new_sf)
    return new_sf


def add_pdf_links_to_plan(plan: Dict[str, Any], cas_dir: Path, pdf_links: list[str]):
    """Add pdf_links to the nested plan structure under the cas_dir name and substantialRiskReports subfolder.
    Duplicate URLs are ignored.
    """
    if not pdf_links:
        return
    cas_folder_name = cas_dir.name
    cas_entry = _ensure_cas_entry(plan, cas_folder_name)
    reports_sf = _ensure_reports_subfolder(cas_entry)
    existing = set(reports_sf.get('downloadList', []))
    added = 0
    skipped_duplicates = 0
    for url in pdf_links:
        if not url:
            continue
        if url in existing:
            skipped_duplicates += 1
            continue
        reports_sf.setdefault('downloadList', []).append(url)
        existing.add(url)
        added += 1



def save_download_plan(plan: Dict[str, Any], debug_out: Path) -> Path:
    """Write the plan to a timestamped JSON file in debug_out and return the path."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"pdfDownloads_{ts}.json"
    out_path = Path(debug_out) / filename
    try:
        with open(out_path, 'w', encoding='utf-8') as fh:
            json.dump(plan, fh, indent=2)
        logger.info("Saved PDF download plan to %s", out_path)
    except Exception as e:
        logger.exception("Failed to save pdf download plan to %s: %s", out_path, e)
    return out_path

def drive_new_chemical_notice_download(url, cas_val, cas_dir: Path, debug_out=None, headless=True, browser=None, page=None, db=None, file_types: Any = None, retry_interval_hours: float = 12.0) -> Dict[str, Any]:
    """ Walk the browser through the web pages and modals we need to capture
    New Chemical Notice html content and pdfs
    """
    result: Dict[str, Any] = {
        'CAS:': cas_val,
        'attempted': False,
        'html': {'success': None, 'local_file_path': None, 'error': None, 'navigate_via': ''},
        'pdf': {'success': None, 'local_file_path': None, 'error': None, 'navigate_via': ''}
    }

    if db is None or file_types is None:
        msg = "Driver requires db and file_types to be provided"
        logger.error(msg)
        result['html']['error'] = msg
        result['pdf']['error'] = msg
        return result

    if not cas_val:
        msg = "cas_val is required"
        logger.error(msg)
        result['html']['error'] = msg
        result['pdf']['error'] = msg
        return result

    need_html = _need_download_from_db(db, cas_val, file_types.new_chemical_notice_html, retry_interval_hours=retry_interval_hours)
    need_pdf = _need_download_from_db(db, cas_val, file_types.new_chemical_notice_pdf, retry_interval_hours=retry_interval_hours)

    if not need_html and not need_pdf:
        logger.info("No downloads needed for cas=%s (substantial risk)", cas_val)
        return result

    # Ensure debug_out path exists and set a fallback default for CAS foldername
    if debug_out is None:
        debug_out = Path("debug_artifacts")
    debug_out = Path(debug_out)
    debug_out.mkdir(parents=True, exist_ok=True)
    if cas_dir is None:
        cas_dir = Path(".")
    # Note: do not create cas_dir here; caller (harvest_framework) is responsible for ensuring cas_dir exists

    logger.info("Start of processing for URL: %s", url)

    if page is None:
        logger.error("No page passed down from framework")
        return result

    # We've passed all the pre-checks; mark that we are attempting processing
    result['attempted'] = True
    # Be pessimistic. Assume failure until success is confirmed.
    if need_html:
        result['html']['success'] = False
    if need_pdf:
        result['pdf']['success'] = False
    # from this point on down, we only need to set the msg value for failures
    # but will need to set 'success' to True on completed, confirmed successes.

    nav_ok = navigate_to_chemical_overview_modal(page, url)
    input("Press Enter after verifying the modal is open...")

    # Post-loop: if we attempted processing then log failures for any file types that were explicitly set to False
    if result.get('attempted'):
        logger.debug(f"After modal scrape attempt, result = {result}")
        if need_html:
            if (result.get('html', {}).get('success') is True):
                try:
                    db.log_success(cas_val, file_types.substantial_risk_html, result.get('html', {}).get('local_file_path'), result.get('html', {}).get('navigate_via'))
                except Exception:
                    logger.exception("Failed to write success to DB for html post-loop")
            else:
                # HTML explicitly failed during processing -> log failure
                msg = result.get('html', {}).get('error') or "HTML processing failed"
                try:
                    db.log_failure(cas_val, file_types.substantial_risk_html, msg)
                except Exception:
                    logger.exception("Failed to write failure to DB for html post-loop")
            if need_pdf:
                if (result.get('pdf', {}).get('success') is True):
                    try:
                        db.log_success(cas_val, file_types.substantial_risk_pdf, result.get('pdf', {}).get('local_file_path'), result.get('pdf', {}).get('navigate_via'))
                    except Exception:
                        logger.exception("Failed to write success to DB for html post-loop")
                else:
                    # PDF explicitly failed during processing -> log failure
                    msg = result.get('pdf', {}).get('error') or "PDF processing failed or no links discovered"
                    try:
                        db.log_failure(cas_val, file_types.substantial_risk_pdf, msg)
                    except Exception:
                        logger.exception("Failed to write failure to DB for pdf post-loop")
    else:
        logger.debug("No downloads attempted.")

    return result


def navigate_to_chemical_overview_modal(page, url: str) -> bool:
    """
    Navigates to the URL and waits for the chemical overview modal content to be visible.

    Uses the Locator API for robust waiting on the modal element.
    """
    selector = "div#chemical-detail-modal-body"
    timeout_ms = 30000
    # 1. Navigate to the page with a single, generous timeout (90s default)
    try:
        # Use page.goto and rely on Playwright's default internal retry mechanisms if needed
        # We target 'domcontentloaded' as it's the fastest signal that the basic page structure is ready.
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        logger.info(f"Navigation to URL successful: {url}")
    except TimeoutError as e:
        logger.error(f"Initial navigation to URL timed out ({timeout_ms}ms): {e}")
        return False
    except Exception as e:
        logger.error(f"Initial navigation failed unexpectedly: {e}")
        return False

    # 2. Use the Locator API to wait for the required modal content.
    # The locator will retry finding and checking the visibility of the element
    # until the timeout (default 30s, or you can pass a custom timeout here).
    try:
        modal_locator = page.locator(selector)
        modal_locator.wait_for(state="visible", timeout=15000)  # Use 15s wait for the modal
        logger.info("Modal content is present and visible.")
        return True
    except TimeoutError as e:
        logger.error(f"Modal content selector '{selector}' not found or visible within timeout (15s).")
        return False
    except Exception as e:
        logger.error(f"Error while waiting for modal visibility: {e}")
        return False
