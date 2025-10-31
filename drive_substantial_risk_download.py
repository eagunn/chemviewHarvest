import re
import requests
import html as html_lib
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import logging
from typing import Dict, Any
from file_types import FileTypes
from HarvestDB import HarvestDB

logger = logging.getLogger(__name__)


def _need_download_from_db(db, cas_val: str, file_type: str) -> bool:
    """Return True if the driver should attempt a download for this cas/file_type.
    Policy: if no record -> True; if record and last_success_datetime is null -> True; if last_failure exists but no success -> skip retry (False).
    """
    record = None
    try:
        record = db.get_harvest_status(cas_val, file_type)
    except Exception:
        logger.exception("DB read failed when checking need for %s / %s", cas_val, file_type)
        return True
    if record:
        last_success = record.get('last_success_datetime')
        last_failure = record.get('last_failure_datetime')
        if not last_success:
            if not last_failure:
                return True
            else:
                logger.info("****Skipping retry FOR NOW for id %s / %s, previous failure at %s", cas_val, file_type, last_failure)
                return False
        return False
    else:
        return True


def drive_substantial_risk_download(url, cas_val, cas_dir: Path, debug_out=None, headless=True, browser=None, page=None, db=None, file_types: Any = None) -> Dict[str, Any]:
    """Stub substantial risk driver. Uses DB to decide whether to attempt, then returns random outcomes and logs them to DB.

    Returns a dict with keys:
      - 'attempted': bool (True if any download was attempted)
      - 'html': {success, local_file_path, error, navigate_via}
      - 'pdf': {success, local_file_path, error, navigate_via}
    """
    result: Dict[str, Any] = {
        'attempted': False,
        'html': {'success': False, 'local_file_path': None, 'error': None, 'navigate_via': ''},
        'pdf': {'success': False, 'local_file_path': None, 'error': None, 'navigate_via': ''}
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

    need_html = _need_download_from_db(db, cas_val, file_types.substantial_risk_html)
    need_pdf = _need_download_from_db(db, cas_val, file_types.substantial_risk_pdf)

    if not need_html and not need_pdf:
        logger.info("No downloads needed for cas=%s (substantial risk)", cas_val)
        return result

    result['attempted'] = True

    # Ensure debug_out and cas_dir exist
    if debug_out is None:
        debug_out = Path("debug_artifacts")
    debug_out = Path(debug_out)
    debug_out.mkdir(parents=True, exist_ok=True)
    if cas_dir is None:
        cas_dir = Path(".")
    cas_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Start of processing for URL: %s", url)

    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        msg = f"Playwright not available; cannot navigate to URL: {e}"
        logger.error(msg)
        result['html']['error'] = msg
        result['pdf']['error'] = msg
        # Record failures in DB for attempted file types
        if need_html:
            try:
                db.log_failure(cas_val, file_types.section5_html, '')
            except Exception:
                logger.exception("Failed to write failure to DB for html")
        if need_pdf:
            try:
                db.log_failure(cas_val, file_types.section5_pdf, '')
            except Exception:
                logger.exception("Failed to write failure to DB for pdf")
        return result

    if page is None:
        logger.error("No page provided for URL: %s", url)
        return result

    page = navigate_to_initial_page(page, url)

    sr_link = find_link_to_next_modal(page)

    # If we found the sr/8e link, then click it to open the summary modal.
    # We always need to open the modal, even if we don't need to save
    # its HTML, because we need to get a download link from it.
    if sr_link:
        try:
            onclick = (sr_link.get_attribute('onclick') or '') or ''
            logger.debug("found onclick attribute in link")
        except Exception:
            logger.warning("Failed to find onclick attribute in link")
            onclick = ''
    else:
        logger.warning("No SR/8e link found on page")
        onclick = ''

    try:
        if 'childModalClick' in onclick or 'modalClick' in onclick:
            logger.debug("Going to try modal click via evaluate")
            try:
                page.evaluate(
                    "(el)=>{ try{ if(typeof childModalClick === 'function'){ childModalClick(new MouseEvent('click',{bubbles:true,cancelable:true}), el); return true; } if(typeof modalClick === 'function'){ modalClick(new MouseEvent('click',{bubbles:true,cancelable:true}), el); return true; } el.dispatchEvent(new MouseEvent('click',{bubbles:true,cancelable:true})); return true;}catch(e){ try{ el.click(); }catch(e){} return false;} }",
                    sr_link,
                )
                logger.debug("Clicked SR/8e element via childModalClick/modalClick evaluate")
            except Exception:
                logger.warning("Exception when trying to click SR/8e element via childModalClick/modalClick evaluate)")
                pass
        else:
            logger.warning("Did not find expected click attributes in SR/8e link")

        try:
            page.wait_for_timeout(2000)
        except Exception:
            logger.warning("Exception when trying to wait for page to load")
            pass
    except Exception as e:
        logger.error("Failed to click global SR/8e element: %s", e)

    logger.info("Waiting for TSCA SECTION 5 ORDER modal to appear...")

    input("Inspect page and then hit Enter to continue...")

    return result


def find_link_to_next_modal(page):
    try:
        anchors = page.query_selector_all("a[href]")
        logger.debug("Found %d href anchors on page", len(anchors))
    except Exception:
        anchors = []

    sr_link = None
    for a in anchors:
        try:
            text = a.inner_text().strip()  # visible text (use text_content() for raw)
            logger.debug("anchor text: %s", text)
            if text.startswith("* TSCA \u00A7 8(e) Submission"):
                sr_link = a
                logger.info("Found SR/8e link")
                logger.debug(a.inner_html)
                break

        except Exception:
            continue
    return sr_link


def navigate_to_initial_page(page, url):
    nav_ok = False
    nav_timeouts = [30000, 60000, 90000]
    for attempt, to in enumerate(nav_timeouts, start=1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=to)
            nav_ok = True
            try:
                page.wait_for_timeout(5000)
            except Exception:
                pass
            break
        except Exception as e:
            logger.warning("Navigation attempt %d failed (timeout=%dms): %s", attempt, to, e)
            try:
                page.wait_for_timeout(500)
            except Exception:
                logging.warning("Navigation attempt %d failed (timeout=%dms)", attempt, to)
                pass

        if not nav_ok:
            logger.error("Navigation ultimately failed for URL, continuing to save whatever we have")
            continue
        else:
            logger.info("Initial navigation succeeded on attemtp %d", attempt)
    return page