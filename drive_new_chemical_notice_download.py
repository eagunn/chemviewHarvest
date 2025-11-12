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
import HarvestDB
import pdf_plan

logger = logging.getLogger(__name__)

# Initialize shared PDF plan module for this driver. The folder name is the only
# driver-specific configuration required here; other drivers may call pdf_plan.init
# with a different folder if needed.
pdf_plan.init(folder='chemview_archive_ncn', out_dir=Path('pdfDownloadsToDo'), batch_size=25)

add_pdf_links_to_plan = pdf_plan.add_pdf_links_to_plan
save_download_plan = pdf_plan.save_download_plan
flush_pdf_plan = pdf_plan.flush


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

    need_html = db.need_download(cas_val, file_types.new_chemical_notice_html, retry_interval_hours=retry_interval_hours)
    need_pdf = db.need_download(cas_val, file_types.new_chemical_notice_pdf, retry_interval_hours=retry_interval_hours)

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
    if nav_ok:
        ncn_list = find_anchor_links_on_chemical_overview_modal(page)
         # Process ncn links which should each have a modal and some PDFs to harvest
        if ncn_list and len(ncn_list) > 0:
             for idx, ncn_link in enumerate(ncn_list, start=1):
                 # Click the ncn anchor and get back a locator for the modal that opened (or None on failure)
                 modal_locator = click_ncn_anchor_link_and_wait_for_modal(page, ncn_link)
                 if modal_locator is None:
                     logger.warning("Skipping ncn link %d for cas %s because modal was not observed", idx, cas_val)
                     continue

                 pdf_link_list = None
                 if need_html or need_pdf:
                     # pass the modal locator (required) so the scraper uses the already-observed modal
                     try:
                         pdf_link_list = scrape_ncn_modal_html_and_gather_pdf_links(page, modal_locator, need_html, need_pdf, cas_dir, cas_val, db, file_types, url, result, item_no=idx)
                     except Exception as e:
                         logger.exception("Exception raised while scraping modal %d: %s", idx, e)
                         # record processing failures
                         result['pdf']['error'] = f"Exception while scraping modal {idx}: {e}"

                 if pdf_link_list:
                     # Add discovered PDF links to the global accumulator (will be flushed to disk in batches)
                     # For now we are simply trusting this to work and assuming success at this point.
                     ############## TEMPORARY - reenable!!!
                     #_accumulate_pdf_links_for_cas(cas_dir, pdf_link_list)
                     result['pdf']['success'] = True
                     result['pdf']['local_file_path'] = str(cas_dir / "substantialRiskReports")
                     result['pdf']['navigate_via'] = url
                 # else: if we didn't find a list we are going to log this as a failure below
        else:
            msg = "No NCN links found on initial page"
            logger.error(msg)
            result['html']['error'] = msg
            result['pdf']['error'] = msg


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


def click_ncn_anchor_link_and_wait_for_modal(page, ncn_link):
    """
    Clicks the given SR anchor and waits robustly for the unique visible modal
    to appear and contain the expected content. Returns the Locator for the modal container.
    """
    if ncn_link is None:
        logger.warning("No NCN link passed to click_anchor_link_and_wait_for_modal")
        return None

    # --- 1. Click the Anchor ---
    try:
        ncn_link.click(timeout=30000)
        logger.debug("Clicked anchor via locator.click()")
    except TimeoutError:
        logger.warning("Failed to click anchor within 30s timeout.")
        return None
    except Exception as e:
        logger.warning("Failed to click anchor due to unexpected error: %s", e)
        return None

    # --- 2. Define the Target Modal Locator (Simplified) ---

    # We rely on the core Bootstrap/CSS classes to find the *unique* visible modal.
    # We remove ALL complex logic involving data-target/href derivation, modal_id,
    # and CSS.escape, as this was the source of the failure.
    visible_modal_locator = page.locator("div.modal.show, div.modal.in")

    # --- 3. Wait for Modal and Expected Content ---

    # Define the locator for the critical content (PDF anchors) *inside* the visible modal.
    # This guarantees the modal has opened AND has finished populating its dynamic content.
    final_content_locator = visible_modal_locator.locator("li a.show_external_link")

    try:
        # Wait for the first PDF anchor to be visible inside the target modal.
        final_content_locator.first.wait_for(state="visible", timeout=20000)

        # Now that we know the content is loaded and the correct modal is up,
        # we return the unique visible modal's locator.
        logger.info("New modal observed and content verified.")

        # We can safely return the simplified locator for the visible modal container.
        return visible_modal_locator

    except TimeoutError:
        logger.warning("Timed out waiting for new modal to open and contain expected PDF anchor content after 20s.")
        return None
    except Exception as e:
        logger.error("Error during final modal wait: %s", e)
        return None

def find_anchor_links_on_chemical_overview_modal(page):
    ncn_link_list = []
    # 1. Define the Locator for the specific anchors you want.
    # Playwright is smart enough to search only within the visible
    # modal if it's the only element matching this selector.
    anchors_locator = page.locator('div#chemical-detail-modal-body a[href]')
    anchors = []
    try:
        # 2. Explicitly wait for the *first* matching anchor to be visible.
        # 8 second wait has been maximum needed when logs are reviewed.
        anchors_locator.first.wait_for(state="visible", timeout=8000)
        # 3. Once at least one is visible, retrieve all matching Locators.
        # Note: .all() returns a list of Locators, ready for iteration.
        anchors = anchors_locator.all()
    except TimeoutError:
        # Handle the case where the element never appears within the timeout
        logger.warning("Timeout: No href anchors appeared before timeout.")
    except Exception as e:
        logger.error(f"An unexpected error occurred while waiting for anchors: {e}")

    # Continue with your logic using the 'anchors' list
    logger.debug("Found %d href anchors on page", len(anchors))
    for i, anchor in enumerate(anchors):
        try:
            text = anchor.inner_text().strip()
            if text is not None:
                logger.debug("examining anchor text: %s", text)
                # Identify SR/8e links by text prefix
                if text.startswith("New Chemical Notice"):
                    logger.debug("Found ncn link")
                    ncn_link_list.append(anchor)
                elif text != "":
                    logger.warning("Found unexpected non-blank link")
                else:
                    logger.debug("Found blank anchor text; skipping")
            else:
                logger.debug("Anchor text is None; skipping")
        except Exception:
            logger.exception("Exception while processing anchor %d", i)
            continue

    logger.info(f"Found {len(ncn_link_list)} NCN links on page")

    return ncn_link_list


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
