import atexit
import logging
import re
from pathlib import Path
from typing import Dict, Any, Optional
import download_plan

logger = logging.getLogger(__name__)

# Initialize shared download plan module for this driver. The folder name is the only
# driver-specific configuration required here; other drivers may call download_plan.init
# with a different folder if needed.
download_plan.init(folder='chemview_archive_ncn', out_dir=Path('downloadsToDo'), batch_size=25)
atexit.register(download_plan.flush)


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
        logger.info("No downloads needed for cas=%s (new chemical notice)", cas_val)
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
                 # Scrape the modal and get the zip download links
                 scrape_success = scrape_modal_and_get_downloads(page, cas_dir, ncn_link, idx, need_html, need_pdf, result)

                 if need_pdf and scrape_success:
                     # declare success for "pdf" / zip downloads
                     result['pdf']['success'] = True
                     result['pdf']['local_file_path'] = str(cas_dir / "*_supporting_docs")
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
                    db.log_success(cas_val, file_types.new_chemical_notice_html, result.get('html', {}).get('local_file_path'), result.get('html', {}).get('navigate_via'))
                except Exception:
                    logger.exception("Failed to write success to DB for html post-loop")
            else:
                # HTML explicitly failed during processing -> log failure
                msg = result.get('html', {}).get('error') or "HTML processing failed"
                try:
                    db.log_failure(cas_val, file_types.new_chemical_notice_html, msg)
                except Exception:
                    logger.exception("Failed to write failure to DB for html post-loop")
            if need_pdf:
                if (result.get('pdf', {}).get('success') is True):
                    try:
                        db.log_success(cas_val, file_types.new_chemical_notice_pdf, result.get('pdf', {}).get('local_file_path'), result.get('pdf', {}).get('navigate_via'))
                    except Exception:
                        logger.exception("Failed to write success to DB for html post-loop")
                else:
                    # PDF explicitly failed during processing -> log failure
                    msg = result.get('pdf', {}).get('error') or "PDF processing failed or no links discovered"
                    try:
                        db.log_failure(cas_val, file_types.new_chemical_notice_pdf, msg)
                    except Exception:
                        logger.exception("Failed to write failure to DB for pdf post-loop")
    else:
        logger.debug("No downloads attempted.")

    return result


def scrape_modal_and_get_downloads(page, cas_dir, ncn_link, idx, need_html: bool, need_pdf: bool, result) -> Optional[Any]:
    """
    Clicks the given NCN anchor and waits robustly for the unique visible modal
    to appear and contain the expected content. Returns a Locator for the modal container
    or None on failure.
    """
    if ncn_link is None:
        logger.warning("No NCN link passed to click_anchor_link_and_wait_for_modal")
        return None

    # --- 1. Click the anchor to bring up the specfic report modal ---
    try:
        # Use Playwright auto-waiting click on the Locator
        ncn_link.click(timeout=30000)
        logger.debug("Clicked NCN link via locator.click()")
    except TimeoutError:
        logger.warning("Failed to click NCN anchor within 30s timeout.")
        return None
    except Exception as e:
        logger.warning("Failed to click NCN anchor: %s", e)
        return None

    # --- 2. Wait for the content we care about (the modal and the first zip download anchor) to become visible ---
    # python
    # Replace the existing \"Wait for the content we care about\" block in `click_ncn_anchor_link_and_wait_for_modal`
    zip_locator = None
    try:
        # Prefer a visible modal container (Bootstrap commonly adds 'show' or older 'in')
        visible_modal_locator = page.locator(
            'div.modal.show div.modal-body.action, div.modal.in div.modal-body.action').first
        # Wait briefly for it to become visible (raises on timeout)
        visible_modal_locator.wait_for(state="visible", timeout=5000)
        logger.debug("found visible modal-body.action via preferred selector")
        # Limit anchors to those with the exact visible text we care about to avoid hidden duplicates
        zip_locator = visible_modal_locator.locator('li a', has_text=" (Download zip)")
        logger.debug("Zip locator count: %d", zip_locator.count())
        zip_locator.first.wait_for(state="visible", timeout=20000)
    except TimeoutError:
        logger.warning("Timed out waiting for zip anchor to appear after clicking anchor.")
        return None
    except Exception as e:
        logger.warning("Error waiting for zip anchor to appear after clicking anchor: %s", e)
        return None

    notice_number = None
    try:
        notice_span = visible_modal_locator.locator('span#Notice_Number').first
        if notice_span.count() > 0:
            raw_notice = notice_span.inner_text().strip()
            # Sanitize for filename: keep alphanum, dash, underscore
            notice_number = re.sub(r'[^A-Za-z0-9\-_]', '_', raw_notice)
            logger.debug(f"Extracted and sanitized notice number: {notice_number}")
        else:
            logger.warning("Notice number span not found in modal")
    except Exception as e:
        logger.warning(f"Error extracting notice number: {e}")
    if notice_number is None:
        notice_number = f"item_{idx}"
        logger.debug(f"Falling back to default notice number: {notice_number}")


    # Extract the html from visible_modal_locator and save it to a file named
    # ncn_<notice_number>.html in the cas_dir
    try:
        modal_html = visible_modal_locator.evaluate("el => el.outerHTML")
        html_path = cas_dir / f"ncn_{notice_number}.html"
        with open(html_path, 'w', encoding='utf-8') as fh:
            fh.write(modal_html)
        logger.info(f"Saved modal HTML to {html_path}")
        result['html']['success'] = True
        result['html']['local_file_path'] = str(html_path)
        result['html']['navigate_via'] = page.url
    except Exception as e:
        logger.warning(f"Error saving modal HTML: {e}")

    # --- 3. Extract zip download links and add to download plan ---
    if need_pdf and zip_locator is not None:
        logger.debug("Finding ZIP download links in the modal")
        zip_link_list = zip_locator.evaluate_all("anchors => anchors.map(a => a.href)")
        logger.info("Found %d ZIP download links", len(zip_link_list))
        if (len(zip_link_list) > 0):
            # We want the zip files segregated into subfolders by notice number
            download_plan.add_links_to_plan(download_plan.DOWNLOAD_PLAN_ACCUM, cas_dir, f"ncn_{notice_number}_supporting_docs", zip_link_list)

    # Close the modal using a robust locator and auto-wait
    # Close button resides in a sibling div to modal-body, so navigate up to modal-content first
    outer_content_locator = visible_modal_locator.locator(
        'xpath=ancestor::div[contains(@class, "modal-content")]').first
    close_btn = outer_content_locator.locator("a.close[data-dismiss='modal']").first
    if close_btn is not None:
        logger.debug("Will try to close modal")
        close_btn.click()
        visible_modal_locator.wait_for(state="hidden", timeout=5000)
        logger.debug("Closed modal successfully")
    else:
        logger.warning("Close button not found in modal; skipping close")

    return True


def find_anchor_links_on_chemical_overview_modal(page):
    ncn_link_list = []
    # 1. Define the Locator for the specific anchors you want.
    # Playwright is smart enough to search only within the visible
    # modal if it's the only element matching this selector.
    anchors_locator = page.locator('div#chemical-detail-modal-body a[href]')
    anchors = []
    try:
        # 2. Explicitly wait for the *first* matching anchor to be visible.
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

    logger.info(f"Found {len(ncn_link_list)} links to NCN modals on page")

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
