"""
drive_section5_download.py

Driver module implementing the Section 5 specific navigation and scraping.

This module is invoked by `harvestSection5.py` via the shared
`harvest_framework.run_harvest` function. It contains the Playwright-driven
logic to open modals, scrape HTML, gather download links, and add entries to
a download plan which will be processed later by a separate script.
We use`HarvestDB` (via the db object passed from the framework) for
read/write of success/failure records.
"""


import atexit
import logging
import re
from pathlib import Path
from typing import Dict, Any, Optional
import download_plan
from urllib.parse import urlparse, parse_qs, parse_qsl, urlencode, urlunparse

logger = logging.getLogger(__name__)

# Do not initialize download_plan at import time (avoids hard-coding the folder
# name and circular imports). Initialize lazily on first driver invocation using
# the `cas_dir` value the framework passes in (derived from Config.archive_root).
_DOWNLOAD_PLAN_INITIALIZED = False
_DOWNLOAD_PLAN_DEFAULT_FOLDER = 'chemview_archive_Section5'


def fixup_back_zip_links(zip_link_list):
    """Normalize a list of back-end ZIP URLs in-place and return the new list.
    We've been seeing bad urls like:
    https://chemview.epa.gov/chemview/admin/proxy?filename=20200213%2FP-20-0015%2FP-20-0015_5.zip&mediaType=zip&mediaType=zip
    The double mediaType parameter appears to be harmless, but the 'admin' path segment
    is wrong. The downloads for these urls always fail with 404. If we take "/admin" out of the path,
    then the downloads succeed.

    Keeps the original query string intact so percent-encoded parts remain.
    """
    fixed = []
    for u in zip_link_list:
        try:
            parsed = urlparse(u.strip())
            path = parsed.path or ''
            # Remove any path segments equal to 'admin' (case-insensitive)
            parts = [p for p in path.split('/') if p and p.lower() != 'admin']
            new_path = ('/' if path.startswith('/') else '') + '/'.join(parts)
            # Collapse repeated slashes
            new_path = re.sub(r'/+', '/', new_path)
            rebuilt = urlunparse((parsed.scheme, parsed.netloc, new_path, parsed.params, parsed.query, parsed.fragment))
            if rebuilt != u:
                logger.debug("fixup_back_zip_links: fixed %s -> %s", u, rebuilt)
            fixed.append(rebuilt)
        except Exception:
            logger.exception("fixup_back_zip_links: error processing %s", u)
            fixed.append(u)
    return fixed


def drive_section5_download(url, cas_val, cas_dir: Path, debug_out=None, headless=True, browser=None, page=None, db=None, file_types: Any = None, retry_interval_hours: float = 12.0, archive_root=None) -> Dict[str, Any]:
    """ Walk the browser through the web pages and modals we need to capture
    and from which we will download supporting files.
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

    # Use db records to determine if we should try/retry this download now
    need_html = db.need_download(cas_val, file_types.section5_html, retry_interval_hours=retry_interval_hours)
    need_pdf = db.need_download(cas_val, file_types.section5_pdf, retry_interval_hours=retry_interval_hours)

    if not need_html and not need_pdf:
        logger.info("No downloads needed for cas=%s (section 5)", cas_val)
        return result

    # Ensure debug_out path exists and set a fallback default for CAS foldername
    if debug_out is None:
        debug_out = Path("debug_artifacts")
    debug_out = Path(debug_out)
    debug_out.mkdir(parents=True, exist_ok=True)
    if cas_dir is None:
        logger.error("cas_dir is required")
        return result

    # Lazy-initialize the download_plan using the configured 
	# archive root folder.
    global _DOWNLOAD_PLAN_INITIALIZED
    if not _DOWNLOAD_PLAN_INITIALIZED:
        try:
            folder_name = archive_root
        except Exception:
            folder_name = _DOWNLOAD_PLAN_DEFAULT_FOLDER
        download_plan.init(folder=folder_name, out_dir=Path('downloadsToDo'), batch_size=25)
        atexit.register(download_plan.flush)
        _DOWNLOAD_PLAN_INITIALIZED = True

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

    # 21 Nov 25: We have started collecting and saving a basic set of chemical
    # info in the db. At this point, we already have 2 of the three bits we
    # want: Cas # and chem_id. Pull them from the URL
    # 26 Nov 25: Doing this revealed that our failing URLs were missing the ch=cas_val
    # parameter, so we now repair the URL if needed and always return it.
    result, url = validate_url_and_get_chem_info_ids(url, cas_val, result)

    nav_ok = navigate_to_chemical_overview_modal(page, url, db)
    if nav_ok:
        section5_list = find_anchor_links_on_chemical_overview_modal(page)
        # Process Section5 links which should each have a modal and one pdf to harvest
        if section5_list and len(section5_list) > 0:
             for idx, section5_link in enumerate(section5_list, start=1):
                 # Scrape the modal and get the zip download links
                 scrape_success = scrape_modal_and_get_downloads(page, cas_dir, section5_link, idx, need_html, need_pdf, result)
                 if need_pdf and scrape_success:
                     # declare success here for "pdf" / zip downloads
                     result['pdf']['success'] = True
                     result['pdf']['local_file_path'] = str(cas_dir / "supporting_docs")
                     result['pdf']['navigate_via'] = url
                 # else: if we didn't find a list we are going to log this as a failure below
        else:
            msg = "No Section5 links found on initial page"
            logger.error(msg)
            result['html']['error'] = msg
            result['pdf']['error'] = msg

    # Record chemical info if we have enough data
    record_chemical_info(result, db)

    # If we attempted processing then record failures for any file
    # types that were explicitly set to False by the processing code.
    # Success will have been logged at the point of processing.
    # Note that "pdf" here is an umbrella term for all non-html downloads,
    # which could be pdfs, zips, or xmls.
    if result.get('attempted'):
        logger.debug(f"After modal scrape attempt, result = {result}")
        if need_html:
            if (result.get('html', {}).get('success') is True):
                try:
                    db.log_success(cas_val, file_types.section5_html, result.get('html', {}).get('local_file_path'), result.get('html', {}).get('navigate_via'))
                except Exception:
                    logger.exception("Failed to write success to DB for html post-loop")
            else:
                # HTML explicitly failed during processing -> log failure
                msg = result.get('html', {}).get('error') or "HTML processing failed"
                try:
                    db.log_failure(cas_val, file_types.section5_html, msg)
                except Exception:
                    logger.exception("Failed to write failure to DB for html post-loop")
            if need_pdf:
                if (result.get('pdf', {}).get('success') is True):
                    try:
                        db.log_success(cas_val, file_types.section5_pdf, result.get('pdf', {}).get('local_file_path'), result.get('pdf', {}).get('navigate_via'))
                    except Exception:
                        logger.exception("Failed to write success to DB for html post-loop")
                else:
                    # PDF explicitly failed during processing -> log failure
                    msg = result.get('pdf', {}).get('error') or "Download processing failed or no links discovered"
                    try:
                        db.log_failure(cas_val, file_types.section5_pdf, msg)
                    except Exception:
                        logger.exception("Failed to write failure to DB for pdf post-loop")
    else:
        logger.debug("No downloads attempted.")

    return result


def scrape_modal_and_get_downloads(page, cas_dir, section5_link, idx, need_html: bool, need_pdf: bool, result) -> Optional[Any]:
    """
    Clicks the given Section5 CO anchor and waits robustly for the unique visible modal
    to appear and contain the expected content. Returns a Locator for the modal container
    or None on failure.
    """
    if section5_link is None:
        logger.warning("No Section5 link passed to click_anchor_link_and_wait_for_modal")
        return None

    # --- 1. Click the anchor to bring up the specfic report modal ---
    logger.debug("In scrape_modal_and_get_downloads for section5 link %s:", section5_link)
    try:
        # Use Playwright auto-waiting click on the Locator
        section5_link.click(timeout=30000)
        logger.debug("Clicked Section5 link via locator.click()")
    except TimeoutError:
        logger.warning("Failed to click Section5 anchor within 30s timeout.")
        return None
    except Exception as e:
        logger.warning("Failed to click Section5 anchor: %s", e)
        return None

    # --- 2. Wait for the modal to become visible ---
    pdf_locator = None
    try:
        # Prefer a visible modal container (Bootstrap commonly adds 'show' or older 'in')
        visible_modal_locator = page.locator(
            'div.modal.show div.modal-body.action, div.modal.in div.modal-body.action').first
        # Wait briefly for it to become visible (raises on timeout)
        visible_modal_locator.wait_for(state="visible", timeout=5000)
        logger.debug("found visible modal-body.action via preferred selector")
    except TimeoutError:
        logger.warning("Timed out waiting for visible modal.")
        return None
    except Exception as e:
        logger.warning("Exception waiting for visible modal: %s", e)
        return None

    # Extract the html and other values from visible_modal_locator. and save it to a file named
    # Since we can have more than one consent order for a chemical, we create an
    # extra layer of subfolder based on the PMN number which AFAICS, each modal contains.
    try:
        modal_html = visible_modal_locator.evaluate("el => el.outerHTML")
        pmn_number = None
        # Extract PMN number from the modal HTML
        m = re.search(r'<span[^>]*\bid=["\']PMN_Number["\'][^>]*>(.*?)</span>', modal_html, re.IGNORECASE | re.DOTALL)
        if m:
            pmn_number = m.group(1).strip()
            logger.debug("Extracted PMN number from modal HTML: %s", pmn_number)
            # preserve in result for downstream use
            result.setdefault('chem_info', {})['pmn_number'] = pmn_number
        else:
            logger.error("PMN_Number span not found in modal HTML")
            # if we don't have a PMN number, use the item number instead
            pmn_number = f"item-{idx}"

        # Create/ensure a folder for this Section5 item
        section5_dir = cas_dir / pmn_number
        logger.debug("Section5 dir: %s", section5_dir)
        section5_dir.mkdir(parents=True, exist_ok=True)
        html_path = section5_dir / f"section5_summary.html"
        with open(html_path, 'w', encoding='utf-8') as fh:
            fh.write(modal_html)
        logger.info(f"Saved modal HTML to {html_path}")
        result['html']['success'] = True
        result['html']['local_file_path'] = str(html_path)
        result['html']['navigate_via'] = page.url
        # Make best-effort attempt to capture chemical's name
        nameSpan = visible_modal_locator.locator("li:has-text('Chemical Name') span span").first
        chem_name = ""
        if nameSpan.count() > 0:
            chem_name = nameSpan.evaluate("el => el.innerText") or ""
        chem_name = re.sub(r'\s+', ' ', chem_name).strip()
        if chem_name:
            logger.debug("Extracted chemical name from modal: %s", chem_name)
            result['chem_info']['chem_name'] = chem_name
        else:
            logger.warning("Chemical name element not found in modal")
    except Exception as e:
        logger.warning(f"Error saving modal HTML: {e}")

    # --- 3. Extract consent order pdf link and add to download plan ---
    if need_pdf and visible_modal_locator is not None:
        logger.debug("Looking for PDF consent order download link in the modal")
        # Limit anchors to those with the exact visible text we care about to avoid hidden duplicates
        pdf_locator = visible_modal_locator.locator(
            "div#snur_external_link a.show_external_link",
            has_text="View TSCA § 5 Order"
        )
        logger.debug("pdf locator count: %d", pdf_locator.count())
        # Not all of these modals have a consent order link; wait briefly to see if it appears
        if pdf_locator.count() > 0:
            pdf_locator.first.wait_for(state="visible", timeout=20000)

            # the download plan expects a list even though we know we will only have one link here
            pdf_link_list = pdf_locator.evaluate_all("anchors => anchors.map(a => a.href)")
            logger.info("Found %d PDF consent order download links", len(pdf_link_list))
            if (len(pdf_link_list) > 0):
                download_plan.add_links_to_plan(download_plan.DOWNLOAD_PLAN_ACCUM, "", section5_dir, pdf_link_list)
            else:
                logger.warning("No consent order link found for %s / %s", result['chem_info']['chem_id'], pmn_number)
        else:
            logger.warning("No pdf locator found for %s / %s", result['chem_info']['chem_id'], pmn_number)

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

def validate_url_and_get_chem_info_ids(url, cas_val, result):
    """Extract two values from the url, if we can:
    - chem_id: which we then sanity check against the cas_val
    - chem_db_id: extracted from modalId= in the URL

    If chem_id is not found, then the url is defective (we are seeing this for most/all
    chemicals with non-numeric cas_vals) and we need to repair it.

    It is somewhat speculative to conclude that the modalId= value is the internal
    chemview database id for the chemical, but this seems relatively likely given the fact
    that the same value appears in a script element at the top of at least some of our
    modal pages with contents like:
          /*
            <![CDATA[*/
      var chemicalDataId = 45102733;
      //]]>
    """
    chem_id = None
    chem_db_id = None
    result['chem_info'] = {
        'chem_id': None,
        'chem_db_id': None,
        'chem_name': None
    }
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        modal_vals = qs.get('modalId')
        if modal_vals:
            chem_db_id = modal_vals[0]
            logger.debug("Extracted modalId/chem_db_id %s from URL", chem_db_id)
        else:
            logger.warning("No modalId found in URL")
        cas_vals = qs.get('ch')
        if cas_vals:
            chem_id = cas_vals[0]
            logger.debug("Extracted chem_id %s from URL", chem_id)
            # Sanity check chem_id against cas_val
            if chem_id != cas_val:
                logger.warning("chem_id %s from URL does not match cas_val %s, will use passed-in cas_val", chem_id, cas_val)
                # if they don't match, use the primary value we trust: cas_val
                chem_id = cas_val
        else:
            logger.info("No chem_id found in URL, will insert cas_val in URL and use for chem_id")
            chem_id = cas_val
            # Repair the URL by adding ch=<cas_val>
            params = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k.lower() != 'ch']
            params.append(('ch', cas_val))
            url = urlunparse(parsed._replace(query=urlencode(params, doseq=True)))
    except Exception:
        logger.exception("Exception while extracting ids from URL: %s", url)

    result['chem_info']['chem_id'] = chem_id
    result['chem_info']['chem_db_id'] = chem_db_id

    return result, url


def find_anchor_links_on_chemical_overview_modal(page):
    section5_link_list = []
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
                if text.upper() == "CO":
                    logger.debug("Found Section5 court order link")
                    section5_link_list.append(anchor)
                elif text != "":
                    logger.warning("Found unexpected non-blank link")
                else:
                    logger.debug("Found blank anchor text; skipping")
            else:
                logger.debug("Anchor text is None; skipping")
        except Exception:
            logger.exception("Exception while processing anchor %d", i)
            continue

    logger.info(f"Found {len(section5_link_list)} links to Section5 modals on page")

    return section5_link_list


def navigate_to_chemical_overview_modal(page, url: str, db) -> bool:
    """
    Navigates to the URL and waits for the chemical overview modal content to be visible.

    Uses the Locator API for robust waiting on the modal element.
    """
    selector = "div#chemical-detail-modal-body"
    timeout_ms = 30000
    # 1. Navigate to the page with a single, generous timeout (90s default)
    nav_ok = False
    try:
        # Use page.goto and rely on Playwright's default internal retry mechanisms if needed
        # We target 'domcontentloaded' as it's the fastest signal that the basic page structure is ready.
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        logger.info(f"Navigation to URL successful: {url}")
    except TimeoutError as e:
        logger.error(f"Initial navigation to URL timed out ({timeout_ms}ms): {e}")
        # nav_ok remains False
    except Exception as e:
        logger.error(f"Initial navigation failed unexpectedly: {e}")
        # nav_ok remains False
    else:
        # 2. Use the Locator API to wait for the required modal content.
        # The locator will retry finding and checking the visibility of the element
        # until the timeout (default 30s, or you can pass a custom timeout here).
        try:
            modal_locator = page.locator(selector)
            modal_locator.wait_for(state="visible", timeout=15000)  # Use 15s wait for the modal
            logger.info("Modal content is present and visible.")
            nav_ok = True
        except TimeoutError as e:
            logger.error(f"Modal content selector '{selector}' not found or visible within timeout (15s).")
            # nav_ok remains False
        except Exception as e:
            logger.error(f"Error while waiting for modal visibility: {e}")
            # nav_ok remains False
    return nav_ok

def record_chemical_info(result, db):
    # Save chem info to DB if we have the three bits of info we need
    chem_info = result.get('chem_info', {})
    logger.debug("in record_chemical_info with chem_info: %s", chem_info)
    if chem_info and chem_info['chem_id'] and chem_info['chem_db_id'] and chem_info['chem_name']:
        try:
            ok = db.save_chemical_info(chem_info['chem_id'], chem_info['chem_db_id'], chem_info['chem_name'])
            if ok:
                logger.debug("Saved chemical info: %s", chem_info)
            else:
                logger.error("HarvestDB.save_chemical_info indicated mismatch or failure for %s", chem_info)
        except Exception:
            logger.exception("Exception calling HarvestDB.save_chemical_info for %s", chem_info)
    else:
        logger.error("Insufficient data to record chemical info: %s", chem_info)
