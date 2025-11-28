"""
drive_premanufacture_notice_download.py

Driver module implementing the Premanufacture Notice specific navigation and scraping.

This module is invoked by `harvestPremanufactureNotice.py` via the shared
`harvest_framework.run_harvest` function. It contains the Playwright-driven
logic to open modals, scrape HTML, gather download links, and add entries to
a download plan which will be processed later by a separate script.
We use `HarvestDB` (via the db object passed from the framework) for
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
_DOWNLOAD_PLAN_DEFAULT_FOLDER = 'chemview_archive_pmn'


def drive_premanufacture_notice_download(url, cas_val, cas_dir: Path, debug_out=None, headless=True, browser=None, page=None, db=None, file_types: Any = None, retry_interval_hours: float = 12.0, archive_root=None) -> Dict[str, Any]:
    """ Walk the browser through the web pages and modals we need to capture
    and from which we will download supporting files.
    """
    logger.debug("In drive_premanufacture_notice_download with url=%s, cas_val=%s, cas_dir=%s, archive_root=%s", url, cas_val, cas_dir, archive_root)
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
    need_html = db.need_download(cas_val, file_types.premanufacture_notice_html, retry_interval_hours=retry_interval_hours)
    need_pdf = db.need_download(cas_val, file_types.premanufacture_notice_pdf, retry_interval_hours=retry_interval_hours)

    if not need_html and not need_pdf:
        logger.info("No downloads needed for cas=%s (premanufacture notice)", cas_val)
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
        pmn_list = find_anchor_links_on_chemical_overview_modal(page)
         # Process PMN links which should each have a modal and some zips to harvest
        if pmn_list and len(pmn_list) > 0:
             for idx, pmn_link in enumerate(pmn_list, start=1):
                 # Scrape the modal and get the zip download links
                 scrape_success = scrape_modal_and_get_downloads(page, cas_dir, pmn_link, idx, need_html, need_pdf, result)
                 if need_pdf and scrape_success:
                     # declare success here for "pdf" / zip downloads
                     result['pdf']['success'] = True
                     result['pdf']['local_file_path'] = str(cas_dir / "*_supporting_docs")
                     result['pdf']['navigate_via'] = url
                 # else: if we didn't find a list we are going to log this as a failure below
        else:
            msg = "No Premanufacture Notice links found on initial page"
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
                    db.log_success(cas_val, file_types.premanufacture_notice_html, result.get('html', {}).get('local_file_path'), result.get('html', {}).get('navigate_via'))
                except Exception:
                    logger.exception("Failed to write success to DB for html post-loop")
            else:
                # HTML explicitly failed during processing -> log failure
                msg = result.get('html', {}).get('error') or "HTML processing failed"
                try:
                    db.log_failure(cas_val, file_types.premanufacture_notice_html, msg)
                except Exception:
                    logger.exception("Failed to write failure to DB for html post-loop")
            if need_pdf:
                if (result.get('pdf', {}).get('success') is True):
                    try:
                        db.log_success(cas_val, file_types.premanufacture_notice_pdf, result.get('pdf', {}).get('local_file_path'), result.get('pdf', {}).get('navigate_via'))
                    except Exception:
                        logger.exception("Failed to write success to DB for html post-loop")
                else:
                    # PDF explicitly failed during processing -> log failure
                    msg = result.get('pdf', {}).get('error') or "Download processing failed or no links discovered"
                    try:
                        db.log_failure(cas_val, file_types.premanufacture_notice_pdf, msg)
                    except Exception:
                        logger.exception("Failed to write failure to DB for pdf post-loop")
    else:
        logger.debug("No downloads attempted.")

    return result


def scrape_modal_and_get_downloads(page, cas_dir, pmn_link, idx, need_html: bool, need_pdf: bool, result) -> Optional[Any]:
    """
    Clicks the given PMN anchor and waits robustly for the unique visible modal
    to appear and contain the expected content. Returns a Locator for the modal container
    or None on failure.
    """
    if pmn_link is None:
        logger.warning("No PMN link passed to click_anchor_link_and_wait_for_modal")
        return None

    # --- 1. Click the anchor to bring up the specific report modal ---
    try:
        # Use Playwright auto-waiting click on the Locator
        pmn_link.click(timeout=30000)
        logger.debug("Clicked PMN link via locator.click()")
    except TimeoutError:
        logger.warning("Failed to click PMN anchor within 30s timeout.")
        return None
    except Exception as e:
        logger.warning("Failed to click PMN anchor: %s", e)
        return None

    # --- 2. Wait for the content we care about (the modal and the first zip download anchor) to become visible ---
    # python
    # Replace the existing "Wait for the content we care about" block in `click_ncn_anchor_link_and_wait_for_modal`
    zip_locator = None
    try:
        # Prefer a visible modal container (Bootstrap commonly adds 'show' or older 'in')
        visible_modal_locator = page.locator(
            'div.modal.show div.modal-body.action, div.modal.in div.modal-body.action').first
        # Wait briefly for it to become visible (raises on timeout)
        visible_modal_locator.wait_for(state="visible", timeout=5000)
        logger.debug("found visible modal-body.action via preferred selector")
    except TimeoutError:
        logger.warning("Timed out waiting for zip anchor to appear after clicking anchor.")
        return None
    except Exception as e:
        logger.warning("Error waiting for zip anchor to appear after clicking anchor: %s", e)
        return None

    pmn_number = None
    raw_pmn = None
    try:
        pmn_span = visible_modal_locator.locator('span#PMN_Number').first
        if pmn_span.count() > 0:
            raw_pmn = pmn_span.inner_text().strip()
            logger.debug(f"Using raw pmn number: {raw_pmn}")
        else:
            logger.warning("pmn number span not found in modal")
            # will attempt to get number from anchor tag instead
            anchor = visible_modal_locator.locator(
                "div.snur_meta:has(span#PMN_Number_label) a.show_external_link").first
            if anchor.count() > 0:
                raw_pmn = anchor.inner_text().strip()
                logger.debug(f"Using raw anchor pmn number: {raw_pmn}")
            else:
                logger.warning("pmn number anchor not found in modal, will fall back to item number")
    except Exception as e:
        logger.warning(f"Error extracting pmn number: {e}")

    if raw_pmn is not None:
        # Sanitize for filename: keep alphanum, dash, underscore
        pmn_number = re.sub(r'[^A-Za-z0-9\-_]', '_', raw_pmn)
        logger.debug(f"Extracted and sanitized pmn number: {pmn_number}")
    else:
        pmn_number = f"item_{idx}"
        logger.debug(f"Falling back to default using item number for pmn number: {pmn_number}")

    # Extract the html from visible_modal_locator and save it to a file named
    # pmn_<pmn_number>.html in notice folder.
    notice_dir = None
    try:
        modal_html = visible_modal_locator.evaluate("el => el.outerHTML")
        # create a folder for this notice number inside cas_dir
        notice_dir = cas_dir / pmn_number
        notice_dir.mkdir(parents=True, exist_ok=True)
        html_path = notice_dir / f"pmn_{pmn_number}.html"
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

    # Locate support document links and add to download plan
    if need_pdf :
        logger.debug("looking for supporting doc links")
        # find all the href anchors in the modal
        anchor_list = visible_modal_locator.locator('a[href]').all()
        # We only want the ones that can be converted into pdf downloads
        logger.info("Found %d file download links", len(anchor_list))
        supporting_doc_list = []
        for i, anchor in enumerate(anchor_list):
            try:
                text = anchor.inner_text().strip()
                href = anchor.get_attribute('href')
                logger.debug("examining anchor text: %s, href: %s", text, href)
                if text.startswith("https://www.regulations.gov/document?"):
                    logger.debug("Found supporting doc link")
                    supporting_doc_list.append(anchor)
                # else we ignore the other links
            except Exception:
                logger.exception("Exception while processing anchor %d", i)
                continue
        if (len(supporting_doc_list) > 0):
            # We have to transform the found links, which are to a page on regulations.gov,
            # and look like:
            # https://www.regulations.gov/document?D=EPA-HQ-OPPT-2017-0366-0179
            # into direct download links for the pdf files, which will look like:
            # https://downloads.regulations.gov/EPA-HQ-OPPT-2017-0366-0179/content.pdf
            pdf_link_list = []
            for doc_anchor in supporting_doc_list:
                try:
                    href = doc_anchor.get_attribute('href')
                    parsed = urlparse(href)
                    qs = parse_qs(parsed.query)
                    doc_ids = qs.get('D')
                    if doc_ids and len(doc_ids) > 0:
                        doc_id = doc_ids[0]
                        pdf_link = f"https://downloads.regulations.gov/{doc_id}/content.pdf"
                        pdf_link_list.append(pdf_link)
                except Exception as e:
                    logger.warning(f"Error parsing supporting doc link: {e}")
                    # and we skip this doc
            # Store the doc files in a subfolder of the notice folder
            pmn_subfolder = f"{cas_dir}/{pmn_number}/supporting_docs"
            download_plan.add_links_to_plan(download_plan.DOWNLOAD_PLAN_ACCUM, "", pmn_subfolder, pdf_link_list)
        else:
            logger.warning("No supporting doc links found")

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
        'chem_name': None  # to be filled later
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
    pmn_link_list = []
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
                # Identify PMN links by text prefix
                if text.startswith("PMN Determination"):
                    logger.debug("Found pmn link")
                    pmn_link_list.append(anchor)
                elif text != "":
                    logger.warning("Found unexpected non-blank link")
                else:
                    logger.debug("Found blank anchor text; skipping")
            else:
                logger.debug("Anchor text is None; skipping")
        except Exception:
            logger.exception("Exception while processing anchor %d", i)
            continue

    logger.info(f"Found {len(pmn_link_list)} links to PMN modals on page")

    return pmn_link_list


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
