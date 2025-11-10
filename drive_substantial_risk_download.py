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
PDF_PLAN_ACCUM: Dict[str, Any] = {'folder': 'chemview_archive_8e', 'subfolderList': [], 'downloadList': []}
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


def drive_substantial_risk_download(url, cas_val, cas_dir: Path, debug_out=None, headless=True, browser=None, page=None, db=None, file_types: Any = None, retry_interval_hours: float = 12.0) -> Dict[str, Any]:
    """Use DB to decide whether to attempt, then returns random outcomes and logs them to DB.

    `retry_interval_hours` controls how long to wait after a recorded failure before retrying (default 12 hours).
    Returns a dict result, see structure below
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

    need_html = _need_download_from_db(db, cas_val, file_types.substantial_risk_html, retry_interval_hours=retry_interval_hours)
    need_pdf = _need_download_from_db(db, cas_val, file_types.substantial_risk_pdf, retry_interval_hours=retry_interval_hours)

    if not need_html and not need_pdf:
        logger.info("No downloads needed for cas=%s (substantial risk)", cas_val)
        return result

    # Ensure debug_out and cas_dir exist
    if debug_out is None:
        debug_out = Path("debug_artifacts")
    debug_out = Path(debug_out)
    debug_out.mkdir(parents=True, exist_ok=True)
    if cas_dir is None:
        cas_dir = Path(".")
    cas_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Start of processing for URL: %s", url)

    if page is None:
        logger.error("No page provided for URL: %s", url)
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

    # Use positive-test style: only proceed when nav_ok is True; otherwise record an error
    if nav_ok:
        sr_link_list = find_submission_links_on_chemical_overview_modal(page)
        if sr_link_list and len(sr_link_list) > 0:
            # Iterate with an explicit 1-based index so we can number modal HTML files when there are multiple links
            any_modal_processed = False
            for idx, sr_link in enumerate(sr_link_list, start=1):
                # Click the SR anchor and get back a locator for the modal that opened (or None on failure)
                modal_locator = click_anchor_link_and_wait_for_modal(page, sr_link)
                if modal_locator is None:
                    logger.warning("Skipping SR link %d for cas %s because modal was not observed", idx, cas_val)
                    continue

                pdf_link_list = None
                if need_html or need_pdf:
                    # pass the modal locator (required) so the scraper uses the already-observed modal
                    try:
                        pdf_link_list = scrape_modal_html_and_gather_pdf_links(page, modal_locator, need_html, need_pdf, cas_dir, cas_val, db, file_types, url, result, item_no=idx)
                    except Exception as e:
                        logger.exception("Exception raised while scraping modal %d: %s", idx, e)
                        # record processing failures
                        result['pdf']['error'] = f"Exception while scraping modal {idx}: {e}"

                if pdf_link_list:
                    # Add discovered PDF links to the global accumulator (will be flushed to disk in batches)
                    # For now we are simply trusting this to work and assuming success at this point.
                    _accumulate_pdf_links_for_cas(cas_dir, pdf_link_list)
                    result['pdf']['success'] = True
                    result['pdf']['local_file_path'] = str(cas_dir / "substantialRiskReports")
                    result['pdf']['navigate_via'] = url
                # else: if we didn't find a list we are going to log this as a failure below
        else:
            msg = "No Substantial Risk / 8e links found on initial page"
            logger.error(msg)
            result['html']['error'] = msg
            result['pdf']['error'] = msg
            # do not return; caller will handle post-loop logging
    else:
        msg = "Navigation to chemical overview modal failed"
        logger.error(msg)
        result['html']['error'] = msg
        result['pdf']['error'] = msg
        # do not return; allow post-loop logic to record failures

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

    return result

def scrape_modal_html_and_gather_pdf_links(
    page, modal_locator, need_html: bool, need_pdf: bool, cas_dir: Path, cas_val, db, file_types: Any, url: str, result: Dict[str, Any], item_no: int = 1
) -> Any:
    logger.info(f"Processing Substantial Risk Reports modal {item_no}...")
    pdf_link_list = None
    try:
        # The modal locator is required and should reference the modal body (or container) that is open.
        modal = modal_locator

        # Extract identifier for logging/debugging
        modal_ident_raw = modal.get_attribute("id") or ""
        # Try to pull an identifier inside square brackets (e.g., '[8EHQ-07-16936]')
        m = re.search(r"\[([^]]+)]", modal_ident_raw)
        if m:
            modal_ident = m.group(1)
        else:
            # fallback to the raw id or use the item number
            modal_ident = modal_ident_raw or f"item_{item_no}"

        # Sanitize identifier for use as a filename: keep letters, digits, hyphen, underscore
        modal_ident_safe = re.sub(r"[^A-Za-z0-9\-_]", "_", modal_ident)
        logger.info("Processing modal with id: %s (sanitized: %s)", modal_ident_raw, modal_ident_safe)

        # Capture the modal-body.action div (outer HTML) if present; otherwise fall back to modal.inner_html()
        modal_body_html = None
        try:
            body_locator = modal.locator("div.modal-body.action").first
            # Ensure it exists and grab outerHTML
            if body_locator and body_locator.count() > 0:
                logger.debug("Found expected body locator inside modal")
                try:
                    modal_body_html = body_locator.evaluate("el => el.outerHTML")
                except Exception:
                    # fallback to inner_html wrapped
                    inner = body_locator.inner_html()
                    modal_body_html = f"<div class='modal-body action'>\n{inner}\n</div>"
        except Exception:
            modal_body_html = None
            result['html']['error'] = "Failed to locate modal body div"

        if modal_body_html is None:
            # final fallback: capture the modal's inner HTML and wrap it
            logger.warning("Did not find expected body locator; falling back to modal inner_html()")
            try:
                modal_html = modal.inner_html()
                modal_body_html = f"<div class='modal-body action'>\n{modal_html}\n</div>"
            except Exception:
                modal_body_html = ""

        if modal_body_html is not None and modal_body_html != "":
            if need_html:
                logger.info("Saving modal HTML")
                html_path = cas_dir / f"sr_{modal_ident_safe}.html"
                with open(html_path, 'w', encoding='utf-8') as fh:
                    fh.write(modal_body_html)
                logger.info("Saved modal HTML to %s", html_path)
                result['html']['success'] = True
                result['html']['local_file_path'] = str(html_path)
                result['html']['navigate_via'] = url

            pdf_link_list = []
            if need_pdf:
                logger.info("Finding PDF download links in the modal")
                pdf_anchors = modal.locator("li a.show_external_link")
                pdf_link_list = pdf_anchors.evaluate_all("anchors => anchors.map(a => a.href)")
                logger.info("Found %d PDF download links", len(pdf_link_list))
                # result success will be declared / filled-in by the caller after values are written to json file

            # Close the modal using a robust locator and auto-wait
            close_btn = modal.locator("a.close[data-dismiss='modal']")
            if close_btn is not None:
                logger.debug("Will try to close modal")
                close_btn.click()
                modal.wait_for(state="hidden", timeout=5000)
                logger.debug("Closed modal successfully")
            else:
                logger.warning("Close button not found in modal; skipping close")

        return pdf_link_list

    except Exception as e:
        logger.exception("Error while processing the modal: %s", e)
        result['html']['error'] = f"Exception while processing modal: {e}"
        result['pdf']['error'] = f"Exception while processing modal: {e}"

    return pdf_link_list


def click_anchor_link_and_wait_for_modal(page, sr_link):
    """
    Clicks the given SR anchor and waits robustly for the unique visible modal
    to appear and contain the expected content. Returns the Locator for the modal container.
    """
    if sr_link is None:
        logger.warning("No SR/8e link passed to click_anchor_link_and_wait_for_modal")
        return None

    # --- 1. Click the Anchor ---
    try:
        sr_link.click(timeout=30000)
        logger.debug("Clicked SR anchor via locator.click()")
    except TimeoutError:
        logger.warning("Failed to click SR anchor within 30s timeout.")
        return None
    except Exception as e:
        logger.warning("Failed to click SR anchor due to unexpected error: %s", e)
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

def find_submission_links_on_chemical_overview_modal(page):
    sr_link_list = []
    summary_link_list = []
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
            logger.debug("anchor text: %s", text)
            # The following was missing some anchors. Trying to be a bit less precise
            #if text.startswith("* TSCA \u00A7 8(e) Submission"):
            if text.startswith("* TSCA \u00A7 8(e) "):
                logger.debug("Adding link to SR/8e list")
                sr_link_list.append(anchor)
            elif text != "":
                logger.debug("Adding link to summary list")
                summary_link_list.append(anchor)
            else:
                logger.debug("Skipping empty-text anchor")
        except Exception:
            logger.warning("Exception while processing anchor %d", i, exc_info=True)
            continue
    logger.info(f"Found {len(sr_link_list)} SR/8e links on page")
    logger.info(f"Found {len(summary_link_list)} summary links on page")

    return sr_link_list


def navigate_to_chemical_overview_modal(page, url: str) -> bool:
    """
    Navigates to the URL and waits for the chemical overview modal content to be visible.

    Uses a single, maximum timeout for reliability, and the Locator API
    for robust waiting on the modal element.
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

def generate_local_pdf_path(pdf_url: str, reports_dir: Path) -> Path:
    """Generate the local file path for a given PDF URL."""
    pdf_url_unescaped = html_lib.unescape(pdf_url or "")
    parsed = urlparse(pdf_url_unescaped)
    filename = ""
    try:
        qs = parse_qs(parsed.query)
        if "filename" in qs and qs["filename"]:
            filename = qs["filename"][0]
    except Exception:
        filename = ""

    if not filename:
        filename = Path(parsed.path).name if parsed.path else ""
    filename = filename.replace("/", "_").strip()
    if not filename:
        filename = "unknown-substantialRisk.pdf"
    if not filename.lower().endswith(".pdf"):
        filename = filename + ".pdf"

    return reports_dir / filename


def download_pdfs(pdf_links: list[str], cas_dir: Path, session: Optional[requests.Session] = None) -> None:
    """Download PDFs reusing an HTTPS session/pool. If `session` is None, create and close one here."""
    # Ensure the substantialRiskReports folder exists
    reports_dir = cas_dir / "substantialRiskReports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    created_session = False
    s = session
    if s is None:
        created_session = True
        s = requests.Session()
        # Configure session with connection pooling and retries
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=Retry(total=2, backoff_factor=0.5))
        s.mount("https://", adapter)
        s.headers.update({"User-Agent": "substantialRiskDownloader/1.0", "Connection": "keep-alive"})

    try:
        for pdf_url in pdf_links:
            try:
                pdf_path = generate_local_pdf_path(pdf_url, reports_dir)

                # Check if the file already exists
                if pdf_path.exists():
                    #logger.debug("Skipping download, file already exists: %s", pdf_path)
                    continue

                pdf_url_unescaped = html_lib.unescape(pdf_url or "")
                # Normalize proxy-relative URLs
                if pdf_url_unescaped.startswith("proxy"):
                    pdf_url_full = f"https://chemview.epa.gov/chemview/{pdf_url_unescaped}"
                elif pdf_url_unescaped.startswith("/"):
                    pdf_url_full = f"https://chemview.epa.gov{pdf_url_unescaped}"
                else:
                    pdf_url_full = pdf_url_unescaped

                logger.info("Downloading PDF from: %s -> %s", pdf_url_full, pdf_path)
                with s.get(pdf_url_full, timeout=30, stream=True) as resp:
                    if resp.status_code == 200 and resp.headers.get("content-type", "").startswith("application/pdf"):
                        with open(pdf_path, "wb") as pf:
                            for chunk in resp.iter_content(chunk_size=8192):
                                if chunk:
                                    pf.write(chunk)
                        logger.info("Saved PDF to %s", pdf_path)
                    else:
                        logger.warning(
                            "Failed to download PDF from %s: status=%s, content-type=%s",
                            pdf_url_full,
                            resp.status_code,
                            resp.headers.get("content-type", ""),
                        )
            except Exception as e:
                logger.exception("Error downloading PDF from %s: %s", pdf_url, e)
    finally:
        if created_session and s is not None:
            try:
                s.close()
            except Exception:
                pass

def scrape_summary_modal_and_save(page, anchor, cas_dir: Path, filename: str):
    """Click the given anchor to open the summary overlay/modal, wait for #viewAllEndpointBody to be visible and populated,
    capture the modal innerHTML, and save to cas_dir/filename.
    Only use element_handle.click() for summary links to avoid double modal opening.
    """
    # (no extra imports needed here)
    try:
        # Click the anchor using only element_handle.click() to avoid double modal opening
        try:
            anchor.click()
            logger.debug("Clicked summary anchor via element_handle.click()")
        except Exception as e:
            logger.warning("Failed to click summary anchor via element_handle.click(): %s", e)
            return False

        # Wait for #viewAllEndpointBody to be visible and populated
        try:
            page.wait_for_selector('#viewAllEndpointBody', timeout=8000, state='visible')
            # Optionally, wait for a table or heading inside the modal to appear
            page.wait_for_function(
                "() => { const el = document.getElementById('viewAllEndpointBody'); return el && el.offsetParent !== null && el.innerText.trim().length > 0; }",
                timeout=4000
            )
            logger.debug("Summary modal #viewAllEndpointBody is visible and populated")
        except Exception:
            logger.debug("Summary modal #viewAllEndpointBody did not appear or populate within timeout; attempting to capture anyway")

        # Capture the modal HTML
        try:
            modal = page.query_selector('#viewAllEndpointBody')
            if not modal:
                logger.error("No #viewAllEndpointBody found to capture for filename %s", filename)
                return False
            modal_html = modal.inner_html()
        except Exception as e:
            logger.error("Failed to get inner_html of #viewAllEndpointBody: %s", e)
            return False

        # Ensure reports dir exists
        cas_dir.mkdir(parents=True, exist_ok=True)
        out_path = cas_dir / filename
        try:
            with open(out_path, 'w', encoding='utf-8') as fh:
                fh.write(f"<div id='viewAllEndpointBody'>\n{modal_html}\n</div>")
            logger.info("Saved summary modal HTML to %s", out_path)
        except Exception as e:
            logger.error("Failed to write summary HTML to %s: %s", out_path, e)
            return False

        # Attempt to close the summary modal (click .close button if present, or hide modal)
        try:
            closed = page.evaluate("() => { const el = document.getElementById('viewAllEndpointBody'); if(!el) return false; const modal = el.closest('.modal'); if(!modal) return false; const btn = modal.querySelector('.close'); if(btn){ btn.click(); return true;} modal.style.display='none'; return true; }")
            logger.debug("Attempted to close summary modal (result=%s)", closed)
        except Exception:
            logger.exception("Failed to close summary modal after saving HTML")

        return True
    except Exception as e:
        logger.exception("Error scraping summary modal: %s", e)
        return False

def _flush_pdf_plan_accum(force: bool = False):
    """Write the accumulated PDF plan to a timestamped JSON file in `pdfDownloadsToDo` and reset the accumulator.
    If `force` is False, will only write if there is at least one CAS entry accumulated.
    """
    global PDF_PLAN_ACCUM, PDF_PLAN_ACCUM_CAS_SET, PDF_PLAN_ACCUM_CAS_SINCE_WRITE
    if not PDF_PLAN_ACCUM.get('subfolderList'):
        return None
    # Ensure output folder exists
    PDF_PLAN_OUT_DIR.mkdir(parents=True, exist_ok=True)
    # Use save_download_plan to write the file
    try:
        path = save_download_plan(PDF_PLAN_ACCUM, PDF_PLAN_OUT_DIR)
        logger.info("Flushed accumulated PDF plan to %s (cas_count=%d)", path, len(PDF_PLAN_ACCUM_CAS_SET))
    except Exception:
        logger.exception("Failed to flush accumulated PDF plan")
        return None
    # Reset accumulator
    PDF_PLAN_ACCUM = {'folder': 'chemview_archive_8e', 'subfolderList': [], 'downloadList': []}
    PDF_PLAN_ACCUM_CAS_SET.clear()
    PDF_PLAN_ACCUM_CAS_SINCE_WRITE = 0
    return path


def _accumulate_pdf_links_for_cas(cas_dir: Path, pdf_links: list[str]):
    """Add pdf_links to the module-level accumulator and flush to disk every PDF_PLAN_WRITE_BATCH_SIZE unique CAS entries."""
    global PDF_PLAN_ACCUM, PDF_PLAN_ACCUM_CAS_SET, PDF_PLAN_ACCUM_CAS_SINCE_WRITE
    if not pdf_links:
        return
    cas_folder_name = cas_dir.name
    # Determine if this is a new CAS entry for the current accumulator
    is_new_cas = cas_folder_name not in PDF_PLAN_ACCUM_CAS_SET
    # Add links into the accumulator
    add_pdf_links_to_plan(PDF_PLAN_ACCUM, cas_dir, pdf_links)
    if is_new_cas:
        PDF_PLAN_ACCUM_CAS_SET.add(cas_folder_name)
        PDF_PLAN_ACCUM_CAS_SINCE_WRITE += 1
    # Flush if we've reached the batch size
    if PDF_PLAN_ACCUM_CAS_SINCE_WRITE >= PDF_PLAN_WRITE_BATCH_SIZE:
        _flush_pdf_plan_accum()


# Register an atexit handler so remaining accumulated plans are flushed when the process exits normally
atexit.register(lambda: _flush_pdf_plan_accum(force=True))
