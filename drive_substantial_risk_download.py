"""drive_substantial_risk_download.py

Driver to navigate EPA ChemView chemical overview modals and extract Substantial Risk (8e) reports
and summary modals. Navigates to a chemical detail page using a Playwright `page`, locates Substantial
Risk and summary anchors, scrapes modal HTML, discovers PDF links, queues PDF download tasks via
`download_plan`, writes HTML files to `cas_dir`, and logs successes/failures to the provided `db`.

Designed for lazy initialization of `download_plan` to avoid import-time folder hard-coding and
circular imports. Expected to be invoked by the framework with `page`, `db`, `file_types`, and `cas_dir`.
"""

import requests
import html as html_lib
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import logging
from typing import Dict, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import atexit
import re
import download_plan

logger = logging.getLogger(__name__)

# Do not initialize download_plan at import time (avoids hard-coding the folder
# name and circular imports). Initialize lazily on first driver invocation using
# the `cas_dir` value the framework passes in (derived from Config.archive_root).
_DOWNLOAD_PLAN_INITIALIZED = False
_DOWNLOAD_PLAN_DEFAULT_FOLDER = 'chemview_archive_substantial_risk'

def drive_substantial_risk_download(url, cas_val, cas_dir: Path, debug_out=None, headless=True, browser=None, page=None, db=None, file_types: Any = None, retry_interval_hours: float = 12.0, archive_root=None) -> Dict[str, Any]:
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

    # TODO: for 8HQ-<value> ids that end in a letter, shortcircuit here
    # and skip processing. Cathy and I agree that all the 8HQ-<value>X
    # entries have the same modal and download content as the plain
    # 8HQ-<value> entry.

    need_html = db.need_download(cas_val, file_types.substantial_risk_html, retry_interval_hours=retry_interval_hours)
    need_pdf = db.need_download(cas_val, file_types.substantial_risk_pdf, retry_interval_hours=retry_interval_hours)

    if not need_html and not need_pdf:
        logger.info("No downloads needed for cas=%s (substantial risk)", cas_val)
        return result

    # Ensure debug_out and cas_dir exist
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

    nav_ok = navigate_to_chemical_overview_modal(page, url)
    #input("you should see a chemical overview page. then hit enter.")
    # Use positive-test style: only proceed when nav_ok is True; otherwise record an error
    if nav_ok:
        sr_link_list, summary_link_list = find_anchor_links_on_chemical_overview_modal(page)
        #input("check log for number of links harvested, then hit enter")
        # Process Substantial Risk / 8e links which should each have PDFs to harvest
        if sr_link_list and len(sr_link_list) > 0:
            # ensure a default reports folder is available in case the scraper does not create a per-modal folder
            subst_risk_dir = cas_dir / "substantialRiskReports"
            for idx, sr_link in enumerate(sr_link_list, start=1):
                # Click the SR anchor and get back a locator for the modal that opened (or None on failure)
                modal_locator = click_sr_anchor_link_and_wait_for_modal(page, sr_link)
                if modal_locator is None:
                    logger.warning("Skipping SR link %d for cas %s because modal was not observed", idx, cas_val)
                    continue

                pdf_link_list = []
                try:
                    if need_html or need_pdf:
                        # pass the modal locator (required) so the scraper uses the already-observed modal
                        pdf_link_list, subst_risk_dir = scrape_sr_modal_html_and_gather_pdf_links(
                            page, modal_locator, need_html, need_pdf, cas_dir, cas_val, db, file_types, url, result, item_no=idx
                        )
                except Exception as e:
                    logger.exception("Exception raised while scraping modal %d: %s", idx, e)
                    # record processing failures
                    result['pdf']['error'] = f"Exception while scraping modal {idx}: {e}"
                    # ensure subst_risk_dir is defined so later logic that references it won't fail
                    subst_risk_dir = cas_dir / "substantialRiskReports"

                if pdf_link_list:
                    # Add discovered PDF links to the global accumulator (will be flushed to disk in batches)
                    download_plan.add_links_to_plan(download_plan.DOWNLOAD_PLAN_ACCUM, "", subst_risk_dir, pdf_link_list)
                    result['pdf']['success'] = True
                    result['pdf']['local_file_path'] = str(subst_risk_dir)
                    result['pdf']['navigate_via'] = url
                    #input("links added to plan, hit enter to continue")
                # else: if we didn't find a list we are going to log this as a failure below
        else:
            msg = "No Substantial Risk / 8e links found on initial page"
            logger.error(msg)
            result['html']['error'] = msg
            result['pdf']['error'] = msg
            # continue to try summary links anyway
        # Process summary links (these open summary/table overlays; no PDFs expected)
        if summary_link_list and len(summary_link_list) > 0:
            for sidx, summary_anchor in enumerate(summary_link_list, start=1):
                modal_locator = click_summary_anchor_link_and_wait_for_modal(page, summary_anchor)
                if modal_locator is None:
                    logger.warning("Skipping summary link %d for cas %s because modal was not observed", sidx, cas_val)
                    continue
                try:
                    success = scrape_summary_modal_from_locator(modal_locator, cas_dir, cas_val, item_no=sidx)
                    if success:
                        logger.info("Saved summary modal %d for cas %s", sidx, cas_val)
                    else:
                        logger.warning("Failed to save summary modal %d for cas %s", sidx, cas_val)
                        # TEMPORARY?
                        # if a scrape fails, bag the rest of the sequence
                        break
                except Exception:
                    logger.exception("Exception while processing summary modal %d for cas %s", sidx, cas_val)
        else:
            logger.debug("No summary links found on initial page (or none visible)")
            # Note we're intentionally not recording this as an error.
            # Not all overview modals have summary links.
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

def scrape_sr_modal_html_and_gather_pdf_links(
    page, modal_locator, need_html: bool, need_pdf: bool, cas_dir: Path, cas_val, db, file_types: Any, url: str, result: Dict[str, Any], item_no: int = 1
) -> Any:
    logger.info(f"Processing Substantial Risk Reports modal {item_no}...")
    #input("About to scrape SR modal. Press enter to continue")
    # default reports directory (used if per-modal folder is not created)
    subst_risk_dir = cas_dir / "substantialRiskReports"
    pdf_link_list = []
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
                # Create/ensure a folder for this Section5 item
                subst_risk_dir = cas_dir / modal_ident_safe
                logger.debug("Substantial risk dir: %s", subst_risk_dir)
                subst_risk_dir.mkdir(parents=True, exist_ok=True)
                html_path = subst_risk_dir / f"sr_{modal_ident_safe}.html"
                with open(html_path, 'w', encoding='utf-8') as fh:
                    fh.write(modal_body_html)
                logger.info("Saved modal HTML to %s", html_path)
                result['html']['success'] = True
                result['html']['local_file_path'] = str(html_path)
                result['html']['navigate_via'] = url

            pdf_link_list = []
            if need_pdf:
                logger.debug("Finding PDF download links in the modal")
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

    except Exception as e:
        logger.exception("Error while processing the modal: %s", e)
        result['html']['error'] = f"Exception while processing modal: {e}"
        result['pdf']['error'] = f"Exception while processing modal: {e}"

    # Always return the found PDF links and the path the caller should use when recording files for this CAS
    #input("have subst risk dir and pdf link list, check log, then hit enter")
    return pdf_link_list, subst_risk_dir


def click_sr_anchor_link_and_wait_for_modal(page, sr_link):
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

def click_summary_anchor_link_and_wait_for_modal(page, anchor, timeout: int = 10000):
    """
    Click the provided summary anchor and wait for the summary modal to appear.

    Waits specifically for the element with id `viewAllEndpointBody` (based on sampleHtml.txt).
    Returns the Locator for that element on success, or None on timeout/error.
    """
    if anchor is None:
        logger.warning("No summary anchor provided to click_summary_anchor_link_and_wait_for_modal")
        return None

    # Try clicking via element handle first to avoid accidental double-open behavior
    try:
        try:
            el_handle = anchor.element_handle()
        except Exception:
            el_handle = None
        if el_handle is not None:
            try:
                el_handle.click()
            except Exception:
                # fallback to locator click
                anchor.click()
        else:
            anchor.click()
    except Exception as e:
        logger.warning("Failed to click summary anchor: %s", e)
        return None

    # Wait for the summary modal body to appear
    try:
        summary_locator = page.locator('#viewAllEndpointBody')
        summary_locator.wait_for(state='visible', timeout=timeout)
        logger.info("Summary modal observed and ready")
        return summary_locator
    except Exception:
        logger.debug("Summary modal not observed after clicking anchor", exc_info=True)
        return None

    # done

def find_anchor_links_on_chemical_overview_modal(page):
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
            if text is not None:
                logger.debug("anchor text: %s", text)
                # Identify SR/8e links by text prefix
                if text.startswith("* TSCA \u00A7 8(e) "):
                    logger.debug("Found SR/8e link")
                    sr_link_list.append(anchor)
                elif text != "":
                    logger.debug("Found summary link")
                    summary_link_list.append(anchor)
                else:
                    logger.debug("Found blank anchor text; skipping")
            else:
                logger.debug("Anchor text is None; skipping")
        except Exception:
            logger.exception("Exception while processing anchor %d", i)
            continue

    logger.info(f"Found {len(sr_link_list)} SR/8e links and {len(summary_link_list)} summary links on page")

    return sr_link_list, summary_link_list


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



def scrape_summary_modal_from_locator(modal_locator, cas_dir: Path, cas_val, item_no: int = 1) -> bool:
    """Capture the summary modal HTML using an already-open modal locator and save it to a file.

    Use the hazard category (h5[data-bind*='endpointName']) when available to build the filename;
    otherwise fall back to a sanitized modal id. Returns True on success, False otherwise.
    Note: this function assumes the caller ensured `cas_dir` exists.
    """
    logger.info(f"Processing Summary Risks modal {item_no} for CAS {cas_val}...")
    try:
        modal = modal_locator

        # Expect to name the file after the hazard category (h5[data-bind*='endpointName']) when available
        category_text = ''
        try:
            hb = modal.locator("h5[data-bind*='endpointName']").first
            if hb and hb.count() > 0:
                try:
                    category_text = hb.inner_text()
                except Exception:
                    try:
                        category_text = hb.evaluate("el => (el.innerText || '').trim()") or ''
                    except Exception:
                        category_text = ''
        except Exception:
            category_text = ''

        if category_text:
            category_text = category_text.strip().lower()
        if category_text:
            category_safe = re.sub(r"[^A-Za-z0-9\-_]", "_", category_text)
            filename_base = f"{category_safe}"
        else:
            filename_base = f"hazard_summary_{item_no}"

        # Capture the full modal container (including header with Close button) if possible
        modal_outer_html = ''
        try:
            modal_outer_html = modal.evaluate(
                "el => { const c = el.closest('.modal-content') || el.closest('.modal') || el; return (c && c.outerHTML) || (el && el.outerHTML) || ''; }"
            ) or ''
        except Exception:
            try:
                modal_outer_html = modal.evaluate('el => el.outerHTML') or ''
            except Exception:
                try:
                    modal_outer_html = modal.inner_html() or ''
                except Exception:
                    modal_outer_html = ''

        if not modal_outer_html:
            logger.warning("No HTML content captured for summary modal %s", item_no)
            return False

        # Write the captured HTML to disk (caller ensures cas_dir exists)
        out_path = cas_dir / f"{filename_base}.html"
        logger.debug("Output path for modal will be: %s", out_path)
        try:
            with open(out_path, 'w', encoding='utf-8') as fh:
                fh.write(modal_outer_html)
            logger.info("Saved summary modal HTML to %s", out_path)
        except Exception as e:
            logger.exception("Failed to write summary modal HTML to %s: %s", out_path, e)
            return False

        # Close the modal using a robust locator and auto-wait
        try:
            # Traverse up from #viewAllEndpointBody to .modal-content
            modal_container = modal.locator("..").locator("..")
            close_btn = modal_container.locator("a.close[data-dismiss='modal']")
            if close_btn and close_btn.count() > 0:
                logger.debug("Will try to close summary modal via Playwright locator (from modal_container)")
                close_btn.first.click()
                modal_container.wait_for(state="hidden", timeout=5000)
                logger.debug("Closed summary modal successfully")
            else:
                logger.error(f"Close button not found in summary modal {item_no}; cannot close modal.")
                return False  # hard failure: do not continue
        except Exception as e:
            logger.exception("Failed to close summary modal %s after saving HTML", item_no)
            # TEMPORARY? If we can't close a modal, we may be hosed for the duration.
            # In any case, for testing, bag it here.
            return False # hard failure: do not continue
        return True
    except Exception:
        logger.exception("Unexpected error capturing summary modal for CAS %s", cas_val)
        return False
