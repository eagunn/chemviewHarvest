import requests
import html as html_lib
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import logging
import time
from typing import Dict, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
                db.log_failure(cas_val, file_types.substantial_risk_html, '')
            except Exception:
                logger.exception("Failed to write failure to DB for html")
        if need_pdf:
            try:
                db.log_failure(cas_val, file_types.substantial_risk_pdf, '')
            except Exception:
                logger.exception("Failed to write failure to DB for pdf")
        return result

    if page is None:
        logger.error("No page provided for URL: %s", url)
        return result

    nav_ok = navigate_to_initial_page(page, url)
    if nav_ok is False:
        msg = "Navigation to initial page failed"
        logger.error(msg)
        result['html']['error'] = msg
        result['pdf']['error'] = msg
        # don't waste any more time on this entry
        return result

    # Wait for SR anchors inside the initial modal to have non-empty text
    # (anchors may be added to the DOM quickly but populated asynchronously).
    try:
        wait_for_sr_anchors(page, modal_selector="#chemical-detail-modal-body", timeout_ms=8000, poll_interval_s=0.25)
    except Exception:
        logger.debug("wait_for_sr_anchors raised an exception or timed out; proceeding to find anchors anyway")

    sr_link_list = find_submission_links_on_first_modal(page)

    ##################################################################
    # Disabled: This code seems to work ok when there ARE summary links,
    # but fails with timeouts and ghost html files when there are none.
    # Since "none" seems to be the more common case and the summary html
    # simply pulls together info available in the other htmls files, I'm
    # disabling this section for now to improve overall reliability.
    # # Also look for summary/endpoint links (e.g. 'Acute toxicity') on the first modal
    # try:
    #     summary_links = find_summary_links_on_first_modal(page)
    #     if summary_links:
    #         logger.info("Found %d summary/endpoint links on initial modal", len(summary_links))
    #         for sidx, s_link in enumerate(summary_links, start=1):
    #             try:
    #                 text = (s_link.inner_text() or "").strip()
    #                 fname = sanitize_filename(text or f"summary_{sidx}") + ".html"
    #                 scrape_summary_modal_and_save(page, s_link, cas_dir, fname)
    #             except Exception as e:
    #                 # Non-fatal: warn and include debug-level stack trace
    #                 logger.warning("Failed to scrape/save summary modal for link index %d: %s", sidx, e)
    #                 logger.debug("Exception details for summary scrape failure", exc_info=True)
    #     else:
    #         # Many initial modals legitimately do not have summary links; debug-level is appropriate
    #         logger.debug("No summary/endpoint links found on initial modal")
    # except Exception:
    #     # Non-fatal: record a warning and keep the detailed stack at debug
    #     import sys
    #     e = sys.exc_info()[1]
    #     logger.warning("Error while searching for or processing summary links on initial modal: %s", e)
    #     logger.debug("Full exception while processing summary links", exc_info=True)

    if not sr_link_list:
        msg = "No Substantial Risk / 8e links found on initial page"
        logger.error(msg)
        result['html']['error'] = msg
        result['pdf']['error'] = msg
        # don't waste any more time on this entry
        return result

    # Iterate with an explicit 1-based index so we can number modal HTML files when there are multiple links
    for idx, sr_link in enumerate(sr_link_list, start=1):
        page = click_anchor_link_and_wait_for_modal(page, sr_link)

        pdf_link_list = None
        if need_html or need_pdf:
            # pass sr_link so the scraper can find the modal specifically opened by this link
            pdf_link_list = scrape_modal_html_and_gather_pdf_links(page, need_html, need_pdf, cas_dir, cas_val, db, file_types, url, result, item_no=idx, sr_link=sr_link)

        if pdf_link_list:
            download_pdfs(pdf_link_list, cas_dir)
            result['pdf']['success'] = True
            result['pdf']['local_file_path'] = str(cas_dir / "substantialRiskReports")
            result['pdf']['navigate_via'] = url
            try:
                db.log_success(cas_val, file_types.substantial_risk_pdf, result['pdf']['local_file_path'],
                               result['pdf']['navigate_via'])
            except Exception:
                logger.exception("Failed to write success to DB for pdf")
    return result

def scrape_modal_html_and_gather_pdf_links(
    page, need_html: bool, need_pdf: bool, cas_dir: Path, cas_val, db, file_types: Any, url: str, result: Dict[str, Any], item_no: int = 1, sr_link=None
) -> Any:
    logger.info(f"Waiting for Substantial Risk Reports modal {item_no} to appear...")
    try:
        # Try to derive a reliable selector for the modal opened by this sr_link
        modal_selector = None
        modal_id = None
        try:
            if sr_link is not None:
                # link may have data-target like '#modal_xxx' or a selector
                target = (sr_link.get_attribute('data-target') or '')
                if not target:
                    # sometimes target is stored in 'data-target' or 'href'
                    target = (sr_link.get_attribute('href') or '')
                if target:
                    # normalize '#id' or '/path#id' -> extract id
                    if target.startswith('#'):
                        modal_id = target.lstrip('#')
                    else:
                        # if href contains a hash, take the fragment
                        if '#' in target:
                            modal_id = target.split('#')[-1]
                    if modal_id:
                        # use attribute selector to be safe with special chars in id
                        modal_selector = f'div[id="{modal_id}"] .modal-body.action'
        except Exception:
            modal_selector = None
            modal_id = None

        # Wait for either the specific modal (if we have a selector) or for any modal to appear
        try:
            if modal_selector:
                page.wait_for_selector(modal_selector, timeout=10000)
            else:
                page.wait_for_selector("div.modal-body.action", timeout=10000)
        except Exception:
            # if waiting for specific modal failed, fall back to any modal
            try:
                page.wait_for_selector("div.modal-body.action", timeout=5000)
            except Exception:
                logger.debug("No modal appeared within wait time")

        # Prefer waiting specifically for PDF anchors inside the targeted modal so we capture populated content
        try:
            if modal_selector:
                page.wait_for_selector(f"{modal_selector} li a.show_external_link", timeout=7000)
            else:
                page.wait_for_selector("div.modal-body.action li a.show_external_link", timeout=7000)
        except Exception:
            logger.debug("PDF anchors didn't appear within timeout; proceeding to capture modal HTML anyway")

        # Now try to query the modal element we want
        found_modal = None
        if modal_selector:
            try:
                found_modal = page.query_selector(modal_selector)
            except Exception:
                found_modal = None

        if found_modal is None:
            # Try to find the visible/topmost modal (by visibility and/or z-index). If none visible, fall back to last modal.
            modals = page.query_selector_all("div.modal-body.action")
            candidate = None
            max_z = -999999
            for m in modals:
                try:
                    visible = m.evaluate("el => { const s = window.getComputedStyle(el); return !!(s && s.display !== 'none' && s.visibility !== 'hidden' && el.offsetHeight > 0); }")
                except Exception:
                    visible = False
                try:
                    z = m.evaluate("el => { const s = window.getComputedStyle(el); const z = parseInt(s.zIndex)||0; return isNaN(z)?0:z; }")
                except Exception:
                    z = 0
                if visible and (candidate is None or z > max_z):
                    candidate = m
                    max_z = z
            if candidate is not None:
                found_modal = candidate
            else:
                if modals:
                    found_modal = modals[-1]

        if found_modal:
            # Diagnostic info: id (if any) and number of PDF anchors
            modal_ident = None
            try:
                parent_div = found_modal.evaluate("el => el.closest('[id]') ? el.closest('[id]').id : null")
                modal_ident = parent_div
            except Exception:
                modal_ident = modal_id
            try:
                anchors = found_modal.query_selector_all("li a.show_external_link")
                anchor_count = len(anchors) if anchors is not None else 0
            except Exception:
                anchor_count = 0
            logger.info("Capturing modal (text=%s) with %d pdf anchors", modal_ident, anchor_count)

            # Extract identifier from modal_ident (e.g., 8EHQ-00-14711)
            extracted_id = "unknown"
            if modal_ident and "[" in modal_ident and "]" in modal_ident:
                extracted_id = modal_ident.split("[")[1].split("]")[0]
                logger.debug("Extracted identifier from modal id: %s", extracted_id)

            # Capture inner HTML after we've given the modal a chance to populate
            modal_html = found_modal.inner_html()
            pdf_link_list = []
            if need_html:
                logger.info("Will attempt to save modal HTML")
                modal_html_wrapped = f"<div class='modal-body action'>\n{modal_html}\n</div>"
                html_path = cas_dir / f"sr_{extracted_id}.html"
                try:
                    with open(html_path, 'w', encoding='utf-8') as fh:
                        fh.write(modal_html_wrapped)
                    logger.info("Saved modal HTML with identifier %s to %s", extracted_id, html_path)
                    result['html']['success'] = True
                    result['html']['local_file_path'] = str(html_path)
                    result['html']['navigate_via'] = url
                    try:
                        db.log_success(cas_val, file_types.substantial_risk_html, str(html_path), result['html']['navigate_via'])
                    except Exception:
                        logger.exception("Failed to write success to DB for html")
                except Exception as e:
                    logger.exception("Failed to save modal HTML: %s", e)
                    result['html']['error'] = str(e)

            if need_pdf:
                logger.info("Will attempt to find all PDF download links in the modal")
                try:
                    pdf_anchors = found_modal.query_selector_all("li a.show_external_link")
                    for anchor in pdf_anchors:
                        href = anchor.get_attribute("href")
                        if href:
                            if href.startswith("proxy"):
                                href = f"https://chemview.epa.gov/chemview/{href}"
                            pdf_link_list.append(href)
                    logger.info("Found %d PDF download links", len(pdf_link_list))
                except Exception as e:
                    logger.exception("Error while finding PDF download links: %s", e)
            return pdf_link_list
        else:
            logger.error("No modal-body action div found on the page.")
    except Exception as e:
        logger.exception("Error while waiting for or processing the modal: %s", e)
    return []


def click_anchor_link_and_wait_for_modal(page, sr_link: Any | None):
    # For summary links, only use element_handle.click() to avoid double modal opening
    if sr_link:
        try:
            sr_link.click(timeout=30000)
            logger.debug("Clicked anchor via element_handle.click() (summary link mode)")
        except Exception as e:
            logger.warning("Failed to click anchor via element_handle.click(): %s", e)
    else:
        logger.warning("No SR/8e link found on page")
    try:
        page.wait_for_timeout(2000)
    except Exception:
        logger.debug("Exception when trying to wait for page to load after click")
        pass
    return page


def find_submission_links_on_first_modal(page):
    try:
        anchors = page.query_selector_all('div#chemical-detail-modal-body a[href]')
        logger.debug("Found %d href anchors on page", len(anchors))
    except Exception:
        anchors = []

    sr_link_list = []
    for a in anchors:
        try:
            text = a.inner_text().strip()  # visible text (use text_content() for raw)
            logger.debug("anchor text: %s", text)
            if text.startswith("* TSCA \u00A7 8(e) Submission"):
                sr_link_list.append(a)
                logger.info("Found SR/8e link")
                logger.debug(a.inner_html)
        except Exception:
            continue
    return sr_link_list


def navigate_to_initial_page(page, url):
    nav_ok = False
    nav_timeouts = [30000, 60000, 90000]
    selector = "div#chemical-detail-modal-body"
    for attempt, to in enumerate(nav_timeouts, start=1):
        try:
            # Try to navigate to the page (DOM content loaded)
            page.goto(url, wait_until="domcontentloaded", timeout=to)
        except Exception as e:
            logger.warning("Navigation attempt %d failed (timeout=%dms): %s", attempt, to, e)
            try:
                # Short wait before next attempt to avoid beating the server
                page.wait_for_timeout(500)
            except Exception:
                pass
            # try the next timeout value
            continue

        # After a successful goto, wait for the modal to appear. Only when this wait
        # succeeds do we consider navigation truly successful for our purposes.
        try:
            page.wait_for_selector(selector, timeout=10000)
            nav_ok = True
            logger.info("Initial navigation and modal presence succeeded on attempt %d", attempt)
            break
        except Exception as e:
            logger.warning("Modal selector '%s' not found after navigation attempt %d: %s", selector, attempt, e)
            try:
                page.wait_for_timeout(500)
            except Exception:
                pass
            # continue to next attempt
            continue

    if not nav_ok:
        logger.error("Navigation ultimately failed for URL, processing should stop")

    return nav_ok

def wait_for_sr_anchors(page, modal_selector: str = '#chemical-detail-modal-body', timeout_ms: int = 7000, poll_interval_s: float = 0.25):
    """
    Wait until at least one anchor inside `modal_selector li a.show_external_link`
    has non-empty text. Returns the list of anchors (may be empty on timeout).
    """
    sel = f"{modal_selector} li a.show_external_link"
    js = (
        "(sel) => {"
        " const anchors = Array.from(document.querySelectorAll(sel));"
        " return anchors.some(a => a.textContent && a.textContent.trim().length > 0);"
        "}"
    )
    try:
        page.wait_for_function(js, sel, timeout=timeout_ms)
    except Exception:
        # fallback: poll until timeout
        end = time.time() + (timeout_ms / 1000.0)
        while time.time() < end:
            anchors = page.query_selector_all(sel)
            for a in anchors:
                try:
                    if a.inner_text() and a.inner_text().strip():
                        return anchors
                except Exception:
                    continue
            try:
                time.sleep(poll_interval_s)
            except Exception:
                break
        return page.query_selector_all(sel)

    # primary path: the function returned successfully; return matching anchors
    return page.query_selector_all(sel)

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
                    logger.debug("Skipping download, file already exists: %s", pdf_path)
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

def find_summary_links_on_first_modal(page):
    """Find links like 'viewAllEndpoint-...' inside the first chemical detail modal.
    Returns a list of element handles (may be empty).
    """
    try:
        # Prefer anchors with ids that start with viewAllEndpoint-
        anchors = page.query_selector_all('div#chemical-detail-modal-body a[id^="viewAllEndpoint-"]')
        if anchors:
            logger.debug("Found %d summary anchors by id prefix", len(anchors))
            return anchors
    except Exception:
        pass

    # Fallback: search for anchors with a visible text that looks like an endpoint name
    try:
        anchors = page.query_selector_all('div#chemical-detail-modal-body a[href="#"], div#chemical-detail-modal-body a[onclick]')
        summary_anchors = []
        for a in anchors:
            try:
                text = (a.inner_text() or "").strip()
                # crude heuristic: endpoint links often contain a space and a number in parentheses
                if text and '(' in text and text.split('(')[0].strip():
                    summary_anchors.append(a)
            except Exception:
                continue
        logger.debug("Found %d summary anchors by heuristic", len(summary_anchors))
        return summary_anchors
    except Exception:
        return []

def sanitize_filename(text: str) -> str:
    """Sanitize anchor text into a filesystem-safe, short filename (lowercase, underscores)."""
    import re

    name = text.strip().lower()
    # Remove parentheses contents first
    name = re.sub(r"\([^)]*\)", "", name)
    # replace non-alphanumeric with underscores
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = name.strip("_")
    if not name:
        name = "summary"
    return name[:100]


def scrape_summary_modal_and_save(page, anchor, cas_dir: Path, filename: str):
    """Click the given anchor to open the summary overlay/modal, wait for #viewAllEndpointBody to be visible and populated,
    capture the modal innerHTML, and save to cas_dir/filename.
    Only use element_handle.click() for summary links to avoid double modal opening.
    """
    import datetime
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

