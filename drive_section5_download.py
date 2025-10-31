import re
import requests
import html as html_lib
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import logging
from typing import Dict, Any
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


def drive_section5_download(url: str, cas_val: str, cas_dir: Path, debug_out=None, headless=True, browser=None, page=None, db=None, db_path: str = None, file_types: Any = None) -> Dict[str, Any]:
    """Section 5 driver: decides what to download, performs navigation and downloads, and records results to DB.

    If `db` is None, a HarvestDB instance will be created using `db_path`.

    Returns a dict with keys:
      - 'attempted': bool (True if any download was attempted)
      - 'html': {success, local_file_path, error, navigate_via}
      - 'pdf': {success, local_file_path, error, navigate_via}

    This function will call db.log_success / db.log_failure as appropriate.
    """
    # ensure a DB instance is available
    if db is None:
        if not db_path:
            msg = "Driver requires either db or db_path to be provided"
            logger.error(msg)
            return {'attempted': False, 'html': {'success': False, 'local_file_path': None, 'error': msg, 'navigate_via': ''}, 'pdf': {'success': False, 'local_file_path': None, 'error': msg, 'navigate_via': ''}}
        try:
            db = HarvestDB(db_path)
        except Exception as e:
            msg = f"Failed to open DB at {db_path}: {e}"
            logger.exception(msg)
            return {'attempted': False, 'html': {'success': False, 'local_file_path': None, 'error': msg, 'navigate_via': ''}, 'pdf': {'success': False, 'local_file_path': None, 'error': msg, 'navigate_via': ''}}

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

    # Decide whether to attempt downloads based on DB state
    need_html = _need_download_from_db(db, cas_val, file_types.section5_html)
    need_pdf = _need_download_from_db(db, cas_val, file_types.section5_pdf)

    if not need_html and not need_pdf:
        logger.info("No downloads needed for cas=%s (section5)", cas_val)
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

    created_playwright = None
    created_browser = False
    created_page = False

    try:
        if page is None:
            if browser is None:
                created_playwright = sync_playwright().start()
                browser = created_playwright.chromium.launch(headless=headless)
                created_browser = True
                logger.debug("Launched new browser (headless=%s)", headless)
            page = browser.new_page()
            created_page = True
            logger.debug("Created new page for browser reuse path")
        else:
            logger.debug("Reusing provided page")

        try:
            page.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})
        except Exception:
            pass

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
                    pass

        if not nav_ok:
            logger.warning("Navigation ultimately failed for URL, continuing to save whatever we have")
        else:
            logger.info("Initial navigation succeeded")

        try:
            anchors = page.query_selector_all("a[href]")
            logger.debug("Found %d href anchors on page", len(anchors))
        except Exception:
            anchors = []

        co_link = None
        fallback_co_link = None
        for a in anchors:
            try:
                title = (a.get_attribute('title') or '').strip()
                to_examine = title.lower()
                if to_examine == 'co chemical details':
                    co_link = a
                    logger.info("Found CO link with title %s", to_examine)
                    logger.debug(a.inner_html)
                    break
                elif to_examine == 'chemical details':
                    fallback_co_link = a
                    logger.info("Found possible fallback CO link with title %s", to_examine)
                    logger.debug(a.inner_html)
            except Exception:
                continue
        if co_link is None and fallback_co_link is not None:
            co_link = fallback_co_link
            logger.info("Going to use fallback CO link")

        # if we found the "CO" link, then click it to open the summary modal
        # we always need to open the modal, even if we don't need to save
        # its HTML, because we need to get the PDF link from it
        if co_link:
            try:
                onclick = (co_link.get_attribute('onclick') or '') or ''
                logger.debug("found onclick attribute in link")
            except Exception:
                onclick = ''
            try:
                if 'childModalClick' in onclick or 'modalClick' in onclick:
                    logger.debug("Going to try modal clicks via evaluate")
                    try:
                        page.evaluate(
                            "(el)=>{ try{ if(typeof childModalClick === 'function'){ childModalClick(new MouseEvent('click',{bubbles:true,cancelable:true}), el); return true; } if(typeof modalClick === 'function'){ modalClick(new MouseEvent('click',{bubbles:true,cancelable:true}), el); return true; } el.dispatchEvent(new MouseEvent('click',{bubbles:true,cancelable:true})); return true;}catch(e){ try{ el.click(); }catch(e){} return false;} }",
                            co_link,
                        )
                        logger.debug("Clicked CO element via childModalClick/modalClick evaluate")
                    except Exception:
                        logger.debug("Going to try to dispatch a mouse event click (inner)")
                        try:
                            page.evaluate("(el) => { try{ el.dispatchEvent(new MouseEvent('click', { bubbles:true, cancelable:true })); }catch(e){ try{ el.click(); }catch(e){} } }", co_link)
                        except Exception:
                            pass
                        logger.debug("Clicked CO element via MouseEvent dispatch fallback")
                else:
                    logger.debug("Going to try to dispatch a mouse event click (outer)")
                    try:
                        page.evaluate("(el) => { try{ el.dispatchEvent(new MouseEvent('click', { bubbles:true, cancelable:true })); }catch(e){ try{ el.click(); }catch(e){} } }", co_link)
                    except Exception:
                        pass
                    logger.debug("Clicked CO element via MouseEvent dispatch (no modalClick)")
                try:
                    page.wait_for_timeout(2000)
                except Exception:
                    pass
            except Exception as e:
                logger.error("Failed to click global CO element: %s", e)

            logger.info("Waiting for TSCA SECTION 5 ORDER modal to appear...")
            try:
                page.wait_for_selector("div.modal-body.action h3", timeout=10000)
                modal_divs = page.query_selector_all("div.modal-body.action")
                found_modal = None
                logger.debug("Found %d modal-body action divs", len(modal_divs))
                for div in modal_divs:
                    try:
                        h3 = div.query_selector("h3")
                        text = h3.inner_text().strip() if h3 else ''
                        logger.debug("Modal h3 text: %s", text)
                        if text == "TSCA SECTION 5 ORDER":
                            found_modal = div
                            break
                    except Exception:
                        continue

                if found_modal:
                    modal_html = found_modal.inner_html()
                    # ensure pdf_url is always defined for later checks
                    pdf_url = None
                    if need_html:
                        logger.info("Will attempt to save modal HTML")
                        modal_html_wrapped = f"<div class='modal-body action'>\n{modal_html}\n</div>"
                        html_path = cas_dir / "section5summary.html"
                        try:
                            with open(html_path, 'w', encoding='utf-8') as fh:
                                fh.write(modal_html_wrapped)
                            logger.info("Saved modal HTML to %s", html_path)
                            result['html']['success'] = True
                            result['html']['local_file_path'] = str(html_path)
                            # we get to the html modal via the main URL
                            result['html']['navigate_via'] = url
                            try:
                                db.log_success(cas_val, file_types.section5_html, str(html_path), result['html']['navigate_via'])
                            except Exception:
                                logger.exception("Failed to write success to DB for html")
                        except Exception as e:
                            msg = f"Failed to save modal HTML: {e}"
                            logger.exception(msg)
                            result['html']['error'] = msg
                            try:
                                db.log_failure(cas_val, file_types.section5_html, '')
                            except Exception:
                                logger.exception("Failed to write failure to DB for html")
                    if need_pdf:
                        logger.info("Will attempt to find and download PDF from modal")
                        match = re.search(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*title=["\']View TSCA § 5 Order["\']', modal_html, re.IGNORECASE)
                        if match:
                            pdf_url = match.group(1)
                    if pdf_url:
                        try:
                            pdf_url_unescaped = html_lib.unescape(pdf_url)
                            if pdf_url_unescaped.startswith('/'):
                                pdf_url_full = f"https://chemview.epa.gov{pdf_url_unescaped}"
                            else:
                                pdf_url_full = pdf_url_unescaped
                            parsed = urlparse(pdf_url_unescaped)
                            filename = ''
                            try:
                                qs = parse_qs(parsed.query)
                                if 'filename' in qs and qs['filename']:
                                    filename = qs['filename'][0]
                            except Exception:
                                filename = ''
                            if not filename:
                                filename = Path(parsed.path).name if parsed.path else ''
                            filename = filename.replace('/', '_').strip()
                            if not filename:
                                filename = f"{cas_val}-section5.pdf"
                            if not filename.lower().endswith('.pdf'):
                                filename = filename + '.pdf'
                            logger.info("Downloading PDF from: %s -> saving as: %s (cas_dir=%s)", pdf_url_full, filename, cas_dir)
                            resp = requests.get(pdf_url_full, timeout=30)
                            if resp.status_code == 200 and resp.headers.get('content-type','').startswith('application/pdf'):
                                pdf_path = cas_dir / filename
                                with open(pdf_path, 'wb') as pf:
                                    pf.write(resp.content)
                                logger.info("Saved PDF to %s", pdf_path)
                                result['pdf']['success'] = True
                                result['pdf']['local_file_path'] = str(pdf_path)
                                # log the direct path to the PDF
                                result['pdf']['navigate_via'] = pdf_url_full
                                try:
                                    db.log_success(cas_val, file_types.section5_pdf, str(pdf_path), result['pdf']['navigate_via'])
                                except Exception:
                                    logger.exception("Failed to write success to DB for pdf")
                            else:
                                msg = f"PDF download returned status/ctype: {resp.status_code} {resp.headers.get('content-type')}"
                                logger.warning(msg)
                                result['pdf']['error'] = msg
                                try:
                                    db.log_failure(cas_val, file_types.section5_pdf, '')
                                except Exception:
                                    logger.exception("Failed to write failure to DB for pdf")
                        except Exception as e:
                            msg = f"Error downloading PDF: {e}"
                            logger.exception(msg)
                            result['pdf']['error'] = msg
                            try:
                                db.log_failure(cas_val, file_types.section5_pdf, '')
                            except Exception:
                                logger.exception("Failed to write failure to DB for pdf")
                else:
                    msg = "Expected Section 5 modal not found"
                    logger.warning(msg)
                    if need_html:
                        result['html']['error'] = msg
                        try:
                            db.log_failure(cas_val, file_types.section5_html, '')
                        except Exception:
                            logger.exception("Failed to write failure to DB for html")
                    if need_pdf:
                        result['pdf']['error'] = msg
                        try:
                            db.log_failure(cas_val, file_types.section5_pdf, '')
                        except Exception:
                            logger.exception("Failed to write failure to DB for pdf")
            except Exception as e:
                msg = f"Error waiting for or capturing modal: {e}"
                logger.exception(msg)
                result['html']['error'] = msg
                try:
                    if need_html:
                        db.log_failure(cas_val, file_types.section5_html, '')
                except Exception:
                    logger.exception("Failed to write failure to DB for html")
        else:
            msg = "No appropriate CO link found on page"
            logger.warning(msg)
            if need_html:
                result['html']['error'] = msg
                try:
                    db.log_failure(cas_val, file_types.section5_html, '')
                except Exception:
                    logger.exception("Failed to write failure to DB for html")

        try:
            page_html = page.content()
            (debug_out / f"{cas_val}.html").write_text(page_html, encoding='utf-8')
            logger.debug("Saved full page HTML to %s", debug_out / f"{cas_val}.html")
        except Exception as e:
            logger.exception("Failed to save full page HTML: %s", e)

        try:
            page.screenshot(path=str(debug_out / f"{cas_val}.png"), full_page=True)
        except Exception:
            pass

        logger.info("drive_section5_download finished for %s", url)

    finally:
        # only close resources we created
        try:
            if created_page and page is not None:
                try:
                    page.close()
                except Exception:
                    pass
            if created_browser and browser is not None:
                try:
                    browser.close()
                except Exception:
                    pass
            if created_playwright is not None:
                try:
                    created_playwright.stop()
                except Exception:
                    pass
        except Exception:
            pass

    return result
