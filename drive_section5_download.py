import re
import requests
import html as html_lib
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

def drive_section5_download(url, cas_dir, need_html_download, need_pdf_download, debug_out=None, headless=True, LOG_FILE=None):
    """Navigate to the given URL with Playwright and capture the CO modal HTML and PDF.
    Returns a dict for each filetype: { 'html': {...}, 'pdf': {...} }
    Each dict contains: success (bool), path (Path or None), error (str or None)
    """
    # result structure: per-filetype dict with success(bool), local_file_path(str|None), error(str|None), navigate_via(str)
    result: Dict[str, Dict[str, Any]] = {
        'html': {'success': False, 'local_file_path': None, 'error': None, 'navigate_via': ''},
        'pdf': {'success': False, 'local_file_path': None, 'error': None, 'navigate_via': ''}
    }

    if url is None:
        msg = "Error: url is required but was not provided to drive_section5_download()."
        logger.error(msg)
        result['html']['error'] = msg
        result['pdf']['error'] = msg
        return result
    if cas_dir is None:
        msg = "Error: cas_dir is required but was not provided to drive_section5_download()."
        logger.error(msg)
        result['html']['error'] = msg
        result['pdf']['error'] = msg
        return result

    logger.info("Start of processing for URL: %s", url)

    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        msg = f"Playwright not available; cannot navigate to URL: {e}"
        logger.error(msg)
        result['html']['error'] = msg
        result['pdf']['error'] = msg
        return result

    if debug_out is None:
        debug_out = Path("debug_artifacts")
        debug_out.mkdir(parents=True, exist_ok=True)

    prefix = cas_dir.name

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})

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
                    if need_html_download:
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
                        except Exception as e:
                            msg = f"Failed to save modal HTML: {e}"
                            logger.exception(msg)
                            result['html']['error'] = msg
                    if need_pdf_download:
                        logger.info("Will attempt to find and download PDF from modal")
                        pdf_url = None
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
                                filename = f"{prefix}-section5.pdf"
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
                                # we get to the PDF via the section5 modal
                                result['pdf']['navigate_via'] = 'section5_html'
                            else:
                                msg = f"PDF download returned status/ctype: {resp.status_code} {resp.headers.get('content-type')}"
                                logger.warning(msg)
                                result['pdf']['error'] = msg
                        except Exception as e:
                            msg = f"Error downloading PDF: {e}"
                            logger.exception(msg)
                            result['pdf']['error'] = msg
            except Exception as e:
                msg = f"Error waiting for or capturing modal: {e}"
                logger.exception(msg)
                result['html']['error'] = msg
        else:
            msg = "No appropriate CO link found on page"
            logger.warning(msg)
            result['html']['error'] = msg

        try:
            page_html = page.content()
            (debug_out / f"{prefix}.html").write_text(page_html, encoding='utf-8')
            logger.debug("Saved full page HTML to %s", debug_out / f"{prefix}.html")
        except Exception as e:
            logger.exception("Failed to save full page HTML: %s", e)

        try:
            page.screenshot(path=str(debug_out / f"{prefix}.png"), full_page=True)
        except Exception:
            pass

        browser.close()
        logger.info("drive_section5_download finished for %s", url)
    return result
