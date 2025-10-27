import re
import requests
import html as html_lib
from pathlib import Path
from urllib.parse import urlparse, parse_qs

def drive_section5_download(url, cas_dir, need_html_download, need_pdf_download, debug_out=None, headless=True, LOG_FILE=None):
    """Navigate to the given URL with Playwright and capture the CO modal HTML and PDF.
    Returns a dict for each filetype: { 'html': {...}, 'pdf': {...} }
    Each dict contains: success (bool), path (Path or None), error (str or None)
    """
    result = {
        'html': {'success': False, 'path': None, 'error': None},
        'pdf': {'success': False, 'path': None, 'error': None}
    }
    if url is None:
        msg = "Error: url is required but was not provided to drive_section5_download()."
        print(msg, file=LOG_FILE)
        result['html']['error'] = msg
        result['pdf']['error'] = msg
        return result
    if cas_dir is None:
        msg = "Error: cas_dir is required but was not provided to drive_section5_download()."
        print(msg, file=LOG_FILE)
        result['html']['error'] = msg
        result['pdf']['error'] = msg
        return result

    print("Start of processing for URL:", url, file=LOG_FILE)

    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        msg = f"Playwright not available; cannot navigate to URL: {e}"
        print(msg, file=LOG_FILE)
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
                print(f"Navigation attempt {attempt} failed (timeout={to}ms):", e, file=LOG_FILE)
                try:
                    page.wait_for_timeout(500)
                except Exception:
                    pass

        if not nav_ok:
            print("Navigation ultimately failed for URL, continuing to save whatever we have", file=LOG_FILE)
        else:
            print("Initial navigation succeeded", file=LOG_FILE)

        try:
            anchors = page.query_selector_all("a[href]")
            print(f"Found {len(anchors)} href anchors on page", file=LOG_FILE)
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
                    print(f"Found CO link with title {to_examine}", file=LOG_FILE)
                    print(a.inner_html, file=LOG_FILE)
                    break
                elif to_examine == 'chemical details':
                    fallback_co_link = a
                    print(f"Found possible fallback CO link with title {to_examine}", file=LOG_FILE)
                    print(a.inner_html, file=LOG_FILE)
            except Exception:
                continue
        if co_link is None and fallback_co_link is not None:
            co_link = fallback_co_link
            print("Going to use fallback CO link", file=LOG_FILE)

        # if we found the "CO" link, then click it to open the summary modal
        # we always need to open the modal, even if we don't need to save
        # its HTML, because we need to get the PDF link from it
        if co_link:
            try:
                onclick = (co_link.get_attribute('onclick') or '') or ''
                print("found onclick attribute in link:", file=LOG_FILE)
            except Exception:
                onclick = ''
            try:
                if 'childModalClick' in onclick or 'modalClick' in onclick:
                    print("Going to try modal clicks via evaluate", file=LOG_FILE)
                    try:
                        page.evaluate(
                            "(el)=>{ try{ if(typeof childModalClick === 'function'){ childModalClick(new MouseEvent('click',{bubbles:true,cancelable:true}), el); return true; } if(typeof modalClick === 'function'){ modalClick(new MouseEvent('click',{bubbles:true,cancelable:true}), el); return true; } el.dispatchEvent(new MouseEvent('click',{bubbles:true,cancelable:true})); return true;}catch(e){ try{ el.click(); }catch(e){} return false;} }",
                            co_link,
                        )
                        print("Clicked CO element via childModalClick/modalClick evaluate", file=LOG_FILE)
                    except Exception:
                        print("Going to try to dispatch a mouse event click (inner)", file=LOG_FILE)
                        page.evaluate("(el) => { try{ el.dispatchEvent(new MouseEvent('click', { bubbles:true, cancelable:true })); }catch(e){ try{ el.click(); }catch(e){} } }", co_link)
                        print("Clicked CO element via MouseEvent dispatch fallback", file=LOG_FILE)
                else:
                    print("Going to try to dispatch a mouse event click (outer)", file=LOG_FILE)
                    page.evaluate("(el) => { try{ el.dispatchEvent(new MouseEvent('click', { bubbles:true, cancelable:true })); }catch(e){ try{ el.click(); }catch(e){} } }", co_link)
                    print("Clicked CO element via MouseEvent dispatch (no modalClick)", file=LOG_FILE)
                try:
                    page.wait_for_timeout(2000)
                except Exception:
                    pass
            except Exception as e:
                print("Failed to click global CO element:", e, file=LOG_FILE)

            print("Waiting for TSCA SECTION 5 ORDER modal to appear...", file=LOG_FILE)
            try:
                page.wait_for_selector("div.modal-body.action h3", timeout=10000)
                modal_divs = page.query_selector_all("div.modal-body.action")
                found_modal = None
                print(f"Found {len(modal_divs)} modal-body action divs", file=LOG_FILE)
                for div in modal_divs:
                    try:
                        h3 = div.query_selector("h3")
                        text = h3.inner_text().strip() if h3 else ''
                        print(f"Modal h3 text: {text}", file=LOG_FILE)
                        if text == "TSCA SECTION 5 ORDER":
                            found_modal = div
                            break
                    except Exception:
                        continue

                if found_modal:
                    modal_html = found_modal.inner_html()
                    if need_html_download:
                        print("Will attempt to save modal HTML", file=LOG_FILE)
                        modal_html_wrapped = f"<div class='modal-body action'>\n{modal_html}\n</div>"
                        html_path = cas_dir / "section5summary.html"
                        try:
                            with open(html_path, 'w', encoding='utf-8') as fh:
                                fh.write(modal_html_wrapped)
                            print("Saved modal HTML to", html_path, file=LOG_FILE)
                            result['html']['success'] = True
                            result['html']['path'] = str(html_path)
                        except Exception as e:
                            msg = f"Failed to save modal HTML: {e}"
                            print(msg, file=LOG_FILE)
                            result['html']['error'] = msg
                    if need_pdf_download:
                        print("Will attempt to find and download PDF from modal", file=LOG_FILE)
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
                            print(f"Downloading PDF from: {pdf_url_full} -> saving as: {filename} (cas_dir={cas_dir})", file=LOG_FILE)
                            resp = requests.get(pdf_url_full, timeout=30)
                            if resp.status_code == 200 and resp.headers.get('content-type','').startswith('application/pdf'):
                                pdf_path = cas_dir / filename
                                with open(pdf_path, 'wb') as pf:
                                    pf.write(resp.content)
                                print("Saved PDF to", pdf_path, file=LOG_FILE)
                                result['pdf']['success'] = True
                                result['pdf']['path'] = str(pdf_path)
                            else:
                                msg = f"PDF download returned status/ctype: {resp.status_code} {resp.headers.get('content-type')}"
                                print(msg, file=LOG_FILE)
                                result['pdf']['error'] = msg
                        except Exception as e:
                            msg = f"Error downloading PDF: {e}"
                            print(msg, file=LOG_FILE)
                            result['pdf']['error'] = msg
            except Exception as e:
                msg = f"Error waiting for or capturing modal: {e}"
                print(msg, file=LOG_FILE)
                result['html']['error'] = msg
        else:
            msg = "No appropriate CO link found on page"
            print(msg, file=LOG_FILE)
            result['html']['error'] = msg

        try:
            page_html = page.content()
            (debug_out / f"{prefix}.html").write_text(page_html, encoding='utf-8')
            print("Saved full page HTML to", debug_out / f"{prefix}.html", file=LOG_FILE)
        except Exception as e:
            print("Failed to save full page HTML:", e, file=LOG_FILE)

        try:
            page.screenshot(path=str(debug_out / f"{prefix}.png"), full_page=True)
        except Exception:
            pass

        browser.close()
        print("drive_section5_download finished for", url, file=LOG_FILE, flush=True)
    return result
