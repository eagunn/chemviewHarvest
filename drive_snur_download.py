"""
drive_snur_download.py

HTTP-based driver skeleton for Significate New Use Report (SNUR) harvesting
based primarily on the current code for New Chemical Notices.

This module is invoked by `harvestSignificateNewUseReport.py` via the shared
`harvest_framework.run_harvest` function. It contains the Beautiful Soup (BS4)
logic to retrieve json and modals, scrape HTML, gather download links, and add entries to
a download plan which will be processed later by a separate script.
We use`HarvestDB` (via the db object passed from the framework) for
read/write of success/failure records.
"""

import atexit
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
import logging
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, parse_qsl
import time
import download_plan
import re

# External deps (ensure installed): requests, bs4
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Do not initialize download_plan at import time (avoids hard-coding the folder
# name and circular imports). Initialize lazily on first driver invocation using
# the `cas_dir` value the framework passes in (derived from Config.archive_root).
_DOWNLOAD_PLAN_INITIALIZED = False
# Keep same logical default as other drivers
_DOWNLOAD_PLAN_DEFAULT_FOLDER = "chemview_archive_snur"

# -- HTTP / parsing helpers ---------------------------------------


def build_session(user_agent: Optional[str] = None, timeout: int = 30) -> requests.Session:
    """
    Return a configured requests.Session with reasonable headers and a retry
    adapter if desired. Caller may further configure.

    We include headers commonly sent by browsers / XHR requests so that
    JSON endpoints which check for AJAX requests (e.g. X-Requested-With)
    or require a Referer do not return an empty/blocked response.
    """
    s = requests.Session()
    s.headers.update({
        "User-Agent": user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        # Allow JSON endpoints to return proper content; be permissive for other endpoints
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        # Mark as an XMLHttpRequest (helps some servers detect AJAX calls)
        "X-Requested-With": "XMLHttpRequest",
        # Some endpoints require a Referer or Origin header; set a sensible referer
        "Referer": "https://chemview.epa.gov/chemview/",
    })
    # TODO: add requests.adapters.Retry + HTTPAdapter if you want automatic retries
    return s


def get_html(session: requests.Session, url: str, timeout: int = 30) -> Optional[str]:
    """
    Fetch url and return the response text (HTML) or None on permanent failure.
    Small wrapper for logging and basic retry behavior.
    """
    try:
        r = session.get(url, timeout=timeout)
        r.raise_for_status()
        return r.text
    except Exception:
        logger.exception("get_html: failed to GET %s", url)
        return None


def get_json(session: requests.Session, url: str, timeout: int = 30) -> Optional[Dict[str, Any]]:
    """
    GET JSON endpoint and return parsed JSON or None on error.

    This wrapper logs response metadata to help diagnose cases where the
    server returns empty JSON or non-JSON content when called from a script.
    """
    logger.debug(f"In get_json for url: {url}")
    try:
        r = session.get(url, timeout=timeout)
        logger.debug("get_json: status=%s, content-type=%s, len=%d", r.status_code, r.headers.get('Content-Type'), len(r.content) if r.content is not None else 0)
        r.raise_for_status()
        # If the server returns something that claims to be JSON but is empty, log the preview
        content_type = r.headers.get('Content-Type', '')
        if 'application/json' not in content_type.lower() and 'json' not in content_type.lower():
            # It's not JSON; log a short preview to aid debugging
            text_preview = (r.text or '')[:400]
            logger.debug("get_json: unexpected content-type for %s: %s; preview=%s", url, content_type, text_preview)
        # Attempt to parse JSON (will raise if not valid)
        parsed = r.json()
        logger.debug("json returned ok; keys=%s", list(parsed.keys()) if isinstance(parsed, dict) else type(parsed))
        return parsed
    except Exception:
        logger.exception("get_json: failed to GET/parse JSON from %s", url)
        return None


def sanitize_cfr_id(cfr_id_text: str) -> str:
    """Return a filesystem-safe identifier from a CFR id text.

    Example: '40 CFR 721.10210' -> '40_CFR_721_10210'
    """
    if not cfr_id_text:
        return ''
    cleaned = re.sub(r'[^A-Za-z0-9]+', '_', cfr_id_text.strip())
    cleaned = re.sub(r'_+', '_', cleaned).strip('_')
    return cleaned


def _extract_chemical_database_ids(json_obj: Dict[str, Any], source_id: Optional[str] = None) -> List[str]:
    snur_chemical_database_ids: List[str] = []
    # The top-level data is in 'chemicalDataTables', then 'chemicalQueryResults' is a list.
    # Assuming the data we want is in the first (and only) item of that list:
    query_results = json_obj['chemicalDataTables']['chemicalQueryResults'][0]

    # Now, we get the list of 'sources'
    sources_list = query_results['sources']
    for source in sources_list:
        if source.get('sourceName') == 'SNUR':
            # This is the list we want to iterate through to get our values
            chemicals_list = source.get('chemicals', [])
            for chemical in chemicals_list:
                chem_id = chemical.get('id')
                if chem_id:
                    snur_chemical_database_ids.append(str(chem_id))
            # No need to check other sources once we've found the SNUR source
            break
    logger.debug("snur_chemical_database_ids: %s", snur_chemical_database_ids)
    return snur_chemical_database_ids


# -- CSV -> modal resolution (pluggable) ---------------------------------

def synthesize_modal_urls_from_export_url(export_url: str, session: requests.Session) -> List[str]:
    """
    Synthesize one or more SNUR modal URLs from a CSV input row by:
      - parsing the provided row's url value to extract modalId and sourceId
      - calling the datatable JSON endpoint to find chemicalDatabaseId(s)
      - composing modal URLs for each chemicalDatabaseId found

    Returns list of fully-qualified modal URL strings (may be empty).
    """
    logger.debug("In synthesize_modal_urls_from_export_row for export url: %s", export_url)
    modal_url_list: List[str] = []

    source_id = "1"  # Significate New Use Report sourceId is known/fixed
    try:
        parsed = urlparse(export_url)
        qs = parse_qs(parsed.query)
        modal_ids = qs.get('modalId')
        modal_id = modal_ids[0] if modal_ids and len(modal_ids) > 0 else None
        if modal_id:
            datatable_url = build_big_ugly_datatable_query_url(modal_id, source_id)
            # Prefetch the ChemView homepage to establish cookies/session state similar to a browser
            try:
                session.get("https://chemview.epa.gov/chemview/", timeout=10)
            except Exception:
                logger.debug("Prefetch to chemview homepage failed (non-fatal) — continuing to datatable call")
            json_resp = get_json(session, datatable_url)
            if json_resp:
                chemical_db_id_list = _extract_chemical_database_ids(json_resp, source_id=source_id)
                if chemical_db_id_list and len(chemical_db_id_list) > 0:
                    # Compose modal URLs
                    for chem_db_id in chemical_db_id_list:
                        modal_url = f"https://chemview.epa.gov/chemview/chemicaldata.do?sourceId={source_id or ''}&chemicalDataId={chem_db_id}&chemicalId={modal_id}"
                        modal_url_list.append(modal_url)
                    logger.debug("Synthesized %d modal urls", len(modal_url_list))
                else:
                    logger.warning("No chemicalDatabaseIds found for modalId=%s", modal_id)
            else:
                logger.warning("No JSON response from datatable URL: %s", datatable_url)
        else:
            logger.warning("No modalId found in export URL: %s", export_url)
    except Exception:
        logger.exception("unexpected error for export url: %s", export_url)

    return modal_url_list


def build_big_ugly_datatable_query_url(modal_id: str, source_id: str) -> str:
    # Build a datatable query URL. We don't know how tolerant the
    # endpoint is to missing params, so we provide all know values.
    # Replace the compact params block in `drive_snur_download.py` with this ordered parameter list
    logger.debug(f"in build_big... for modal_id: {modal_id}, source_id: {source_id}")
    base = 'https://chemview.epa.gov/chemview/chemicals/datatable'
    params = [
        ('isTemplateFilter', 'false'),
        ('chemicalIds', modal_id),
        ('synonymIds', ''),
        ('sourceIds', source_id or ''),
        ('draw', '6'),
        ('columns[0][data]', '0'),
        ('columns[0][name]', ''),
        ('columns[0][searchable]', 'true'),
        ('columns[0][orderable]', 'false'),
        ('columns[0][search][value]', ''),
        ('columns[0][search][regex]', 'false'),
        ('columns[1][data]', '1'),
        ('columns[1][name]', ''),
        ('columns[1][searchable]', 'true'),
        ('columns[1][orderable]', 'true'),
        ('columns[1][search][value]', ''),
        ('columns[1][search][regex]', 'false'),
        ('columns[2][data]', '2'),
        ('columns[2][name]', ''),
        ('columns[2][searchable]', 'true'),
        ('columns[2][orderable]', 'false'),
        ('columns[2][search][value]', ''),
        ('columns[2][search][regex]', 'false'),
        ('columns[3][data]', '3'),
        ('columns[3][name]', ''),
        ('columns[3][searchable]', 'true'),
        ('columns[3][orderable]', 'false'),
        ('columns[3][search][value]', ''),
        ('columns[3][search][regex]', 'false'),
        ('columns[4][data]', '4'),
        ('columns[4][name]', ''),
        ('columns[4][searchable]', 'true'),
        ('columns[4][orderable]', 'false'),
        ('columns[4][search][value]', ''),
        ('columns[4][search][regex]', 'false'),
        ('columns[5][data]', '5'),
        ('columns[5][name]', ''),
        ('columns[5][searchable]', 'true'),
        ('columns[5][orderable]', 'false'),
        ('columns[5][search][value]', ''),
        ('columns[5][search][regex]', 'false'),
        ('order[0][column]', '1'),
        ('order[0][dir]', 'asc'),
        ('order[0][name]', ''),
        ('start', '0'),
        ('length', '10'),
        ('search[value]', ''),
        ('search[regex]', 'false'),
        ('_', str(int(time.time() * 1000))),
    ]
    datatable_url = base + '?' + urlencode(params, doseq=True)
    logger.debug("Built datatable URL: %s", datatable_url[:121])
    return datatable_url


# -- modal parsing (BeautifulSoup) ---------------------------------------
def parse_modal_html_for_notice_and_links(html: str) -> Dict[str, Any]:

    """
    Parse modal HTML (string) and extract:
    cfr_citation -- where/when the snur was published
    cfr_id -- best id for the snur
    chem_name

    Note, there are links to pdfs on this page that we could capture for
    later download. But all of them are to FR pages/docs that cover multiple
    chemicals and/or multiple new uses. It doesn't seem appropriate to store
    that info local to this single chemical/snur.

    """
    #logger.debug(f"In parse_modal_html_for_notice_and_links: {html}")
    soup = BeautifulSoup(html, "html.parser")
    result = {
        "chem_name": None,
        "cfr_citation": None,
        "cfr_id": None
    }

    # Extract chemical name if present (best-effort)
    try:
        # Find the <strong> element whose text starts with 'Chemical Name'
        strong = soup.find('strong', text=lambda s: s and s.strip().lower().startswith('chemical name'))
        #logger.debug(f"Found strong for chemical name: {strong}")
        if strong:
            li = strong.find_parent('li')
            #logger.debug(f"Found parent <li> for chemical name: {li}")
            if li:
                # Find the first <span> inside the <li>
                span = li.find('span')
                #logger.debug(f"Found first <span> in <li>: {span}")
                if span:
                    # Find the innermost <span> with the actual name
                    inner_span = span.find('span')
                    #logger.debug(f"Found inner <span> for chemical name: {inner_span}")
                    if inner_span and inner_span.text:
                        chem_text = inner_span.text.strip()
                        logger.debug(f"Extracted chemical name text: {chem_text}")
                        result['chem_name'] = chem_text
                    else:
                        # Fallback: use outer span text
                        chem_text = span.text.strip()
                        logger.warning(f"Fallback chemical name text: {chem_text}")
                        result['chem_name'] = chem_text
                else:
                    logger.warning("No <span> found inside <li> for chemical name")
            else:
                logger.warning("No parent <li> found for chemical name <strong>")
        else:
            logger.warning("No <strong> found for chemical name")
    except Exception as e:
        logger.warning(f'Failed to extract chemical name from modal HTML: {e}')

    # Extract cfr citation if present (best-effort)
    try:
        # Find the <strong> element whose text starts with 'Federal Register Citation'
        frc_strong = soup.find('strong', text=lambda s: s and s.strip().lower().startswith('federal register citation'))
        if frc_strong:
            frc_li = frc_strong.find_parent('li')
            if frc_li:
                frc_a = frc_li.find('a')
                if frc_a and frc_a.text:
                    frc_text = frc_a.get_text().strip()
                    result['cfr_citation'] = frc_text
                    logger.debug(f"Extracted Federal Register citation: {frc_text}")
                else:
                    logger.warning("Found Federal Register Citation <li> but no <a> with text inside it")
            else:
                logger.warning("Found Federal Register Citation <strong> but parent <li> not found")
        else:
            logger.warning("No <strong> element found for 'Federal Register Citation' in modal HTML")
    except Exception as e:
        logger.debug(f'Failed to extract Federal Register citation from modal HTML: {e}')

    # Extract Code of Federal Regulations identifier (e.g., '40 CFR 721.10210')
    # This is understood to be the most accurate id for the snur
    try:
        code_strong = soup.find('strong', text=lambda s: s and s.strip().lower().startswith('code of federal regulations'))
        if code_strong:
            code_li = code_strong.find_parent('li')
            if code_li:
                code_a = code_li.find('a')
                if code_a and code_a.text:
                    code_text = code_a.get_text().strip()
                    result['cfr_id'] = code_text
                    logger.debug(f"Extracted CFR id: {code_text}")
                else:
                    # Fallback: check for text in the <li> indicating this is a proposed regulation
                    # Example (sampleSNURproposed.html): a span contains 'None applicable. This is a proposed regulation.'
                    li_text = ' '.join([s.strip() for s in code_li.stripped_strings if s and s.strip()])
                    if 'proposed regulation' in li_text.lower() or 'proposed' in li_text.lower():
                        result['cfr_id'] = 'proposed'
                        logger.debug("Detected proposed regulation in CFR <li>; set cfr_id='proposed'")
                    else:
                        logger.warning("Found 'Code of Federal Regulations' <li> but no <a> with text inside it")
            else:
                logger.warning("Found 'Code of Federal Regulations' <strong> but parent <li> not found")
        else:
            logger.warning("No <strong> element found for 'Code of Federal Regulations' in modal HTML")
    except Exception as e:
        logger.debug(f'Failed to extract CFR id from modal HTML: {e}')

    logger.debug("parse modal result: %s", result)
    return result


# -- download-plan wrapper helper ----------------------------------------

def add_plan_links_for_notice(cas_dir: Path, subfolder_path: Union[Path, str], links: List[str]) -> None:
    """
    Thin wrapper that calls download_plan.add_links_to_plan with a path structure.
    Keep the call-site simple; the actual download_plan API expects:
      download_plan.add_links_to_plan(accum, cas_dir_str, subfolder, list_of_urls)
    We intentionally do not import download_plan here so that tests can stub the function. When
    this is implemented in full, use the same API as existing drivers.
    """
    import download_plan  # local import to avoid heavy coupling at top-level
    # Ensure subfolder_path is a Path
    subfolder = Path(subfolder_path)
    # Pass cas_dir as the CAS folder root and the relative subfolder as the subfolder_name
    download_plan.add_links_to_plan(download_plan.DOWNLOAD_PLAN_ACCUM, cas_dir, subfolder, links)


# -- main driver entrypoint (drop-in signature) ---------------------------

def drive_snur_download(url, cas_val, cas_dir: Path, debug_out=None, headless=True, browser=None, page=None, db=None, file_types: Any = None, retry_interval_hours: float = 12.0, archive_root=None) -> Dict[str, Any]:
    """
    Driver entrypoint: same signature shape as the Playwright-based driver so it can be
    substituted without changes to harvest_framework.py.
    """
    result: Dict[str, Any] = {
        "CAS:": cas_val,
        "attempted": False,
        "html": {"success": None, "local_file_path": None, "error": None, "navigate_via": ""},
        "pdf": {"success": None, "local_file_path": None, "error": None, "navigate_via": ""},
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
    need_html = db.need_download(cas_val, file_types.snur_html, retry_interval_hours=retry_interval_hours)
    #need_pdf = db.need_download(cas_val, file_types.snur_pdf, retry_interval_hours=retry_interval_hours)
    need_pdf = False # we don't download any files, pdf or zip, for snurs

    if not need_html and not need_pdf:
        logger.info("No downloads needed for cas=%s (Significate New Use Report)", cas_val)
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

    # We've passed all the pre-checks; mark that we are attempting processing


    result["attempted"] = True
    # Be pessimistic. Assume failure until success is confirmed.
    if need_html:
        result["html"]["success"] = False
    if need_pdf:
        result["pdf"]["success"] = False
    # from this point on down, we only need to set the msg value for failures
    # but will need to set 'success' to True on completed, confirmed successes.

    # 21 Nov 25: We have started collecting and saving a basic set of chemical
    # info in the db. At this point, we already have 2 of the three bits we
    # want: Cas # and chem_id. Pull them from the URL
    # 26 Nov 25: Doing this revealed that our failing URLs were missing the ch=cas_val
    # parameter, so we now repair the URL if needed and always return it.
    result, url = validate_url_and_get_chem_info_ids(url, cas_val, result)

    # Prepare to make HTTPS requests
    session = build_session()
    # Attempt to synthesize modal URLs from the input row.
    modal_urls = synthesize_modal_urls_from_export_url(url, session)

    if modal_urls:
        # For each resolved modal URL: fetch HTML, parse for links, save HTML, and add links to plan
        for modal_url in modal_urls:
            html = get_html(session, modal_url)
            if not html:
                logger.warning("Failed to fetch modal HTML from %s", modal_url)
                continue
            parsed = parse_modal_html_for_notice_and_links(html)
            result["chem_info"]['chem_name'] = parsed.get('chem_name')
            cfr_id_text = parsed.get('cfr_id')
            # Create a safe single-token identifier for CFR-based folder/filename use
            safe_cfr_id = None
            if cfr_id_text:
                safe_cfr_id = sanitize_cfr_id(cfr_id_text)
            else:
                logger.warning('No CFR id text found to sanitize for cas %s', cas_val)
                safe_cfr_id = "unknown"
            # ensure cas_dir exists
            cas_dir = Path(cas_dir)
            cas_dir.mkdir(parents=True, exist_ok=True)
            # save modal HTML
            snur_dir = cas_dir / safe_cfr_id
            snur_dir.mkdir(parents=True, exist_ok=True)
            html_path = snur_dir / f"snur_{safe_cfr_id}.html"

            try:
                html_path.write_text(parsed.get("modal_html", html), encoding="utf-8")
                logger.info("Saved modal HTML to %s", html_path)
                result["html"]["success"] = True
                result["html"]["local_file_path"] = str(html_path)
                result["html"]["navigate_via"] = modal_url
            except Exception:
                logger.exception("Failed to save modal HTML to %s", html_path)

        # Record chemical info if we have enough data
        record_chemical_info(result, db)
    else:
        msg = "No modal URLs resolved for given input row"
        logger.warning(msg)
        result["html"]["error"] = msg
        result["pdf"]["error"] = msg

    # Note that "pdf" here is an umbrella term for all non-html downloads,
    # which could be pdfs, zips, or xmls.
    if result.get('attempted'):
        logger.debug(f"After modal scrape attempt, result = {result}")
        if need_html:
            if (result.get('html', {}).get('success') is True):
                try:
                    db.log_success(cas_val, file_types.snur_html, result.get('html', {}).get('local_file_path'), result.get('html', {}).get('navigate_via'))
                except Exception:
                    logger.exception("Failed to write success to DB for html post-loop")
            else:
                # HTML explicitly failed during processing -> log failure
                msg = result.get('html', {}).get('error') or "HTML processing failed"
                try:
                    db.log_failure(cas_val, file_types.snur_html, msg)
                except Exception:
                    logger.exception("Failed to write failure to DB for html post-loop")
            if need_pdf:
                if (result.get('pdf', {}).get('success') is True):
                    try:
                        db.log_success(cas_val, file_types.snur_pdf, result.get('pdf', {}).get('local_file_path'), result.get('pdf', {}).get('navigate_via'))
                    except Exception:
                        logger.exception("Failed to write success to DB for html post-loop")
                else:
                    # PDF explicitly failed during processing -> log failure
                    msg = result.get('pdf', {}).get('error') or "Download processing failed or no links discovered"
                    try:
                        db.log_failure(cas_val, file_types.snur_pdf, msg)
                    except Exception:
                        logger.exception("Failed to write failure to DB for pdf post-loop")
    else:
        logger.debug("No downloads attempted.")
        return result
    return result


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
