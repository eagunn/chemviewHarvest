from pathlib import Path
import random
import logging
from typing import Dict, Any
from file_types import FileTypes
from HarvestDB import HarvestDB

logger = logging.getLogger(__name__)


def _need_download_from_db(db, cas_val: str, file_type: str) -> bool:
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
    Returns dict with 'attempted' and per-file results similar to other drivers.
    """
    # If db not provided, we can't run
    if db is None:
        msg = "Driver requires db, but none provided"
        logger.error(msg)
        return {'attempted': False, 'html': {'success': False, 'local_file_path': None, 'error': 'no db', 'navigate_via': ''}, 'pdf': {'success': False, 'local_file_path': None, 'error': 'no db', 'navigate_via': ''}}

    result: Dict[str, Any] = {
        'attempted': False,
        'html': {'success': False, 'local_file_path': None, 'error': None, 'navigate_via': ''},
        'pdf': {'success': False, 'local_file_path': None, 'error': None, 'navigate_via': ''}
    }

    if db is None or file_types is None:
        msg = "Driver requires db and file_types"
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

    # ensure cas_dir exists
    if cas_dir is None:
        cas_dir = Path('.')
    cas_dir.mkdir(parents=True, exist_ok=True)

    # Simulate download attempts with random success/failure
    try:
        if need_html:
            html_ok = random.choice([True, False])
            if html_ok:
                # create a dummy file to represent the saved HTML
                html_path = cas_dir / f"substantial_risk_{cas_val}.html"
                html_path.write_text(f"stub html for {cas_val}", encoding='utf-8')
                result['html']['success'] = True
                result['html']['local_file_path'] = str(html_path)
                result['html']['navigate_via'] = ''
                try:
                    db.log_success(cas_val, file_types.substantial_risk_html, str(html_path), result['html']['navigate_via'])
                except Exception:
                    logger.exception("Failed to write success to DB for substantial risk html")
            else:
                result['html']['error'] = 'stub failure'
                try:
                    db.log_failure(cas_val, file_types.substantial_risk_html, '')
                except Exception:
                    logger.exception("Failed to write failure to DB for substantial risk html")

        if need_pdf:
            pdf_ok = random.choice([True, False])
            if pdf_ok:
                pdf_path = cas_dir / f"substantial_risk_{cas_val}.pdf"
                pdf_path.write_bytes(b"stub pdf content")
                result['pdf']['success'] = True
                result['pdf']['local_file_path'] = str(pdf_path)
                result['pdf']['navigate_via'] = ''
                try:
                    db.log_success(cas_val, file_types.substantial_risk_pdf, str(pdf_path), result['pdf']['navigate_via'])
                except Exception:
                    logger.exception("Failed to write success to DB for substantial risk pdf")
            else:
                result['pdf']['error'] = 'stub failure'
                try:
                    db.log_failure(cas_val, file_types.substantial_risk_pdf, '')
                except Exception:
                    logger.exception("Failed to write failure to DB for substantial risk pdf")
    except Exception as e:
        logger.exception("Unexpected error in stub substantial risk driver: %s", e)

    return result