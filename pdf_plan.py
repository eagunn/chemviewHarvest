from pathlib import Path
from datetime import datetime
from typing import Dict, Any
import json
import logging

logger = logging.getLogger(__name__)

# Module-level plan state (initialized via init())
PDF_PLAN_ACCUM: Dict[str, Any] = {'folder': 'chemview_archive', 'subfolderList': [], 'downloadList': []}
PDF_PLAN_ACCUM_CAS_SET: set = set()
PDF_PLAN_ACCUM_CAS_SINCE_WRITE: int = 0
PDF_PLAN_WRITE_BATCH_SIZE: int = 25
PDF_PLAN_OUT_DIR: Path = Path('pdfDownloadsToDo')


def init(folder: str = 'chemview_archive_ncn', out_dir: Path | str = 'pdfDownloadsToDo', batch_size: int = 25):
    """Initialize module-level plan state. Call from driver to configure folder names and write behaviour."""
    global PDF_PLAN_ACCUM, PDF_PLAN_ACCUM_CAS_SET, PDF_PLAN_ACCUM_CAS_SINCE_WRITE, PDF_PLAN_WRITE_BATCH_SIZE, PDF_PLAN_OUT_DIR
    PDF_PLAN_ACCUM = {'folder': folder, 'subfolderList': [], 'downloadList': []}
    PDF_PLAN_ACCUM_CAS_SET = set()
    PDF_PLAN_ACCUM_CAS_SINCE_WRITE = 0
    PDF_PLAN_WRITE_BATCH_SIZE = int(batch_size)
    PDF_PLAN_OUT_DIR = Path(out_dir)
    PDF_PLAN_OUT_DIR.mkdir(parents=True, exist_ok=True)


# --- internal helpers ---

def _ensure_cas_entry(plan: Dict[str, Any], cas_folder_name: str) -> Dict[str, Any]:
    for entry in plan.get('subfolderList', []):
        if entry.get('folder') == cas_folder_name:
            return entry
    new_entry = {'folder': cas_folder_name, 'subfolderList': [], 'downloadList': []}
    plan.setdefault('subfolderList', []).append(new_entry)
    return new_entry


def _ensure_reports_subfolder(cas_entry: Dict[str, Any], reports_name: str = 'substantialRiskReports') -> Dict[str, Any]:
    for sf in cas_entry.get('subfolderList', []):
        if sf.get('folder') == reports_name:
            return sf
    new_sf = {'folder': reports_name, 'subfolderList': [], 'downloadList': []}
    cas_entry.setdefault('subfolderList', []).append(new_sf)
    return new_sf


# --- public API ---

def add_pdf_links_to_plan(plan: Dict[str, Any], cas_dir: Path, pdf_links: list[str]) -> tuple[int, int]:
    """Add pdf_links to the nested plan structure under the cas_dir name and substantialRiskReports subfolder.
    Duplicate URLs are ignored. Returns (added, skipped_duplicates).
    Also manages batching: if enough distinct CAS entries have been added since last write,
    the plan is written to disk automatically.
    """
    global PDF_PLAN_ACCUM, PDF_PLAN_ACCUM_CAS_SET, PDF_PLAN_ACCUM_CAS_SINCE_WRITE
    if not pdf_links:
        return 0, 0
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

    # track CAS for batching
    if cas_folder_name not in PDF_PLAN_ACCUM_CAS_SET:
        PDF_PLAN_ACCUM_CAS_SET.add(cas_folder_name)
        PDF_PLAN_ACCUM_CAS_SINCE_WRITE += 1

    # If our plan variable is the module's plan, update module-level reference
    if plan is not PDF_PLAN_ACCUM:
        # nothing special, caller may pass a separate plan
        pass

    # Auto-save if threshold reached
    try:
        if PDF_PLAN_ACCUM_CAS_SINCE_WRITE >= PDF_PLAN_WRITE_BATCH_SIZE:
            _write_plan_to_disk(PDF_PLAN_ACCUM, PDF_PLAN_OUT_DIR)
            PDF_PLAN_ACCUM_CAS_SINCE_WRITE = 0
            PDF_PLAN_ACCUM_CAS_SET.clear()
    except Exception:
        logger.exception("Failed to auto-save PDF plan")

    return added, skipped_duplicates


def _write_plan_to_disk(plan: Dict[str, Any], out_dir: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"pdfDownloads_{ts}.json"
    out_path = Path(out_dir) / filename
    with open(out_path, 'w', encoding='utf-8') as fh:
        json.dump(plan, fh, indent=2)
    logger.info("Saved PDF download plan to %s", out_path)
    return out_path


def save_download_plan(plan: Dict[str, Any], debug_out: Path) -> Path:
    """Write the plan to a timestamped JSON file in debug_out and return the path."""
    try:
        return _write_plan_to_disk(plan, Path(debug_out))
    except Exception as e:
        logger.exception("Failed to save pdf download plan to %s: %s", debug_out, e)
        raise


def flush():
    """Force-write any pending plan to disk.
    Returns path to written file or None if nothing was written.
    """
    global PDF_PLAN_ACCUM_CAS_SINCE_WRITE, PDF_PLAN_ACCUM
    if not PDF_PLAN_ACCUM.get('subfolderList') and not PDF_PLAN_ACCUM.get('downloadList'):
        return None
    try:
        path = _write_plan_to_disk(PDF_PLAN_ACCUM, PDF_PLAN_OUT_DIR)
        PDF_PLAN_ACCUM = {'folder': PDF_PLAN_ACCUM.get('folder', 'chemview_archive'), 'subfolderList': [], 'downloadList': []}
        PDF_PLAN_ACCUM_CAS_SET.clear()
        PDF_PLAN_ACCUM_CAS_SINCE_WRITE = 0
        return path
    except Exception:
        logger.exception("Failed to flush PDF plan to disk")
        return None

