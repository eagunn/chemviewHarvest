from pathlib import Path
from datetime import datetime
from typing import Dict, Any
import json
import logging
import re

logger = logging.getLogger(__name__)

# Module-level plan state (initialized via init())
DOWNLOAD_PLAN_ACCUM: Dict[str, Any] = {'folder': 'chemview_archive', 'subfolderList': [], 'downloadList': []}
DOWNLOAD_PLAN_ACCUM_CAS_SET: set = set()
DOWNLOAD_PLAN_ACCUM_CAS_SINCE_WRITE: int = 0
DOWNLOAD_PLAN_WRITE_BATCH_SIZE: int = 25
#DOWNLOAD_PLAN_WRITE_BATCH_SIZE: int = 3  # for testing only
DOWNLOAD_PLAN_OUT_DIR: Path = Path('downloadsToDo')


def init(folder: str = 'chemview_archive',
         out_dir: Path | str = 'downloadsToDo',
         batch_size: int | None = None):
    """Initialize module-level plan state. Call from driver to configure folder names and write behaviour."""
    global DOWNLOAD_PLAN_ACCUM, DOWNLOAD_PLAN_ACCUM_CAS_SET, DOWNLOAD_PLAN_ACCUM_CAS_SINCE_WRITE, DOWNLOAD_PLAN_WRITE_BATCH_SIZE, DOWNLOAD_PLAN_OUT_DIR
    DOWNLOAD_PLAN_ACCUM = {'folder': folder, 'subfolderList': [], 'downloadList': []}
    DOWNLOAD_PLAN_ACCUM_CAS_SET = set()
    DOWNLOAD_PLAN_ACCUM_CAS_SINCE_WRITE = 0
    if batch_size is not None:
        DOWNLOAD_PLAN_WRITE_BATCH_SIZE = int(batch_size)
    DOWNLOAD_PLAN_OUT_DIR = Path(out_dir)
    DOWNLOAD_PLAN_OUT_DIR.mkdir(parents=True, exist_ok=True)


# --- internal helpers ---

def _ensure_cas_entry(plan: Dict[str, Any], cas_folder_name: str) -> Dict[str, Any]:
    for entry in plan.get('subfolderList', []):
        if entry.get('folder') == cas_folder_name:
            return entry
    new_entry = {'folder': cas_folder_name, 'subfolderList': [], 'downloadList': []}
    plan.setdefault('subfolderList', []).append(new_entry)
    return new_entry


def _ensure_downloads_subfolder(cas_entry: Dict[str, Any], subfolder_name: str) -> Dict[str, Any]:
    for sf in cas_entry.get('subfolderList', []):
        if sf.get('folder') == subfolder_name:
            return sf
    new_sf = {'folder': subfolder_name, 'subfolderList': [], 'downloadList': []}
    cas_entry.setdefault('subfolderList', []).append(new_sf)
    return new_sf


# New helper to ensure an arbitrary nested subfolder path exists under a CAS entry
def _normalize_subpath(subfolder_name) -> list:
    """Normalize various subfolder path inputs into a list of path parts.
    Accepts a Path, string with '/' or '\\' separators, or a list/tuple of parts.
    Empty or None -> empty list.
    """
    if not subfolder_name:
        return []
    if isinstance(subfolder_name, (list, tuple)):
        return [str(p) for p in subfolder_name if p]
    s = str(subfolder_name)
    # split on both forward and back slashes to be robust across platforms
    parts = [p for p in re.split(r'[\\/]+', s) if p]
    return parts


def _ensure_subfolder_path(cas_entry: Dict[str, Any], subfolder_name) -> Dict[str, Any]:
    """Ensure the nested subfolder path exists under `cas_entry` and return the leaf entry.
    `subfolder_name` may be a Path, a slash/backslash-separated string, or a list of folder names.
    If subfolder_name is empty, return the cas_entry itself (top-level).
    """
    logger.debug("Ensuring subfolder path: %s", subfolder_name)
    parts = _normalize_subpath(subfolder_name)
    current = cas_entry
    for part in parts:
        # find existing subfolder
        found = None
        for sf in current.get('subfolderList', []):
            if sf.get('folder') == part:
                found = sf
                break
        if found:
            current = found
        else:
            new_sf = {'folder': part, 'subfolderList': [], 'downloadList': []}
            current.setdefault('subfolderList', []).append(new_sf)
            current = new_sf
    return current


# --- public API ---

def add_links_to_plan(plan: Dict[str, Any], cas_dir: Path, subfolder_name, links: list[str]) -> tuple[int, int]:
    """Add links to the nested download plan structure
    `subfolder_name` may be a single folder name (old behavior) or a nested path
    (string with separators, Path, or list of parts). Duplicate URLs are ignored.
    Returns (added, skipped_duplicates).
    Also manages batching: if enough distinct CAS entries have been added since last write,
    the plan is written to disk automatically.

    This function now accepts either:
    - a non-empty `cas_dir` Path and a relative `subfolder_name` (legacy), or
    - a falsy `cas_dir` and a full path in `subfolder_name` which includes the CAS folder.
    """
    logger.debug("in add_links_to_plan: cas_dir=%s, subfolder_name=%s, num_links=%d", cas_dir, subfolder_name, len(links))
    global DOWNLOAD_PLAN_ACCUM, DOWNLOAD_PLAN_ACCUM_CAS_SET, DOWNLOAD_PLAN_ACCUM_CAS_SINCE_WRITE
    if not links:
        logger.warning("No links to add to plan")
        return 0, 0

    if not cas_dir and not subfolder_name:
        logger.error("Both cas_dir and subfolder_name are empty; cannot determine CAS folder")
        return 0, 0

    # Normalize the incoming subfolder representation into path parts
    parts = _normalize_subpath(subfolder_name)

    # Determine CAS folder name and the parts that follow it (the relative subpath)
    cas_folder_name = None
    relative_parts = []

    if cas_dir:
        # Legacy path: cas_dir provided as Path; prefer that folder name
        logger.debug("Using provided cas_dir for CAS folder name: %s", cas_dir)
        cas_folder_name = Path(cas_dir).name
        # If caller passed a full path in subfolder_name that contains the CAS folder,
        # strip everything up to and including that CAS segment so we only use the tail as relative parts.
        if parts:
            try:
                idx = parts.index(cas_folder_name)
                relative_parts = parts[idx+1:]
            except ValueError:
                # No CAS segment in provided subpath: treat `parts` as relative to the cas_dir
                relative_parts = parts
    else:
        # No cas_dir provided: expect the subfolder_name to include the CAS folder.
        logger.debug("No cas_dir provided; extracting CAS folder name from subfolder_name parts")
        # Try to find a part that looks like a CAS folder. This is tricky. The don't all start
        # CAS-. But, I do believe that at this stage in the process, they are the only part of
        # the folder tree that would contain a hyphen. So we can use that as a heuristic.
        cas_idx = None
        for i, p in enumerate(parts):
            # test if the part contains a hyphen (common in CAS folder names)
            if '-' in p:
                logger.debug("Found CAS-like segment '%s' at index %d", p, i)
                cas_idx = i
                break
        if cas_idx is None:
            logger.error("Could not find CAS folder segment in subfolder_name: %s", subfolder_name)
            return 0, 0

        # Build CAS folder name and the relative trailing parts
        cas_folder_name = parts[cas_idx] if parts else ''
        relative_parts = parts[cas_idx+1:] if parts else []

    # Ensure the CAS entry exists and walk/create the nested subfolders for the relative path
    cas_folder_name = str(cas_folder_name).strip()
    if not cas_folder_name:
        logger.error("Could not determine CAS folder name from inputs: cas_dir=%s, subfolder_name=%s", cas_dir, subfolder_name)
        return 0, 0
    cas_entry = _ensure_cas_entry(plan, cas_folder_name)
    reports_sf = _ensure_subfolder_path(cas_entry, relative_parts)

    existing = set(reports_sf.get('downloadList', []))
    added = 0
    skipped_duplicates = 0
    for url in links:
        if not url:
            continue
        if url in existing:
            skipped_duplicates += 1
            continue
        reports_sf.setdefault('downloadList', []).append(url)
        existing.add(url)
        added += 1

    # track CAS for batching
    # If this is a new CAS folder and we're operating on the module-level accumulator,
    # ensure we don't split a single CAS across two files: if the accumulator is already
    # at-or-above the batch threshold, flush first so the new CAS begins in a fresh file.
    if cas_folder_name not in DOWNLOAD_PLAN_ACCUM_CAS_SET:
        if plan is DOWNLOAD_PLAN_ACCUM:
            try:
                logger.debug(
                    "about to add new CAS %s; accumulator=%d, batch=%d",
                    cas_folder_name,
                    DOWNLOAD_PLAN_ACCUM_CAS_SINCE_WRITE,
                    DOWNLOAD_PLAN_WRITE_BATCH_SIZE,
                )
                # If adding this CAS would exceed the batch, flush now so the CAS starts
                # in a fresh output file and all its entries go to the same file.
                if DOWNLOAD_PLAN_ACCUM_CAS_SINCE_WRITE >= DOWNLOAD_PLAN_WRITE_BATCH_SIZE:
                    logger.info(
                        "download plan threshold reached; flushing before adding CAS %s",
                        cas_folder_name,
                    )
                    _write_plan_to_disk(DOWNLOAD_PLAN_ACCUM, DOWNLOAD_PLAN_OUT_DIR)
                    _reset_module_plan()
            except Exception:
                logger.exception("Failed to auto-save download plan before adding new CAS")

        DOWNLOAD_PLAN_ACCUM_CAS_SET.add(cas_folder_name)
        DOWNLOAD_PLAN_ACCUM_CAS_SINCE_WRITE += 1

    # Note: auto-save-after-add removed. We flush before adding a new CAS to avoid
    # splitting a CAS across files; additional auto-save here would be redundant.

    return added, skipped_duplicates


def _write_plan_to_disk(plan: Dict[str, Any], out_dir: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"downloads_{ts}.json"
    out_path = Path(out_dir) / filename
    with open(out_path, 'w', encoding='utf-8') as fh:
        json.dump(plan, fh, indent=2)
    logger.info("Saved download plan to %s", out_path)
    return out_path


def save_download_plan(plan: Dict[str, Any], debug_out: Path) -> Path:
    """Write the plan to a timestamped JSON file in debug_out and return the path."""
    try:
        return _write_plan_to_disk(plan, Path(debug_out))
    except Exception as e:
        logger.exception("Failed to save download plan to %s: %s", debug_out, e)
        raise


def _reset_module_plan(folder_name: str | None = None):
    """Reset the module-level download plan accumulator and batching counters.
    If folder_name is provided, preserve it as the top-level folder; otherwise preserve
    the current DOWNLOAD_PLAN_ACCUM['folder'] or default to 'chemview_archive'.
    """
    global DOWNLOAD_PLAN_ACCUM, DOWNLOAD_PLAN_ACCUM_CAS_SET, DOWNLOAD_PLAN_ACCUM_CAS_SINCE_WRITE
    # Determine the folder to preserve
    if folder_name is None:
        try:
            folder_name = DOWNLOAD_PLAN_ACCUM.get('folder', 'chemview_archive')
        except Exception:
            folder_name = 'chemview_archive'
    # Reinitialize
    DOWNLOAD_PLAN_ACCUM = {'folder': folder_name, 'subfolderList': [], 'downloadList': []}
    DOWNLOAD_PLAN_ACCUM_CAS_SET.clear()
    DOWNLOAD_PLAN_ACCUM_CAS_SINCE_WRITE = 0


def flush():
    """Force-write any pending plan to disk.
    Returns path to written file or None if nothing was written.
    """
    global DOWNLOAD_PLAN_ACCUM_CAS_SINCE_WRITE, DOWNLOAD_PLAN_ACCUM
    if not DOWNLOAD_PLAN_ACCUM.get('subfolderList') and not DOWNLOAD_PLAN_ACCUM.get('downloadList'):
        return None
    try:
        path = _write_plan_to_disk(DOWNLOAD_PLAN_ACCUM, DOWNLOAD_PLAN_OUT_DIR)
        # Reset module-level plan preserving folder name
        _reset_module_plan(DOWNLOAD_PLAN_ACCUM.get('folder', 'chemview_archive'))
        return path
    except Exception:
        logger.exception("Failed to flush download plan to disk")
        return None
