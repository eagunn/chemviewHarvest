ChemView harvest - README
=========================

Overview
--------
This folder contains the code to harvest HTML and PDF artifacts from EPA ChemView export CSVs. A small framework centralizes the common work (CSV input loop, DB updates, Playwright browser reuse, logging, and timing). Individual report types (Section 5, 8E, New Chemical Notice, etc.) should provide a small driver that contains the navigation and extraction logic for that report.

This README explains:
- how to run the existing Section 5 harvest wrapper
- the driver interface your custom drivers must follow
- how to add new report drivers and wrappers
- basic troubleshooting and dependencies

Repository layout (important files)
----------------------------------
- `harvest_framework.py`  – the shared harvesting framework (loop, DB, browser reuse, timing, DB logging)
- `harvestSection5.py`    – thin wrapper for Section 5; preserves original comments & CLI and delegates to the framework
- `drive_section5_download.py` – Section 5 driver implementing the navigation & download logic
- `HarvestDB.py`          – DB access and logging helpers used by the framework
- `logging_setup.py`      – centralized logging configuration used by all modules
- `input_files/`          – CSV export file(s) used as input (default: `input_files/s5ExportTest2.csv`)
- `chemview_archive/`     – default output archive root for per-CAS folders

Quick start (Windows cmd.exe)
-----------------------------
1) (Optional) Create and activate your virtual environment, then install dependencies.

```cmd
cd C:\openSource\dataPreservation\chemView\harvest
python -m venv .venv
.venv\Scripts\activate
```

2) Install required Python packages (Playwright and requests). If you already have a `requirements.txt` for the project, prefer that. Otherwise:

```cmd
pip install playwright requests
python -m playwright install chromium
```

3) Run a short smoke test (headless recommended for CI/test runs):

```cmd
python harvestSection5.py --max-downloads 1 --headless
```

Or run with the browser visible (useful while developing driver navigation):

```cmd
python harvestSection5.py --max-downloads 1
```

What the smoke test does
- Opens the CSV specified by `--input-file` (default `input_files/s5ExportTest2.csv`).
- Attempts up to `--max-downloads` driver calls that need downloads (rows already completed in the DB are skipped and do not count against `--max-downloads`).
- Reuses a single Playwright browser/page across driver calls for significantly better performance.
- Logs results to the DB via `HarvestDB.log_success`/`log_failure` and prints a heartbeat line to the console for each processed row.

Driver interface (how to write a new driver)
-------------------------------------------
The framework expects a driver callable with the following signature (positional and keyword args accepted):

- Parameters (informal):
  - `url` (str) – the starting ChemView URL for this row
  - `cas_dir` (pathlib.Path) – location to save harvested artifacts for this CAS
  - `need_html_download` (bool) – whether to attempt HTML capture
  - `need_pdf_download` (bool) – whether to attempt PDF download
  - `debug_out` (pathlib.Path or str) – folder to save debug artifacts
  - `headless` (bool) – run headless or visible
  - `LOG_FILE` (optional) – optional path/name of logfile previously used
  - `browser` (optional) – Playwright Browser instance to reuse (may be None)
  - `page` (optional) – Playwright Page instance to reuse (may be None)

- Return value: a dict with at least the keys `'html'` and `'pdf'`. Each value is a dict containing:
  - `success` (bool) — whether the artifact was successfully saved
  - `local_file_path` (str|null) — filesystem path to the saved artifact or None
  - `error` (str|null) — error message on failure, or None
  - `navigate_via` (str) — descriptive string identifying how the artifact was reached (used to populate the DB `navigate_via` field)

Example return structure (illustrative):

```json
{
  "html": {"success": true, "local_file_path": "chemview_archive/CAS-.../section5summary.html", "error": null, "navigate_via": "main_url"},
  "pdf":  {"success": false, "local_file_path": null, "error": "404 not found", "navigate_via": "section5_html"}
}
```

Driver best practices
- Accept and reuse `browser` and `page` if provided; only create/close Playwright resources if the driver created them.
- Keep `navigate_via` meaningful (e.g., `main_url`, `section5_html`, `8E_modal`) so DB callers can later filter by navigation method.
- Save debug artifacts to `debug_out` and normal artifacts to `cas_dir`.
- Avoid excessive fixed sleeps; prefer `wait_for_selector()` with sensible timeouts for robustness and speed.

How to add a new report type (brief)
-----------------------------------
1) Create `drive_<report>_download.py` implementing the driver signature and return contract above.
2) Add a thin wrapper script `harvest<report>.py` (copy `harvestSection5.py`) that imports the new driver and calls:

```python
run_harvest(CONFIG, drive_<report>_download, FileTypes)
```

3) Run and iterate. Tests with `--max-downloads 1` are useful during development.

Logging and configuration
-------------------------
- Logging is centralized via `logging_setup.initialize_logging()`. Ensure your `logging_setup.py` is configured to route logs to your desired file/location.
- The framework calls `initialize_logging()` before running; drivers should use `logging.getLogger(__name__)` for module-level logs so they follow the same configuration.

Troubleshooting
---------------
- If Playwright fails to start at the top of the framework run, the framework logs a warning and each driver call will create/close Playwright resources on demand (slower).
- If downloads are slower than expected, the bottleneck is usually network bandwidth or remote server speed (PDF downloads). Consider running multiple downloads in parallel if you have sufficient network capacity and the remote server allows it.
- If DB updates are not appearing, check `HarvestDB.py` for the DB path being used and confirm `CONFIG.db_path` points at the DB you expect.

Next steps & suggestions
------------------------
- Add `drive_8e_download.py` and a matching wrapper `harvest8E.py` that calls `run_harvest()` with your new driver.
- Add unit tests for `fixup_url()` and `do_need_download()` in `harvest_framework.py` if you want automated regression testing.
- If you plan parallel downloads, we can extend the framework to maintain a pool of pages or contexts (larger change).

Contact / developer notes
-------------------------
If you want, I can:
- scaffold a new driver template for 8E and a small wrapper for you to iterate, or
- run a headless smoke test here and paste the logs for inspection.


Thank you — when you finish manual testing, tell me the results and I will help iterate further.
