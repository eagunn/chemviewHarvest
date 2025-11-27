# This code diverges somewhat from the version I've used in multiple projects.
# For example, it handles a %2F encoding found in the PDF links.
# And I've converted to using official logging instead of print statements.

from bs4 import BeautifulSoup
import json
import os
from pathlib import Path
import requests
import time
import urllib.parse
import logging
import re

# module-level logger
logger = logging.getLogger(__name__)

SLEEP_SECONDS_AFTER_DOWNLOAD = 1
def makeAndChangeToFolder(folderName):
    if not os.path.exists(folderName):
        os.makedirs(folderName)
        logger.debug(f"Made new folder and will change to it: {os.getcwd()}")
    os.chdir(folderName)



def extract_filename_from_url(downloadURL: str) -> str:
    """Extract a safe filename from a URL.

    - Prefer an uninterpreted 'filename=' query value when present (preserve percent-encoding).
    - Otherwise use the basename of the path.
    - Replace any percent-encoded forward slashes (%2F) with '_' so they cannot be interpreted
      as path separators, and replace literal '/' or '\\' with '_'.
    - Strip or replace characters illegal on Windows (<>:"|?*) and control characters with '_'.
    - Return a non-empty filename (falls back to 'unknown_download').
    """
    parsed = urllib.parse.urlparse(downloadURL)
    filename = None

    # Look for an uninterpreted 'filename=' key in the raw query string to preserve percent-encoding
    q = parsed.query or ''
    if 'filename=' in q:
        for pair in q.split('&'):
            if pair.startswith('filename='):
                filename = pair[len('filename='):]
                break

    if not filename:
        # fallback to path basename (may be percent-encoded)
        filename = os.path.basename(parsed.path) or ''

    # Special-case common generic filename 'content.pdf': use the parent path segment
    # as a more informative filename when available. Do this *before* the later
    # %2F-handling and sanitization.
    try:
        if filename and filename.lower() == 'content.pdf':
            # Decode percent-encodings in the path and split on '/'
            path_parts = [p for p in urllib.parse.unquote(parsed.path).split('/') if p]
            if len(path_parts) >= 2:
                # take the immediate parent segment (the segment before 'content.pdf')
                parent_seg = path_parts[-2]
                ext = os.path.splitext(filename)[1] or ''
                filename = f"{parent_seg}{ext}"
                logger.info("Converted generic filename 'content.pdf' to '%s' using URL path", filename)
    except Exception:
        logger.exception("Error while handling content.pdf special-case for URL: %s", downloadURL)

    # If the filename contains percent-encoded forward-slash sequences (%2F),
    # take only the part after the final %2F (case-insensitive). This mirrors
    # browser behavior where the last segment is the actual file name.
    if filename and re.search(r'(?i)%2f', filename):
        parts = re.split(r'(?i)%2f', filename)
        if parts:
            filename = parts[-1]

    # Trim quotes/whitespace
    filename = filename.strip('"\'" ')

    # Normalize percent-encoded forward slashes to underscores so they cannot become path separators
    filename = re.sub(r'(?i)%2f', '_', filename)
    # Replace any literal path separators with underscore
    filename = filename.replace('/', '_').replace('\\', '_')

    # Sanitize filename: remove characters illegal on Windows and replace with underscore
    forbidden = set('<>:"|?*')
    safe_chars = []
    for ch in filename:
        # control characters and forbidden characters get converted to underscores
        if ord(ch) < 32 or ch in forbidden:
            safe_chars.append('_')
        else:
            safe_chars.append(ch)
    safe = ''.join(safe_chars).strip()

    if not safe:
        safe = 'unknown_download'

    logger.debug("extracted filename: %s from URL: %s", safe, downloadURL)
    return safe


def getOneFile(downloadURL, stats):
    logger.debug("in getOneFile for: %s", downloadURL)
    # Derive the local filename from either the 'filename' query param or the path basename
    filename = extract_filename_from_url(downloadURL)
    logger.debug("derived local filename: %s", filename)
    if os.path.exists(filename):
        logger.info("skipping %s already exists", filename)
        stats["skipCount"] += 1
    else:
        logger.info("about to get: %s", downloadURL)
        downloadOk = False
        try:
            # Some browser block non-browser clients, so we set a User-Agent header
            # to mimic a common browser.
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                # Optionally add "Referer" if the site requires it
            }
            response = requests.get(downloadURL, headers=headers, stream=True, timeout=30)
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
            if response.status_code == 200:  # 200 means the file exists
                with open(filename, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info("File written successfully to: %s", os.path.abspath(filename))
                stats["downloadCount"] += 1
                downloadOk = True
                time.sleep(SLEEP_SECONDS_AFTER_DOWNLOAD)  # pause between files
                if stats["downloadCount"] % 10 == 0:
                    # don't write this to log file, want to see in the terminal
                    print("proof of life, download count is:", stats["downloadCount"])
            elif response.status_code == 404:
                logger.warning("File not found: %s", downloadURL)
            else:
                logger.warning("Request failed with status code: %s for %s", response.status_code, downloadURL)
        except requests.exceptions.MissingSchema as e:
            logger.error("Error: Invalid URL - %s", e)
        except requests.exceptions.RequestException as e:
            logger.error("Error during download: %s", e)
        except OSError as e:
            logger.error("Error saving file: %s", e)
        except Exception as e:  # Catch any other type of error.
            logger.exception("An unexpected error occurred: %s", e)
        if downloadOk == False:
            stats["errorCount"] += 1

def savePage(pageToSave):
    logger.info("in savePage for: %s", pageToSave["url"])
    if os.path.exists(pageToSave["filename"]):
        logger.info("skipping %s already exists", pageToSave["filename"])
    else:
        pageSoup = None
        response = None
        try:
            response = requests.get(pageToSave["url"], stream=True)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            response.encoding = 'utf-8'
            pageSoup = BeautifulSoup(response.content, "html.parser")
        except FileNotFoundError:
            logger.error("***Error: Page not found at %s", pageToSave["url"])
        except Exception as e:
            logger.exception("***An error occurred: %s", e)

        if pageSoup is not None:
            try:
                with open(pageToSave["filename"], "w", encoding="utf-8") as file:
                    # Prefer to write the raw response text if available, otherwise fall back to the parsed HTML
                    if response is not None:
                        file.write(response.text)
                    else:
                        file.write(pageSoup.prettify())
                logger.info("current page saved to: %s", pageToSave["filename"])
            except Exception as e:
                logger.exception("Failed to write page to %s: %s", pageToSave["filename"], e)


# Lordy, lordy a legitimate use for recursion!
def processNestedDictionary(nestedDict, stats, stop_path):
    makeAndChangeToFolder(nestedDict["folder"])

    # Process downloadList
    for fileUrl in nestedDict.get("downloadList", []):
        if mustStop(stop_path):
            # terminate recursion with prejudice
            return
        getOneFile(fileUrl, stats)

    if nestedDict.get("pageToSave", "") != "":
        if mustStop(stop_path):
            # terminate recursion with prejudice
            return
        savePage(nestedDict["pageToSave"])

    # Recurse into subfolders
    for subfolder in nestedDict.get("subfolderList", []):
        if mustStop(stop_path):
            # terminate recursion with prejudice
            return
        processNestedDictionary(subfolder, stats, stop_path)

    os.chdir("..")  # Move back up after processing

def mustStop(stop_path: os.PathLike) -> bool:
    must_stop = False
    try:
        p = Path(stop_path)
        if p.exists():
            logger.info("Stop file detected at %s; terminating recursion with prejudice.", p)
            print("Stop file detected; terminating getFiles recursion.")
            must_stop = True
    except Exception:
        logger.exception("Error while checking stop file path: %s", stop_path)
    return must_stop

def main():
    import sys

    # Require the input JSON file path as the first argument. Exit code 2 if missing.
    if len(sys.argv) < 2 or not sys.argv[1].strip():
        print("Usage: python getFiles.py <path to download-files-json>", file=sys.stderr)
        sys.exit(2)

    jsonPath = sys.argv[1]

    # We need a way to get this long-running process to terminate gracefully
    # We use a semaphore file in the current working directory
    stop_path = Path.cwd() / Path("getFiles.stop")
    logger.info("Will watch for stop file: %s", stop_path)

    # configure logging to write to get.log (append mode)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(name)s:%(lineno)d] %(message)s',
        handlers=[logging.FileHandler('getFiles.log', mode='a', encoding='utf-8')]
    )
    logger.info("in main, about to open json file")
    startTime = time.time()
    logger.info("Start: %s", time.ctime(startTime))

    downloadDict = {}
    try:
        with open(jsonPath, "r") as json_file:
            downloadDict = json.load(json_file)
            logger.info("Dictionary loaded from %s", jsonPath)
    except OSError as e:
        logger.error("Error loading dictionary from file: %s", e)
        sys.exit(3)
    except json.JSONDecodeError as e:
        logger.error("Error decoding json: %s", e)
        sys.exit(3)

    stats = { "downloadCount" : 0, "errorCount" : 0, "skipCount" : 0 }
    processNestedDictionary(downloadDict, stats, stop_path)
    logger.info(json.dumps(stats, indent=4))
    endTime = time.time()
    logger.info("End: %s", time.ctime(endTime))
    logger.info("Elapsed time: %s seconds", endTime - startTime)

    # Exit with an appropriate code: 0=success, 1=processing errors
    # 2025 11 25 -- seeing one file fail out of hundreds, so treat as non-fatal
    # but let the user see if she is paying attention
    error_count = stats.get("errorCount", 0)
    if error_count > 0:
        print("Completed with some errors; see log for details.")
        print(stats)
    sys.exit(0)

main()