# This code has been tested against the downloadDict.json files from multiple parsePage
# scripts.

from bs4 import BeautifulSoup, Tag
import json
import os
import requests
import time
import urllib.parse
import logging

# module-level logger
logger = logging.getLogger(__name__)

SLEEP_SECONDS_AFTER_DOWNLOAD = 1
def makeAndChangeToFolder(folderName):
    if not os.path.exists(folderName):
        os.makedirs(folderName)
    os.chdir(folderName)
    logger.info(f"About to process files for folder: {folderName}")


def extract_filename_from_url(downloadURL: str) -> str:
    """Extract a filename from a URL.
    - If the query contains a 'filename=' parameter, return its raw value (preserve percent-encoding).
    - Otherwise return the basename of the path.
    The returned filename is sanitized to remove characters not allowed in filenames.
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
        # fallback to path basename (already not percent-decoded)
        filename = os.path.basename(parsed.path)

    # Sanitize filename: remove characters illegal on Windows and replace with underscore
    # Preserve percent-encoding like '%2F' (do not unquote)
    forbidden = set('<>:"/\\|?*')
    safe_chars = []
    for ch in filename:
        if ch in forbidden or ord(ch) < 32:
            safe_chars.append('_')
        else:
            safe_chars.append(ch)
    safe = ''.join(safe_chars).strip()
    if not safe:
        safe = 'unknown_download'
    # Replace percent signs with underscores to avoid percent characters in local filenames
    safe = safe.replace('%', '_')
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
            response = requests.get(downloadURL)
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
            if response.status_code == 200:  # 200 means the file exists

                with open(filename, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info("File written successfully to: %s", filename)
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
        try:
            response = requests.get(pageToSave["url"])
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            response.encoding = 'utf-8'
            pageSoup = BeautifulSoup(response.content, "html.parser")
            # print("have collection page!", file=log)
        except FileNotFoundError:
            logger.error("***Error: Page not found at %s", pageToSave["url"])
        except Exception as e:
            logger.exception("***An error occurred: %s", e)

        if pageSoup is not None:
            with open(pageToSave["filename"], "w", encoding="utf-8") as file:
                file.write(response.text)
                logger.info("current page saved to: %s", pageToSave["filename"])


# Lordy, lordy a legitimate use for recursion!
def processNestedDictionary(nestedDict, stats):
    makeAndChangeToFolder(nestedDict["folder"])

    # Process downloadList
    for fileUrl in nestedDict.get("downloadList", []):
        getOneFile(fileUrl, stats)

    if nestedDict.get("pageToSave", "") != "":
        savePage(nestedDict["pageToSave"])

    # Recurse into subfolders
    for subfolder in nestedDict.get("subfolderList", []):
        processNestedDictionary(subfolder, stats)

    os.chdir("..")  # Move back up after processing

def main():
    import sys

    # Require the input JSON file path as the first argument. Exit code 2 if missing.
    if len(sys.argv) < 2 or not sys.argv[1].strip():
        print("Usage: python getFiles.py <path-to-download-json>", file=sys.stderr)
        sys.exit(2)

    jsonPath = sys.argv[1]

    # configure logging to write to get.log (overwrite each run)
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s [%(name)s:%(lineno)d] %(message)s',
        handlers=[logging.FileHandler('getFiles.log', mode='w', encoding='utf-8')]
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
        logger.error("Error reading from file: %s", e)
        sys.exit(3)
    except json.JSONDecodeError as e:
        logger.error("Error decoding json: %s", e)
        sys.exit(3)

    stats = { "downloadCount" : 0, "errorCount" : 0, "skipCount" : 0 }
    processNestedDictionary(downloadDict, stats)
    logger.info(json.dumps(stats, indent=4))
    endTime = time.time()
    logger.info("End: %s", time.ctime(endTime))
    logger.info("Elapsed time: %s seconds", endTime - startTime)

    # Exit with an appropriate code: 0=success, 1=processing errors
    if stats.get("errorCount", 0) > 0:
        sys.exit(1)
    sys.exit(0)

main()