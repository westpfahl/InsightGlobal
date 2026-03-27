"""
cms_downloader.py
-----------------
Production-grade script to download all CMS Provider Data datasets
tagged under the "Hospitals" theme.

Features:
  - Fetches dataset catalog from CMS metastore API
  - Filters datasets by theme = "Hospitals"
  - Downloads CSV files in parallel (configurable thread pool)
  - Converts all column headers to snake_case
  - Tracks run metadata in a local JSON state file so only
    files modified since the previous run are re-downloaded
  - Structured logging to both console and rotating log file
  - Designed to be scheduled daily (cron / Task Scheduler)

Usage:
  python cms_downloader.py [--output-dir ./data] [--workers 4] [--force]

Author: Keith Westpfahl
Last updated: 2026-03-27
"""

import argparse
import csv
import hashlib
import io
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from loguru import logger
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CMS_METASTORE_URL = (
    "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
)
TARGET_THEME = "Hospitals"
STATE_FILE = "run_state.json"
LOG_FILE = "cms_downloader.log"
DEFAULT_OUTPUT_DIR = "./data"
DEFAULT_WORKERS = 4
REQUEST_TIMEOUT = 60          # seconds per HTTP request
MAX_RETRIES = 3
BACKOFF_FACTOR = 1.5


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
def setup_logging(log_dir: Path) -> logging.Logger:
    """
    Configure structured logging to console + rotating file.
    Rotating file keeps 5 x 5MB logs — appropriate for a daily job
    that may produce verbose output over time.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("cms_downloader")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(threadName)-20s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # Console handler — INFO and above
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    # rotating file handler captures DEBUG details for audit trail without filling the disk, max 25MB across 5 files, 5MB per file
    fh = RotatingFileHandler(
        log_dir / LOG_FILE,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(ch)   # ch = ConsoleHandler  → sends logs to your terminal
    logger.addHandler(fh)   # fh = FileHandler     → sends logs to a .log file
    return logger


# ---------------------------------------------------------------------------
# HTTP session with retry logic
# ---------------------------------------------------------------------------
def build_session() -> requests.Session:
    """
    use a session with retry + backoff rather than
    bare requests.get() calls. CMS API can return transient 5xx errors.
    """
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR, # exponential backoff: 1s, 1.5s, 2.25s between retries
        status_forcelist=[429, 500, 502, 503, 504], # force a retry on these status codes
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "CMS-Hospital-Downloader/1.0"})
    return session


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------
def load_state(state_path: Path) -> dict:
    """
    Load previous run state from JSON file.
    State stores: { dataset_identifier: { "modified": ISO str, "md5": str } }

    Persist state to a durable file rather than relying
    on filesystem mtime, which can be unreliable across OS/copy operations.
    """
    if state_path.exists():
        with open(state_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(state_path: Path, state: dict) -> None:
    """Atomically write state file using a temp file + rename."""
    tmp = state_path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)
    # Atomic rename — avoids corrupt state if process is killed mid-write
    tmp.replace(state_path)


# ---------------------------------------------------------------------------
# Column name normalization
# ---------------------------------------------------------------------------
def to_snake_case(name: str) -> str:
    """
    Convert a mixed-case column name with spaces and special characters
    to snake_case.

    Examples:
      "Patients' rating of the facility linear mean score" -> "patients_rating_of_the_facility_linear_mean_score"
      "Hospital Name"    -> "hospital_name"
      "ZIP Code"         -> "zip_code"
      "% of patients"    -> "percent_of_patients"
    """
    # 1. Lowercase everything
    s = name.lower()
    # 2. Add 'percent' prefix for columns that start with % or contain 'percent'
    s = re.sub(r"%", "percent", s)
    # 3. Replace apostrophes/special punctuation with nothing (possessives etc.)
    s = re.sub(r"[''\".,;:!?@#$%^&*()\[\]{}<>/\\|`~]", "", s)
    # 4. Replace any run of non-alphanumeric characters with a single underscore
    s = re.sub(r"[^a-z0-9]+", "_", s)
    # 5. Strip leading/trailing underscores
    s = s.strip("_")
    
    return s


# ---------------------------------------------------------------------------
# CMS metastore helpers
# ---------------------------------------------------------------------------
def fetch_hospital_datasets(session: requests.Session, logger: logging.Logger) -> list:
    """
    Fetch all datasets from the CMS metastore and filter to those
    whose theme list contains TARGET_THEME ("Hospitals").
    """
    logger.info("Fetching dataset catalog from CMS metastore...")
    resp = session.get(CMS_METASTORE_URL, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    all_datasets = resp.json()
    logger.info(f"Total datasets in catalog: {len(all_datasets)}")

    hospital_datasets = []
    for ds in all_datasets:
        themes = ds.get("theme", [])
        # theme can be a list of strings or list of dicts
        theme_values = []
        for t in themes:
            if isinstance(t, str):
                theme_values.append(t)
            # handle case where theme is a dict with a 'data' key containing the theme name
            elif isinstance(t, dict): 
                theme_values.append(t.get("data", ""))

        if any(TARGET_THEME.lower() in tv.lower() for tv in theme_values):
            hospital_datasets.append(ds)

    logger.info(f"Datasets matching theme '{TARGET_THEME}': {len(hospital_datasets)}")
    return hospital_datasets


def extract_download_url(dataset: dict) -> Optional[str]:
    """
    Navigate the CMS dataset structure to find the CSV download URL.
    The distribution list contains objects with mediaType = 'text/csv'.
    """
    for dist in dataset.get("distribution", []):
        # distribution entries may be dicts with a 'data' sub-key
        data = dist.get("data", dist)
        media_type = data.get("mediaType", "")
        if "csv" in media_type.lower():
            return data.get("downloadURL")
    return None


def get_dataset_modified(dataset: dict) -> Optional[str]:
    """Extract the last-modified timestamp from dataset metadata."""
    return dataset.get("modified") or dataset.get("issued")


# ---------------------------------------------------------------------------
# Core download + transform worker
# ---------------------------------------------------------------------------
def process_dataset(
    dataset: dict,
    output_dir: Path,
    state: dict,
    session: requests.Session,
    logger: logging.Logger,
    force: bool = False,
) -> dict:
    """
    Download a single dataset CSV, rename columns to snake_case,
    and write to output_dir.

    Returns a result dict for state tracking and reporting.
    """
    identifier = dataset.get("identifier", "unknown")
    title = dataset.get("title", identifier)
    modified = get_dataset_modified(dataset)

    result = {
        "identifier": identifier,
        "title": title,
        "status": None,
        "file": None,
        "modified": modified,
        "md5": None,
        "rows": None,
        "columns": None,
        "error": None,
    }

    # -----------------------------------------------------------------------
    # Skip check: only process if modified since last run (unless --force)
    # -----------------------------------------------------------------------
    if not force and identifier in state:
        prev = state[identifier]
        if prev.get("modified") == modified and prev.get("md5"):
            logger.debug(f"SKIP (not modified): {title}")
            result["status"] = "skipped"
            result["md5"] = prev.get("md5")
            return result

    # -----------------------------------------------------------------------
    # Find download URL
    # -----------------------------------------------------------------------
    download_url = extract_download_url(dataset)
    if not download_url:
        logger.warning(f"No CSV download URL found for: {title}")
        result["status"] = "no_url"
        return result

    # -----------------------------------------------------------------------
    # Download
    # -----------------------------------------------------------------------
    logger.info(f"Downloading: {title}")
    logger.debug(f"  URL: {download_url}")

    try:
        resp = session.get(download_url, timeout=REQUEST_TIMEOUT, stream=True)
        resp.raise_for_status()
        raw_bytes = resp.content
    except requests.RequestException as e:
        logger.error(f"Download failed [{title}]: {e}")
        result["status"] = "download_error"
        result["error"] = str(e)
        return result

    # -----------------------------------------------------------------------
    # Compute MD5 for change detection
    # Hashing the raw file to detect content changes even
    # when the modified timestamp hasn't been updated by the source.
    # -----------------------------------------------------------------------
    md5 = hashlib.md5(raw_bytes).hexdigest()

    if not force and identifier in state:
        if state[identifier].get("md5") == md5:
            logger.debug(f"SKIP (content unchanged, same MD5): {title}")
            result["status"] = "skipped"
            result["md5"] = md5
            return result

    # -----------------------------------------------------------------------
    # Parse CSV + rename columns to snake_case
    # -----------------------------------------------------------------------
    try:
        text = raw_bytes.decode("utf-8-sig")   # utf-8-sig strips BOM if present
        reader = csv.DictReader(io.StringIO(text))
        original_fields = reader.fieldnames or []

        if not original_fields:
            logger.warning(f"No columns found in CSV for: {title}")
            result["status"] = "empty_csv"
            return result

        # Build column mapping: original → snake_case
        snake_fields = [to_snake_case(f) for f in original_fields]

        # Detect and resolve duplicate snake_case names
        # (e.g., "Hospital Name" and "hospital name" both → "hospital_name")
        seen: dict[str, int] = {}
        deduped_fields = []
        for sf in snake_fields:
            if sf in seen:
                seen[sf] += 1
                deduped_fields.append(f"{sf}_{seen[sf]}")
                logger.warning(
                    f"Duplicate snake_case column '{sf}' in {title} — "
                    f"renamed to '{sf}_{seen[sf]}'"
                )
            else:
                seen[sf] = 0
                deduped_fields.append(sf)

        # Write transformed CSV
        safe_title = re.sub(r"[^a-z0-9_]", "_", title.lower()).strip("_")
        safe_title = re.sub(r"_+", "_", safe_title)
        out_file = output_dir / f"{safe_title}.csv"

        rows_written = 0
        with open(out_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=deduped_fields)
            writer.writeheader()
            for row in reader:
                renamed = {
                    deduped_fields[i]: row[original_fields[i]]
                    for i in range(len(original_fields))
                }
                writer.writerow(renamed)
                rows_written += 1

        logger.info(
            f"  ✓ {title} → {out_file.name} "
            f"({rows_written} rows, {len(deduped_fields)} cols)"
        )

        result.update(
            {
                "status": "downloaded",
                "file": str(out_file),
                "md5": md5,
                "rows": rows_written,
                "columns": len(deduped_fields),
                "column_mapping": dict(zip(original_fields, deduped_fields)),
            }
        )

    except Exception as e:
        logger.error(f"Processing failed [{title}]: {e}", exc_info=True)
        result["status"] = "processing_error"
        result["error"] = str(e)

    return result


# ---------------------------------------------------------------------------
# Run summary report
# ---------------------------------------------------------------------------
def write_run_report(report_path: Path, results: list, run_meta: dict) -> None:
    """
    Write a JSON run report summarizing outcomes for every dataset.
    Maintain a run report separate from state — useful
    for auditing, alerting, and debugging without parsing state files.
    """
    report = {
        "run_metadata": run_meta,
        "summary": {
            "total": len(results),
            "downloaded": sum(1 for r in results if r["status"] == "downloaded"),
            "skipped": sum(1 for r in results if r["status"] == "skipped"),
            "errors": sum(
                1 for r in results if r["status"] not in ("downloaded", "skipped")
            ),
        },
        "datasets": results,
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Download CMS Hospital datasets and normalize column names."
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory to write processed CSV files (default: ./data)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Number of parallel download threads (default: 4)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download all files regardless of modification state",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger = setup_logging(output_dir)

    run_start = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info(f"CMS Hospital Downloader — run started: {run_start.isoformat()}")
    logger.info(f"Output directory : {output_dir.resolve()}")
    logger.info(f"Parallel workers : {args.workers}")
    logger.info(f"Force re-download: {args.force}")
    logger.info("=" * 60)

    state_path = output_dir / STATE_FILE
    state = load_state(state_path)
    logger.info(
        f"Loaded state: {len(state)} previously tracked datasets"
    )

    session = build_session()

    # Fetch catalog
    try:
        hospital_datasets = fetch_hospital_datasets(session, logger)
    except Exception as e:
        logger.critical(f"Failed to fetch dataset catalog: {e}", exc_info=True)
        sys.exit(1)

    if not hospital_datasets:
        logger.warning("No hospital datasets found. Exiting.")
        sys.exit(0)

    # -----------------------------------------------------------------------
    # Parallel download using ThreadPoolExecutor
    # I/O-bound work like HTTP downloads benefits from
    # threads. For CPU-bound transforms, ProcessPoolExecutor would be better.
    # -----------------------------------------------------------------------
    results = []
    new_state = dict(state)  # copy — update as jobs complete

    with ThreadPoolExecutor(
        max_workers=args.workers,
        thread_name_prefix="cms-worker"
    ) as executor:
        futures = {
            executor.submit(
                process_dataset,
                ds,
                output_dir,
                state,
                session,
                logger,
                args.force,
            ): ds
            for ds in hospital_datasets
        }

        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)

                # Update state for successfully downloaded or skipped datasets
                if result["status"] in ("downloaded", "skipped"):
                    new_state[result["identifier"]] = {
                        "modified": result["modified"],
                        "md5": result["md5"],
                        "last_seen": run_start.isoformat(),
                        "title": result["title"],
                    }
            except Exception as e:
                ds = futures[future]
                logger.error(
                    f"Unexpected error for {ds.get('title', 'unknown')}: {e}",
                    exc_info=True,
                )
                results.append(
                    {
                        "identifier": ds.get("identifier"),
                        "title": ds.get("title"),
                        "status": "unexpected_error",
                        "error": str(e),
                    }
                )

    # -----------------------------------------------------------------------
    # Persist updated state
    # -----------------------------------------------------------------------
    save_state(state_path, new_state)
    logger.info(f"State saved: {len(new_state)} datasets tracked -> {state_path}")

    # -----------------------------------------------------------------------
    # Run report
    # -----------------------------------------------------------------------
    run_end = datetime.now(timezone.utc)
    run_meta = {
        "run_start": run_start.isoformat(),
        "run_end": run_end.isoformat(),
        "duration_seconds": round((run_end - run_start).total_seconds(), 2),
        "workers": args.workers,
        "force": args.force,
        "output_dir": str(output_dir.resolve()),
    }

    report_name = f"run_report_{run_start.strftime('%Y%m%d_%H%M%S')}.json"
    report_path = output_dir / report_name
    write_run_report(report_path, results, run_meta)

    # -----------------------------------------------------------------------
    # Final summary log
    # -----------------------------------------------------------------------
    downloaded = sum(1 for r in results if r["status"] == "downloaded")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    errors = sum(
        1 for r in results if r["status"] not in ("downloaded", "skipped")
    )

    logger.info("=" * 60)
    logger.info(f"Run complete in {run_meta['duration_seconds']}s")
    logger.info(f"  Downloaded : {downloaded}")
    logger.info(f"  Skipped    : {skipped}  (not modified since last run)")
    logger.info(f"  Errors     : {errors}")
    logger.info(f"  Run report : {report_path}")
    logger.info("=" * 60)

    # Exit non-zero if any errors — important for scheduler alerting
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
