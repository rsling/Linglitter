#!/usr/bin/env python3
"""
Download open-access PDFs using the Unpaywall API.

Queries Unpaywall for DOIs in linglitter.db, downloads available PDFs,
and tracks download status in the database.

Usage:
    python scrape_pdfs.py
    python scrape_pdfs.py --config myconfig.json
    python scrape_pdfs.py --limit 100
    python scrape_pdfs.py --dry-run
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote, urlparse

import requests

UNPAYWALL_API = "https://api.unpaywall.org/v2"

log = logging.getLogger(__name__)

# Browser-like headers for session requests
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "DNT": "1",
    "Sec-GPC": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Priority": "u=0, i",
}

# Per-publisher session storage: {publisher: (session, last_used_datetime)}
_publisher_sessions = {}


def get_publisher_session(publisher):
    """Get or create a requests session for a publisher.

    Sessions are reused across downloads from the same publisher,
    preserving cookies and appearing more like normal browser behavior.

    Returns (session, is_new, elapsed_str) where:
    - is_new: True if a fresh session was created
    - elapsed_str: For continued sessions, time since last use as "HH:MM"
    """
    now = datetime.now()
    if publisher not in _publisher_sessions:
        session = requests.Session()
        session.headers.update(BROWSER_HEADERS)
        _publisher_sessions[publisher] = (session, now)
        return session, True, None
    session, last_used = _publisher_sessions[publisher]
    elapsed = now - last_used
    total_minutes = int(elapsed.total_seconds() // 60)
    hours, minutes = divmod(total_minutes, 60)
    elapsed_str = f"{hours:02d}:{minutes:02d}"
    _publisher_sessions[publisher] = (session, now)
    return session, False, elapsed_str


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def load_config(config_path):
    """Load configuration from JSON file."""
    with open(config_path) as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def ensure_schema(conn):
    """Add PDF-related columns if they don't exist (for older databases)."""
    for col, coltype in [
        ("availability", "TEXT"),
        ("source", "TEXT"),
        ("attempts", "INTEGER DEFAULT 0"),
        ("response", "INTEGER DEFAULT 0"),
        ("timestamp", "TEXT"),
        ("file", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE articles ADD COLUMN {col} {coltype}")
        except sqlite3.OperationalError:
            pass  # Column already exists
    conn.commit()


def get_random_candidate(conn, years, journals):
    """Select a random article that needs OA checking/downloading.

    Excludes:
    - Articles already downloaded (file IS NOT NULL)
    - Articles confirmed as no-oa (availability = 'no-oa')
    """
    placeholders = ",".join("?" for _ in journals)
    query = f"""
        SELECT doi, publisher, journal, year
        FROM articles
        WHERE year >= ? AND year <= ?
          AND journal IN ({placeholders})
          AND file IS NULL
          AND (availability IS NULL OR availability != 'no-oa')
        ORDER BY RANDOM()
        LIMIT 1
    """
    params = [years[0], years[1]] + journals
    row = conn.execute(query, params).fetchone()
    if row:
        return {"doi": row[0], "publisher": row[1], "journal": row[2], "year": row[3]}
    return None


def get_last_global_timestamp(conn):
    """Get the timestamp of the most recent download attempt."""
    row = conn.execute("""
        SELECT MAX(timestamp) FROM articles WHERE timestamp IS NOT NULL
    """).fetchone()
    return row[0] if row and row[0] else None


def get_last_publisher_timestamp(conn, publisher):
    """Get the timestamp of the most recent download attempt for a publisher."""
    row = conn.execute("""
        SELECT MAX(timestamp) FROM articles
        WHERE publisher = ? AND timestamp IS NOT NULL
    """, (publisher,)).fetchone()
    return row[0] if row and row[0] else None


def update_article(conn, doi, availability, source, attempts, response, timestamp, file_path):
    """Update an article's PDF-related fields."""
    conn.execute("""
        UPDATE articles
        SET availability = ?,
            source = ?,
            attempts = ?,
            response = ?,
            timestamp = ?,
            file = ?
        WHERE doi = ?
    """, (availability, source, attempts, response, timestamp, file_path, doi))
    conn.commit()


def get_article_attempts(conn, doi):
    """Get the current number of attempts for an article."""
    row = conn.execute("SELECT attempts FROM articles WHERE doi = ?", (doi,)).fetchone()
    return row[0] if row and row[0] else 0


# ---------------------------------------------------------------------------
# Politeness checks
# ---------------------------------------------------------------------------

def parse_timestamp(ts_str):
    """Parse ISO timestamp string to datetime."""
    if not ts_str:
        return None
    return datetime.fromisoformat(ts_str)


def check_politeness(conn, publisher, global_interval, publisher_interval):
    """Check if we can proceed without violating politeness intervals.

    Returns (ok, wait_seconds) where ok is True if we can proceed,
    or False with wait_seconds indicating how long to wait.
    """
    now = datetime.now()

    # Check global interval
    last_global = get_last_global_timestamp(conn)
    if last_global:
        last_dt = parse_timestamp(last_global)
        if last_dt:
            elapsed = (now - last_dt).total_seconds()
            if elapsed < global_interval:
                return False, global_interval - elapsed

    # Check per-publisher interval
    last_pub = get_last_publisher_timestamp(conn, publisher)
    if last_pub:
        last_dt = parse_timestamp(last_pub)
        if last_dt:
            elapsed = (now - last_dt).total_seconds()
            if elapsed < publisher_interval:
                return False, publisher_interval - elapsed

    return True, 0


# ---------------------------------------------------------------------------
# Unpaywall API
# ---------------------------------------------------------------------------

def query_unpaywall(doi, mailto):
    """Query Unpaywall API for a DOI.

    Returns dict with keys:
    - is_oa: bool
    - pdf_url: str or None (direct PDF URL if available)
    - landing_url: str or None (article landing page URL)
    - response_code: int
    """
    if not mailto:
        log.error("Unpaywall requires an email address (set 'mailto' in config)")
        return {"is_oa": False, "pdf_url": None, "landing_url": None, "response_code": 0}

    url = f"{UNPAYWALL_API}/{quote(doi, safe='')}"
    params = {"email": mailto}

    try:
        resp = requests.get(url, params=params, timeout=30)
        code = resp.status_code

        if code == 404:
            # DOI not found in Unpaywall
            return {"is_oa": False, "pdf_url": None, "landing_url": None, "response_code": code}

        if code != 200:
            log.warning("Unpaywall returned %d for %s", code, doi)
            return {"is_oa": False, "pdf_url": None, "landing_url": None, "response_code": code}

        data = resp.json()
        is_oa = data.get("is_oa", False)

        pdf_url = None
        landing_url = None
        if is_oa:
            # Try best_oa_location first
            best = data.get("best_oa_location")
            if best:
                pdf_url = best.get("url_for_pdf")
                landing_url = best.get("url_for_landing_page") or best.get("url")

            # Fall back to other locations if needed
            if not pdf_url:
                for loc in data.get("oa_locations", []):
                    pdf_url = loc.get("url_for_pdf")
                    landing_url = loc.get("url_for_landing_page") or loc.get("url")
                    if pdf_url:
                        break

        return {"is_oa": is_oa, "pdf_url": pdf_url, "landing_url": landing_url, "response_code": code}

    except requests.exceptions.RequestException as exc:
        log.warning("Unpaywall request failed for %s: %s", doi, exc)
        return {"is_oa": False, "pdf_url": None, "landing_url": None, "response_code": 0}


# ---------------------------------------------------------------------------
# PDF download
# ---------------------------------------------------------------------------

def encode_doi_for_filename(doi):
    """Encode DOI to be safe as a filename.

    Replaces /, \\, :, *, ?, ", <, >, |, and . with underscores.
    """
    unsafe_chars = r'[/\\:*?"<>|.]'
    return re.sub(unsafe_chars, "_", doi)


def build_pdf_path(data_dir, publisher, journal, year, doi):
    """Build the full path for storing a PDF.

    Returns (absolute_path, relative_path) where relative_path is stored in DB.
    """
    safe_doi = encode_doi_for_filename(doi)
    # Also sanitize publisher and journal names for filesystem
    safe_publisher = re.sub(r'[/\\:*?"<>|]', "_", publisher) if publisher else "unknown"
    safe_journal = re.sub(r'[/\\:*?"<>|]', "_", journal) if journal else "unknown"

    relative = Path(safe_publisher) / safe_journal / str(year) / f"{safe_doi}.pdf"
    absolute = Path(data_dir) / relative

    return absolute, str(relative)


def download_pdf(pdf_url, dest_path, landing_url=None, timeout=60, session=None):
    """Download a PDF from a URL.

    If landing_url is provided, first visits the landing page to collect
    session cookies, then downloads the PDF. This helps with publishers
    that require cookies for PDF access.

    If session is provided, uses that session (preserving cookies from
    previous requests to the same publisher). Otherwise creates a new session.

    Returns (success, http_status_code).
    """
    # Use provided session or create a new one
    if session is None:
        session = requests.Session()
        session.headers.update(BROWSER_HEADERS)

    try:
        # First visit landing page to collect cookies if provided
        if landing_url:
            log.debug("  Visiting landing page for cookies: %s", landing_url)
            try:
                landing_resp = session.get(landing_url, timeout=timeout, allow_redirects=True)
                log.debug("  Landing page status: %d, cookies: %d",
                         landing_resp.status_code, len(session.cookies))
                # Small delay to appear more human-like
                time.sleep(0.5)
            except requests.exceptions.RequestException as exc:
                log.debug("  Landing page visit failed: %s", exc)
                # Continue anyway, might still work

        # Set referer to landing page or PDF domain
        parsed = urlparse(pdf_url)
        referer = landing_url if landing_url else f"{parsed.scheme}://{parsed.netloc}/"
        session.headers["Referer"] = referer
        # Update Sec-Fetch for same-origin navigation
        session.headers["Sec-Fetch-Site"] = "same-origin"

        # Now fetch the PDF with PDF-specific Accept header
        session.headers["Accept"] = "application/pdf,*/*;q=0.9"
        resp = session.get(pdf_url, timeout=timeout, stream=True, allow_redirects=True)
        code = resp.status_code

        if code != 200:
            log.warning("Download failed with status %d: %s", code, pdf_url)
            return False, code

        # Check Content-Type to ensure we got a PDF, not an HTML error page
        content_type = resp.headers.get("Content-Type", "").lower()
        if "html" in content_type:
            log.warning("Got HTML instead of PDF (Content-Type: %s): %s", content_type, pdf_url)
            return False, 403  # Treat as forbidden

        # Ensure directory exists
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Write content
        with open(dest_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8192):
                fh.write(chunk)

        # Verify we got something reasonable (and it's actually a PDF)
        if dest_path.stat().st_size < 1000:
            log.warning("Downloaded file suspiciously small: %s", dest_path)
            dest_path.unlink()
            return False, code

        # Check PDF magic bytes
        with open(dest_path, "rb") as fh:
            magic = fh.read(5)
        if magic != b"%PDF-":
            log.warning("Downloaded file is not a PDF (magic: %s): %s", magic[:20], dest_path)
            dest_path.unlink()
            return False, 403

        return True, code

    except requests.exceptions.RequestException as exc:
        log.warning("Download request failed: %s", exc)
        return False, 0


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def process_one(conn, config, dry_run=False):
    """Process a single article.

    Returns:
    - "downloaded": successfully downloaded
    - "no-oa": confirmed not open access
    - "failed": download attempted but failed
    - "skipped": politeness violation, try again later
    - "exhausted": max attempts reached
    - None: no candidates left
    """
    years = config["years"]
    journals = config["journals"]
    unpaywall_cfg = config["unpaywall"]
    data_dir = config.get("data_dir", "data")

    # Get a random candidate
    candidate = get_random_candidate(conn, years, journals)
    if not candidate:
        return None, None

    doi = candidate["doi"]
    publisher = candidate["publisher"]
    journal = candidate["journal"]
    year = candidate["year"]

    # Check attempt count
    attempts = get_article_attempts(conn, doi)
    if attempts >= unpaywall_cfg["max_attempts"]:
        log.debug("Max attempts reached for %s", doi)
        return "exhausted", doi

    # Check politeness
    ok, wait = check_politeness(
        conn, publisher,
        unpaywall_cfg["politeness_interval"],
        unpaywall_cfg["publisher_interval"]
    )
    if not ok:
        log.debug("Politeness wait %.1fs for publisher %s", wait, publisher)
        return "skipped", doi

    log.info("Processing: %s", doi)

    if dry_run:
        log.info("  [DRY RUN] Would query Unpaywall and attempt download")
        return "skipped", doi

    # Query Unpaywall
    result = query_unpaywall(doi, unpaywall_cfg.get("mailto"))
    now = datetime.now().isoformat()
    attempts += 1

    if not result["is_oa"]:
        # Not open access
        log.info("  Not OA (Unpaywall response: %d)", result["response_code"])
        update_article(conn, doi,
                      availability="no-oa",
                      source=None,
                      attempts=attempts,
                      response=result["response_code"],
                      timestamp=now,
                      file_path=None)
        return "no-oa", doi

    pdf_url = result["pdf_url"]
    landing_url = result["landing_url"]

    if not pdf_url:
        log.warning("  OA but no PDF URL found")
        update_article(conn, doi,
                      availability="oa",
                      source=None,
                      attempts=attempts,
                      response=result["response_code"],
                      timestamp=now,
                      file_path=None)
        return "failed", doi

    log.info("  OA PDF: %s", pdf_url)
    if landing_url:
        log.debug("  Landing page: %s", landing_url)

    # Build paths
    abs_path, rel_path = build_pdf_path(data_dir, publisher, journal, year, doi)

    # Get or create session for this publisher (preserves cookies across downloads)
    session, is_new_session, elapsed_str = get_publisher_session(publisher)
    parsed_url = urlparse(pdf_url)
    server_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    if is_new_session:
        log.info("  Session started: %s (%s)", publisher, server_url)
    else:
        log.info("  Session continued: %s (%s) [%s]", publisher, server_url, elapsed_str)

    # Download (visit landing page first to collect cookies)
    success, http_code = download_pdf(pdf_url, abs_path, landing_url=landing_url, session=session)

    if success:
        log.info("  Downloaded: %s", rel_path)
        update_article(conn, doi,
                      availability="oa",
                      source=pdf_url,
                      attempts=attempts,
                      response=http_code,
                      timestamp=now,
                      file_path=rel_path)
        return "downloaded", doi
    else:
        log.warning("  Download failed (HTTP %d)", http_code)
        update_article(conn, doi,
                      availability="oa",
                      source=pdf_url,
                      attempts=attempts,
                      response=http_code,
                      timestamp=now,
                      file_path=None)
        return "failed", doi


def main():
    parser = argparse.ArgumentParser(
        description="Download open-access PDFs using Unpaywall API.")
    parser.add_argument("--config", type=str, default="config.json",
                        help="Path to config JSON (default: config.json)")
    parser.add_argument("--db", type=str, default="linglitter.db",
                        help="Path to SQLite database (default: linglitter.db)")
    parser.add_argument("--mailto", type=str, default=None,
                        help="Email for Unpaywall API (overrides config.json)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Maximum number of articles to process")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without downloading")
    parser.add_argument("--continuous", action="store_true",
                        help="Run continuously until no candidates remain")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    # Load config
    config_path = Path(args.config)
    if not config_path.exists():
        log.error("Config file not found: %s", config_path)
        return 1

    config = load_config(config_path)

    # Command line --mailto overrides config
    if args.mailto:
        config["unpaywall"]["mailto"] = args.mailto

    if not config["unpaywall"].get("mailto"):
        log.error("Please set 'mailto' via --mailto or in config.json for Unpaywall API access")
        return 1

    # Connect to database
    db_path = Path(args.db)
    if not db_path.exists():
        log.error("Database not found: %s (run scrape_dois.py first)", db_path)
        return 1

    conn = sqlite3.connect(args.db)
    ensure_schema(conn)  # Add missing columns if needed

    # Ensure data directory exists
    data_dir = Path(config.get("data_dir", "data"))
    data_dir.mkdir(parents=True, exist_ok=True)

    # Stats
    stats = {"downloaded": 0, "no-oa": 0, "failed": 0, "skipped": 0, "exhausted": 0}
    processed = 0

    log.info("Starting PDF scraper (years %d–%d, %d journals)",
             config["years"][0], config["years"][1], len(config["journals"]))

    try:
        while True:
            result, doi = process_one(conn, config, args.dry_run)

            if result is None:
                log.info("No more candidates to process")
                break

            if result in stats:
                stats[result] += 1

            if result == "skipped":
                # Wait a bit before next attempt to avoid busy-looping
                time.sleep(0.5)
                continue

            processed += 1

            if args.limit and processed >= args.limit:
                log.info("Reached limit of %d articles", args.limit)
                break

            if not args.continuous and result in ("downloaded", "no-oa", "failed"):
                # In non-continuous mode, process one article and exit
                break

            # Small delay between articles
            time.sleep(config["unpaywall"]["politeness_interval"])

    except KeyboardInterrupt:
        log.info("Interrupted by user")

    conn.close()

    log.info("Done — downloaded: %d, no-oa: %d, failed: %d, skipped: %d, exhausted: %d",
             stats["downloaded"], stats["no-oa"], stats["failed"],
             stats["skipped"], stats["exhausted"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
