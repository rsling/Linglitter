#!/usr/bin/env python3
"""
Download PDFs from local/university repositories for non-OA articles.

Attempts to fetch PDFs from configured repository URLs for articles
marked as 'no-oa' in linglitter.db.

Usage:
    python scrape_repo.py
    python scrape_repo.py --config myconfig.json
    python scrape_repo.py --limit 100
    python scrape_repo.py --dry-run
"""

import argparse
import json
import logging
import random
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests

log = logging.getLogger(__name__)

# Minimal curl-like headers (curl sends very few headers by default)
# Some servers reject requests with too many headers
CURL_HEADERS = {
    "User-Agent": "curl/8.5.0",
    "Accept": "*/*",
}

# Browser-like headers for PDF downloads (after session is established)
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

# Per-repo session storage: {repo_url: (session, last_used_datetime)}
_repo_sessions = {}

# Per-repo failure counter: {repo_url: failure_count}
_repo_failures = {}


def get_repo_session(repo_url):
    """Get or create a requests session for a repository.

    Sessions are reused across downloads from the same repository,
    preserving cookies and appearing more like normal browser behavior.

    Returns (session, is_new, elapsed_str) where:
    - is_new: True if a fresh session was created
    - elapsed_str: For continued sessions, time since last use as "HH:MM"
    """
    now = datetime.now()
    if repo_url not in _repo_sessions:
        session = requests.Session()
        session.headers.update(CURL_HEADERS)
        _repo_sessions[repo_url] = (session, now)
        return session, True, None
    session, last_used = _repo_sessions[repo_url]
    elapsed = now - last_used
    total_minutes = int(elapsed.total_seconds() // 60)
    hours, minutes = divmod(total_minutes, 60)
    elapsed_str = f"{hours:02d}:{minutes:02d}"
    _repo_sessions[repo_url] = (session, now)
    return session, False, elapsed_str


def record_repo_failure(repo_url):
    """Record a failed fetch attempt for a repository."""
    _repo_failures[repo_url] = _repo_failures.get(repo_url, 0) + 1


def get_repo_failures(repo_url):
    """Get the number of failed attempts for a repository."""
    return _repo_failures.get(repo_url, 0)


def is_repo_disabled(repo_url, max_failures):
    """Check if a repository has been disabled due to too many failures."""
    return _repo_failures.get(repo_url, 0) >= max_failures


def get_active_repos(repos, max_failures):
    """Get list of repositories that are still active (not disabled)."""
    return [r for r in repos if not is_repo_disabled(r, max_failures)]


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

def get_random_nonoa_candidate(conn, years, journals):
    """Select a random article that is marked as no-oa and not yet downloaded.

    Selects from articles where availability = 'no-oa' and file IS NULL.
    """
    placeholders = ",".join("?" for _ in journals)
    query = f"""
        SELECT doi, publisher, journal, year
        FROM articles
        WHERE year >= ? AND year <= ?
          AND journal IN ({placeholders})
          AND file IS NULL
          AND availability = 'no-oa'
        ORDER BY RANDOM()
        LIMIT 1
    """
    params = [years[0], years[1]] + journals
    row = conn.execute(query, params).fetchone()
    if row:
        return {"doi": row[0], "publisher": row[1], "journal": row[2], "year": row[3]}
    return None


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
    safe_publisher = re.sub(r'[/\\:*?"<>|]', "_", publisher) if publisher else "unknown"
    safe_journal = re.sub(r'[/\\:*?"<>|]', "_", journal) if journal else "unknown"

    relative = Path(safe_publisher) / safe_journal / str(year) / f"{safe_doi}.pdf"
    absolute = Path(data_dir) / relative

    return absolute, str(relative)


def extract_download_link(html_content):
    """Extract PDF download link from HTML page.

    Looks for <div class="download"> containing an <a href="..."> tag.
    Returns the href value (relative URL starting with /) or None if not found.
    """
    # Look for <div class="download"...>...<a href="...">
    div_pattern = re.compile(
        r'<div[^>]*\bclass\s*=\s*["\'][^"\']*\bdownload\b[^"\']*["\'][^>]*>(.*?)</div>',
        re.IGNORECASE | re.DOTALL
    )
    div_match = div_pattern.search(html_content)
    if not div_match:
        return None

    div_content = div_match.group(1)

    # Extract href from <a> tag within the div
    href_pattern = re.compile(r'<a[^>]*\bhref\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE)
    href_match = href_pattern.search(div_content)
    if not href_match:
        return None

    return href_match.group(1)


def fetch_landing_page(page_url, timeout=60, session=None):
    """Fetch the HTML landing page for an article.

    Uses minimal curl-like headers to avoid 403 errors from servers
    that reject requests with too many headers.

    Returns (html_content, http_status_code) or (None, code) on failure.
    """
    if session is None:
        session = requests.Session()
        session.headers.update(CURL_HEADERS)

    try:
        # Keep headers minimal like curl
        resp = session.get(page_url, timeout=timeout, allow_redirects=True)
        code = resp.status_code

        if code != 200:
            log.debug("Landing page fetch failed with status %d: %s", code, page_url)
            return None, code

        return resp.text, code

    except requests.exceptions.RequestException as exc:
        log.debug("Landing page request failed: %s", exc)
        return None, 0


def download_pdf_direct(pdf_url, dest_path, referer_url, timeout=60, session=None):
    """Download a PDF from a direct URL.

    If session is provided, uses that session (preserving cookies from
    previous requests to the same repository). Otherwise creates a new session.

    Uses minimal curl-like headers to avoid 403 errors.

    Returns (success, http_status_code).
    """
    if session is None:
        session = requests.Session()
        session.headers.update(CURL_HEADERS)

    try:
        # Keep headers minimal like curl, just add referer
        session.headers["Referer"] = referer_url

        resp = session.get(pdf_url, timeout=timeout, stream=True, allow_redirects=True)
        code = resp.status_code

        if code != 200:
            log.debug("Download failed with status %d: %s", code, pdf_url)
            return False, code

        # Check Content-Type to ensure we got a PDF, not an HTML error page
        content_type = resp.headers.get("Content-Type", "").lower()
        if "html" in content_type:
            log.debug("Got HTML instead of PDF (Content-Type: %s): %s", content_type, pdf_url)
            return False, 403

        # Ensure directory exists
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Write content
        with open(dest_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8192):
                fh.write(chunk)

        # Verify we got something reasonable
        if dest_path.stat().st_size < 1000:
            log.debug("Downloaded file suspiciously small: %s", dest_path)
            dest_path.unlink()
            return False, code

        # Check PDF magic bytes
        with open(dest_path, "rb") as fh:
            magic = fh.read(5)
        if magic != b"%PDF-":
            log.debug("Downloaded file is not a PDF (magic: %s): %s", magic[:20], dest_path)
            dest_path.unlink()
            return False, 403

        return True, code

    except requests.exceptions.RequestException as exc:
        log.debug("Download request failed: %s", exc)
        return False, 0


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def process_one(conn, config, dry_run=False):
    """Process a single non-OA article by trying all repositories.

    Returns:
    - "downloaded": successfully downloaded from a repository
    - "failed": tried all active repositories, none succeeded
    - "all_disabled": all repositories have been disabled due to failures
    - None: no candidates left
    """
    years = config["years"]
    journals = config["journals"]
    local_cfg = config.get("local", {})
    data_dir = config.get("data_dir", "data")

    repos = local_cfg.get("repos", [])
    if not repos:
        log.error("No repositories configured in config.json 'local.repos'")
        return None, None

    max_repo_failures = local_cfg.get("max_repo_failures", 10)
    politeness_min = local_cfg.get("politeness_min", 180)
    politeness_random = local_cfg.get("politeness_random", 20)

    # Filter to only active (non-disabled) repositories
    active_repos = get_active_repos(repos, max_repo_failures)
    if not active_repos:
        return "all_disabled", None

    # Get a random non-OA candidate
    candidate = get_random_nonoa_candidate(conn, years, journals)
    if not candidate:
        return None, None

    doi = candidate["doi"]
    publisher = candidate["publisher"]
    journal = candidate["journal"]
    year = candidate["year"]

    log.info("Processing: %s", doi)

    if dry_run:
        log.info("  [DRY RUN] Would try %d active repositories", len(active_repos))
        return "skipped", doi

    attempts = get_article_attempts(conn, doi)
    now = datetime.now().isoformat()

    # Build paths
    abs_path, rel_path = build_pdf_path(data_dir, publisher, journal, year, doi)

    # Try each active repository in sequence
    for i, repo_url in enumerate(active_repos):
        # Construct the landing page URL: repo_prefix + DOI
        landing_url = repo_url + doi

        # Get or create session for this repository
        session, is_new_session, elapsed_str = get_repo_session(repo_url)
        parsed_url = urlparse(repo_url)
        server_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

        failures = get_repo_failures(repo_url)
        if is_new_session:
            log.info("  [%d/%d] Session started: %s (failures: %d/%d)",
                     i + 1, len(active_repos), server_url, failures, max_repo_failures)
        else:
            log.info("  [%d/%d] Session continued: %s [%s] (failures: %d/%d)",
                     i + 1, len(active_repos), server_url, elapsed_str, failures, max_repo_failures)

        log.info("  Fetching landing page: %s", landing_url)

        # Step 1: Fetch the HTML landing page
        html_content, http_code = fetch_landing_page(landing_url, session=session)

        if html_content is None:
            log.info("  Landing page fetch failed (HTTP %d). No PDF saved.", http_code)
            record_repo_failure(repo_url)
            new_failures = get_repo_failures(repo_url)
            if is_repo_disabled(repo_url, max_repo_failures):
                log.warning("  Repository %s disabled after %d failures", server_url, max_repo_failures)
            if i < len(active_repos) - 1:
                log.info("  Waiting %d seconds before next repository...", politeness_min)
                time.sleep(politeness_min)
            continue

        # Step 2: Extract the download link from HTML
        download_href = extract_download_link(html_content)

        if download_href is None:
            # Got HTML but no download link - skip to next DOI (other repos won't have it either)
            log.warning("  Download link not found in HTML (no <div class=\"download\"> with <a href>). No PDF saved.")
            log.info("  Skipping to next DOI (article not available for download).")
            attempts += 1
            update_article(conn, doi,
                          availability="no-oa",
                          source=None,
                          attempts=attempts,
                          response=http_code,
                          timestamp=now,
                          file_path=None)
            return "no_download_link", doi

        # Step 3: Construct full PDF URL (server + relative href)
        pdf_url = server_url + download_href
        log.info("  Found download link: %s", pdf_url)

        # Small delay to appear more human-like
        time.sleep(0.5)

        # Step 4: Download the PDF
        success, http_code = download_pdf_direct(pdf_url, abs_path, referer_url=landing_url, session=session)

        if success:
            log.info("  Downloaded: %s", rel_path)
            attempts += 1
            update_article(conn, doi,
                          availability="repo",
                          source=pdf_url,
                          attempts=attempts,
                          response=http_code,
                          timestamp=now,
                          file_path=rel_path)
            return "downloaded", doi

        # Record the failure
        log.info("  PDF download failed (HTTP %d). No PDF saved.", http_code)
        record_repo_failure(repo_url)
        new_failures = get_repo_failures(repo_url)
        log.debug("  Repository %s (failures: %d/%d)",
                  server_url, new_failures, max_repo_failures)

        if is_repo_disabled(repo_url, max_repo_failures):
            log.warning("  Repository %s disabled after %d failures", server_url, max_repo_failures)

        # Wait politeness_min seconds before trying the next repository
        if i < len(active_repos) - 1:
            log.info("  Waiting %d seconds before next repository...", politeness_min)
            time.sleep(politeness_min)

    # All active repositories failed for this DOI
    log.info("  All %d active repositories failed for %s. No PDF saved.", len(active_repos), doi)
    attempts += 1
    update_article(conn, doi,
                  availability="no-oa",
                  source=None,
                  attempts=attempts,
                  response=0,
                  timestamp=now,
                  file_path=None)
    return "failed", doi


def main():
    parser = argparse.ArgumentParser(
        description="Download PDFs from local/university repositories for non-OA articles.")
    parser.add_argument("--config", type=str, default="config.json",
                        help="Path to config JSON (default: config.json)")
    parser.add_argument("--db", type=str, default="linglitter.db",
                        help="Path to SQLite database (default: linglitter.db)")
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

    local_cfg = config.get("local", {})
    if not local_cfg.get("repos"):
        log.error("No repositories configured. Add 'local.repos' list to config.json")
        return 1

    # Connect to database
    db_path = Path(args.db)
    if not db_path.exists():
        log.error("Database not found: %s (run scrape_dois.py first)", db_path)
        return 1

    conn = sqlite3.connect(args.db)

    # Ensure data directory exists
    data_dir = Path(config.get("data_dir", "data"))
    data_dir.mkdir(parents=True, exist_ok=True)

    # Get politeness settings
    politeness_min = local_cfg.get("politeness_min", 180)
    politeness_random = local_cfg.get("politeness_random", 20)

    # Stats
    stats = {"downloaded": 0, "failed": 0, "skipped": 0, "no_download_link": 0}
    processed = 0
    max_repo_failures = local_cfg.get("max_repo_failures", 10)

    log.info("Starting repository scraper (years %d–%d, %d journals, %d repos, max %d failures/repo)",
             config["years"][0], config["years"][1], len(config["journals"]),
             len(local_cfg.get("repos", [])), max_repo_failures)

    try:
        while True:
            result, doi = process_one(conn, config, args.dry_run)

            if result is None:
                log.info("No more candidates to process")
                break

            if result == "all_disabled":
                log.error("All repositories have been disabled due to reaching %d failures each",
                          max_repo_failures)
                log.error("Exiting. Check repository URLs and network connectivity.")
                break

            if result in stats:
                stats[result] += 1

            if result == "skipped":
                continue

            processed += 1

            if args.limit and processed >= args.limit:
                log.info("Reached limit of %d articles", args.limit)
                break

            if not args.continuous and result in ("downloaded", "failed", "no_download_link"):
                break

            # Wait politeness_min plus random 5 to politeness_random seconds
            random_wait = random.randint(5, politeness_random)
            total_wait = politeness_min + random_wait
            log.info("Waiting %d seconds before next DOI (base %d + random %d)...",
                     total_wait, politeness_min, random_wait)
            time.sleep(total_wait)

    except KeyboardInterrupt:
        log.info("Interrupted by user")

    conn.close()

    log.info("Done — downloaded: %d, failed: %d, no_download_link: %d, skipped: %d",
             stats["downloaded"], stats["failed"], stats["no_download_link"], stats["skipped"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
