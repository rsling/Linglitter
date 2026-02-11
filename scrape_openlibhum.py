#!/usr/bin/env python3
"""
Download PDFs from OpenLibHum-based journal websites.

Crawls journal websites (Glossa, STAR, etc.) starting from the issues page,
following internal links depth-first to find article pages.
Downloads PDFs for articles in linglitter.db that haven't been downloaded yet.

Usage:
    python scrape_openlibhum.py --journal glossa
    python scrape_openlibhum.py --journal star
    python scrape_openlibhum.py --journal glossa --config myconfig.json
    python scrape_openlibhum.py --journal glossa --limit 100
    python scrape_openlibhum.py --journal glossa --dry-run
"""

import argparse
import json
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

log = logging.getLogger(__name__)

# Minimal headers - OpenLibHum's bot protection blocks browser-like User-Agents
MINIMAL_HEADERS = {
    "Accept": "*/*",
}


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

def get_journal_dois(conn, db_journal):
    """Get all DOIs for a journal that need downloading (file IS NULL).

    Returns a set of DOIs for quick lookup.
    """
    rows = conn.execute("""
        SELECT doi FROM articles
        WHERE journal = ? AND file IS NULL
    """, (db_journal,)).fetchall()
    return {row[0] for row in rows}


def get_article_info(conn, doi):
    """Get article metadata for a DOI."""
    row = conn.execute("""
        SELECT doi, publisher, journal, year, attempts
        FROM articles
        WHERE doi = ?
    """, (doi,)).fetchone()
    if row:
        return {
            "doi": row[0],
            "publisher": row[1],
            "journal": row[2],
            "year": row[3],
            "attempts": row[4] or 0,
        }
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


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------

def extract_internal_links(html_content, base_url, domain):
    """Extract all internal links from HTML.

    Returns a list of absolute URLs on the same domain.
    """
    links = []
    href_pattern = re.compile(r'<a[^>]*\bhref\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE)

    for match in href_pattern.finditer(html_content):
        href = match.group(1)
        # Skip anchors and javascript
        if href.startswith('#') or href.startswith('javascript:'):
            continue

        # Convert to absolute URL
        absolute_url = urljoin(base_url, href)
        parsed = urlparse(absolute_url)

        # Only include links on the same domain
        if parsed.netloc == domain:
            # Normalize: remove fragment, ensure https
            normalized = f"https://{parsed.netloc}{parsed.path}"
            if parsed.query:
                normalized += f"?{parsed.query}"
            links.append(normalized)

    return links


def extract_doi_from_page(html_content):
    """Extract DOI from article page.

    Looks for <input id="share-link" value="https://doi.org/...">
    Returns the bare DOI (without https://doi.org/ prefix) or None.
    """
    # Look for <input id="share-link" ... value="...">
    pattern = re.compile(
        r'<input[^>]*\bid\s*=\s*["\']share-link["\'][^>]*\bvalue\s*=\s*["\']([^"\']+)["\']',
        re.IGNORECASE
    )
    match = pattern.search(html_content)
    if not match:
        # Try alternative order: value before id
        pattern2 = re.compile(
            r'<input[^>]*\bvalue\s*=\s*["\']([^"\']+)["\'][^>]*\bid\s*=\s*["\']share-link["\']',
            re.IGNORECASE
        )
        match = pattern2.search(html_content)

    if not match:
        return None

    doi_url = match.group(1)

    # Strip DOI from URL (e.g., https://doi.org/10.5334/gjgl.1234 -> 10.5334/gjgl.1234)
    if 'doi.org/' in doi_url:
        return doi_url.split('doi.org/')[-1]

    return doi_url


def extract_pdf_link(html_content):
    """Extract PDF download link from article page.

    Looks for <a href="...">Download PDF</a>
    Returns the href value or None.
    """
    # Look for <a href="...">Download PDF</a> or similar
    pattern = re.compile(
        r'<a[^>]*\bhref\s*=\s*["\']([^"\']+)["\'][^>]*>\s*Download\s+PDF\s*</a>',
        re.IGNORECASE
    )
    match = pattern.search(html_content)
    if match:
        return match.group(1)
    return None


# ---------------------------------------------------------------------------
# PDF download
# ---------------------------------------------------------------------------

def encode_doi_for_filename(doi):
    """Encode DOI to be safe as a filename."""
    unsafe_chars = r'[/\\:*?"<>|.]'
    return re.sub(unsafe_chars, "_", doi)


def build_pdf_path(data_dir, publisher, journal, year, doi):
    """Build the full path for storing a PDF."""
    safe_doi = encode_doi_for_filename(doi)
    safe_publisher = re.sub(r'[/\\:*?"<>|]', "_", publisher) if publisher else "unknown"
    safe_journal = re.sub(r'[/\\:*?"<>|]', "_", journal) if journal else "unknown"

    relative = Path(safe_publisher) / safe_journal / str(year) / f"{safe_doi}.pdf"
    absolute = Path(data_dir) / relative

    return absolute, str(relative)


def download_pdf(pdf_url, dest_path, referer_url, session, timeout=60):
    """Download a PDF from a URL.

    Returns (success, http_status_code).
    """
    try:
        session.headers["Referer"] = referer_url
        session.headers["Accept"] = "application/pdf,*/*;q=0.9"

        resp = session.get(pdf_url, timeout=timeout, stream=True, allow_redirects=True)
        code = resp.status_code

        if code != 200:
            log.warning("Download failed with status %d: %s", code, pdf_url)
            return False, code

        # Check Content-Type
        content_type = resp.headers.get("Content-Type", "").lower()
        if "html" in content_type:
            log.warning("Got HTML instead of PDF: %s", pdf_url)
            return False, 403

        # Ensure directory exists
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Write content
        with open(dest_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8192):
                fh.write(chunk)

        # Verify file size
        if dest_path.stat().st_size < 1000:
            log.warning("Downloaded file suspiciously small: %s", dest_path)
            dest_path.unlink()
            return False, code

        # Check PDF magic bytes
        with open(dest_path, "rb") as fh:
            magic = fh.read(5)
        if magic != b"%PDF-":
            log.warning("Downloaded file is not a PDF: %s", dest_path)
            dest_path.unlink()
            return False, 403

        return True, code

    except requests.exceptions.RequestException as exc:
        log.warning("Download request failed: %s", exc)
        return False, 0


# ---------------------------------------------------------------------------
# Crawler
# ---------------------------------------------------------------------------

def crawl_journal(conn, config, journal_cfg, session, dry_run=False, limit=None):
    """Crawl an OpenLibHum journal website depth-first looking for articles to download.

    Returns dict with download statistics.
    """
    start_url = journal_cfg["start_url"]
    domain = journal_cfg["domain"]
    article_prefix = journal_cfg["article_prefix"]
    db_journal = journal_cfg["db_journal"]
    politeness = journal_cfg.get("politeness", 15)
    data_dir = config.get("data_dir", "data")

    # Get DOIs we need to download
    needed_dois = get_journal_dois(conn, db_journal)
    log.info("Found %d articles needing download for journal '%s'", len(needed_dois), db_journal)

    if not needed_dois:
        return {"downloaded": 0, "failed": 0, "not_in_db": 0, "pages_visited": 0}

    # Track visited URLs and DOIs
    visited_urls = set()
    downloaded_dois = set()
    stats = {"downloaded": 0, "failed": 0, "not_in_db": 0, "pages_visited": 0}

    # Stack for depth-first traversal
    stack = [start_url]

    log.info("Starting crawl from: %s", start_url)
    log.info("Politeness delay: %d seconds", politeness)

    try:
        while stack:
            # Check limit
            if limit and stats["downloaded"] >= limit:
                log.info("Reached download limit of %d", limit)
                break

            # Check if all needed DOIs have been downloaded
            if not needed_dois - downloaded_dois:
                log.info("All needed DOIs have been downloaded")
                break

            url = stack.pop()

            # Skip if already visited
            if url in visited_urls:
                continue
            visited_urls.add(url)

            # Fetch page
            log.debug("Fetching: %s", url)
            try:
                resp = session.get(url, timeout=60)
                if resp.status_code != 200:
                    log.debug("Failed to fetch %s (HTTP %d)", url, resp.status_code)
                    continue
                html = resp.text
                stats["pages_visited"] += 1
            except requests.exceptions.RequestException as exc:
                log.debug("Failed to fetch %s: %s", url, exc)
                continue

            # Check if this is an article page
            if url.startswith(article_prefix):
                # Extract DOI
                doi = extract_doi_from_page(html)
                if not doi:
                    log.debug("No DOI found on article page: %s", url)
                    continue

                log.info("Found article: %s", doi)

                # Check if we need this DOI
                if doi not in needed_dois:
                    log.debug("  DOI not in database or already downloaded, skipping")
                    stats["not_in_db"] += 1
                    time.sleep(politeness)
                    continue

                if doi in downloaded_dois:
                    log.debug("  Already downloaded this session, skipping")
                    time.sleep(politeness)
                    continue

                # Get article info from database
                article = get_article_info(conn, doi)
                if not article:
                    log.debug("  Article not found in database")
                    stats["not_in_db"] += 1
                    time.sleep(politeness)
                    continue

                # Extract PDF link
                pdf_href = extract_pdf_link(html)
                if not pdf_href:
                    log.warning("  No PDF link found on page")
                    stats["failed"] += 1
                    time.sleep(politeness)
                    continue

                # Build full PDF URL
                pdf_url = urljoin(url, pdf_href)
                log.info("  PDF link: %s", pdf_url)

                if dry_run:
                    log.info("  [DRY RUN] Would download PDF")
                    stats["downloaded"] += 1
                    downloaded_dois.add(doi)
                    time.sleep(politeness)
                    continue

                # Build paths
                abs_path, rel_path = build_pdf_path(
                    data_dir, article["publisher"], article["journal"], article["year"], doi
                )

                # Download PDF
                now = datetime.now().isoformat()
                attempts = article["attempts"] + 1

                success, http_code = download_pdf(pdf_url, abs_path, url, session)

                if success:
                    log.info("  Downloaded: %s", rel_path)
                    update_article(conn, doi,
                                   availability="oa",
                                   source=pdf_url,
                                   attempts=attempts,
                                   response=http_code,
                                   timestamp=now,
                                   file_path=rel_path)
                    stats["downloaded"] += 1
                    downloaded_dois.add(doi)
                else:
                    log.warning("  Download failed (HTTP %d)", http_code)
                    update_article(conn, doi,
                                   availability="oa",
                                   source=pdf_url,
                                   attempts=attempts,
                                   response=http_code,
                                   timestamp=now,
                                   file_path=None)
                    stats["failed"] += 1

                # Politeness delay after article processing
                time.sleep(politeness)

            else:
                # Not an article page, extract and queue internal links
                links = extract_internal_links(html, url, domain)
                new_links = [l for l in links if l not in visited_urls]

                # Prioritize article and issue links (add them last so they're popped first)
                def link_priority(link):
                    if '/article/' in link:
                        return 0  # Highest priority
                    if '/issue/' in link:
                        return 1
                    return 2  # Other links

                new_links.sort(key=link_priority, reverse=True)
                for link in new_links:
                    stack.append(link)

                # Politeness delay between page fetches
                time.sleep(politeness)

    except KeyboardInterrupt:
        log.info("Interrupted by user")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Download PDFs from OpenLibHum-based journal websites.")
    parser.add_argument("--journal", type=str, required=True,
                        help="Journal section name in config.json (e.g., 'glossa', 'star')")
    parser.add_argument("--config", type=str, default="config.json",
                        help="Path to config JSON (default: config.json)")
    parser.add_argument("--db", type=str, default="linglitter.db",
                        help="Path to SQLite database (default: linglitter.db)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Maximum number of PDFs to download")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without downloading")
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

    # Get journal-specific configuration
    journal_cfg = config.get(args.journal)
    if not journal_cfg:
        log.error("Journal section '%s' not found in config.json", args.journal)
        log.error("Available sections: %s", ", ".join(
            k for k in config.keys() if isinstance(config[k], dict) and "start_url" in config[k]
        ))
        return 1

    # Validate required fields
    required_fields = ["start_url", "domain", "article_prefix", "db_journal"]
    missing = [f for f in required_fields if f not in journal_cfg]
    if missing:
        log.error("Journal section '%s' is missing required fields: %s", args.journal, ", ".join(missing))
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

    # Create session
    session = requests.Session()
    session.headers.update(MINIMAL_HEADERS)

    start_url = journal_cfg["start_url"]
    db_journal = journal_cfg["db_journal"]
    politeness = journal_cfg.get("politeness", 15)

    log.info("Starting OpenLibHum scraper for journal '%s'", args.journal)
    log.info("  DB journal name: %s", db_journal)
    log.info("  Start URL: %s", start_url)
    log.info("  Politeness: %d seconds", politeness)

    # Crawl and download
    stats = crawl_journal(conn, config, journal_cfg, session, dry_run=args.dry_run, limit=args.limit)

    conn.close()

    log.info("Done — pages visited: %d, downloaded: %d, failed: %d, not in DB: %d",
             stats["pages_visited"], stats["downloaded"], stats["failed"], stats["not_in_db"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
