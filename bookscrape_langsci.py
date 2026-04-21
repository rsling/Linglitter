#!/usr/bin/env python3
"""
Download open-access books from Language Science Press.

Crawls series overview pages listed in books_langsci.json, extracts book
metadata (title, authors, DOI), downloads PDFs, and stores entries in
linglitter.db (as type='book').

Usage:
    python bookscrape_langsci.py
    python bookscrape_langsci.py --series "Open Generative Syntax"
    python bookscrape_langsci.py --limit 10
    python bookscrape_langsci.py --dry-run
"""

import argparse
import json
import logging
import re
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import requests

log = logging.getLogger(__name__)

POLITENESS = 5  # seconds between requests

# Browser-like headers to avoid bot detection on LangSci-hosted PDFs
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


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db(db_path):
    """Open the linglitter database."""
    conn = sqlite3.connect(db_path)
    return conn


def book_exists(conn, doi):
    """Check if an entry with this DOI is already in the database."""
    row = conn.execute("SELECT 1 FROM articles WHERE doi = ?", (doi,)).fetchone()
    return row is not None


def insert_book(conn, doi, title, authors, publisher, series, file_path):
    """Insert a new book entry. LangSci books are always OA."""
    conn.execute("""
        INSERT INTO articles (doi, title, authors, publisher, journal, file,
                              availability, type)
        VALUES (?, ?, ?, ?, ?, ?, 'oa', 'book')
    """, (doi, title, authors, publisher, series, file_path))
    conn.commit()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def encode_doi_for_filename(doi):
    """Encode DOI to be safe as a filename."""
    unsafe_chars = r'[/\\:*?"<>|.]'
    return re.sub(unsafe_chars, "_", doi)


def fetch_page(url, session, retries=3):
    """Fetch an HTML page with retries. Returns (html_string, success)."""
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=60)
            if resp.status_code != 200:
                log.warning("HTTP %d for %s", resp.status_code, url)
                return None, False
            return resp.text, True
        except requests.exceptions.RequestException as exc:
            if attempt < retries - 1:
                wait = POLITENESS * (attempt + 1)
                log.debug("Request failed (attempt %d/%d), retrying in %ds: %s",
                         attempt + 1, retries, wait, exc)
                time.sleep(wait)
            else:
                log.warning("Request failed after %d attempts for %s: %s",
                           retries, url, exc)
                return None, False


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def extract_book_links(html, base_url):
    """Extract book URLs from a series overview page.

    Looks for the 'All books' heading, then finds all book links
    in the <div class="row"> elements that follow.
    """
    # Find the "All books" section
    all_books_match = re.search(
        r'<h2[^>]*class="[^"]*title[^"]*"[^>]*>.*?All\s+books.*?</h2>',
        html, re.IGNORECASE | re.DOTALL
    )
    if not all_books_match:
        # Try simpler match
        all_books_match = re.search(r'All\s+[Bb]ooks', html)
        if not all_books_match:
            log.warning("Could not find 'All books' section")
            return []

    # Search for book links after the heading
    rest = html[all_books_match.end():]

    # Book links follow pattern: <a href="/catalog/book/NNN"> or full URL
    links = []
    seen = set()
    for m in re.finditer(
        r'<a\s[^>]*href=["\']([^"\']*?/catalog/book/\d+)["\']',
        rest, re.IGNORECASE
    ):
        href = m.group(1)
        abs_url = urljoin(base_url, href)
        if abs_url not in seen:
            seen.add(abs_url)
            links.append(abs_url)

    return links


def extract_book_metadata(html):
    """Extract title, authors, DOI, and PDF URL from a book page.

    Returns dict with keys: title, authors, doi, pdf_url (any may be None).
    """
    result = {"title": None, "authors": None, "doi": None, "pdf_url": None}

    # Title: <h1 class="title">...</h1>
    m = re.search(
        r'<h1[^>]*class="[^"]*title[^"]*"[^>]*>(.*?)</h1>',
        html, re.DOTALL | re.IGNORECASE
    )
    if m:
        title = re.sub(r'<[^>]+>', '', m.group(1)).strip()
        result["title"] = title

    # Authors: <div class="langsci_author">...</div>
    authors = []
    for m in re.finditer(
        r'<div[^>]*class="[^"]*langsci_author[^"]*"[^>]*>(.*?)</div>',
        html, re.DOTALL | re.IGNORECASE
    ):
        author = re.sub(r'<[^>]+>', '', m.group(1)).strip()
        if author:
            authors.append(author)
    if authors:
        result["authors"] = "; ".join(authors)

    # DOI: <h2>doi</h2> followed by <div class="value">...</div>
    m = re.search(
        r'<h2[^>]*>\s*doi\s*</h2>\s*<div[^>]*class="[^"]*value[^"]*"[^>]*>(.*?)</div>',
        html, re.DOTALL | re.IGNORECASE
    )
    if m:
        doi = re.sub(r'<[^>]+>', '', m.group(1)).strip()
        result["doi"] = doi

    # PDF download link: <a href="..."> PDF </a>
    # Match by text content "PDF" (with optional whitespace), class-agnostic.
    m = re.search(
        r'<a\s[^>]*href=["\']([^"\']+)["\'][^>]*>\s*PDF\s*</a>',
        html, re.IGNORECASE
    )
    if m:
        result["pdf_url"] = m.group(1)

    return result


def download_pdf(url, dest_path, session, book_page_url=None):
    """Download a PDF file, spoofing browser navigation.

    If book_page_url is provided, sets Referer and Sec-Fetch headers
    to look like a click from the book page. For LangSci-hosted PDFs
    this is needed to avoid bot blocking.

    Returns True on success.
    """
    try:
        # Set headers to look like a same-origin navigation from the book page
        if book_page_url:
            session.headers["Referer"] = book_page_url
            session.headers["Sec-Fetch-Site"] = "same-origin"
        session.headers["Accept"] = "application/pdf,*/*;q=0.9"

        resp = session.get(url, timeout=120, stream=True, allow_redirects=True)
        code = resp.status_code

        if code != 200:
            log.warning("PDF download failed with HTTP %d: %s", code, url)
            return False

        # Check for HTML instead of PDF (bot challenge page)
        content_type = resp.headers.get("Content-Type", "").lower()
        if "html" in content_type:
            resp.close()
            log.warning("Got HTML instead of PDF (bot block?): %s", url)
            return False

        dest_path.parent.mkdir(parents=True, exist_ok=True)

        with open(dest_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8192):
                fh.write(chunk)

        # Verify file
        if dest_path.stat().st_size < 1000:
            log.warning("Downloaded file suspiciously small: %s", dest_path)
            dest_path.unlink()
            return False

        with open(dest_path, "rb") as fh:
            magic = fh.read(5)
        if magic != b"%PDF-":
            log.warning("Downloaded file is not a PDF: %s", dest_path)
            dest_path.unlink()
            return False

        # Restore HTML Accept header for subsequent page fetches
        session.headers["Accept"] = BROWSER_HEADERS["Accept"]
        session.headers["Sec-Fetch-Site"] = "none"

        return True

    except requests.exceptions.RequestException as exc:
        log.warning("PDF download request failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process_series(conn, session, publisher_name, series_title, series_url,
                   pdf_dir, dry_run=False, limit=None):
    """Process all books in one series. Returns (downloaded, skipped, failed)."""
    log.info("Series: %s — %s", series_title, series_url)

    html, ok = fetch_page(series_url, session)
    if not ok:
        log.error("Failed to fetch series page: %s", series_url)
        return 0, 0, 0

    book_links = extract_book_links(html, series_url)
    log.info("  Found %d books", len(book_links))

    downloaded = 0
    skipped = 0
    failed = 0

    for i, book_url in enumerate(book_links):
        if limit is not None and downloaded >= limit:
            break

        time.sleep(POLITENESS)

        log.info("  [%d/%d] %s", i + 1, len(book_links), book_url)

        book_html, ok = fetch_page(book_url, session)
        if not ok:
            failed += 1
            continue

        meta = extract_book_metadata(book_html)

        if not meta["doi"]:
            log.warning("    No DOI found, skipping")
            failed += 1
            continue

        if not meta["title"]:
            log.warning("    No title found for %s", meta["doi"])

        doi = meta["doi"]
        log.info("    Title: %s", meta["title"] or "(unknown)")
        log.info("    Authors: %s", meta["authors"] or "(unknown)")
        log.info("    DOI: %s", doi)

        if book_exists(conn, doi):
            log.info("    Already in database, skipping")
            skipped += 1
            continue

        if not meta["pdf_url"]:
            log.warning("    No PDF link found")
            failed += 1
            continue

        pdf_url = meta["pdf_url"]
        log.info("    PDF: %s", pdf_url)

        # Build destination path (flat in pdf/)
        safe_doi = encode_doi_for_filename(doi)
        filename = f"{safe_doi}.pdf"
        abs_path = Path(pdf_dir) / filename

        if dry_run:
            log.info("    [DRY RUN] Would download to pdf/%s", filename)
            continue

        time.sleep(POLITENESS)

        if download_pdf(pdf_url, abs_path, session, book_page_url=book_url):
            log.info("    Downloaded: %s", filename)
            insert_book(conn, doi, meta["title"], meta["authors"],
                       publisher_name, series_title, filename)
            downloaded += 1
        else:
            log.warning("    Download failed")
            failed += 1

    return downloaded, skipped, failed


def main():
    parser = argparse.ArgumentParser(
        description="Download open-access books from Language Science Press.")
    parser.add_argument("--config", type=str, default="books_langsci.json",
                        help="Path to books JSON (default: books_langsci.json)")
    parser.add_argument("--db", type=str, default="linglitter.db",
                        help="Path to SQLite database (default: linglitter.db)")
    parser.add_argument("--pdf-dir", type=str, default="pdf",
                        help="Directory for downloaded PDFs (default: pdf)")
    parser.add_argument("--series", type=str, default=None,
                        help="Process only this series (by title)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Maximum number of books to download per series")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without downloading")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    config_path = Path(args.config)
    if not config_path.exists():
        log.error("Config file not found: %s", config_path)
        return 1

    with open(config_path) as fh:
        config = json.load(fh)

    conn = init_db(args.db)

    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)

    total_downloaded = 0
    total_skipped = 0
    total_failed = 0

    for publisher in config["publishers"]:
        publisher_name = publisher["name"]
        log.info("Publisher: %s", publisher_name)

        for series in publisher["series"]:
            series_title = series["title"]
            series_url = series["url"]

            if args.series and series_title != args.series:
                continue

            d, s, f = process_series(
                conn, session, publisher_name, series_title, series_url,
                args.pdf_dir, dry_run=args.dry_run, limit=args.limit
            )
            total_downloaded += d
            total_skipped += s
            total_failed += f

            time.sleep(POLITENESS)

    conn.close()

    log.info("Done — downloaded: %d, skipped: %d, failed: %d",
             total_downloaded, total_skipped, total_failed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
