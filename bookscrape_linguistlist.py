#!/usr/bin/env python3
"""
Scrape book announcements from the LINGUIST List.

Crawls the LINGUIST List book announcements hub, extracts metadata
(title, authors, publisher, series, year, book URL), and stores
entries in linglitter.db (as type='book') for books from configured publishers.

Usage:
    python bookscrape_linguistlist.py
    python bookscrape_linguistlist.py --from-year 2020 --until-year 2025
    python bookscrape_linguistlist.py --limit 50
    python bookscrape_linguistlist.py --dry-run
"""

import argparse
import json
import logging
import re
import sqlite3
import sys
import time
from pathlib import Path

import requests

log = logging.getLogger(__name__)

LINGUISTLIST_HUB = "https://linguistlist.org/issues/"
POLITENESS = 3  # seconds between requests

# LINGUIST List volume = year - 1989
VOLUME_OFFSET = 1989

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Default publishers to include (substring matching)
DEFAULT_PUBLISHERS = [
    "Cambridge University Press",
    "De Gruyter",
    "Edinburgh University Press",
    "John Benjamins",
    "MIT Press",
    "Narr Francke Attempto",
]


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def encode_for_key(s):
    """Encode a string for use as a DOI-like key: non-alphanum → underscore."""
    return re.sub(r'[^a-zA-Z0-9]', '_', s)


def generate_book_key(book_url, publisher, title):
    """Generate a synthetic primary key for a DOI-less book."""
    if book_url:
        url = re.sub(r'^https?://', '', book_url)
        return encode_for_key(url)
    raw = f"book:{publisher or 'unknown'}:{title or 'unknown'}"
    return encode_for_key(raw)


def init_db(db_path):
    """Open the linglitter database."""
    conn = sqlite3.connect(db_path)
    return conn


def book_url_exists(conn, book_url):
    """Check if an entry with this book URL is already in the database."""
    row = conn.execute("SELECT 1 FROM articles WHERE jump_url = ?",
                      (book_url,)).fetchone()
    return row is not None


def book_title_exists(conn, title, publisher):
    """Check if a book with this title+publisher is already in the database."""
    row = conn.execute(
        "SELECT 1 FROM articles WHERE title = ? AND publisher = ? AND type = 'book'",
        (title, publisher)).fetchone()
    return row is not None


def insert_book(conn, title, authors, publisher, series, year, book_url):
    """Insert a new book entry from LINGUIST List (no DOI, no file)."""
    doi_key = generate_book_key(book_url, publisher, title)
    try:
        conn.execute("""
            INSERT INTO articles (doi, title, authors, publisher, journal,
                                  year, jump_url, type)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'book')
        """, (doi_key, title, authors, publisher, series, year, book_url))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        log.debug("  Already in database (duplicate key)")
        return False


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

def fetch_page(url, session):
    """Fetch an HTML page. Returns (html, success)."""
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code != 200:
            log.warning("HTTP %d for %s", resp.status_code, url)
            return None, False
        return resp.text, True
    except requests.exceptions.RequestException as exc:
        log.warning("Request failed: %s", exc)
        return None, False


def extract_issue_links(html):
    """Extract issue URLs from a hub page."""
    return re.findall(r'href="(/issues/\d+/\d+/)"', html)


def extract_metadata(html):
    """Extract book metadata from a LINGUIST List detail page.

    Parses the simple text-based metadata format:
        Title: ...
        Subtitle: ...
        Series Title: ...
        Publication Year: ...
        Publisher: ...
        Book URL: <a href="...">...</a>
        Author(s): ...
    """
    result = {}

    # Title
    m = re.search(r'Title:\s*(.*?)(?:<br|</?p)', html, re.DOTALL)
    if m:
        result["title"] = re.sub(r'<[^>]+>', '', m.group(1)).strip()

    # Subtitle (append to title if present)
    m = re.search(r'Subtitle:\s*(.*?)(?:<br|</?p)', html, re.DOTALL)
    if m:
        subtitle = re.sub(r'<[^>]+>', '', m.group(1)).strip()
        if subtitle and result.get("title"):
            result["title"] += ": " + subtitle

    # Series Title
    m = re.search(r'Series Title:\s*(.*?)(?:<br|</?p)', html, re.DOTALL)
    if m:
        result["series"] = re.sub(r'<[^>]+>', '', m.group(1)).strip()

    # Publication Year
    m = re.search(r'Publication Year:\s*(\d{4})', html)
    if m:
        result["year"] = int(m.group(1))

    # Publisher
    m = re.search(r'Publisher:\s*(.*?)(?:<br|</?p)', html, re.DOTALL)
    if m:
        result["publisher"] = re.sub(r'<[^>]+>', '', m.group(1)).strip()

    # Book URL: extract from <a href="...">
    m = re.search(r'Book URL:\s*<a[^>]*href="([^"]+)"', html)
    if m:
        result["book_url"] = m.group(1)

    # Author(s): may contain multiple names separated by <br>
    m = re.search(r'Author\(s\):\s*(.*?)(?:</?p|<br\s*/?\s*>\s*<br)', html, re.DOTALL)
    if m:
        authors = re.sub(r'<[^>]+>', '', m.group(1)).strip()
        authors = re.sub(r'\s+', ' ', authors)
        if authors:
            result["authors"] = authors

    # Fallback: extract authors from page <title> tag
    # Pattern: "Books: <Title>: <Authors> (eds.) (<Year>)" or similar
    if "authors" not in result:
        m = re.search(r'<title>.*?Books:\s*.*?:\s*(.*?)\s*\(\d{4}\)\s*</title>',
                      html, re.DOTALL)
        if m:
            authors = m.group(1).strip()
            # Remove trailing "(eds.)" or "(ed.)"
            authors = re.sub(r'\s*\(eds?\.?\)\s*$', '', authors).strip()
            if authors:
                result["authors"] = authors

    return result


def publisher_matches(publisher, filter_list):
    """Check if publisher matches any entry in the filter list (substring)."""
    if not publisher:
        return False
    pub_lower = publisher.lower()
    return any(f.lower() in pub_lower for f in filter_list)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Scrape book announcements from the LINGUIST List.")
    parser.add_argument("--db", type=str, default="linglitter.db",
                        help="Path to SQLite database (default: linglitter.db)")
    parser.add_argument("--from-year", type=int, default=2005,
                        help="Start year (default: 2005)")
    parser.add_argument("--until-year", type=int, default=2026,
                        help="End year (default: 2026)")
    parser.add_argument("--publishers", type=str, nargs="+",
                        default=DEFAULT_PUBLISHERS,
                        help="Publisher name substrings to include")
    parser.add_argument("--limit", type=int, default=None,
                        help="Maximum number of entries to process")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without writing to DB")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    conn = init_db(args.db)

    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)

    # Volume range: volume = year - 1989
    from_vol = args.from_year - VOLUME_OFFSET
    until_vol = args.until_year - VOLUME_OFFSET

    log.info("Scraping LINGUIST List books: years %d–%d (volumes %d–%d)",
             args.from_year, args.until_year, from_vol, until_vol)
    log.info("Publishers: %s", ", ".join(args.publishers))

    stats = {"added": 0, "skipped": 0, "filtered": 0, "failed": 0}
    done = False

    # NOTE: The LINGUIST List hub ignores the volume= parameter.
    # Entries are served in reverse chronological order across all
    # volumes, so we paginate from page 1 until we pass our target
    # year range (volume < from_vol).

    try:
        page = 1
        while not done:
            if args.limit and stats["added"] >= args.limit:
                log.info("Reached limit of %d entries", args.limit)
                break

            hub_url = f"{LINGUISTLIST_HUB}?topic=Books&page={page}"
            time.sleep(POLITENESS)

            html, ok = fetch_page(hub_url, session)
            if not ok:
                break

            issue_links = extract_issue_links(html)
            if not issue_links:
                break

            # Deduplicate (links appear twice in some layouts)
            seen = set()
            unique_links = []
            for link in issue_links:
                if link not in seen:
                    seen.add(link)
                    unique_links.append(link)

            # Check volume range of entries on this page
            page_volumes = set()
            for link in unique_links:
                m = re.match(r'/issues/(\d+)/', link)
                if m:
                    page_volumes.add(int(m.group(1)))

            if page_volumes:
                min_vol = min(page_volumes)
                max_vol = max(page_volumes)
                min_year = min_vol + VOLUME_OFFSET
                max_year = max_vol + VOLUME_OFFSET
                log.info("Page %d: %d entries (years %d–%d)",
                         page, len(unique_links), min_year, max_year)

                # If all entries on this page are before our range, stop
                if max_vol < from_vol:
                    log.info("Passed target year range, stopping")
                    break
            else:
                log.info("Page %d: %d entries", page, len(unique_links))

            for link in unique_links:
                if args.limit and stats["added"] >= args.limit:
                    break

                # Check volume from the link URL
                m = re.match(r'/issues/(\d+)/', link)
                if m:
                    entry_vol = int(m.group(1))
                    # Skip entries outside our year range
                    if entry_vol > until_vol:
                        continue
                    if entry_vol < from_vol:
                        # Past our range; since entries are chronological,
                        # remaining entries on this page may still be in range
                        # (pages can span volume boundaries), so just skip this one
                        continue

                issue_url = f"https://linguistlist.org{link}"
                time.sleep(POLITENESS)

                detail_html, ok = fetch_page(issue_url, session)
                if not ok:
                    stats["failed"] += 1
                    continue

                meta = extract_metadata(detail_html)

                if not meta.get("title"):
                    log.debug("  No title found: %s", issue_url)
                    stats["failed"] += 1
                    continue

                publisher = meta.get("publisher", "")

                if not publisher_matches(publisher, args.publishers):
                    log.debug("  Filtered publisher: %s", publisher)
                    stats["filtered"] += 1
                    continue

                title = meta["title"]
                authors = meta.get("authors", "")
                series = meta.get("series", "")
                pub_year = meta.get("year")
                book_url = meta.get("book_url", "")

                log.info("  %s", title)
                log.info("    Authors: %s", authors or "(unknown)")
                log.info("    Publisher: %s", publisher)
                if series:
                    log.info("    Series: %s", series)
                log.info("    Year: %s", pub_year or "(unknown)")
                if book_url:
                    log.info("    URL: %s", book_url)

                # Check for duplicates
                if book_url and book_url_exists(conn, book_url):
                    log.info("    Already in database (by URL), skipping")
                    stats["skipped"] += 1
                    continue
                if title and publisher and book_title_exists(conn, title, publisher):
                    log.info("    Already in database (by title+publisher), skipping")
                    stats["skipped"] += 1
                    continue

                if args.dry_run:
                    log.info("    [DRY RUN] Would add to database")
                    stats["added"] += 1
                    continue

                if insert_book(conn, title, authors, publisher,
                             series, pub_year, book_url):
                    log.info("    Added to database")
                    stats["added"] += 1
                else:
                    stats["skipped"] += 1

            # Check for next page: if we got fewer than 20 items, last page
            if len(unique_links) < 20:
                break
            page += 1

    except KeyboardInterrupt:
        log.info("Interrupted by user")

    conn.close()

    log.info("Done — added: %d, skipped: %d, filtered: %d, failed: %d",
             stats["added"], stats["skipped"], stats["filtered"], stats["failed"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
