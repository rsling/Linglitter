#!/usr/bin/env python3
"""
Scrape DOIs and bibliographic metadata from CrossRef for specified journals.

Usage:
    python scrape_dois.py
    python scrape_dois.py --from-year 2015 --until-year 2024
    python scrape_dois.py --journal "Cognitive Linguistics"
    python scrape_dois.py --publisher Benjamins --from-year 2020
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

CROSSREF_API = "https://api.crossref.org"
ROWS_PER_PAGE = 100
MAX_RETRIES = 5
RETRY_BACKOFF = 2  # seconds, doubled on each retry

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def setup_db(db_path):
    """Create SQLite database and articles table."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            doi          TEXT PRIMARY KEY,
            title        TEXT,
            authors      TEXT,
            journal      TEXT,
            year         INTEGER,
            volume       TEXT,
            issue        TEXT,
            pages        TEXT,
            publisher    TEXT
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_year ON articles(year)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_journal ON articles(journal)
    """)
    conn.commit()
    return conn


def upsert_article(conn, meta):
    """Insert or replace an article record (DOI is the primary key)."""
    conn.execute("""
        INSERT INTO articles (doi, title, authors, journal, year,
                              volume, issue, pages, publisher)
        VALUES (:doi, :title, :authors, :journal, :year,
                :volume, :issue, :pages, :publisher)
        ON CONFLICT(doi) DO UPDATE SET
            title     = excluded.title,
            authors   = excluded.authors,
            journal   = excluded.journal,
            year      = excluded.year,
            volume    = excluded.volume,
            issue     = excluded.issue,
            pages     = excluded.pages,
            publisher = excluded.publisher
    """, meta)


# ---------------------------------------------------------------------------
# CrossRef API
# ---------------------------------------------------------------------------

def _request_with_retry(url, params, timeout=30):
    """GET with exponential backoff on transient errors and rate limits."""
    delay = RETRY_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", delay))
                log.warning("Rate-limited, waiting %ds (attempt %d/%d)",
                            wait, attempt, MAX_RETRIES)
                time.sleep(wait)
                delay *= 2
                continue
            return resp
        except requests.exceptions.RequestException as exc:
            if attempt == MAX_RETRIES:
                raise
            log.warning("Request failed (%s), retrying in %ds (attempt %d/%d)",
                        exc, delay, attempt, MAX_RETRIES)
            time.sleep(delay)
            delay *= 2
    return None  # unreachable, but satisfies linters


def fetch_journal_works(issn, from_year, until_year, mailto=None):
    """Yield CrossRef work items for a journal ISSN within a year range.

    Only items of type ``journal-article`` are returned.
    """
    url = f"{CROSSREF_API}/journals/{issn}/works"
    params = {
        "filter": (f"from-pub-date:{from_year},"
                   f"until-pub-date:{until_year},"
                   "type:journal-article"),
        "rows": ROWS_PER_PAGE,
        "cursor": "*",
        "select": ("DOI,title,author,container-title,"
                    "published-print,published-online,"
                    "volume,issue,page,publisher"),
    }
    if mailto:
        params["mailto"] = mailto

    while True:
        resp = _request_with_retry(url, params)
        if resp.status_code == 404:
            log.warning("ISSN %s not found in CrossRef — skipping", issn)
            return
        resp.raise_for_status()

        data = resp.json()["message"]
        items = data.get("items", [])
        if not items:
            break

        yield from items

        next_cursor = data.get("next-cursor")
        if not next_cursor or len(items) < ROWS_PER_PAGE:
            break
        params["cursor"] = next_cursor
        time.sleep(0.15)  # stay well within polite-pool limits


# ---------------------------------------------------------------------------
# Metadata extraction
# ---------------------------------------------------------------------------

def extract_metadata(item, publisher_name):
    """Turn a CrossRef work item into a flat dict for the database."""
    doi = item.get("DOI", "")

    titles = item.get("title", [])
    title = re.sub(r"<[^>]+>", "", titles[0]) if titles else ""

    # Authors as "Family, Given; Family, Given; …"
    author_list = item.get("author", [])
    authors = "; ".join(
        f"{a.get('family', '')}, {a.get('given', '')}".strip(", ")
        for a in author_list
    )

    container = item.get("container-title", [])
    journal = container[0] if container else ""

    # Prefer print date, fall back to online date
    date_info = item.get("published-print") or item.get("published-online") or {}
    year = None
    if "date-parts" in date_info:
        parts = date_info["date-parts"]
        if parts and parts[0]:
            year = parts[0][0]

    volume = item.get("volume", "")
    issue = item.get("issue", "")
    pages = item.get("page", "")

    return {
        "doi": doi,
        "title": title,
        "authors": authors,
        "journal": journal,
        "year": year,
        "volume": volume,
        "issue": issue,
        "pages": pages,
        "publisher": publisher_name,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_journals(path, filter_name=None, filter_publisher=None):
    """Load and optionally filter the journal registry."""
    with open(path) as fh:
        journals = json.load(fh)

    if filter_name:
        journals = [j for j in journals
                    if j["name"].lower() == filter_name.lower()]
    if filter_publisher:
        journals = [j for j in journals
                    if j["publisher"].lower() == filter_publisher.lower()]
    return journals


def main():
    parser = argparse.ArgumentParser(
        description="Scrape DOIs and metadata from CrossRef for linguistics journals.")
    parser.add_argument("--from-year", type=int, default=2005,
                        help="Start of publication-year range (inclusive, default: 2005)")
    parser.add_argument("--until-year", type=int, default=2025,
                        help="End of publication-year range (inclusive, default: 2025)")
    parser.add_argument("--journal", type=str, default=None,
                        help="Scrape only this journal (exact name from journals.json)")
    parser.add_argument("--publisher", type=str, default=None,
                        help="Scrape only journals from this publisher")
    parser.add_argument("--db", type=str, default="linglitter.db",
                        help="Path to SQLite database (default: linglitter.db)")
    parser.add_argument("--journals-file", type=str, default="journals.json",
                        help="Path to journal registry JSON (default: journals.json)")
    parser.add_argument("--mailto", type=str, default=None,
                        help="Email for CrossRef polite pool (faster rate limits)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    journals_path = Path(args.journals_file)
    if not journals_path.exists():
        log.error("Journal registry not found: %s", journals_path)
        return 1

    journals = load_journals(journals_path, args.journal, args.publisher)
    if not journals:
        log.error("No matching journals found in %s", journals_path)
        return 1

    log.info("Scraping %d journal(s), years %d–%d",
             len(journals), args.from_year, args.until_year)

    conn = setup_db(args.db)
    total = 0

    for jrnl in journals:
        name = jrnl["name"]
        publisher = jrnl["publisher"]
        issns = jrnl["issn"]
        count = 0

        log.info("── %s  [%s]", name, publisher)

        for issn in issns:
            for item in fetch_journal_works(issn, args.from_year,
                                            args.until_year, args.mailto):
                meta = extract_metadata(item, publisher)
                if not meta["doi"]:
                    continue
                upsert_article(conn, meta)
                count += 1
                if count % 100 == 0:
                    conn.commit()
                    log.info("   … %d articles", count)

        conn.commit()
        total += count
        log.info("   %d articles saved", count)

    conn.close()
    log.info("Done — %d articles total in %s", total, args.db)
    return 0


if __name__ == "__main__":
    sys.exit(main())
