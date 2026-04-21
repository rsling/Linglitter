#!/usr/bin/env python3
"""
Enrich book entries in linglitter.db with missing metadata from Google Books and CrossRef.

Only touches book entries where file IS NULL. For each such entry, if any of
authors, doi, or year is missing, queries Google Books API and CrossRef API
to fill in the gaps. Also sets availability='oa' if OA status is confirmed
by either source (no extra queries for this).

Usage:
    python enrich_lingbooks.py
    python enrich_lingbooks.py --limit 100
    python enrich_lingbooks.py --dry-run
    python enrich_lingbooks.py --source crossref   # skip Google Books
    python enrich_lingbooks.py --source google      # skip CrossRef
"""

import argparse
import logging
import re
import sqlite3
import sys
import time
from urllib.parse import quote_plus

import requests

log = logging.getLogger(__name__)

DB_PATH = "linglitter.db"
POLITENESS_GOOGLE = 1.0   # seconds between Google Books requests
POLITENESS_CROSSREF = 1.0  # seconds between CrossRef requests

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "LingLitter/1.0 (mailto:roland.schaefer@uni-jena.de)",
})


# ---------------------------------------------------------------------------
# Google Books API
# ---------------------------------------------------------------------------

def query_google_books(title, publisher):
    """Search Google Books by title and publisher. Returns dict or None."""
    q_parts = []
    if title:
        q_parts.append(f"intitle:{title}")
    if publisher:
        q_parts.append(f"inpublisher:{publisher}")
    if not q_parts:
        return None

    q = "+".join(q_parts)
    url = f"https://www.googleapis.com/books/v1/volumes?q={quote_plus(q)}&maxResults=3"

    try:
        resp = SESSION.get(url, timeout=30)
        if resp.status_code == 429:
            log.warning("Google Books rate limit hit, backing off 30s")
            time.sleep(30)
            resp = SESSION.get(url, timeout=30)
        if resp.status_code != 200:
            log.warning("Google Books HTTP %d", resp.status_code)
            return None
        data = resp.json()
    except (requests.RequestException, ValueError) as exc:
        log.warning("Google Books request failed: %s", exc)
        return None

    if data.get("totalItems", 0) == 0:
        return None

    # Pick best match: check if title is a close match
    for item in data.get("items", []):
        vol = item.get("volumeInfo", {})
        candidate_title = vol.get("title", "")
        if _title_match(title, candidate_title):
            result = {}
            authors = vol.get("authors")
            if authors:
                result["authors"] = "; ".join(authors)
            pub_date = vol.get("publishedDate", "")
            year = _extract_year(pub_date)
            if year:
                result["year"] = year
            # OA: check accessInfo
            access = item.get("accessInfo", {})
            if access.get("publicDomain") or access.get("epub", {}).get("isAvailable"):
                viewability = access.get("viewability", "")
                if viewability in ("ALL_PAGES", "PARTIAL_FREE"):
                    result["oa"] = True
            return result if result else None
    return None


# ---------------------------------------------------------------------------
# CrossRef API
# ---------------------------------------------------------------------------

def query_crossref(title, publisher):
    """Search CrossRef by title and publisher. Returns dict or None."""
    if not title:
        return None

    params = {
        "query.bibliographic": title,
        "rows": 3,
    }
    if publisher:
        params["query.publisher-name"] = publisher

    url = "https://api.crossref.org/works"

    try:
        resp = SESSION.get(url, params=params, timeout=30)
        if resp.status_code == 429:
            log.warning("CrossRef rate limit hit, backing off 30s")
            time.sleep(30)
            resp = SESSION.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            log.warning("CrossRef HTTP %d", resp.status_code)
            return None
        data = resp.json()
    except (requests.RequestException, ValueError) as exc:
        log.warning("CrossRef request failed: %s", exc)
        return None

    items = data.get("message", {}).get("items", [])
    if not items:
        return None

    for item in items:
        # CrossRef titles are in a list
        cr_titles = item.get("title", [])
        cr_title = cr_titles[0] if cr_titles else ""
        if not _title_match(title, cr_title):
            continue

        result = {}

        # DOI
        doi = item.get("DOI")
        if doi:
            result["doi"] = doi

        # Authors
        author_list = item.get("author", [])
        if author_list:
            names = []
            for a in author_list:
                given = a.get("given", "")
                family = a.get("family", "")
                if given and family:
                    names.append(f"{given} {family}")
                elif family:
                    names.append(family)
            if names:
                result["authors"] = "; ".join(names)

        # Year
        issued = item.get("issued", {}).get("date-parts", [[]])
        if issued and issued[0] and issued[0][0]:
            result["year"] = issued[0][0]

        # OA (from license or is-referenced-by-count won't help, but
        # some CrossRef records have license info)
        licenses = item.get("license", [])
        for lic in licenses:
            lic_url = lic.get("URL", "")
            if "creativecommons.org" in lic_url:
                result["oa"] = True
                break

        return result if result else None

    return None


# ---------------------------------------------------------------------------
# Matching helpers
# ---------------------------------------------------------------------------

def _sanitise_title(title):
    """Strip HTML garbage and metadata leakage from scraped titles."""
    if not title:
        return title
    # Truncate at common metadata leakage markers
    for marker in ["Subtitle:", "Series Title:", "Published:", "Publisher:",
                    "&nbsp;", "http://", "https://"]:
        idx = title.find(marker)
        if idx > 0:
            title = title[:idx]
    # Strip HTML entities and tags
    title = re.sub(r'&\w+;', ' ', title)
    title = re.sub(r'<[^>]+>', ' ', title)
    title = re.sub(r'\s+', ' ', title).strip()
    return title


def _normalise(s):
    """Lowercase, strip punctuation and extra whitespace."""
    s = s.lower()
    s = re.sub(r'[^\w\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _title_match(our_title, candidate_title):
    """Check if candidate is a reasonable match for our title."""
    a = _normalise(our_title)
    b = _normalise(candidate_title)
    if not a or not b:
        return False
    # Exact match after normalisation
    if a == b:
        return True
    # One contains the other (handles subtitle variations)
    if a in b or b in a:
        return True
    # Token overlap: at least 70% of words in common
    words_a = set(a.split())
    words_b = set(b.split())
    if not words_a or not words_b:
        return False
    overlap = len(words_a & words_b)
    shorter = min(len(words_a), len(words_b))
    if shorter > 0 and overlap / shorter >= 0.7:
        return True
    return False


def _extract_year(date_str):
    """Extract a 4-digit year from a date string like '2020-03-15' or '2020'."""
    m = re.search(r'(\d{4})', date_str)
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

def get_entries_to_enrich(conn):
    """Fetch book entries with file IS NULL that have missing metadata."""
    cursor = conn.execute("""
        SELECT rowid, doi, title, authors, publisher, year, availability
        FROM articles
        WHERE type = 'book'
          AND file IS NULL
          AND (
            authors IS NULL OR authors = ''
            OR year IS NULL
          )
        ORDER BY title
    """)
    return cursor.fetchall()


def update_entry(conn, rowid, new_data):
    """Update an entry with enriched data. Uses rowid for safe NULL-key matching."""
    updates = []
    params = []

    if "authors" in new_data:
        updates.append("authors = ?")
        params.append(new_data["authors"])
    if "year" in new_data:
        updates.append("year = ?")
        params.append(new_data["year"])
    if "oa" in new_data:
        updates.append("availability = 'oa'")
    if "doi" in new_data:
        updates.append("doi = ?")
        params.append(new_data["doi"])

    if not updates:
        return []

    params.append(rowid)
    sql = f"UPDATE articles SET {', '.join(updates)} WHERE rowid = ?"

    try:
        conn.execute(sql, params)
        conn.commit()
    except sqlite3.IntegrityError:
        # DOI conflict: another entry already has this DOI.
        # Update without changing the DOI.
        if "doi" in new_data:
            log.debug("DOI conflict for %s, updating without DOI change",
                      new_data["doi"])
            new_data_no_doi = {k: v for k, v in new_data.items() if k != "doi"}
            return update_entry(conn, rowid, new_data_no_doi)
        return []

    return [k for k in new_data if k != "oa" or "oa" in new_data]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Enrich book entries in linglitter.db with metadata from Google Books and CrossRef.")
    parser.add_argument("--db", default=DB_PATH,
                        help="Path to SQLite database (default: %(default)s)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max entries to process")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without writing to DB")
    parser.add_argument("--source", choices=["both", "google", "crossref"],
                        default="both",
                        help="Which API(s) to query (default: both)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    conn = sqlite3.connect(args.db)
    entries = get_entries_to_enrich(conn)
    total = len(entries)

    if args.limit:
        entries = entries[:args.limit]

    log.info("Found %d entries to enrich (processing %d)", total, len(entries))

    use_google = args.source in ("both", "google")
    use_crossref = args.source in ("both", "crossref")

    stats = {"updated": 0, "no_match": 0, "errors": 0,
             "fields": {"authors": 0, "year": 0, "doi": 0, "oa": 0}}

    try:
        for i, (rowid, doi, title, authors, publisher, year, availability) in enumerate(entries):
            log.info("[%d/%d] %s", i + 1, len(entries), title)

            # Clean up title for API queries
            clean_title = _sanitise_title(title)
            if not clean_title:
                log.warning("  Empty title after sanitisation, skipping")
                stats["errors"] += 1
                continue

            # Determine what we're missing
            need_authors = not authors
            need_doi = not doi or not doi.startswith("10.")
            need_year = year is None

            merged = {}

            # --- Google Books ---
            if use_google and (need_authors or need_year):
                time.sleep(POLITENESS_GOOGLE)
                gb = query_google_books(clean_title, publisher)
                if gb:
                    log.info("  Google Books: found match")
                    if need_authors and "authors" in gb:
                        merged["authors"] = gb["authors"]
                        need_authors = False
                    if need_year and "year" in gb:
                        merged["year"] = gb["year"]
                        need_year = False
                    if gb.get("oa") and availability != "oa":
                        merged["oa"] = True
                else:
                    log.debug("  Google Books: no match")

            # --- CrossRef ---
            if use_crossref and (need_authors or need_year or need_doi):
                time.sleep(POLITENESS_CROSSREF)
                cr = query_crossref(clean_title, publisher)
                if cr:
                    log.info("  CrossRef: found match")
                    if need_doi and "doi" in cr:
                        merged["doi"] = cr["doi"]
                    if need_authors and "authors" in cr:
                        merged["authors"] = cr["authors"]
                    if need_year and "year" in cr:
                        merged["year"] = cr["year"]
                    if cr.get("oa") and availability != "oa":
                        merged["oa"] = True
                else:
                    log.debug("  CrossRef: no match")

            if not merged:
                log.info("  No new data found")
                stats["no_match"] += 1
                continue

            log.info("  Enriched: %s", ", ".join(
                f"{k}={v}" for k, v in merged.items()))

            if args.dry_run:
                stats["updated"] += 1
                for k in merged:
                    if k in stats["fields"]:
                        stats["fields"][k] += 1
                continue

            updated = update_entry(conn, rowid, merged)
            if updated:
                stats["updated"] += 1
                for k in merged:
                    if k in stats["fields"]:
                        stats["fields"][k] += 1
            else:
                stats["errors"] += 1

    except KeyboardInterrupt:
        log.info("Interrupted by user")

    conn.close()

    log.info("Done — updated: %d, no match: %d, errors: %d",
             stats["updated"], stats["no_match"], stats["errors"])
    log.info("Fields filled: authors=%d, year=%d, doi=%d, oa=%d",
             stats["fields"]["authors"], stats["fields"]["year"],
             stats["fields"]["doi"], stats["fields"]["oa"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
