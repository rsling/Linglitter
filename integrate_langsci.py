#!/usr/bin/env python3
"""
Integrate manually downloaded Language Science Press PDFs.

Reads PDFs from books/_Manual, reconstructs DOIs from filenames,
fetches metadata from LangSci book pages (via DOI redirect), and
interactively moves files into pdf/ with DB entries in linglitter.db.

Usage:
    python integrate_langsci.py
    python integrate_langsci.py --manual-dir books/_Manual
    python integrate_langsci.py --dry-run
"""

import argparse
import re
import sqlite3
import shutil
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import requests

PUBLISHER = "Language Science Press"
POLITENESS = 3

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Accept": "text/html,*/*;q=0.8",
}

# Known DOI prefix patterns and how to reconstruct them from encoded filenames.
# Encoded: all non-alnum → _
# We try patterns in order, first match wins.
DOI_PATTERNS = [
    # 10_5281_zenodo_NNNNN → 10.5281/zenodo.NNNNN
    (r'^10_5281_zenodo_(\d+)$', r'10.5281/zenodo.\1'),
    # 10_17169_langsci_bNNN_NNN → 10.17169/langsci.bNNN.NNN
    (r'^10_17169_langsci_b(\d+)_(\d+)$', r'10.17169/langsci.b\1.\2'),
    # 10_17169_FUDOCS_document_NNNNNNNNNNNN → 10.17169/FUDOCS_document_NNNNNNNNNNNN
    # (FUDOCS uses underscores in the actual DOI)
    (r'^10_17169_FUDOCS_document_(\d+)$', r'10.17169/FUDOCS_document_\1'),
]


def decode_doi(encoded):
    """Reconstruct DOI from encoded filename (without .pdf extension)."""
    for pattern, replacement in DOI_PATTERNS:
        m = re.match(pattern, encoded)
        if m:
            return re.sub(pattern, replacement, encoded)
    return None


def encode_doi_for_filename(doi):
    """Encode DOI to be safe as a filename."""
    return re.sub(r'[^a-zA-Z0-9]', '_', doi)


def resolve_doi(doi):
    """Follow DOI redirect to find the landing page URL."""
    try:
        resp = requests.head(f"https://doi.org/{doi}", allow_redirects=True,
                           timeout=15, headers=HEADERS)
        return resp.url
    except requests.exceptions.RequestException:
        return None


def fetch_langsci_metadata(url):
    """Scrape title, authors, series, and DOI from a LangSci book page."""
    try:
        resp = requests.get(url, timeout=30, headers=HEADERS)
        if resp.status_code != 200:
            return None
        html = resp.text
    except requests.exceptions.RequestException:
        return None

    result = {}

    # Title: <h1 class="title">...</h1>
    m = re.search(r'<h1[^>]*class="[^"]*title[^"]*"[^>]*>(.*?)</h1>',
                  html, re.DOTALL | re.IGNORECASE)
    if m:
        result["title"] = re.sub(r'<[^>]+>', '', m.group(1)).strip()

    # Authors: <div class="langsci_author">...</div>
    authors = []
    for m in re.finditer(r'<div[^>]*class="[^"]*langsci_author[^"]*"[^>]*>(.*?)</div>',
                         html, re.DOTALL | re.IGNORECASE):
        a = re.sub(r'<[^>]+>', '', m.group(1)).strip()
        if a:
            authors.append(a)
    if authors:
        result["authors"] = "; ".join(authors)

    # Series: look for series link in breadcrumbs or metadata
    # Pattern: <a href="/catalog/series/...">Series Name</a>
    m = re.search(r'<a[^>]*href="[^"]*?/catalog/series/[^"]*"[^>]*>(.*?)</a>',
                  html, re.DOTALL | re.IGNORECASE)
    if m:
        result["series"] = re.sub(r'<[^>]+>', '', m.group(1)).strip()

    return result if result else None


def fetch_zenodo_metadata(doi):
    """Fetch title and authors from Zenodo API."""
    # Extract record ID from DOI
    m = re.match(r'10\.5281/zenodo\.(\d+)', doi)
    if not m:
        return None
    record_id = m.group(1)

    try:
        resp = requests.get(f"https://zenodo.org/api/records/{record_id}",
                          timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        meta = data.get("metadata", {})
        result = {}
        if meta.get("title"):
            result["title"] = meta["title"]
        creators = meta.get("creators", [])
        if creators:
            result["authors"] = "; ".join(c.get("name", "") for c in creators)
        return result if result else None
    except requests.exceptions.RequestException:
        return None


def init_db(db_path):
    """Open the linglitter database."""
    conn = sqlite3.connect(db_path)
    return conn


def book_exists(conn, doi):
    """Check if an entry with this DOI is already in the database."""
    row = conn.execute("SELECT file FROM articles WHERE doi = ?", (doi,)).fetchone()
    return row is not None


def insert_book(conn, doi, title, authors, publisher, series, file_path):
    """Insert a book entry into the database."""
    conn.execute("""
        INSERT INTO articles (doi, title, authors, publisher, journal, file,
                              availability, type)
        VALUES (?, ?, ?, ?, ?, ?, 'oa', 'book')
    """, (doi, title, authors, publisher, series, file_path))
    conn.commit()


def prompt_user(question, default="y"):
    """Ask the user a yes/no question. Returns True for yes."""
    suffix = " [Y/n] " if default == "y" else " [y/N] "
    answer = input(question + suffix).strip().lower()
    if not answer:
        return default == "y"
    return answer.startswith("y")


def prompt_edit(field_name, value):
    """Let the user confirm or edit a metadata field. Returns the final value."""
    answer = input(f"  {field_name}: {value}  [Enter=OK, or type new value] ").strip()
    return answer if answer else value


def main():
    parser = argparse.ArgumentParser(
        description="Integrate manually downloaded LangSci Press PDFs.")
    parser.add_argument("--manual-dir", type=str, default="books/_Manual",
                        help="Directory with manual downloads (default: books/_Manual)")
    parser.add_argument("--pdf-dir", type=str, default="pdf",
                        help="Directory for PDFs (default: pdf)")
    parser.add_argument("--db", type=str, default="linglitter.db",
                        help="Path to SQLite database (default: linglitter.db)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without moving files")
    args = parser.parse_args()

    manual_dir = Path(args.manual_dir)
    pdf_dir = Path(args.pdf_dir)

    if not manual_dir.exists():
        print(f"Error: Manual directory not found: {manual_dir}", file=sys.stderr)
        return 1

    pdfs = sorted(manual_dir.glob("*.pdf"))
    if not pdfs:
        print("No PDF files found in", manual_dir)
        return 0

    conn = init_db(args.db)
    session = requests.Session()
    session.headers.update(HEADERS)

    processed = 0
    skipped = 0
    failed = 0

    print(f"Found {len(pdfs)} PDFs in {manual_dir}\n")

    for i, pdf_path in enumerate(pdfs):
        encoded = pdf_path.stem  # filename without .pdf
        doi = decode_doi(encoded)

        print(f"[{i+1}/{len(pdfs)}] {pdf_path.name}")

        if doi is None:
            print(f"  Could not decode DOI from filename: {encoded}")
            print(f"  Enter DOI manually (or press Enter to skip): ", end="")
            manual_doi = input().strip()
            if not manual_doi:
                print("  Skipped.\n")
                skipped += 1
                continue
            doi = manual_doi

        print(f"  DOI: {doi}")

        if book_exists(conn, doi):
            print("  Already in database, skipping.\n")
            skipped += 1
            continue

        # Fetch metadata: try LangSci book page first (via DOI redirect),
        # fall back to Zenodo API for zenodo DOIs.
        time.sleep(POLITENESS)
        landing_url = resolve_doi(doi)
        meta = {}

        if landing_url and "langsci-press.org" in landing_url:
            print(f"  LangSci page: {landing_url}")
            time.sleep(POLITENESS)
            meta = fetch_langsci_metadata(landing_url) or {}
        elif landing_url and "zenodo.org" in landing_url:
            print(f"  Zenodo page: {landing_url}")
            # Try Zenodo API for title/authors
            meta = fetch_zenodo_metadata(doi) or {}
            # Also try to find LangSci book page for series info
            # Search LangSci by DOI
            time.sleep(POLITENESS)
            search_url = f"https://langsci-press.org/catalog"
            # We can't easily search, so series will need manual input
        else:
            print(f"  Landing page: {landing_url or '(resolution failed)'}")

        title = meta.get("title", "(unknown)")
        authors = meta.get("authors", "(unknown)")
        series = meta.get("series", "")

        # Interactive confirmation and editing
        print(f"  Title: {title}")
        print(f"  Authors: {authors}")
        if series:
            print(f"  Series: {series}")
        else:
            print(f"  Series: (not found)")

        if not series:
            series = input("  Enter series name (required): ").strip()
            if not series:
                print("  No series provided, skipping.\n")
                skipped += 1
                continue

        # Let user edit fields
        title = prompt_edit("Title", title)
        authors = prompt_edit("Authors", authors)
        series = prompt_edit("Series", series)

        # Build destination path (flat in pdf/)
        safe_doi = encode_doi_for_filename(doi)
        filename = f"{safe_doi}.pdf"
        abs_path = pdf_dir / filename

        print(f"  Destination: pdf/{filename}")

        if not prompt_user("  Proceed?"):
            print("  Skipped.\n")
            skipped += 1
            continue

        if args.dry_run:
            print(f"  [DRY RUN] Would move and insert.\n")
            continue

        # Move file
        pdf_dir.mkdir(parents=True, exist_ok=True)
        if abs_path.exists():
            print(f"  Warning: destination already exists, overwriting.")
        shutil.move(str(pdf_path), str(abs_path))

        # Insert into DB
        insert_book(conn, doi, title, authors, PUBLISHER, series, filename)
        print(f"  Done.\n")
        processed += 1

    conn.close()
    print(f"\nFinished: {processed} integrated, {skipped} skipped, {failed} failed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
