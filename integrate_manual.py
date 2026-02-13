#!/usr/bin/env python3
"""
Integrate manually downloaded PDFs into the data directory structure.

Scans the manual_dir for PDF files, matches them to articles in linglitter.db
by decoding the filename to a DOI, moves matched files to the appropriate
location under data_dir, and updates the database.

Usage:
    python integrate_manual.py
    python integrate_manual.py --config myconfig.json
    python integrate_manual.py --dry-run
"""

import argparse
import json
import logging
import re
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)


def encode_doi_for_filename(doi):
    """Encode DOI to be safe as a filename.

    Replaces /, \\, :, *, ?, ", <, >, |, and . with underscores.
    """
    unsafe_chars = r'[/\\:*?"<>|.]'
    return re.sub(unsafe_chars, "_", doi)


def load_config(config_path):
    """Load configuration from JSON file."""
    with open(config_path) as fh:
        return json.load(fh)


def build_doi_lookup(conn):
    """Build a mapping from encoded DOI to article metadata.

    Returns dict: encoded_doi -> (doi, publisher, journal, year)
    """
    query = """
        SELECT doi, publisher, journal, year
        FROM articles
    """
    lookup = {}
    for row in conn.execute(query).fetchall():
        doi, publisher, journal, year = row
        encoded = encode_doi_for_filename(doi)
        lookup[encoded] = (doi, publisher, journal, year)
    return lookup


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


def update_article(conn, doi, source, file_path):
    """Update an article's source and file fields."""
    now = datetime.now().isoformat()
    conn.execute("""
        UPDATE articles
        SET source = ?,
            file = ?,
            timestamp = ?
        WHERE doi = ?
    """, (source, file_path, now, doi))
    conn.commit()


def get_pdf_files(manual_dir):
    """Get all PDF files in the manual directory."""
    manual_path = Path(manual_dir)
    if not manual_path.exists():
        return []
    return list(manual_path.glob("*.pdf"))


def main():
    parser = argparse.ArgumentParser(
        description="Integrate manually downloaded PDFs into the data directory.")
    parser.add_argument("--config", type=str, default="config.json",
                        help="Path to config JSON (default: config.json)")
    parser.add_argument("--db", type=str, default="linglitter.db",
                        help="Path to SQLite database (default: linglitter.db)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without moving files")
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
    data_dir = config.get("data_dir", "data")
    manual_dir = config.get("manual_dir", "manual")

    # Connect to database
    db_path = Path(args.db)
    if not db_path.exists():
        log.error("Database not found: %s", db_path)
        return 1

    conn = sqlite3.connect(args.db)

    # Build lookup table: encoded_doi -> (doi, publisher, journal, year)
    log.info("Building DOI lookup table from database...")
    doi_lookup = build_doi_lookup(conn)
    log.info("Loaded %d DOIs from database", len(doi_lookup))

    # Get PDF files from manual directory
    pdf_files = get_pdf_files(manual_dir)
    if not pdf_files:
        log.info("No PDF files found in %s", manual_dir)
        conn.close()
        return 0

    log.info("Found %d PDF files in %s", len(pdf_files), manual_dir)

    # Stats
    matched = 0
    unmatched = 0
    errors = 0

    for pdf_path in pdf_files:
        # Extract encoded DOI from filename (remove .pdf suffix)
        encoded_doi = pdf_path.stem

        if encoded_doi not in doi_lookup:
            log.warning("No matching entry found for: %s", pdf_path.name)
            unmatched += 1
            continue

        doi, publisher, journal, year = doi_lookup[encoded_doi]
        log.info("Matched: %s -> %s", pdf_path.name, doi)

        # Build destination path
        dest_abs, dest_rel = build_pdf_path(data_dir, publisher, journal, year, doi)

        if args.dry_run:
            log.info("  [DRY RUN] Would move to: %s", dest_rel)
            log.info("  [DRY RUN] Would set source: https://doi.org/%s", doi)
            matched += 1
            continue

        try:
            # Ensure destination directory exists
            dest_abs.parent.mkdir(parents=True, exist_ok=True)

            # Move the file
            shutil.move(str(pdf_path), str(dest_abs))
            log.info("  Moved to: %s", dest_rel)

            # Update database
            source_url = f"https://doi.org/{doi}"
            update_article(conn, doi, source_url, dest_rel)
            log.info("  Updated database: source=%s, file=%s", source_url, dest_rel)

            matched += 1

        except Exception as e:
            log.error("  Error processing %s: %s", pdf_path.name, e)
            errors += 1

    conn.close()

    log.info("Done — matched: %d, unmatched: %d, errors: %d", matched, unmatched, errors)
    return 0


if __name__ == "__main__":
    sys.exit(main())
