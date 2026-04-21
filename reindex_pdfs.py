#!/usr/bin/env python3
"""
Re-index PDFs in the data directory against linglitter.db.

Scans the data/ directory for PDFs, matches them to database entries
by encoded DOI filename, and updates the 'file' column for entries
that have a PDF on disk but aren't linked.

Usage:
    python reindex_pdfs.py --dry-run    # report only
    python reindex_pdfs.py              # report and fix unlinked entries
"""

import argparse
import logging
import os
import re
import sqlite3
import sys
from pathlib import Path

log = logging.getLogger(__name__)


def encode_doi_for_filename(doi):
    """Encode DOI to be safe as a filename."""
    return re.sub(r'[/\\:*?"<>|.]', '_', doi)


def build_filename_to_doi(conn):
    """Build a map from encoded DOI filenames to DOIs."""
    rows = conn.execute("SELECT doi FROM articles").fetchall()
    mapping = {}
    for (doi,) in rows:
        filename = encode_doi_for_filename(doi) + ".pdf"
        mapping[filename] = doi
    return mapping


def main():
    parser = argparse.ArgumentParser(
        description="Re-index PDFs in pdf/ against linglitter.db.")
    parser.add_argument("--db", type=str, default="linglitter.db",
                        help="Path to SQLite database (default: linglitter.db)")
    parser.add_argument("--pdf-dir", type=str, default="pdf",
                        help="Path to PDF directory (default: pdf)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report only, do not modify the database")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    db_path = Path(args.db)
    pdf_dir = Path(args.pdf_dir)

    if not db_path.exists():
        log.error("Database not found: %s", db_path)
        return 1
    if not pdf_dir.exists():
        log.error("PDF directory not found: %s", pdf_dir)
        return 1

    conn = sqlite3.connect(str(db_path))

    # Build filename-to-DOI map from all DB entries
    log.info("Building filename-to-DOI map from database…")
    fn_to_doi = build_filename_to_doi(conn)
    log.info("  %d DOIs in database", len(fn_to_doi))

    # Build set of currently linked file paths
    linked_files = set()
    for (fpath,) in conn.execute("SELECT file FROM articles WHERE file IS NOT NULL"):
        linked_files.add(fpath)

    # Scan PDFs on disk
    log.info("Scanning %s for PDFs…", pdf_dir)

    correctly_linked = 0
    relinked = 0
    no_match = 0
    relink_updates = []  # (doi, rel_path) pairs to apply

    for filename in os.listdir(str(pdf_dir)):
        if not filename.lower().endswith(".pdf"):
            continue

        rel_path = filename

        if rel_path in linked_files:
            correctly_linked += 1
            continue

        # Not linked — try to match by filename
        doi = fn_to_doi.get(filename)
        if doi:
            relinked += 1
            relink_updates.append((doi, rel_path))
            log.info("  Relink: %s → %s", rel_path, doi)
        else:
            no_match += 1
            log.info("  No match: %s", rel_path)

    # Check DB entries pointing to missing files
    db_missing = 0
    for (doi, fpath) in conn.execute(
            "SELECT doi, file FROM articles WHERE file IS NOT NULL"):
        full = pdf_dir / fpath
        if not full.exists():
            db_missing += 1
            log.info("  Missing file: %s (DOI: %s)", fpath, doi)

    # Apply relinks
    if relink_updates and not args.dry_run:
        log.info("Updating %d entries in database…", len(relink_updates))
        for doi, rel_path in relink_updates:
            conn.execute("UPDATE articles SET file = ? WHERE doi = ?",
                        (rel_path, doi))
        conn.commit()
        log.info("Done.")
    elif relink_updates and args.dry_run:
        log.info("[DRY RUN] Would update %d entries.", len(relink_updates))

    conn.close()

    # Summary
    total_pdfs = correctly_linked + relinked + no_match
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  PDFs on disk:                    {total_pdfs}")
    print(f"  Correctly linked in DB:          {correctly_linked}")
    print(f"  Relinkable (DOI matched):        {relinked}")
    print(f"  No DB match found:               {no_match}")
    print(f"  DB entries pointing to missing:  {db_missing}")
    if args.dry_run and relink_updates:
        print(f"\n  Run without --dry-run to fix the {relinked} relinkable entries.")
    elif relink_updates:
        print(f"\n  Fixed {relinked} entries.")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
