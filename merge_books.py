#!/usr/bin/env python3
"""
Merge lingbooks.db into linglitter.db and move book PDFs to pdf/.

Steps:
  1. Add 'type' column to articles table (default 'article')
  2. Import books from lingbooks.db as type='book':
     - Real DOIs (10.*): use as-is, skip 5 that overlap with articles
     - No DOI but has book_url: strip "https://", encode non-alphanum → _
     - No DOI, no book_url: encode "book:<publisher>:<title>" non-alphanum → _
  3. Move book PDFs from books/ to pdf/, update file column

Usage:
    python merge_books.py --dry-run    # preview
    python merge_books.py              # execute
"""

import argparse
import logging
import re
import shutil
import sqlite3
import sys
from pathlib import Path

log = logging.getLogger(__name__)

PDF_DIR = "pdf"
BOOKS_DIR = "books"


def encode_for_key(s):
    """Encode a string for use as a DOI-like key: non-alphanum → underscore."""
    return re.sub(r'[^a-zA-Z0-9]', '_', s)


def generate_book_key(book_url, publisher, title):
    """Generate a synthetic primary key for a DOI-less book."""
    if book_url:
        # Strip https:// or http://
        url = re.sub(r'^https?://', '', book_url)
        return encode_for_key(url)
    # Fallback: publisher + title
    raw = f"book:{publisher or 'unknown'}:{title or 'unknown'}"
    return encode_for_key(raw)


def main():
    parser = argparse.ArgumentParser(
        description="Merge lingbooks.db into linglitter.db and move book PDFs to pdf/.")
    parser.add_argument("--db", type=str, default="linglitter.db",
                        help="Path to linglitter database (default: linglitter.db)")
    parser.add_argument("--books-db", type=str, default="lingbooks.db",
                        help="Path to lingbooks database (default: lingbooks.db)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without changing anything")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    db_path = Path(args.db)
    books_db_path = Path(args.books_db)
    pdf_dir = Path(PDF_DIR)
    books_dir = Path(BOOKS_DIR)

    if not db_path.exists():
        log.error("Database not found: %s", db_path)
        return 1
    if not books_db_path.exists():
        log.error("Books database not found: %s", books_db_path)
        return 1

    conn = sqlite3.connect(args.db)

    # Step 1: Add 'type' column if it doesn't exist
    try:
        conn.execute("ALTER TABLE articles ADD COLUMN type TEXT DEFAULT 'article'")
        conn.commit()
        log.info("Added 'type' column to articles table")
    except sqlite3.OperationalError:
        log.info("'type' column already exists")

    if not args.dry_run:
        pdf_dir.mkdir(parents=True, exist_ok=True)

    # Step 2: Load books from lingbooks.db
    books_conn = sqlite3.connect(args.books_db)
    books_conn.row_factory = sqlite3.Row
    books = books_conn.execute("""
        SELECT doi, title, authors, publisher, series, file,
               year, book_url, availability
        FROM books
    """).fetchall()
    books_conn.close()

    log.info("Loaded %d books from %s", len(books), args.books_db)

    # Get existing article DOIs to detect overlaps
    existing_dois = set()
    for (doi,) in conn.execute("SELECT doi FROM articles"):
        existing_dois.add(doi)

    # Stats
    imported = 0
    skipped_overlap = 0
    skipped_duplicate = 0
    files_moved = 0
    files_missing = 0
    file_collisions = 0
    errors = 0

    for book in books:
        doi = book["doi"]
        title = book["title"]
        authors = book["authors"]
        publisher = book["publisher"]
        series = book["series"]
        old_file = book["file"]
        year = book["year"]
        book_url = book["book_url"]
        availability = book["availability"]

        # Determine the primary key
        if doi and doi.startswith("10."):
            # Real DOI
            new_doi = doi
            if new_doi in existing_dois:
                log.info("Overlap (dropping book): %s — %s", new_doi, title)
                skipped_overlap += 1
                continue
        else:
            # Synthetic key
            new_doi = generate_book_key(book_url, publisher, title)

        # Check if this key already exists (from a previous run or collision)
        if new_doi in existing_dois:
            log.debug("Duplicate key, skipping: %s", new_doi)
            skipped_duplicate += 1
            continue

        # Handle file: move from books/ to pdf/
        new_file = None
        if old_file:
            old_abs = books_dir / old_file
            filename = Path(old_file).name
            new_abs = pdf_dir / filename
            new_file = filename

            if new_abs.exists() and not old_abs.exists():
                # Already moved (previous partial run)
                pass
            elif not old_abs.exists():
                log.warning("Book PDF missing: %s (DOI: %s)", old_file, new_doi)
                files_missing += 1
                new_file = None
            elif new_abs.exists():
                log.warning("COLLISION: %s already exists in pdf/ (DOI: %s). Skipping file move.",
                            filename, new_doi)
                file_collisions += 1
                new_file = None
            else:
                if not args.dry_run:
                    try:
                        shutil.move(str(old_abs), str(new_abs))
                        files_moved += 1
                    except Exception as e:
                        log.error("Error moving %s: %s", old_file, e)
                        errors += 1
                        new_file = None
                else:
                    log.info("Would move: %s -> pdf/%s", old_file, filename)
                    files_moved += 1

        # Insert into articles table
        if not args.dry_run:
            try:
                conn.execute("""
                    INSERT INTO articles
                        (doi, title, authors, journal, year, publisher,
                         file, jump_url, availability, type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'book')
                """, (new_doi, title, authors, series, year, publisher,
                      new_file, book_url, availability))
                conn.commit()
            except sqlite3.IntegrityError as e:
                log.error("Insert failed for %s: %s", new_doi, e)
                errors += 1
                continue

        existing_dois.add(new_doi)
        imported += 1

    conn.close()

    # Report
    print()
    print("=" * 60)
    print("MERGE SUMMARY")
    print("=" * 60)
    print(f"  Books in lingbooks.db:       {len(books)}")
    print(f"  Imported into linglitter.db: {imported}")
    print(f"  Skipped (DOI overlap):       {skipped_overlap}")
    print(f"  Skipped (duplicate key):     {skipped_duplicate}")
    print(f"  Files moved to pdf/:         {files_moved}")
    print(f"  Files missing:               {files_missing}")
    print(f"  File collisions:             {file_collisions}")
    print(f"  Errors:                      {errors}")
    if args.dry_run:
        print()
        print("  [DRY RUN] No changes were made.")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
