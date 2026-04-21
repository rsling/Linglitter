#!/usr/bin/env python3
"""
Migrate PDFs from hierarchical data/<Publisher>/<Journal>/<Year>/ structure
to a flat pdf/ directory, updating linglitter.db accordingly.

For each article where 'file' IS NOT NULL:
  1. Check if the file exists at the old location (data_dir / file)
  2. Move it to pdf/<filename> (just the encoded-DOI basename)
  3. Update the DB 'file' column to the new relative path

If a filename collision is detected, emit a WARNING and skip that entry.

Usage:
    python migrate_pdfs.py --dry-run    # report only
    python migrate_pdfs.py              # actually migrate
"""

import argparse
import json
import logging
import shutil
import sqlite3
import sys
from pathlib import Path

log = logging.getLogger(__name__)

PDF_DIR = "pdf"


def load_config(config_path="config.json"):
    with open(config_path) as fh:
        return json.load(fh)


def main():
    parser = argparse.ArgumentParser(
        description="Migrate PDFs from hierarchical data/ to flat pdf/ directory.")
    parser.add_argument("--config", type=str, default="config.json",
                        help="Path to config JSON (default: config.json)")
    parser.add_argument("--db", type=str, default="linglitter.db",
                        help="Path to SQLite database (default: linglitter.db)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would be done without moving files or updating DB")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    config = load_config(args.config)
    old_data_dir = Path(config.get("data_dir", "data"))
    pdf_dir = Path(PDF_DIR)

    if not args.dry_run:
        pdf_dir.mkdir(parents=True, exist_ok=True)

    db_path = Path(args.db)
    if not db_path.exists():
        log.error("Database not found: %s", db_path)
        return 1

    conn = sqlite3.connect(args.db)

    # Get all articles with a file path
    rows = conn.execute(
        "SELECT doi, file FROM articles WHERE file IS NOT NULL"
    ).fetchall()

    log.info("Found %d articles with file paths in database", len(rows))

    # Stats
    moved = 0
    already_in_pdf = 0
    file_missing = 0
    collisions = 0
    db_updated = 0
    errors = 0

    # Track destination filenames to detect collisions within this run
    seen_filenames = {}  # filename -> doi

    for doi, old_rel_path in rows:
        old_abs_path = old_data_dir / old_rel_path
        filename = Path(old_rel_path).name  # e.g. 10_1515_cog-2018-0098.pdf
        new_rel_path = filename  # flat: just the filename
        new_abs_path = pdf_dir / filename

        # Check for filename collision
        if filename in seen_filenames:
            log.warning("COLLISION: %s — DOI %s conflicts with DOI %s. "
                        "Skipping both.", filename, doi, seen_filenames[filename])
            collisions += 1
            continue
        seen_filenames[filename] = doi

        # Check if file already lives in pdf/
        if old_abs_path == new_abs_path or old_rel_path == new_rel_path:
            already_in_pdf += 1
            continue

        # Check if file is already at the new location (from a previous partial run)
        if new_abs_path.exists() and not old_abs_path.exists():
            # File already migrated, just update DB
            if not args.dry_run:
                conn.execute("UPDATE articles SET file = ? WHERE doi = ?",
                             (new_rel_path, doi))
                conn.commit()
            db_updated += 1
            log.debug("DB updated (file already at destination): %s", doi)
            continue

        # Check if source file exists
        if not old_abs_path.exists():
            log.warning("File missing: %s (DOI: %s)", old_rel_path, doi)
            file_missing += 1
            continue

        # Check collision with existing file at destination
        if new_abs_path.exists():
            log.warning("COLLISION: destination already exists: %s (DOI: %s). "
                        "Skipping.", filename, doi)
            collisions += 1
            continue

        # Move file
        if args.dry_run:
            log.info("Would move: %s -> %s", old_rel_path, new_rel_path)
        else:
            try:
                shutil.move(str(old_abs_path), str(new_abs_path))
                conn.execute("UPDATE articles SET file = ? WHERE doi = ?",
                             (new_rel_path, doi))
                conn.commit()
                log.debug("Moved: %s -> %s", old_rel_path, new_rel_path)
            except Exception as e:
                log.error("Error moving %s: %s", old_rel_path, e)
                errors += 1
                continue

        moved += 1

    conn.close()

    # Report
    print()
    print("=" * 60)
    print("MIGRATION SUMMARY")
    print("=" * 60)
    print(f"  Articles with file paths:    {len(rows)}")
    print(f"  Successfully moved:          {moved}")
    print(f"  Already in pdf/:             {already_in_pdf}")
    print(f"  DB-only updates (pre-moved): {db_updated}")
    print(f"  Source file missing:         {file_missing}")
    print(f"  Filename collisions:         {collisions}")
    print(f"  Errors:                      {errors}")
    if args.dry_run:
        print()
        print("  [DRY RUN] No files were moved and no DB entries were changed.")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
