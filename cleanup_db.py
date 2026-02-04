#!/usr/bin/env python3
"""
Clean up linglitter.db:
  - Move junk entries (empty authors, review articles) to linglitter_trash.db
  - Normalize author name formatting

Usage:
    python cleanup_db.py
"""

import re
import sqlite3
import sys

MAIN_DB = "linglitter.db"
TRASH_DB = "linglitter_trash.db"

# Rows matching any of these conditions are moved to the trash DB.
# SQLite LIKE is case-insensitive for ASCII letters by default.
TRASH_CONDITION = """
    authors IS NULL OR authors = ''
    OR title LIKE 'Review%'
    OR title LIKE '%Book Review%'
    OR title LIKE '%(review)%'
"""


def create_table(conn):
    """Create the articles table if it doesn't exist."""
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
    conn.commit()


def normalize_authors(authors_str):
    """Normalize author names.

    - ALL-CAPS names → title-cased  (e.g. "SMITH, JOHN" → "Smith, John")
    - Bare single-letter initials get a period  (e.g. "Smith, J K" → "Smith, J. K.")
    """
    if not authors_str:
        return authors_str

    authors = authors_str.split("; ")
    result = []

    for author in authors:
        parts = author.split(", ", 1)
        new_parts = []
        for part in parts:
            alpha = [c for c in part if c.isalpha()]
            if alpha and all(c.isupper() for c in alpha):
                part = part.title()
            new_parts.append(part)

        # Add periods after bare single-letter initials in given/middle names.
        # Matches an uppercase letter that is NOT preceded by a letter and
        # NOT followed by a letter or period.
        if len(new_parts) > 1:
            new_parts[1] = re.sub(
                r'(?<![A-Za-z])([A-Z])(?![a-zA-Z.])',
                r'\1.',
                new_parts[1],
            )

        result.append(", ".join(new_parts))

    return "; ".join(result)


def main():
    main_conn = sqlite3.connect(MAIN_DB)
    main_conn.execute("PRAGMA journal_mode=WAL")

    trash_conn = sqlite3.connect(TRASH_DB)
    trash_conn.execute("PRAGMA journal_mode=WAL")
    create_table(trash_conn)

    # ── Step 1: Move trash entries ────────────────────────────────────────

    before = main_conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]

    # Per-category counts (may overlap)
    empty_authors = main_conn.execute(
        "SELECT COUNT(*) FROM articles WHERE authors IS NULL OR authors = ''"
    ).fetchone()[0]
    title_review = main_conn.execute(
        "SELECT COUNT(*) FROM articles WHERE title LIKE 'Review%'"
    ).fetchone()[0]
    book_review = main_conn.execute(
        "SELECT COUNT(*) FROM articles WHERE title LIKE '%Book Review%'"
    ).fetchone()[0]
    paren_review = main_conn.execute(
        "SELECT COUNT(*) FROM articles WHERE title LIKE '%(review)%'"
    ).fetchone()[0]

    # Move matching rows to trash DB
    rows = main_conn.execute(
        f"SELECT * FROM articles WHERE {TRASH_CONDITION}"
    ).fetchall()

    trashed = len(rows)
    if rows:
        trash_conn.executemany(
            """INSERT OR IGNORE INTO articles
               (doi, title, authors, journal, year, volume, issue, pages, publisher)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        trash_conn.commit()
        main_conn.execute(f"DELETE FROM articles WHERE {TRASH_CONDITION}")
        main_conn.commit()

    after_trash = main_conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]

    print(f"Articles before cleanup:   {before}")
    print(f"Moved to trash:            {trashed}")
    print(f"  - empty authors:         {empty_authors}")
    print(f"  - title starts 'Review': {title_review}")
    print(f"  - contains 'Book Review':{book_review}")
    print(f"  - contains '(review)':   {paren_review}")
    print(f"  (categories may overlap; total deduplicated: {trashed})")
    print(f"Articles after trash:      {after_trash}")
    print()

    # ── Step 2: Normalize author names ────────────────────────────────────

    main_conn.create_function("norm_authors", 1, normalize_authors)

    changed = main_conn.execute(
        "SELECT COUNT(*) FROM articles WHERE norm_authors(authors) != authors"
    ).fetchone()[0]

    main_conn.execute("UPDATE articles SET authors = norm_authors(authors)")
    main_conn.commit()

    print(f"Author names normalized:   {changed} rows updated")

    # ── Step 3: Reclaim space ─────────────────────────────────────────────

    print("Vacuuming databases...")
    main_conn.execute("VACUUM")
    trash_conn.execute("VACUUM")

    final = main_conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    print(f"Final article count:       {final}")

    main_conn.close()
    trash_conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
