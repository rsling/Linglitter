#!/usr/bin/env python3
"""
Integrate PDFs with title-based filenames into the data directory structure.

Scans subdirectories of renaming_dir (named after journals) for PDF files,
fuzzy-matches filenames against article titles in linglitter.db, and with
user confirmation moves matched files to the appropriate location.

Usage:
    python integrate_renaming.py
    python integrate_renaming.py --config myconfig.json
"""

import argparse
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

log = logging.getLogger(__name__)

# Try to use rapidfuzz for better fuzzy matching, fall back to difflib
try:
    from rapidfuzz import fuzz
    HAVE_RAPIDFUZZ = True
except ImportError:
    HAVE_RAPIDFUZZ = False
    log.debug("rapidfuzz not available, using difflib for fuzzy matching")


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


def normalize_text(text):
    """Normalize text for fuzzy matching.

    Lowercases, removes punctuation, and collapses whitespace.
    """
    if not text:
        return ""
    # Lowercase
    text = text.lower()
    # Replace punctuation with spaces
    text = re.sub(r'[^\w\s]', ' ', text)
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def fuzzy_match_score(query, candidate):
    """Calculate fuzzy match score between query and candidate.

    Returns a score from 0-100, where 100 is a perfect match.
    Handles prefix matching (query may be beginning of candidate).
    """
    query_norm = normalize_text(query)
    candidate_norm = normalize_text(candidate)

    if not query_norm or not candidate_norm:
        return 0

    if HAVE_RAPIDFUZZ:
        # Use partial_ratio for substring/prefix matching
        # and ratio for overall similarity
        partial = fuzz.partial_ratio(query_norm, candidate_norm)
        full = fuzz.ratio(query_norm, candidate_norm)

        # If query is a prefix of candidate, boost the score
        if candidate_norm.startswith(query_norm):
            return max(partial, 95)  # High score for prefix match

        # Return the better of partial and full match
        return max(partial, full)
    else:
        # Fallback to difflib
        # Check prefix match first
        if candidate_norm.startswith(query_norm):
            return 95

        # Use SequenceMatcher for similarity
        ratio = SequenceMatcher(None, query_norm, candidate_norm).ratio()

        # Also check if query is a substring
        if query_norm in candidate_norm:
            # Score based on how much of the candidate is covered
            coverage = len(query_norm) / len(candidate_norm)
            return max(int(ratio * 100), int(70 + coverage * 25))

        return int(ratio * 100)


def find_matching_articles(conn, filename, journal, threshold=60, limit=5):
    """Find articles that fuzzy-match the filename.

    Args:
        conn: Database connection
        filename: The PDF filename (without .pdf suffix)
        journal: Journal name to filter by
        threshold: Minimum fuzzy match score (0-100)
        limit: Maximum number of matches to return

    Returns:
        List of (score, doi, title, authors, year, volume, issue, publisher)
        sorted by score descending.
    """
    # Query articles for this journal
    query = """
        SELECT doi, title, authors, year, volume, issue, publisher
        FROM articles
        WHERE journal = ?
    """
    rows = conn.execute(query, (journal,)).fetchall()

    if not rows:
        return []

    # Score each article
    matches = []
    for row in rows:
        doi, title, authors, year, volume, issue, publisher = row
        if not title:
            continue

        score = fuzzy_match_score(filename, title)
        if score >= threshold:
            matches.append((score, doi, title, authors, year, volume, issue, publisher))

    # Sort by score descending and limit
    matches.sort(key=lambda x: x[0], reverse=True)
    return matches[:limit]


def format_citation(authors, year, title, journal, volume, issue):
    """Format a bibliographic citation for display."""
    parts = []
    if authors:
        parts.append(authors)
    if year:
        parts.append(f"({year})")
    if title:
        parts.append(f'"{title}"')

    journal_part = []
    if journal:
        journal_part.append(journal)
    if volume:
        journal_part.append(volume)
    if issue:
        journal_part.append(f"({issue})")

    if journal_part:
        parts.append(" ".join(journal_part))

    return " ".join(parts)


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


def ask_user(pdf_path, filename, match, journal):
    """Ask user to confirm a match.

    Args:
        pdf_path: Path to the PDF file
        filename: The filename without .pdf
        match: Tuple of (score, doi, title, authors, year, volume, issue, publisher)
        journal: Journal name

    Returns:
        'yes', 'delete', or 'retain'
    """
    score, doi, title, authors, year, volume, issue, publisher = match

    print()
    print("=" * 70)
    print(f"FILE: {pdf_path}")
    print(f"  Filename: {filename}")
    print(f"  Journal:  {journal}")
    print()
    print(f"MATCH (score: {score}/100):")
    print(f"  DOI:      {doi}")
    print(f"  Title:    {title}")
    citation = format_citation(authors, year, title, journal, volume, issue)
    print(f"  Citation: {citation}")
    print()

    while True:
        print("Options:")
        print("  [Y]es    - This is the correct match. Rename and move the file.")
        print("  [D]elete - Wrong match. Delete the file.")
        print("  [R]etain - Wrong match. Keep the file for manual handling.")
        print()
        choice = input("Your choice [Y/D/R]: ").strip().lower()

        if choice in ('y', 'yes'):
            return 'yes'
        elif choice in ('d', 'delete'):
            return 'delete'
        elif choice in ('r', 'retain'):
            return 'retain'
        else:
            print("Invalid choice. Please enter Y, D, or R.")


def ask_user_no_match(pdf_path, filename, journal):
    """Ask user what to do when no match was found.

    Returns:
        'delete' or 'retain'
    """
    print()
    print("=" * 70)
    print(f"FILE: {pdf_path}")
    print(f"  Filename: {filename}")
    print(f"  Journal:  {journal}")
    print()
    print("NO MATCH FOUND in database.")
    print()

    while True:
        print("Options:")
        print("  [D]elete - Delete the file.")
        print("  [R]etain - Keep the file for manual handling.")
        print()
        choice = input("Your choice [D/R]: ").strip().lower()

        if choice in ('d', 'delete'):
            return 'delete'
        elif choice in ('r', 'retain'):
            return 'retain'
        else:
            print("Invalid choice. Please enter D or R.")


def process_file(conn, pdf_path, journal, data_dir):
    """Process a single PDF file.

    Returns:
        'moved', 'deleted', 'retained', or 'skipped'
    """
    filename = pdf_path.stem  # filename without .pdf

    # Find matching articles
    matches = find_matching_articles(conn, filename, journal)

    if not matches:
        choice = ask_user_no_match(pdf_path, filename, journal)
        if choice == 'delete':
            pdf_path.unlink()
            log.info("Deleted: %s", pdf_path)
            return 'deleted'
        else:
            log.info("Retained: %s", pdf_path)
            return 'retained'

    # Present the best match to the user
    best_match = matches[0]
    choice = ask_user(pdf_path, filename, best_match, journal)

    if choice == 'yes':
        score, doi, title, authors, year, volume, issue, publisher = best_match

        # Build destination path
        dest_abs, dest_rel = build_pdf_path(data_dir, publisher, journal, year, doi)

        try:
            # Ensure destination directory exists
            dest_abs.parent.mkdir(parents=True, exist_ok=True)

            # Move the file
            shutil.move(str(pdf_path), str(dest_abs))
            log.info("Moved: %s -> %s", pdf_path.name, dest_rel)

            # Update database
            source_url = f"https://doi.org/{doi}"
            update_article(conn, doi, source_url, dest_rel)
            log.info("Updated database: doi=%s, file=%s", doi, dest_rel)

            return 'moved'

        except Exception as e:
            log.error("Error moving %s: %s", pdf_path, e)
            return 'skipped'

    elif choice == 'delete':
        pdf_path.unlink()
        log.info("Deleted: %s", pdf_path)
        return 'deleted'

    else:  # retain
        log.info("Retained: %s", pdf_path)
        return 'retained'


def get_journal_dirs(renaming_dir):
    """Get all subdirectories (journals) in renaming_dir."""
    renaming_path = Path(renaming_dir)
    if not renaming_path.exists():
        return []
    return [d for d in renaming_path.iterdir() if d.is_dir()]


def get_pdf_files(journal_dir):
    """Get all PDF files in a journal directory."""
    return list(journal_dir.glob("*.pdf"))


def main():
    parser = argparse.ArgumentParser(
        description="Integrate PDFs with title-based filenames into the data directory.")
    parser.add_argument("--config", type=str, default="config.json",
                        help="Path to config JSON (default: config.json)")
    parser.add_argument("--db", type=str, default="linglitter.db",
                        help="Path to SQLite database (default: linglitter.db)")
    parser.add_argument("--threshold", type=int, default=60,
                        help="Minimum fuzzy match score 0-100 (default: 60)")
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
    renaming_dir = config.get("renaming_dir", "renaming")

    # Connect to database
    db_path = Path(args.db)
    if not db_path.exists():
        log.error("Database not found: %s", db_path)
        return 1

    conn = sqlite3.connect(args.db)

    # Report fuzzy matching backend
    if HAVE_RAPIDFUZZ:
        log.info("Using rapidfuzz for fuzzy matching")
    else:
        log.info("Using difflib for fuzzy matching (install rapidfuzz for better results)")

    # Get journal directories
    journal_dirs = get_journal_dirs(renaming_dir)
    if not journal_dirs:
        log.info("No journal directories found in %s", renaming_dir)
        conn.close()
        return 0

    log.info("Found %d journal directories in %s", len(journal_dirs), renaming_dir)

    # Stats
    stats = {'moved': 0, 'deleted': 0, 'retained': 0, 'skipped': 0}

    try:
        for journal_dir in sorted(journal_dirs):
            journal = journal_dir.name
            pdf_files = get_pdf_files(journal_dir)

            if not pdf_files:
                continue

            log.info("Processing journal: %s (%d files)", journal, len(pdf_files))

            for pdf_path in sorted(pdf_files):
                result = process_file(conn, pdf_path, journal, data_dir)
                stats[result] += 1

    except KeyboardInterrupt:
        print("\nInterrupted by user")

    conn.close()

    print()
    log.info("Done — moved: %d, deleted: %d, retained: %d, skipped: %d",
             stats['moved'], stats['deleted'], stats['retained'], stats['skipped'])
    return 0


if __name__ == "__main__":
    sys.exit(main())
