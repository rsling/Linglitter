#!/usr/bin/env python3
"""
Data consistency checker for Linglitter.
Compares PDF files in data directory with database entries and allows user to resolve discrepancies.
"""

import json
import logging
import os
import re
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path


# Configure logging
def setup_logging(log_file="consistency.log"):
    """Set up logging to file."""
    logger = logging.getLogger("consistency")
    logger.setLevel(logging.INFO)

    # File handler
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter("%(message)s")
    fh.setFormatter(formatter)

    logger.addHandler(fh)
    return logger


def log_action(logger, scenario, action, rel_path, entry=None, file_info=None, details=None):
    """
    Log a user action to the consistency log.

    Args:
        logger: The logger instance
        scenario: One of 'FILE_WITHOUT_ENTRY', 'FILE_WITH_NULL_ENTRY', 'ENTRY_WITHOUT_FILE'
        action: What was done ('ignored', 'quarantined', 'file_added', 'entry_reset')
        rel_path: File path relative to pdf_dir
        entry: Database entry dict (if available)
        file_info: File info dict from scan (if available)
        details: Additional details about the action
    """
    timestamp = datetime.now().isoformat()

    log_lines = [
        "",
        "=" * 70,
        f"TIMESTAMP: {timestamp}",
        f"SCENARIO: {scenario}",
        f"ACTION: {action}",
        "-" * 70,
        "FILE LOCATION:",
        f"  Relative path: {rel_path}",
    ]

    if file_info:
        log_lines.extend([
            f"  Filename: {file_info.get('filename', 'N/A')}",
            f"  DOI (from filename lookup): {file_info.get('doi', 'N/A')}",
        ])

    log_lines.append("-" * 70)
    log_lines.append("DATABASE ENTRY:")

    if entry:
        log_lines.extend([
            f"  DOI: {entry.get('doi', 'N/A')}",
            f"  Title: {entry.get('title', 'N/A')}",
            f"  Authors: {entry.get('authors', 'N/A')}",
            f"  Year: {entry.get('year', 'N/A')}",
            f"  Journal: {entry.get('journal', 'N/A')}",
            f"  Publisher: {entry.get('publisher', 'N/A')}",
            f"  File (in DB): {entry.get('file', 'NULL')}",
            f"  Availability: {entry.get('availability', 'N/A')}",
            f"  Source: {entry.get('source', 'N/A')}",
        ])
    else:
        log_lines.append("  No matching entry found in database")

    log_lines.append("-" * 70)
    log_lines.append("ACTION DETAILS:")

    if details:
        for line in details:
            log_lines.append(f"  {line}")
    else:
        log_lines.append("  No additional details")

    log_lines.append("=" * 70)

    logger.info("\n".join(log_lines))


def load_config(config_path="config.json"):
    """Load configuration from JSON file."""
    with open(config_path, "r") as f:
        config = json.load(f)
    return config


def encode_doi_for_filename(doi):
    """Encode DOI to be safe as a filename.

    Replaces /, \\, :, *, ?, ", <, >, |, and . with underscores.
    This must match the encoding used by scrape_pdfs.py and scrape_repo.py.
    """
    unsafe_chars = r'[/\\:*?"<>|.]'
    return re.sub(unsafe_chars, "_", doi)



def compute_expected_path(entry):
    """Compute the expected filesystem path for a database entry.

    PDFs are stored flat in pdf_dir as <encoded_doi>.pdf.
    Returns the filename (relative to pdf_dir).
    """
    doi = entry.get("doi", "")
    filename = encode_doi_for_filename(doi) + ".pdf"

    return filename


def build_filename_to_doi_map(conn):
    """
    Build a mapping from encoded filenames to DOIs using the database.
    This is more reliable than trying to reverse the encoding, since
    the encoding is lossy (multiple characters become underscores).
    """
    cursor = conn.execute("SELECT doi FROM articles")
    mapping = {}
    for row in cursor.fetchall():
        doi = row["doi"]
        filename = encode_doi_for_filename(doi) + ".pdf"
        mapping[filename] = doi
    return mapping


def get_db_connection(db_path="linglitter.db"):
    """Create database connection."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def scan_pdf_directory(pdf_dir, filename_to_doi_map):
    """
    Scan PDF directory for PDF files.
    Returns dict mapping filenames to extracted info (DOI).

    Args:
        pdf_dir: Path to PDF directory
        filename_to_doi_map: Mapping from encoded filenames to DOIs (from database)
    """
    files = {}
    pdf_path = Path(pdf_dir)

    if not pdf_path.exists():
        print(f"Warning: PDF directory '{pdf_dir}' does not exist.")
        return files

    for fpath in pdf_path.glob("*.pdf"):
        filename = fpath.name
        doi = filename_to_doi_map.get(filename)

        files[filename] = {
            "filename": filename,
            "doi": doi,
            "full_path": str(fpath),
        }

    return files


def get_db_entries_with_files(conn):
    """Get all database entries where file IS NOT NULL."""
    cursor = conn.execute(
        """SELECT doi, title, authors, journal, year, publisher, file
           FROM articles WHERE file IS NOT NULL"""
    )
    return {row["doi"]: dict(row) for row in cursor.fetchall()}


def get_all_db_entries(conn):
    """Get all database entries indexed by DOI."""
    cursor = conn.execute(
        """SELECT doi, title, authors, journal, year, publisher, file, availability, source
           FROM articles"""
    )
    return {row["doi"]: dict(row) for row in cursor.fetchall()}


def format_entry(entry):
    """Format a database entry for display."""
    authors = entry.get("authors", "Unknown")
    year = entry.get("year", "?")
    title = entry.get("title", "Unknown")
    publisher = entry.get("publisher", "Unknown")
    journal = entry.get("journal", "Unknown")
    doi = entry.get("doi", "Unknown")

    return f"""  Authors:   {authors}
  Year:      {year}
  Title:     {title}
  Publisher: {publisher}
  Journal:   {journal}
  DOI:       {doi}"""


def reset_entry(conn, doi):
    """Reset entry: set availability=NULL, source=NULL, attempts=0, response=0, timestamp=0, file=NULL."""
    conn.execute(
        """UPDATE articles
           SET availability = NULL, source = NULL, attempts = 0,
               response = 0, timestamp = 0, file = NULL
           WHERE doi = ?""",
        (doi,)
    )
    conn.commit()
    print(f"  -> Entry reset for DOI: {doi}")
    return [
        "Database UPDATE executed:",
        f"  SET availability = NULL",
        f"  SET source = NULL",
        f"  SET attempts = 0",
        f"  SET response = 0",
        f"  SET timestamp = 0",
        f"  SET file = NULL",
        f"  WHERE doi = '{doi}'",
    ]


def add_file_location(conn, doi, rel_path):
    """Add file location to entry and set availability='recheck', source='stray'."""
    conn.execute(
        """UPDATE articles
           SET file = ?, availability = 'recheck', source = 'stray'
           WHERE doi = ?""",
        (rel_path, doi)
    )
    conn.commit()
    print(f"  -> File location added for DOI: {doi}")
    return [
        "Database UPDATE executed:",
        f"  SET file = '{rel_path}'",
        f"  SET availability = 'recheck'",
        f"  SET source = 'stray'",
        f"  WHERE doi = '{doi}'",
    ]


def quarantine_file(full_path, rel_path, quarantine_dir):
    """Move file to quarantine directory, preserving relative structure."""
    quarantine_path = Path(quarantine_dir)
    dest_path = quarantine_path / rel_path

    # Create parent directories if needed
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    shutil.move(full_path, dest_path)
    print(f"  -> File moved to quarantine: {dest_path}")
    return [
        "File moved:",
        f"  From: {full_path}",
        f"  To: {dest_path}",
    ]


def prompt_user_file_without_entry(file_info, quarantine_dir):
    """Handle case: file exists but no matching DB entry found."""
    print("\n" + "=" * 70)
    print("FILE WITHOUT DATABASE ENTRY")
    print("=" * 70)
    print(f"File: {file_info['full_path']}")
    print(f"Filename: {file_info['filename']}")
    print("\nNo matching DOI found in database for this filename.")
    print("\nOptions:")
    print("  [1] Ignore (leave as is)")
    print("  [3] Quarantine file (move to quarantine directory)")

    while True:
        choice = input("\nYour choice (1/3): ").strip()
        if choice == "1":
            print("  -> Ignored")
            return "ignored"
        elif choice == "3":
            return "quarantine"
        else:
            print("Invalid choice. Please enter 1 or 3.")


def prompt_user_file_entry_null(file_info, entry, rel_path):
    """Handle case: file exists, DB entry exists, but file column is NULL."""
    print("\n" + "=" * 70)
    print("FILE EXISTS BUT DATABASE ENTRY HAS file=NULL")
    print("=" * 70)
    print(f"File: {file_info['full_path']}")
    print("\nDatabase entry:")
    print(format_entry(entry))
    print("\nOptions:")
    print("  [1] Ignore (leave as is)")
    print("  [2] Add file location (set file path and mark for recheck)")

    while True:
        choice = input("\nYour choice (1/2): ").strip()
        if choice == "1":
            print("  -> Ignored")
            return "ignored"
        elif choice == "2":
            return "add_file"
        else:
            print("Invalid choice. Please enter 1 or 2.")


def prompt_user_entry_without_file(entry, expected_path):
    """Handle case: DB entry has file NOT NULL but file doesn't exist."""
    print("\n" + "=" * 70)
    print("DATABASE ENTRY REFERENCES MISSING FILE")
    print("=" * 70)
    print(f"Expected file: {expected_path}")
    print("\nDatabase entry:")
    print(format_entry(entry))
    print("\nOptions:")
    print("  [1] Ignore (leave as is)")
    print("  [2] Reset entry (clear file info and retry download)")

    while True:
        choice = input("\nYour choice (1/2): ").strip()
        if choice == "1":
            print("  -> Ignored")
            return "ignored"
        elif choice == "2":
            return "reset"
        else:
            print("Invalid choice. Please enter 1 or 2.")


def main():
    # Load configuration
    config = load_config()
    pdf_dir = config.get("pdf_dir", "pdf")
    quarantine_dir = config.get("quarantine_dir", "quarantine")

    # Set up logging
    logger = setup_logging()
    logger.info(f"\n{'#' * 70}")
    logger.info(f"# CONSISTENCY CHECK SESSION STARTED: {datetime.now().isoformat()}")
    logger.info(f"# PDF directory: {pdf_dir}")
    logger.info(f"# Quarantine directory: {quarantine_dir}")
    logger.info(f"{'#' * 70}")

    print("=" * 70)
    print("LINGLITTER DATA CONSISTENCY CHECKER")
    print("=" * 70)
    print(f"PDF directory: {pdf_dir}")
    print(f"Quarantine directory: {quarantine_dir}")
    print(f"Logging to: consistency.log")
    print()

    # Get database entries first (needed for filename-to-DOI mapping)
    print("Loading database entries...")
    conn = get_db_connection()
    all_entries = get_all_db_entries(conn)

    # Build filename-to-DOI mapping from database
    print("Building filename to DOI mapping...")
    filename_to_doi_map = build_filename_to_doi_map(conn)

    # Scan filesystem
    print("Scanning PDF directory...")
    files_on_disk = scan_pdf_directory(pdf_dir, filename_to_doi_map)
    print(f"Found {len(files_on_disk)} PDF files on disk.")
    entries_with_files = {doi: e for doi, e in all_entries.items() if e["file"] is not None}
    print(f"Found {len(all_entries)} total entries in database.")
    print(f"Found {len(entries_with_files)} entries with file paths.")
    print()

    # Track statistics
    stats = {
        "files_without_entry": 0,
        "files_with_null_entry": 0,
        "entries_without_file": 0,
        "matched": 0,
        "ignored": 0,
        "reset": 0,
        "added": 0,
        "quarantined": 0,
    }

    # Check 1: Files on disk vs database
    print("Checking files against database...")
    for rel_path, file_info in files_on_disk.items():
        doi = file_info["doi"]

        if doi is None:
            # Could not determine DOI from filename
            stats["files_without_entry"] += 1
            action = prompt_user_file_without_entry(file_info, quarantine_dir)
            if action == "quarantine":
                details = quarantine_file(file_info["full_path"], rel_path, quarantine_dir)
                log_action(logger, "FILE_WITHOUT_ENTRY", "quarantined", rel_path,
                           entry=None, file_info=file_info, details=details)
                stats["quarantined"] += 1
            else:
                log_action(logger, "FILE_WITHOUT_ENTRY", "ignored", rel_path,
                           entry=None, file_info=file_info, details=["No action taken"])
                stats["ignored"] += 1
            continue

        if doi not in all_entries:
            # No matching entry in database at all
            stats["files_without_entry"] += 1
            action = prompt_user_file_without_entry(file_info, quarantine_dir)
            if action == "quarantine":
                details = quarantine_file(file_info["full_path"], rel_path, quarantine_dir)
                log_action(logger, "FILE_WITHOUT_ENTRY", "quarantined", rel_path,
                           entry=None, file_info=file_info, details=details)
                stats["quarantined"] += 1
            else:
                log_action(logger, "FILE_WITHOUT_ENTRY", "ignored", rel_path,
                           entry=None, file_info=file_info, details=["No action taken"])
                stats["ignored"] += 1
            continue

        entry = all_entries[doi]

        # Compute the expected path based on entry metadata using sanitization rules
        expected_path = compute_expected_path(entry)

        # Check consistency: DB file column must match actual path
        if entry["file"] == rel_path:
            # Exact match - fully consistent
            stats["matched"] += 1
        elif entry["file"] is None:
            # Entry exists but file column is NULL - need to update DB
            stats["files_with_null_entry"] += 1
            action = prompt_user_file_entry_null(file_info, entry, rel_path)
            if action == "add_file":
                details = add_file_location(conn, doi, rel_path)
                log_action(logger, "FILE_WITH_NULL_ENTRY", "file_added", rel_path,
                           entry=entry, file_info=file_info, details=details)
                stats["added"] += 1
            else:
                log_action(logger, "FILE_WITH_NULL_ENTRY", "ignored", rel_path,
                           entry=entry, file_info=file_info, details=["No action taken"])
                stats["ignored"] += 1
        elif rel_path == expected_path:
            # File is at expected location but DB has wrong path - prompt to fix
            stats["files_with_null_entry"] += 1
            print(f"\nNote: DB path incorrect for DOI {doi}")
            print(f"  DB path:       {entry['file']}")
            print(f"  Expected path: {expected_path}")
            print(f"  Actual path:   {rel_path}")
            action = prompt_user_file_entry_null(file_info, entry, rel_path)
            if action == "add_file":
                details = add_file_location(conn, doi, rel_path)
                details.insert(0, f"Note: DB had incorrect path '{entry['file']}', updated to '{rel_path}'")
                log_action(logger, "FILE_WITH_NULL_ENTRY", "file_added", rel_path,
                           entry=entry, file_info=file_info, details=details)
                stats["added"] += 1
            else:
                log_action(logger, "FILE_WITH_NULL_ENTRY", "ignored", rel_path,
                           entry=entry, file_info=file_info,
                           details=[f"DB path mismatch ignored - DB has '{entry['file']}', actual is '{rel_path}'"])
                stats["ignored"] += 1
        else:
            # File path mismatch - file is not where DB says, nor at expected location
            stats["files_with_null_entry"] += 1
            print(f"\nNote: File path mismatch for DOI {doi}")
            print(f"  DB path:       {entry['file']}")
            print(f"  Expected path: {expected_path}")
            print(f"  Actual path:   {rel_path}")
            action = prompt_user_file_entry_null(file_info, entry, rel_path)
            if action == "add_file":
                details = add_file_location(conn, doi, rel_path)
                details.insert(0, f"Note: Path mismatch - DB had '{entry['file']}', expected '{expected_path}', actual is '{rel_path}'")
                log_action(logger, "FILE_WITH_NULL_ENTRY", "file_added", rel_path,
                           entry=entry, file_info=file_info, details=details)
                stats["added"] += 1
            else:
                log_action(logger, "FILE_WITH_NULL_ENTRY", "ignored", rel_path,
                           entry=entry, file_info=file_info,
                           details=[f"Path mismatch ignored - DB has '{entry['file']}', expected '{expected_path}', actual is '{rel_path}'"])
                stats["ignored"] += 1

    # Check 2: Database entries with files vs actual files
    print("\nChecking database entries against filesystem...")
    for doi, entry in entries_with_files.items():
        # Compute expected path using sanitization rules
        computed_rel_path = compute_expected_path(entry)
        computed_full_path = Path(pdf_dir) / computed_rel_path

        # Also check the path stored in DB (might be different)
        db_rel_path = entry["file"]
        db_full_path = Path(pdf_dir) / db_rel_path

        # File exists if it's at either the computed location or the DB-stored location
        file_exists = computed_full_path.exists() or db_full_path.exists()

        if not file_exists:
            # File referenced in DB doesn't exist at either expected or stored path
            stats["entries_without_file"] += 1
            # Show both paths in the prompt
            display_path = computed_full_path if computed_rel_path != db_rel_path else db_full_path
            action = prompt_user_entry_without_file(entry, display_path)
            if action == "reset":
                details = reset_entry(conn, doi)
                if computed_rel_path != db_rel_path:
                    details.insert(0, f"Checked both paths:")
                    details.insert(1, f"  - Computed: {computed_full_path}")
                    details.insert(2, f"  - DB stored: {db_full_path}")
                else:
                    details.insert(0, f"Expected file not found: {computed_full_path}")
                log_action(logger, "ENTRY_WITHOUT_FILE", "entry_reset", computed_rel_path,
                           entry=entry, file_info=None, details=details)
                stats["reset"] += 1
            else:
                details = []
                if computed_rel_path != db_rel_path:
                    details.append(f"Checked both paths:")
                    details.append(f"  - Computed: {computed_full_path}")
                    details.append(f"  - DB stored: {db_full_path}")
                else:
                    details.append(f"Expected file not found: {computed_full_path}")
                details.append("No action taken")
                log_action(logger, "ENTRY_WITHOUT_FILE", "ignored", computed_rel_path,
                           entry=entry, file_info=None, details=details)
                stats["ignored"] += 1

    conn.close()

    # Log session end
    logger.info(f"\n{'#' * 70}")
    logger.info(f"# SESSION ENDED: {datetime.now().isoformat()}")
    logger.info(f"# Matched: {stats['matched']}, Ignored: {stats['ignored']}, Reset: {stats['reset']}, Added: {stats['added']}, Quarantined: {stats['quarantined']}")
    logger.info(f"{'#' * 70}")

    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Files on disk:                    {len(files_on_disk)}")
    print(f"DB entries with file paths:       {len(entries_with_files)}")
    print(f"Matched (consistent):             {stats['matched']}")
    print()
    print("Issues found:")
    print(f"  Files without DB entry:         {stats['files_without_entry']}")
    print(f"  Files with NULL in DB:          {stats['files_with_null_entry']}")
    print(f"  DB entries without file:        {stats['entries_without_file']}")
    print()
    print("Actions taken:")
    print(f"  Ignored:                        {stats['ignored']}")
    print(f"  Entries reset:                  {stats['reset']}")
    print(f"  File locations added:           {stats['added']}")
    print(f"  Files quarantined:              {stats['quarantined']}")


if __name__ == "__main__":
    main()
