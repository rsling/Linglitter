#!/usr/bin/env python3
"""
Fix publisher names in linglitter.db using publishers.json mappings.

Replaces all publisher values with their canonical names from publishers.json.
"""

import json
import sqlite3
from pathlib import Path


def main():
    db_path = Path("linglitter.db")
    publishers_path = Path("publishers.json")

    if not db_path.exists():
        print(f"Error: Database not found: {db_path}")
        return 1

    if not publishers_path.exists():
        print(f"Error: Publishers file not found: {publishers_path}")
        return 1

    # Load alias -> canonical mapping
    with open(publishers_path) as fh:
        publishers = json.load(fh)

    alias_to_canonical = {}
    for p in publishers:
        canonical = p["publisher"]
        for alias in p["aliases"]:
            alias_to_canonical[alias] = canonical

    # Update database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get distinct publishers
    rows = cursor.execute("SELECT DISTINCT publisher FROM articles").fetchall()
    current_publishers = [row[0] for row in rows]

    updated = 0
    for alias in current_publishers:
        if alias not in alias_to_canonical:
            print(f"Warning: Unknown publisher '{alias}' - skipping")
            continue

        canonical = alias_to_canonical[alias]
        if alias != canonical:
            cursor.execute(
                "UPDATE articles SET publisher = ? WHERE publisher = ?",
                (canonical, alias)
            )
            count = cursor.rowcount
            print(f"  {alias} -> {canonical} ({count} rows)")
            updated += count

    conn.commit()
    conn.close()

    print(f"Done. Updated {updated} rows.")
    return 0


if __name__ == "__main__":
    exit(main())
