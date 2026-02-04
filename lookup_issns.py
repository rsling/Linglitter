#!/usr/bin/env python3
"""
Interactive helper to look up ISSNs for journals via the CrossRef API
and append confirmed entries to journals.json.

Usage:
    python lookup_issns.py
    python lookup_issns.py --mailto you@example.com
    python lookup_issns.py --dry-run          # preview without writing
"""

import argparse
import json
import sys
import time
from pathlib import Path

import requests

CROSSREF_API = "https://api.crossref.org"
JOURNALS_FILE = Path("journals.json")

# The 38 journals to add (from README.md "additional journals to include")
JOURNALS_TO_ADD = [
    "Glossa",
    "Linguistic Inquiry",
    "Linguistic Analysis",
    "Theoretical Linguistics",
    "Natural Language and Linguistic Theory",
    "The Linguistic Review",
    "Journal of Memory and Language",
    "Mind and Language",
    "Language Sciences",
    "Language and Cognitive Processes",
    "Journal of Child Language",
    "Journal of Linguistics",
    "Linguistics Vanguard",
    "Languages",
    "Linguistics",
    "Lingua",
    "Language",
    "Written Language and Literacy",
    "Journal of Pragmatics",
    "Journal of Semantics",
    "Syntax and Semantics",
    "Language Variation and Change",
    "Register Studies",
    "Computational Linguistics",
    "Linguistics and Philosophy",
    "Natural Language Semantics",
    "Semantics and Pragmatics",
    "Syntax",
    "Proceedings of Sinn und Bedeutung",
    "Proceedings of the International Conference on Head-Driven Phrase Structure Grammar",
    "Cognition",
    "Language Learning",
    "Journal of Germanic Linguistics",
    "Zeitschrift für Sprachwissenschaft",
    "Zeitschrift für germanistische Linguistik",
    "Linguistische Berichte",
    "Germanistische Linguistik",
    "Zeitschrift für Dialektologie und Linguistik",
]


def query_crossref(name, mailto=None):
    """Query CrossRef for journals matching *name*. Returns up to 5 hits."""
    url = f"{CROSSREF_API}/journals"
    params = {"query": name, "rows": 5}
    if mailto:
        params["mailto"] = mailto
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()["message"]["items"]


def display_matches(matches):
    """Print a numbered list of CrossRef journal matches."""
    for i, m in enumerate(matches, 1):
        title = m.get("title", "(no title)")
        publisher = m.get("publisher", "(unknown publisher)")
        issns = m.get("ISSN", [])
        total = m.get("counts", {}).get("total-dois", 0)
        print(f"  {i}) {title}")
        print(f"     Publisher: {publisher}  |  ISSN(s): {', '.join(issns)}  |  DOIs: {total}")
    print()


def prompt_user(name):
    """Prompt the user to pick a match, skip, or enter a manual ISSN."""
    print(f"  [1-5] select match  |  [s] skip  |  [m] enter ISSN manually")
    while True:
        choice = input(f"  Choice for '{name}': ").strip().lower()
        if choice in ("s", "skip"):
            return "skip", None
        if choice in ("m", "manual"):
            return "manual", None
        if choice.isdigit() and 1 <= int(choice) <= 5:
            return "pick", int(choice)
        print("  Invalid choice. Enter 1-5, 's' to skip, or 'm' for manual.")


def manual_entry(name):
    """Collect publisher and ISSN(s) from the user."""
    publisher = input(f"  Publisher for '{name}': ").strip()
    issn_raw = input(f"  ISSN(s) for '{name}' (comma-separated): ").strip()
    issns = [s.strip() for s in issn_raw.split(",") if s.strip()]
    if not issns:
        print("  No ISSNs provided — skipping.")
        return None
    return {"name": name, "publisher": publisher, "issn": issns}


def main():
    parser = argparse.ArgumentParser(
        description="Interactively look up ISSNs and add journals to journals.json")
    parser.add_argument("--mailto", type=str, default=None,
                        help="Email for CrossRef polite pool")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview results without writing to journals.json")
    args = parser.parse_args()

    # Load existing journals
    if JOURNALS_FILE.exists():
        with open(JOURNALS_FILE) as fh:
            existing = json.load(fh)
    else:
        existing = []

    existing_names = {j["name"].lower() for j in existing}
    new_entries = []

    print(f"\nLooking up {len(JOURNALS_TO_ADD)} journals via CrossRef.\n")

    for idx, name in enumerate(JOURNALS_TO_ADD, 1):
        if name.lower() in existing_names:
            print(f"[{idx}/{len(JOURNALS_TO_ADD)}] {name} — already in journals.json, skipping.\n")
            continue

        print(f"[{idx}/{len(JOURNALS_TO_ADD)}] {name}")
        print("-" * 60)

        try:
            matches = query_crossref(name, args.mailto)
        except Exception as exc:
            print(f"  API error: {exc}")
            print(f"  Skipping '{name}'.\n")
            time.sleep(1)
            continue

        if not matches:
            print("  No matches found in CrossRef.")
            choice = input("  [s] skip  |  [m] enter manually: ").strip().lower()
            if choice in ("m", "manual"):
                entry = manual_entry(name)
                if entry:
                    new_entries.append(entry)
                    print(f"  ✓ Added manually: {entry['name']}\n")
            else:
                print(f"  Skipped.\n")
            time.sleep(1)
            continue

        display_matches(matches)
        action, pick = prompt_user(name)

        if action == "skip":
            print(f"  Skipped.\n")
        elif action == "manual":
            entry = manual_entry(name)
            if entry:
                new_entries.append(entry)
                print(f"  Added manually: {entry['name']}\n")
        elif action == "pick":
            m = matches[pick - 1]
            entry = {
                "name": name,
                "publisher": m.get("publisher", ""),
                "issn": m.get("ISSN", []),
            }
            new_entries.append(entry)
            print(f"  Added: {entry['name']}  ISSN={entry['issn']}\n")

        time.sleep(1)

    # Summary
    print("=" * 60)
    print(f"  {len(new_entries)} new journal(s) confirmed.\n")

    if not new_entries:
        print("Nothing to add.")
        return 0

    for e in new_entries:
        print(f"  - {e['name']}  [{e['publisher']}]  ISSN: {', '.join(e['issn'])}")
    print()

    if args.dry_run:
        print("Dry run — not writing to journals.json.")
        return 0

    # Append and write
    combined = existing + new_entries
    with open(JOURNALS_FILE, "w") as fh:
        json.dump(combined, fh, indent=2, ensure_ascii=False)
        fh.write("\n")

    print(f"Wrote {len(combined)} entries to {JOURNALS_FILE}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
