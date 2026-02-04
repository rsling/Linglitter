#!/usr/bin/env python3
"""Generate a BibTeX database from linglitter.db."""

import html
import re
import sqlite3
import unicodedata
from collections import Counter
from pathlib import Path

DB_PATH = Path(__file__).parent / "linglitter.db"
BIB_PATH = Path(__file__).parent / "linglitter.bib"

# Characters that must be escaped in BibTeX field values.
BIBTEX_SPECIAL = str.maketrans({
    "&": r"\&",
    "%": r"\%",
    "#": r"\#",
    "$": r"\$",
    "_": r"\_",
})

# German umlaut / ß replacements (case-sensitive, for citation keys).
_UMLAUT_MAP = [
    ("Ä", "Ae"), ("Ö", "Oe"), ("Ü", "Ue"),
    ("ä", "ae"), ("ö", "oe"), ("ü", "ue"),
    ("ß", "ss"),
]


def nfc(text: str) -> str:
    """Return NFC-normalised form of *text*."""
    return unicodedata.normalize("NFC", text)


def clean_field(value: str) -> str:
    """Decode HTML entities, NFC-normalise, and escape for BibTeX."""
    value = html.unescape(value)
    value = nfc(value)
    value = value.translate(BIBTEX_SPECIAL)
    return value


def authors_to_bibtex(authors_str: str) -> str:
    """Convert ``Last, First; Last2, First2`` to BibTeX ``Last, First and Last2, First2``."""
    authors_str = html.unescape(authors_str)
    authors_str = nfc(authors_str)
    result = " and ".join(a.strip() for a in authors_str.split(";"))
    return result.translate(BIBTEX_SPECIAL)


# ── Citation key helpers ─────────────────────────────────────────────


def _replace_umlauts(name: str) -> str:
    for src, dst in _UMLAUT_MAP:
        name = name.replace(src, dst)
    return name


def _strip_diacritics(text: str) -> str:
    """Remove diacritics other than the German ones (already handled)."""
    decomposed = unicodedata.normalize("NFD", text)
    return "".join(c for c in decomposed if unicodedata.category(c) != "Mn")


def _process_lastname(name: str) -> str:
    """Turn a single author's surname into its citation-key form.

    Rules:
    * German umlauts → vowel+e, ß → ss.
    * Remaining diacritics are stripped.
    * Hyphenated double names: first part capitalised, rest lowercased,
      joined without hyphen  (``Wöllstein-Leisten`` → ``Woellsteinleisten``).
    * Spaces removed, first letter upper-cased.
    """
    name = nfc(name)
    name = _replace_umlauts(name)
    name = _strip_diacritics(name)

    if "-" in name:
        parts = name.split("-")
        name = parts[0] + "".join(p.lower() for p in parts[1:])

    name = name.replace(" ", "")
    name = re.sub(r"[^A-Za-z]", "", name)

    if name:
        name = name[0].upper() + name[1:]
    return name


def _first_author_surname(authors_str: str) -> str:
    first = authors_str.split(";")[0].strip()
    if "," in first:
        return first.split(",")[0].strip()
    return first.strip()


def _author_surnames(authors_str: str) -> list[str]:
    """Return list of all author surnames."""
    authors = [a.strip() for a in authors_str.split(";")]
    surnames = []
    for a in authors:
        if "," in a:
            surnames.append(a.split(",")[0].strip())
        else:
            surnames.append(a.strip())
    return surnames


def _collision_suffix(index: int) -> str:
    """Generate a, b, …, z, aa, ab, …, az, ba, … for collision resolution."""
    width = 1
    count = 26
    i = index
    while i >= count:
        i -= count
        width += 1
        count = 26 ** width
    result = ""
    for _ in range(width):
        result = chr(ord("a") + i % 26) + result
        i //= 26
    return result


def make_citekey(authors_str: str, year: int | None) -> str:
    """Build a base citation key (before collision resolution)."""
    surnames = _author_surnames(authors_str)
    if len(surnames) <= 2:
        key = "".join(_process_lastname(s) for s in surnames)
    else:
        key = _process_lastname(surnames[0]) + "Ea"
    key += str(year) if year else "nd"
    return key


# ── BibTeX formatting ────────────────────────────────────────────────


def format_entry(row: dict, citekey: str) -> str:
    """Return a single @article BibTeX entry as a string."""
    lines = [f"@article{{{citekey},"]

    field_map = [
        ("author",  authors_to_bibtex(row["authors"])),
        ("title",   clean_field(row["title"])),
        ("journal", clean_field(row["journal"])),
        ("year",    str(row["year"]) if row["year"] else ""),
        ("volume",  clean_field(row["volume"]) if row["volume"] else ""),
        ("number",  clean_field(row["issue"]) if row["issue"] else ""),
        ("pages",   clean_field(row["pages"]) if row["pages"] else ""),
        ("doi",     clean_field(row["doi"]) if row["doi"] else ""),
    ]

    for field, value in field_map:
        if value:
            lines.append(f"  {field} = {{{value}}},")

    lines.append("}")
    return "\n".join(lines)


# ── Main ─────────────────────────────────────────────────────────────


def main() -> None:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT doi, title, authors, journal, year, volume, issue, pages "
        "FROM articles ORDER BY year, authors"
    ).fetchall()
    con.close()

    # Build base keys and resolve collisions.
    base_keys = [make_citekey(row["authors"], row["year"]) for row in rows]
    key_counts: Counter[str] = Counter(base_keys)
    key_index: Counter[str] = Counter()
    citekeys: list[str] = []
    for bk in base_keys:
        if key_counts[bk] > 1:
            citekeys.append(bk + _collision_suffix(key_index[bk]))
        else:
            citekeys.append(bk)
        key_index[bk] += 1

    entries = [format_entry(row, ck) for row, ck in zip(rows, citekeys)]

    BIB_PATH.write_text("\n\n".join(entries) + "\n", encoding="utf-8")
    print(f"Wrote {len(entries)} entries to {BIB_PATH}")


if __name__ == "__main__":
    main()
