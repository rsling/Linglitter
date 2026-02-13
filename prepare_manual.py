#!/usr/bin/env python3
"""
Generate manual.html listing articles that require manual download.

Queries linglitter.db for articles where availability='manual' and file IS NULL,
then generates an HTML file with bibliographic information, DOI links, and
JavaScript helpers for tracking clicked links and copying filenames to clipboard.

Usage:
    python prepare_manual.py
    python prepare_manual.py --db other.db
    python prepare_manual.py --output downloads.html
"""

import argparse
import html
import re
import sqlite3
import sys
from pathlib import Path


def encode_doi_for_filename(doi):
    """Encode DOI to be safe as a filename.

    Replaces /, \\, :, *, ?, ", <, >, |, and . with underscores.
    """
    unsafe_chars = r'[/\\:*?"<>|.]'
    return re.sub(unsafe_chars, "_", doi)


def get_manual_articles(conn):
    """Get all articles marked for manual download."""
    query = """
        SELECT doi, title, authors, journal, year, volume, issue
        FROM articles
        WHERE availability = 'manual' AND file IS NULL
        ORDER BY journal, year, authors
    """
    return conn.execute(query).fetchall()


def format_citation(authors, year, title, journal, volume, issue):
    """Format a bibliographic citation."""
    parts = []

    # Authors (Year) Title
    if authors:
        parts.append(html.escape(authors))
    if year:
        parts.append(f"({year})")
    if title:
        parts.append(html.escape(title) + ".")

    # Journal Volume(Issue)
    journal_part = []
    if journal:
        journal_part.append(f"<em>{html.escape(journal)}</em>")
    if volume:
        journal_part.append(html.escape(volume))
    if issue:
        journal_part.append(f"({html.escape(issue)})")

    if journal_part:
        parts.append(" ".join(journal_part) + ".")

    return " ".join(parts)


def generate_html(articles, output_path):
    """Generate the HTML file with the article list."""
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Manual Downloads ({len(articles)} articles)</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            line-height: 1.6;
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }}
        h1 {{
            color: #333;
            border-bottom: 2px solid #007bff;
            padding-bottom: 10px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        th, td {{
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background: #007bff;
            color: white;
            font-weight: 600;
        }}
        tr:hover {{
            background: #f8f9fa;
        }}
        .citation {{
            max-width: 600px;
        }}
        .doi-link {{
            color: #007bff;
            text-decoration: none;
            word-break: break-all;
        }}
        .doi-link:hover {{
            text-decoration: underline;
        }}
        .filename {{
            font-family: monospace;
            font-size: 0.85em;
            color: #666;
            word-break: break-all;
        }}
        .status-dot {{
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background-color: #dc3545;
            margin-left: 10px;
            vertical-align: middle;
            transition: background-color 0.3s ease;
        }}
        .status-dot.clicked {{
            background-color: #28a745;
        }}
        .copy-notice {{
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: #28a745;
            color: white;
            padding: 10px 20px;
            border-radius: 5px;
            opacity: 0;
            transition: opacity 0.3s ease;
            z-index: 1000;
        }}
        .copy-notice.show {{
            opacity: 1;
        }}
        .stats {{
            margin-bottom: 20px;
            padding: 15px;
            background: white;
            border-radius: 5px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        .stats span {{
            margin-right: 20px;
        }}
        .count-clicked {{
            color: #28a745;
            font-weight: bold;
        }}
        .count-remaining {{
            color: #dc3545;
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <h1>Manual Downloads</h1>
    <div class="stats">
        <span>Total: <strong>{len(articles)}</strong></span>
        <span>Clicked: <span class="count-clicked" id="count-clicked">0</span></span>
        <span>Remaining: <span class="count-remaining" id="count-remaining">{len(articles)}</span></span>
    </div>
    <table>
        <thead>
            <tr>
                <th>#</th>
                <th>Citation</th>
                <th>DOI Link</th>
                <th>Filename</th>
                <th>Status</th>
            </tr>
        </thead>
        <tbody>
"""

    for i, (doi, title, authors, journal, year, volume, issue) in enumerate(articles, 1):
        citation = format_citation(authors, year, title, journal, volume, issue)
        doi_url = f"https://doi.org/{html.escape(doi)}"
        encoded_doi = encode_doi_for_filename(doi)
        filename = f"{encoded_doi}.pdf"

        html_content += f"""            <tr data-index="{i}">
                <td>{i}</td>
                <td class="citation">{citation}</td>
                <td><a href="{doi_url}" target="_blank" rel="noopener" class="doi-link" data-filename="{html.escape(filename)}" onclick="handleClick(this, {i})">{doi_url}</a></td>
                <td class="filename">{html.escape(filename)}</td>
                <td><span class="status-dot" id="dot-{i}"></span></td>
            </tr>
"""

    html_content += """        </tbody>
    </table>
    <div class="copy-notice" id="copy-notice">Filename copied to clipboard!</div>

    <script>
        // Track clicked state in localStorage for persistence across page reloads
        const STORAGE_KEY = 'manual_downloads_clicked';

        function getClickedSet() {
            try {
                const stored = localStorage.getItem(STORAGE_KEY);
                return stored ? new Set(JSON.parse(stored)) : new Set();
            } catch (e) {
                return new Set();
            }
        }

        function saveClickedSet(clickedSet) {
            try {
                localStorage.setItem(STORAGE_KEY, JSON.stringify([...clickedSet]));
            } catch (e) {
                // localStorage might be unavailable
            }
        }

        function updateCounts() {
            const clickedSet = getClickedSet();
            const total = document.querySelectorAll('.status-dot').length;
            const clicked = clickedSet.size;
            document.getElementById('count-clicked').textContent = clicked;
            document.getElementById('count-remaining').textContent = total - clicked;
        }

        function showCopyNotice() {
            const notice = document.getElementById('copy-notice');
            notice.classList.add('show');
            setTimeout(function() {
                notice.classList.remove('show');
            }, 2000);
        }

        function copyToClipboard(text) {
            // Modern Clipboard API (works in Chromium-based browsers)
            if (navigator.clipboard && navigator.clipboard.writeText) {
                navigator.clipboard.writeText(text).then(function() {
                    showCopyNotice();
                }).catch(function(err) {
                    // Fallback for clipboard permission denied
                    fallbackCopy(text);
                });
            } else {
                // Fallback for older browsers
                fallbackCopy(text);
            }
        }

        function fallbackCopy(text) {
            // Create a temporary textarea element
            var textarea = document.createElement('textarea');
            textarea.value = text;
            textarea.style.position = 'fixed';
            textarea.style.left = '-9999px';
            textarea.style.top = '0';
            document.body.appendChild(textarea);
            textarea.focus();
            textarea.select();

            try {
                var successful = document.execCommand('copy');
                if (successful) {
                    showCopyNotice();
                }
            } catch (err) {
                console.error('Fallback copy failed:', err);
            }

            document.body.removeChild(textarea);
        }

        function handleClick(linkElement, index) {
            // Get the filename from data attribute
            var filename = linkElement.getAttribute('data-filename');

            // Copy to clipboard
            copyToClipboard(filename);

            // Mark as clicked
            var dot = document.getElementById('dot-' + index);
            dot.classList.add('clicked');

            // Save to localStorage
            var clickedSet = getClickedSet();
            clickedSet.add(index);
            saveClickedSet(clickedSet);

            // Update counts
            updateCounts();
        }

        // Restore clicked state on page load
        function restoreClickedState() {
            var clickedSet = getClickedSet();
            clickedSet.forEach(function(index) {
                var dot = document.getElementById('dot-' + index);
                if (dot) {
                    dot.classList.add('clicked');
                }
            });
            updateCounts();
        }

        // Initialize on page load
        document.addEventListener('DOMContentLoaded', restoreClickedState);
    </script>
</body>
</html>
"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)


def main():
    parser = argparse.ArgumentParser(
        description="Generate HTML file listing articles for manual download.")
    parser.add_argument("--db", type=str, default="linglitter.db",
                        help="Path to SQLite database (default: linglitter.db)")
    parser.add_argument("--output", type=str, default="manual.html",
                        help="Output HTML file (default: manual.html)")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(args.db)
    articles = get_manual_articles(conn)
    conn.close()

    if not articles:
        print("No articles found with availability='manual' and file=NULL")
        return 0

    generate_html(articles, args.output)
    print(f"Generated {args.output} with {len(articles)} articles")
    return 0


if __name__ == "__main__":
    sys.exit(main())
