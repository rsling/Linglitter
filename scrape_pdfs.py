#!/usr/bin/env python3
"""
Download open-access PDFs using multiple OA sources.

Queries Unpaywall, Semantic Scholar, OpenAlex, CORE, and LingBuzz
for DOIs in linglitter.db, downloads available PDFs, and tracks
download status in the database.

Usage:
    python scrape_pdfs.py
    python scrape_pdfs.py --config myconfig.json
    python scrape_pdfs.py --limit 100
    python scrape_pdfs.py --continuous
    python scrape_pdfs.py --reset-oa-attempts   # retry previously failed OA articles
    python scrape_pdfs.py --dry-run
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
import time
from datetime import datetime
from html import unescape as html_unescape
from pathlib import Path
from urllib.parse import quote, urlparse

import requests

UNPAYWALL_API = "https://api.unpaywall.org/v2"
SEMANTIC_SCHOLAR_API = "https://api.semanticscholar.org/graph/v1"
OPENALEX_API = "https://api.openalex.org"
CORE_API = "https://api.core.ac.uk/v3"
LINGBUZZ_URL = "https://lingbuzz.net/lingbuzz"

log = logging.getLogger(__name__)

# Browser-like headers for session requests
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "DNT": "1",
    "Sec-GPC": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Priority": "u=0, i",
}

# Per-publisher session storage: {publisher: (session, last_used_datetime)}
_publisher_sessions = {}

# Per-service rate limiting: minimum seconds between requests to each service.
# Semantic Scholar: 100 req/5min unauthenticated ≈ 1 req/3s
# OpenAlex: very generous, but recommends polite pool via mailto
# CORE: 1 req/s on free tier — we use 3s to be safe
# LingBuzz: small volunteer-run site — be extra gentle
SERVICE_INTERVALS = {
    "semantic_scholar": 3,
    "openalex": 2,
    "core": 3,
    "lingbuzz": 10,
}

# Tracks last request time per service: {service_name: datetime}
_service_last_request = {}


def service_wait(service):
    """Sleep if needed to respect the per-service rate limit.

    Call this BEFORE making a request to the given service.
    """
    interval = SERVICE_INTERVALS.get(service)
    if not interval:
        return
    now = datetime.now()
    last = _service_last_request.get(service)
    if last:
        elapsed = (now - last).total_seconds()
        if elapsed < interval:
            wait = interval - elapsed
            log.debug("  Rate limit: waiting %.1fs for %s", wait, service)
            time.sleep(wait)
    _service_last_request[service] = datetime.now()


def service_backoff(service, multiplier=3):
    """Temporarily increase a service's interval after a 429.

    Triples the interval (capped at 120s) so subsequent articles
    back off from the overloaded service.
    """
    current = SERVICE_INTERVALS.get(service, 3)
    new_interval = min(current * multiplier, 120)
    if new_interval != current:
        SERVICE_INTERVALS[service] = new_interval
        log.warning("  Rate-limited by %s — interval increased to %ds", service, new_interval)


def get_publisher_session(publisher):
    """Get or create a requests session for a publisher.

    Sessions are reused across downloads from the same publisher,
    preserving cookies and appearing more like normal browser behavior.

    Returns (session, is_new, elapsed_str) where:
    - is_new: True if a fresh session was created
    - elapsed_str: For continued sessions, time since last use as "HH:MM"
    """
    now = datetime.now()
    if publisher not in _publisher_sessions:
        session = requests.Session()
        session.headers.update(BROWSER_HEADERS)
        _publisher_sessions[publisher] = (session, now)
        return session, True, None
    session, last_used = _publisher_sessions[publisher]
    elapsed = now - last_used
    total_minutes = int(elapsed.total_seconds() // 60)
    hours, minutes = divmod(total_minutes, 60)
    elapsed_str = f"{hours:02d}:{minutes:02d}"
    _publisher_sessions[publisher] = (session, now)
    return session, False, elapsed_str


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def load_config(config_path):
    """Load configuration from JSON file."""
    with open(config_path) as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def ensure_schema(conn):
    """Add PDF-related columns if they don't exist (for older databases)."""
    for col, coltype in [
        ("availability", "TEXT"),
        ("source", "TEXT"),
        ("attempts", "INTEGER DEFAULT 0"),
        ("response", "INTEGER DEFAULT 0"),
        ("timestamp", "TEXT"),
        ("file", "TEXT"),
        ("jump_url", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE articles ADD COLUMN {col} {coltype}")
        except sqlite3.OperationalError:
            pass  # Column already exists
    conn.commit()


def get_random_candidate(conn, years, journals):
    """Select a random article that needs OA checking/downloading.

    Only selects articles with availability IS NULL (untried).
    Articles that have already been processed by this tool move to
    'oa' (success, with file) or 'no-oa' (failed or genuinely not OA)
    and are not retried.
    """
    placeholders = ",".join("?" for _ in journals)
    query = f"""
        SELECT doi, publisher, journal, year, title
        FROM articles
        WHERE type = 'article'
          AND year >= ? AND year <= ?
          AND journal IN ({placeholders})
          AND file IS NULL
          AND availability IS NULL
        ORDER BY RANDOM()
        LIMIT 1
    """
    params = [years[0], years[1]] + journals
    row = conn.execute(query, params).fetchone()
    if row:
        return {"doi": row[0], "publisher": row[1], "journal": row[2],
                "year": row[3], "title": row[4]}
    return None


def get_last_global_timestamp(conn):
    """Get the timestamp of the most recent download attempt."""
    row = conn.execute("""
        SELECT MAX(timestamp) FROM articles WHERE timestamp IS NOT NULL
    """).fetchone()
    return row[0] if row and row[0] else None


def get_last_publisher_timestamp(conn, publisher):
    """Get the timestamp of the most recent download attempt for a publisher."""
    row = conn.execute("""
        SELECT MAX(timestamp) FROM articles
        WHERE publisher = ? AND timestamp IS NOT NULL
    """, (publisher,)).fetchone()
    return row[0] if row and row[0] else None


def update_article(conn, doi, availability, source, attempts, response, timestamp,
                   file_path, jump_url=None):
    """Update an article's PDF-related fields."""
    conn.execute("""
        UPDATE articles
        SET availability = ?,
            source = ?,
            attempts = ?,
            response = ?,
            timestamp = ?,
            file = ?,
            jump_url = ?
        WHERE doi = ?
    """, (availability, source, attempts, response, timestamp, file_path, jump_url, doi))
    conn.commit()


def get_article_attempts(conn, doi):
    """Get the current number of attempts for an article."""
    row = conn.execute("SELECT attempts FROM articles WHERE doi = ?", (doi,)).fetchone()
    return row[0] if row and row[0] else 0


# ---------------------------------------------------------------------------
# Politeness checks
# ---------------------------------------------------------------------------

def parse_timestamp(ts_str):
    """Parse ISO timestamp string to datetime."""
    if not ts_str:
        return None
    return datetime.fromisoformat(ts_str)


def check_politeness(conn, publisher, global_interval, publisher_interval):
    """Check if we can proceed without violating politeness intervals.

    Returns (ok, wait_seconds) where ok is True if we can proceed,
    or False with wait_seconds indicating how long to wait.
    """
    now = datetime.now()

    # Check global interval
    last_global = get_last_global_timestamp(conn)
    if last_global:
        last_dt = parse_timestamp(last_global)
        if last_dt:
            elapsed = (now - last_dt).total_seconds()
            if elapsed < global_interval:
                return False, global_interval - elapsed

    # Check per-publisher interval
    last_pub = get_last_publisher_timestamp(conn, publisher)
    if last_pub:
        last_dt = parse_timestamp(last_pub)
        if last_dt:
            elapsed = (now - last_dt).total_seconds()
            if elapsed < publisher_interval:
                return False, publisher_interval - elapsed

    return True, 0


# ---------------------------------------------------------------------------
# Unpaywall API
# ---------------------------------------------------------------------------

def query_unpaywall(doi, mailto):
    """Query Unpaywall API for a DOI.

    Returns dict with keys:
    - is_oa: bool
    - pdf_url: str or None (direct PDF URL if available)
    - landing_url: str or None (article landing page URL)
    - response_code: int
    """
    if not mailto:
        log.error("Unpaywall requires an email address (set 'mailto' in config)")
        return {"is_oa": False, "pdf_url": None, "landing_url": None, "response_code": 0}

    url = f"{UNPAYWALL_API}/{quote(doi, safe='')}"
    params = {"email": mailto}

    try:
        resp = requests.get(url, params=params, timeout=30)
        code = resp.status_code

        if code == 404:
            # DOI not found in Unpaywall
            return {"is_oa": False, "pdf_url": None, "landing_url": None, "response_code": code}

        if code != 200:
            log.warning("Unpaywall returned %d for %s", code, doi)
            return {"is_oa": False, "pdf_url": None, "landing_url": None, "response_code": code}

        data = resp.json()
        is_oa = data.get("is_oa", False)

        pdf_url = None
        landing_url = None
        if is_oa:
            # Try best_oa_location first
            best = data.get("best_oa_location")
            if best:
                pdf_url = best.get("url_for_pdf")
                landing_url = best.get("url_for_landing_page") or best.get("url")

            # Fall back to other locations if needed
            if not pdf_url:
                for loc in data.get("oa_locations", []):
                    pdf_url = loc.get("url_for_pdf")
                    landing_url = loc.get("url_for_landing_page") or loc.get("url")
                    if pdf_url:
                        break

        return {"is_oa": is_oa, "pdf_url": pdf_url, "landing_url": landing_url, "response_code": code}

    except requests.exceptions.RequestException as exc:
        log.warning("Unpaywall request failed for %s: %s", doi, exc)
        return {"is_oa": False, "pdf_url": None, "landing_url": None, "response_code": 0}


# ---------------------------------------------------------------------------
# Fallback OA sources
# ---------------------------------------------------------------------------

def query_semantic_scholar(doi):
    """Query Semantic Scholar API for an open-access PDF URL.

    Returns (pdf_url, landing_url) or (None, None).
    """
    service_wait("semantic_scholar")
    url = f"{SEMANTIC_SCHOLAR_API}/paper/DOI:{quote(doi, safe='')}"
    params = {"fields": "openAccessPdf,url"}

    try:
        resp = requests.get(url, timeout=30, headers={
            "User-Agent": "Linglitter/1.0 (academic research tool)",
        })
        if resp.status_code == 404:
            log.debug("  Semantic Scholar: DOI not found")
            return None, None
        if resp.status_code == 429:
            log.debug("  Semantic Scholar: rate limited")
            service_backoff("semantic_scholar")
            return None, None
        if resp.status_code != 200:
            log.debug("  Semantic Scholar: HTTP %d", resp.status_code)
            return None, None

        data = resp.json()
        oa_pdf = data.get("openAccessPdf")
        landing = data.get("url")
        if oa_pdf and oa_pdf.get("url"):
            return oa_pdf["url"], landing
        return None, landing

    except requests.exceptions.RequestException as exc:
        log.debug("  Semantic Scholar request failed: %s", exc)
        return None, None


def query_openalex(doi, mailto=None):
    """Query OpenAlex API for an open-access PDF URL.

    Returns (pdf_url, landing_url) or (None, None).
    """
    service_wait("openalex")
    url = f"{OPENALEX_API}/works/doi:{quote(doi, safe='')}"
    params = {"select": "open_access,best_oa_location"}
    if mailto:
        params["mailto"] = mailto

    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 404:
            log.debug("  OpenAlex: DOI not found")
            return None, None
        if resp.status_code == 429:
            log.debug("  OpenAlex: rate limited")
            service_backoff("openalex")
            return None, None
        if resp.status_code != 200:
            log.debug("  OpenAlex: HTTP %d", resp.status_code)
            return None, None

        data = resp.json()

        # Try best_oa_location first
        best = data.get("best_oa_location")
        if best:
            pdf_url = best.get("pdf_url")
            landing = best.get("landing_page_url")
            if pdf_url:
                return pdf_url, landing

        # Fall back to open_access.oa_url
        oa = data.get("open_access", {})
        oa_url = oa.get("oa_url")
        if oa_url:
            return oa_url, None

        return None, None

    except requests.exceptions.RequestException as exc:
        log.debug("  OpenAlex request failed: %s", exc)
        return None, None


def query_core(doi, api_key):
    """Query CORE API for an open-access PDF URL.

    Requires a free API key from https://core.ac.uk/api-keys/register.
    Returns (pdf_url, landing_url) or (None, None).
    """
    if not api_key:
        return None, None

    service_wait("core")
    url = f"{CORE_API}/search/works"
    params = {"q": f'doi:"{doi}"', "limit": 1}
    headers = {"Authorization": f"Bearer {api_key}"}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        if resp.status_code == 429:
            log.debug("  CORE: rate limited")
            service_backoff("core")
            return None, None
        if resp.status_code != 200:
            log.debug("  CORE: HTTP %d", resp.status_code)
            return None, None

        data = resp.json()
        results = data.get("results", [])
        if not results:
            log.debug("  CORE: no results")
            return None, None

        hit = results[0]
        download_url = hit.get("downloadUrl")
        source_url = hit.get("sourceFulltextUrls") or []
        landing = hit.get("links", [{}])

        if download_url:
            return download_url, None

        # Try sourceFulltextUrls
        if source_url and isinstance(source_url, list) and source_url[0]:
            return source_url[0], None

        return None, None

    except requests.exceptions.RequestException as exc:
        log.debug("  CORE request failed: %s", exc)
        return None, None


def _normalize_title(title):
    """Lowercase, strip punctuation for fuzzy comparison."""
    title = title.lower().strip()
    title = re.sub(r'[^\w\s]', '', title)
    title = re.sub(r'\s+', ' ', title)
    return title


def _title_similarity(a, b):
    """Compute similarity ratio between two titles (0..1)."""
    from difflib import SequenceMatcher
    return SequenceMatcher(None, _normalize_title(a), _normalize_title(b)).ratio()


def search_lingbuzz(title):
    """Search LingBuzz by title and return a PDF URL if a good match is found.

    Returns (pdf_url, landing_url) or (None, None).
    """
    if not title or len(title) < 10:
        return None, None

    service_wait("lingbuzz")

    # Use first ~8 significant words for the search query
    words = re.sub(r'[^\w\s]', ' ', title).split()
    # Drop very short words that add noise
    words = [w for w in words if len(w) > 2][:8]
    query = "+".join(words)

    search_url = f"{LINGBUZZ_URL}?_s={quote(query)}"

    try:
        resp = requests.get(search_url, timeout=30, headers={
            "User-Agent": "Linglitter/1.0 (academic research tool)",
            "Accept": "text/html",
        })
        if resp.status_code != 200:
            log.debug("  LingBuzz: HTTP %d", resp.status_code)
            return None, None

        html = resp.text

        # Parse search results: find paper links and their titles.
        # LingBuzz results have rows with links like /lingbuzz/NNNNNN
        # and title text near them.
        # Pattern: <a href="/lingbuzz/NNNNNN">title text</a>
        matches = re.findall(
            r'<a\s+href="(/lingbuzz/(\d{6}))"[^>]*>\s*(.*?)\s*</a>',
            html, re.DOTALL
        )

        if not matches:
            log.debug("  LingBuzz: no results for query")
            return None, None

        best_score = 0.0
        best_id = None
        for path, paper_id, link_text in matches:
            # Clean HTML from link text
            link_text = re.sub(r'<[^>]+>', '', link_text).strip()
            link_text = html_unescape(link_text)
            if not link_text or len(link_text) < 10:
                continue

            score = _title_similarity(title, link_text)
            if score > best_score:
                best_score = score
                best_id = paper_id

        if best_id and best_score >= 0.75:
            pdf_url = f"{LINGBUZZ_URL}/{best_id}/current.pdf"
            landing_url = f"{LINGBUZZ_URL}/{best_id}"
            log.info("  LingBuzz: matched (score=%.2f) → %s", best_score, landing_url)
            return pdf_url, landing_url
        elif best_id:
            log.debug("  LingBuzz: best match score %.2f too low", best_score)

        return None, None

    except requests.exceptions.RequestException as exc:
        log.debug("  LingBuzz search failed: %s", exc)
        return None, None


# ---------------------------------------------------------------------------
# PDF download
# ---------------------------------------------------------------------------

def encode_doi_for_filename(doi):
    """Encode DOI to be safe as a filename.

    Replaces /, \\, :, *, ?, ", <, >, |, and . with underscores.
    """
    unsafe_chars = r'[/\\:*?"<>|.]'
    return re.sub(unsafe_chars, "_", doi)


def build_pdf_path(pdf_dir, doi):
    """Build the full path for storing a PDF.

    Returns (absolute_path, relative_path) where relative_path is stored in DB.
    """
    safe_doi = encode_doi_for_filename(doi)
    filename = f"{safe_doi}.pdf"
    absolute = Path(pdf_dir) / filename

    return absolute, filename


def _is_cloudflare_challenge(html):
    """Detect Cloudflare-type browser/CAPTCHA challenges in HTML."""
    cf_markers = [
        "cf-browser-verification",
        "cf_chl_opt",
        "cloudflare",
        "checking your browser",
        "verify you are human",
        "just a moment",
        "challenge-platform",
        "turnstile",
        "hcaptcha",
        "recaptcha",
    ]
    html_lower = html.lower()
    return any(marker in html_lower for marker in cf_markers)


def extract_pdf_links(html, base_url):
    """Extract candidate PDF download links from an HTML page.

    Looks for <a> tags whose href points to a PDF (by extension, query
    parameter, or link text), and returns a deduplicated list of absolute URLs.
    """
    from urllib.parse import urljoin

    candidates = []
    seen = set()

    # Find all <a href="..."> with surrounding text
    for m in re.finditer(
        r'<a\s[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        html, re.DOTALL | re.IGNORECASE
    ):
        href, link_text = m.group(1), m.group(2)
        link_text_clean = re.sub(r'<[^>]+>', '', link_text).strip().lower()

        # Skip empty/anchor/javascript hrefs
        if not href or href.startswith('#') or href.startswith('javascript:'):
            continue

        href_lower = href.lower()
        is_pdf_link = False

        # Heuristic 1: URL contains .pdf
        if '.pdf' in href_lower:
            is_pdf_link = True
        # Heuristic 2: URL has format/type parameter suggesting PDF
        if re.search(r'[?&](format|type|mime)=pdf', href_lower):
            is_pdf_link = True
        # Heuristic 3: link text suggests PDF download
        if re.search(r'\bpdf\b|\bdownload\b|\bfull.text\b', link_text_clean):
            is_pdf_link = True
        # Heuristic 4: URL path contains download/pdf segments
        if re.search(r'/download/|/pdf/|/fulltext/', href_lower):
            is_pdf_link = True

        if is_pdf_link:
            abs_url = urljoin(base_url, href)
            if abs_url not in seen:
                seen.add(abs_url)
                candidates.append(abs_url)

    # Also check <meta> refresh/redirect pointing to PDF
    for m in re.finditer(
        r'<meta[^>]+url=(["\']?)([^"\'\s>]+\.pdf[^"\']*)\1',
        html, re.IGNORECASE
    ):
        abs_url = urljoin(base_url, m.group(2))
        if abs_url not in seen:
            seen.add(abs_url)
            candidates.append(abs_url)

    return candidates


def download_pdf(pdf_url, dest_path, landing_url=None, timeout=60, session=None):
    """Download a PDF from a URL.

    If landing_url is provided, first visits the landing page to collect
    session cookies, then downloads the PDF. This helps with publishers
    that require cookies for PDF access.

    If session is provided, uses that session (preserving cookies from
    previous requests to the same publisher). Otherwise creates a new session.

    Returns (success, http_status_code).
    """
    # Use provided session or create a new one
    if session is None:
        session = requests.Session()
        session.headers.update(BROWSER_HEADERS)

    try:
        # First visit landing page to collect cookies if provided
        if landing_url:
            log.debug("  Visiting landing page for cookies: %s", landing_url)
            try:
                landing_resp = session.get(landing_url, timeout=timeout, allow_redirects=True)
                log.debug("  Landing page status: %d, cookies: %d",
                         landing_resp.status_code, len(session.cookies))
                # Small delay to appear more human-like
                time.sleep(0.5)
            except requests.exceptions.RequestException as exc:
                log.debug("  Landing page visit failed: %s", exc)
                # Continue anyway, might still work

        # Set referer to landing page or PDF domain
        parsed = urlparse(pdf_url)
        referer = landing_url if landing_url else f"{parsed.scheme}://{parsed.netloc}/"
        session.headers["Referer"] = referer
        # Update Sec-Fetch for same-origin navigation
        session.headers["Sec-Fetch-Site"] = "same-origin"

        # Now fetch the PDF with PDF-specific Accept header
        session.headers["Accept"] = "application/pdf,*/*;q=0.9"
        resp = session.get(pdf_url, timeout=timeout, stream=True, allow_redirects=True)
        code = resp.status_code

        if code != 200:
            log.warning("Download failed with status %d: %s", code, pdf_url)
            return False, code

        # Check Content-Type to ensure we got a PDF, not an HTML error page
        content_type = resp.headers.get("Content-Type", "").lower()
        if "html" in content_type:
            # Read the HTML body to look for PDF links
            html_body = resp.text
            resp.close()

            if _is_cloudflare_challenge(html_body):
                log.warning("Got Cloudflare/bot challenge instead of PDF: %s", pdf_url)
                return False, 403

            pdf_links = extract_pdf_links(html_body, pdf_url)

            if len(pdf_links) == 1:
                # Single PDF link found — follow it automatically
                follow_url = pdf_links[0]
                log.info("  HTML page contained PDF link, following: %s", follow_url)
                time.sleep(0.5)
                session.headers["Referer"] = pdf_url
                return _follow_pdf_link(follow_url, dest_path, session, timeout)

            if len(pdf_links) > 1:
                # --- TEMPORARY: multiple links, log for manual review ---
                # TODO(link-selection): Replace this block with automatic
                # link selection once patterns are understood.
                log.warning("Got HTML instead of PDF with %d candidate PDF links:", len(pdf_links))
                for i, link in enumerate(pdf_links, 1):
                    log.warning("  [%d] %s", i, link)
                log.warning("  Manual review needed for: %s", pdf_url)
                # --- END TEMPORARY ---
                return False, 403

            log.warning("Got HTML instead of PDF, no PDF links found: %s", pdf_url)
            return False, 403

        # Ensure directory exists
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Write content
        with open(dest_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8192):
                fh.write(chunk)

        # Verify we got something reasonable (and it's actually a PDF)
        if dest_path.stat().st_size < 1000:
            log.warning("Downloaded file suspiciously small: %s", dest_path)
            dest_path.unlink()
            return False, code

        # Check PDF magic bytes
        with open(dest_path, "rb") as fh:
            magic = fh.read(5)
        if magic != b"%PDF-":
            log.warning("Downloaded file is not a PDF (magic: %s): %s", magic[:20], dest_path)
            dest_path.unlink()
            return False, 403

        return True, code

    except requests.exceptions.RequestException as exc:
        log.warning("Download request failed: %s", exc)
        return False, 0


def _follow_pdf_link(pdf_url, dest_path, session, timeout=60):
    """Follow a single PDF link extracted from an HTML page.

    Returns (success, http_status_code).
    """
    try:
        session.headers["Accept"] = "application/pdf,*/*;q=0.9"
        resp = session.get(pdf_url, timeout=timeout, stream=True, allow_redirects=True)
        code = resp.status_code

        if code != 200:
            log.warning("  Followed PDF link failed with status %d: %s", code, pdf_url)
            return False, code

        content_type = resp.headers.get("Content-Type", "").lower()
        if "html" in content_type:
            resp.close()
            log.warning("  Followed PDF link returned HTML again: %s", pdf_url)
            return False, 403

        dest_path.parent.mkdir(parents=True, exist_ok=True)

        with open(dest_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8192):
                fh.write(chunk)

        if dest_path.stat().st_size < 1000:
            log.warning("  Followed PDF link: file suspiciously small: %s", dest_path)
            dest_path.unlink()
            return False, code

        with open(dest_path, "rb") as fh:
            magic = fh.read(5)
        if magic != b"%PDF-":
            log.warning("  Followed PDF link: not a PDF (magic: %s): %s", magic[:20], dest_path)
            dest_path.unlink()
            return False, 403

        return True, code

    except requests.exceptions.RequestException as exc:
        log.warning("  Followed PDF link request failed: %s", exc)
        return False, 0


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def _try_download(pdf_url, abs_path, landing_url=None, publisher=None):
    """Try downloading a PDF, managing per-publisher sessions.

    Returns (success, http_code).
    """
    if publisher:
        session, is_new_session, elapsed_str = get_publisher_session(publisher)
        parsed_url = urlparse(pdf_url)
        server_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        if is_new_session:
            log.info("  Session started: %s (%s)", publisher, server_url)
        else:
            log.info("  Session continued: %s (%s) [%s]", publisher, server_url, elapsed_str)
    else:
        session = requests.Session()
        session.headers.update(BROWSER_HEADERS)

    return download_pdf(pdf_url, abs_path, landing_url=landing_url, session=session)


def process_one(conn, config, dry_run=False):
    """Process a single article using cascading OA sources.

    Tries Unpaywall → Semantic Scholar → OpenAlex → CORE → LingBuzz.
    Each source is only queried if the previous one did not yield a
    successful download.

    Returns:
    - "downloaded": successfully downloaded
    - "no-oa": confirmed not open access
    - "failed": download attempted but failed from all sources
    - "skipped": politeness violation, try again later
    - "exhausted": max attempts reached
    - None: no candidates left
    """
    years = config["years"]
    journals = config["journals"]
    unpaywall_cfg = config["unpaywall"]
    pdf_dir = config.get("pdf_dir", "pdf")
    core_api_key = config.get("core_api_key")
    mailto = unpaywall_cfg.get("mailto")

    # Get a random candidate
    candidate = get_random_candidate(conn, years, journals)
    if not candidate:
        return None, None

    doi = candidate["doi"]
    publisher = candidate["publisher"]
    journal = candidate["journal"]
    year = candidate["year"]
    title = candidate["title"]

    # Check attempt count
    attempts = get_article_attempts(conn, doi)
    if attempts >= unpaywall_cfg["max_attempts"]:
        log.debug("Max attempts reached for %s", doi)
        return "exhausted", doi

    # Check politeness
    ok, wait = check_politeness(
        conn, publisher,
        unpaywall_cfg["politeness_interval"],
        unpaywall_cfg["publisher_interval"]
    )
    if not ok:
        log.debug("Politeness wait %.1fs for publisher %s", wait, publisher)
        return "skipped", doi

    log.info("Processing: %s", doi)

    if dry_run:
        log.info("  [DRY RUN] Would query Unpaywall and attempt download")
        return "skipped", doi

    now = datetime.now().isoformat()
    attempts += 1

    # Build paths
    abs_path, rel_path = build_pdf_path(pdf_dir, doi)

    # ------------------------------------------------------------------
    # Source 1: Unpaywall
    # ------------------------------------------------------------------
    result = query_unpaywall(doi, mailto)

    if not result["is_oa"]:
        log.info("  Not OA (Unpaywall response: %d)", result["response_code"])
        update_article(conn, doi, availability="no-oa", source=None,
                      attempts=attempts, response=result["response_code"],
                      timestamp=now, file_path=None)
        return "no-oa", doi

    # It's OA. Try to download from cascading sources.
    pdf_url = result["pdf_url"]
    landing_url = result["landing_url"]
    tried_urls = set()
    # Collect the best landing page URL for jump_url (used by prepare_manual.py)
    best_landing = landing_url

    if pdf_url:
        log.info("  [Unpaywall] PDF: %s", pdf_url)
        tried_urls.add(pdf_url)
        success, http_code = _try_download(
            pdf_url, abs_path, landing_url=landing_url, publisher=publisher)
        if success:
            log.info("  Downloaded via Unpaywall: %s", rel_path)
            update_article(conn, doi, availability="oa", source=pdf_url,
                          attempts=attempts, response=http_code,
                          timestamp=now, file_path=rel_path)
            return "downloaded", doi
        log.info("  [Unpaywall] download failed (HTTP %d), trying fallbacks…", http_code)
    else:
        log.info("  [Unpaywall] OA but no PDF URL, trying fallbacks…")

    # ------------------------------------------------------------------
    # Source 2: Semantic Scholar
    # ------------------------------------------------------------------
    log.info("  Trying Semantic Scholar…")
    s2_url, s2_landing = query_semantic_scholar(doi)
    if s2_landing and not best_landing:
        best_landing = s2_landing
    if s2_url and s2_url not in tried_urls:
        log.info("  [Semantic Scholar] PDF: %s", s2_url)
        tried_urls.add(s2_url)
        success, http_code = _try_download(s2_url, abs_path, landing_url=s2_landing)
        if success:
            log.info("  Downloaded via Semantic Scholar: %s", rel_path)
            update_article(conn, doi, availability="oa", source=s2_url,
                          attempts=attempts, response=http_code,
                          timestamp=now, file_path=rel_path)
            return "downloaded", doi
        log.info("  [Semantic Scholar] download failed (HTTP %d)", http_code)
    elif s2_url:
        log.debug("  [Semantic Scholar] same URL as Unpaywall, skipping")
    else:
        log.debug("  [Semantic Scholar] no PDF URL")

    # ------------------------------------------------------------------
    # Source 3: OpenAlex
    # ------------------------------------------------------------------
    log.info("  Trying OpenAlex…")
    oa_url, oa_landing = query_openalex(doi, mailto=mailto)
    if oa_landing and not best_landing:
        best_landing = oa_landing
    if oa_url and oa_url not in tried_urls:
        log.info("  [OpenAlex] PDF: %s", oa_url)
        tried_urls.add(oa_url)
        success, http_code = _try_download(oa_url, abs_path, landing_url=oa_landing)
        if success:
            log.info("  Downloaded via OpenAlex: %s", rel_path)
            update_article(conn, doi, availability="oa", source=oa_url,
                          attempts=attempts, response=http_code,
                          timestamp=now, file_path=rel_path)
            return "downloaded", doi
        log.info("  [OpenAlex] download failed (HTTP %d)", http_code)
    elif oa_url:
        log.debug("  [OpenAlex] same URL already tried, skipping")
    else:
        log.debug("  [OpenAlex] no PDF URL")

    # ------------------------------------------------------------------
    # Source 4: CORE
    # ------------------------------------------------------------------
    if core_api_key:
        log.info("  Trying CORE…")
        core_url, core_landing = query_core(doi, core_api_key)
        if core_landing and not best_landing:
            best_landing = core_landing
        if core_url and core_url not in tried_urls:
            log.info("  [CORE] PDF: %s", core_url)
            tried_urls.add(core_url)
            success, http_code = _try_download(core_url, abs_path, landing_url=core_landing)
            if success:
                log.info("  Downloaded via CORE: %s", rel_path)
                update_article(conn, doi, availability="oa", source=core_url,
                              attempts=attempts, response=http_code,
                              timestamp=now, file_path=rel_path)
                return "downloaded", doi
            log.info("  [CORE] download failed (HTTP %d)", http_code)
        elif core_url:
            log.debug("  [CORE] same URL already tried, skipping")
        else:
            log.debug("  [CORE] no PDF URL")

    # ------------------------------------------------------------------
    # Source 5: LingBuzz (title search, last resort)
    # ------------------------------------------------------------------
    if title:
        log.info("  Trying LingBuzz…")
        lb_url, lb_landing = search_lingbuzz(title)
        if lb_landing and not best_landing:
            best_landing = lb_landing
        if lb_url and lb_url not in tried_urls:
            log.info("  [LingBuzz] PDF: %s", lb_url)
            tried_urls.add(lb_url)
            success, http_code = _try_download(lb_url, abs_path, landing_url=lb_landing)
            if success:
                log.info("  Downloaded via LingBuzz: %s", rel_path)
                update_article(conn, doi, availability="oa", source=lb_url,
                              attempts=attempts, response=http_code,
                              timestamp=now, file_path=rel_path)
                return "downloaded", doi
            log.info("  [LingBuzz] download failed (HTTP %d)", http_code)
        elif lb_url:
            log.debug("  [LingBuzz] same URL already tried, skipping")
        else:
            log.debug("  [LingBuzz] no match found")

    # ------------------------------------------------------------------
    # All sources exhausted — mark as no-oa so scrape_repo.py picks it up
    # ------------------------------------------------------------------
    log.warning("  All sources exhausted for %s", doi)
    if best_landing:
        log.info("  Saving jump URL: %s", best_landing)
    source_tried = pdf_url  # original Unpaywall URL, if any
    update_article(conn, doi, availability="no-oa", source=source_tried,
                  attempts=attempts, response=result.get("response_code", 0),
                  timestamp=now, file_path=None, jump_url=best_landing)
    return "failed", doi


def main():
    parser = argparse.ArgumentParser(
        description="Download open-access PDFs using multiple OA sources.")
    parser.add_argument("--config", type=str, default="config.json",
                        help="Path to config JSON (default: config.json)")
    parser.add_argument("--db", type=str, default="linglitter.db",
                        help="Path to SQLite database (default: linglitter.db)")
    parser.add_argument("--mailto", type=str, default=None,
                        help="Email for Unpaywall/OpenAlex APIs (overrides config.json)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Maximum number of articles to process")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without downloading")
    parser.add_argument("--continuous", action="store_true",
                        help="Run continuously until no candidates remain")
    parser.add_argument("--reset-oa-attempts", action="store_true",
                        help="Reset attempt counters for OA articles without files, "
                             "so they are retried with fallback sources")
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

    # Command line --mailto overrides config
    if args.mailto:
        config["unpaywall"]["mailto"] = args.mailto

    if not config["unpaywall"].get("mailto"):
        log.error("Please set 'mailto' via --mailto or in config.json for Unpaywall API access")
        return 1

    # Connect to database
    db_path = Path(args.db)
    if not db_path.exists():
        log.error("Database not found: %s (run scrape_dois.py first)", db_path)
        return 1

    conn = sqlite3.connect(args.db)
    ensure_schema(conn)  # Add missing columns if needed

    # Reset failed OA articles back to NULL so they re-enter the pipeline
    if args.reset_oa_attempts:
        cur = conn.execute("""
            UPDATE articles SET availability = NULL, attempts = 0
            WHERE type = 'article' AND availability = 'no-oa' AND file IS NULL
        """)
        conn.commit()
        log.info("Reset %d failed articles back to NULL for retry", cur.rowcount)

    # Ensure PDF directory exists
    pdf_dir = Path(config.get("pdf_dir", "pdf"))
    pdf_dir.mkdir(parents=True, exist_ok=True)

    # Stats
    stats = {"downloaded": 0, "no-oa": 0, "failed": 0, "skipped": 0, "exhausted": 0}
    processed = 0

    log.info("Starting PDF scraper (years %d–%d, %d journals)",
             config["years"][0], config["years"][1], len(config["journals"]))

    try:
        while True:
            result, doi = process_one(conn, config, args.dry_run)

            if result is None:
                log.info("No more candidates to process")
                break

            if result in stats:
                stats[result] += 1

            if result == "skipped":
                # Wait a bit before next attempt to avoid busy-looping
                time.sleep(0.5)
                continue

            processed += 1

            if args.limit and processed >= args.limit:
                log.info("Reached limit of %d articles", args.limit)
                break

            if not args.continuous and result in ("downloaded", "no-oa", "failed"):
                # In non-continuous mode, process one article and exit
                break

            # Small delay between articles
            time.sleep(config["unpaywall"]["politeness_interval"])

    except KeyboardInterrupt:
        log.info("Interrupted by user")

    conn.close()

    log.info("Done — downloaded: %d, no-oa: %d, failed: %d, skipped: %d, exhausted: %d",
             stats["downloaded"], stats["no-oa"], stats["failed"],
             stats["skipped"], stats["exhausted"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
