import os, sys, time, sqlite3, threading, queue, re, json, signal
from contextlib import contextmanager
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

# Parsers
from bs4 import BeautifulSoup
from selectolax.parser import HTMLParser

# Async stack
import asyncio, aiohttp, async_timeout, aiosqlite

# Selenium (optional fallback)
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service as FxService
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from collections import defaultdict

try:
    import uvloop
    uvloop.install()
except Exception:
    pass

import shutil
import pathlib
import random

# -------- pretty colors (disabled if not a TTY) ----------
# Try to make Windows & IDEs happy
try:
    import colorama
    colorama.just_fix_windows_console()
except Exception:
    pass


#---------------------------- Color ----------------------------

def banner(tag: str, msg: str, color: str) -> None:
    print(f"{color}[{tag}]{C.reset} {msg}")

IS_PYCHARM   = os.environ.get("PYCHARM_HOSTED") == "1"
# In PyCharm, force colors on. Else allow override via FORCE_COLOR=1
FORCE_COLOR  = IS_PYCHARM or os.environ.get("FORCE_COLOR", "").lower() in ("1","true","yes","on")
USE_COLOR    = sys.stdout.isatty() or FORCE_COLOR

class C:
    reset   = "\x1b[0m"      if USE_COLOR else ""
    bold    = "\x1b[1m"      if USE_COLOR else ""
    dim     = "\x1b[2m"      if USE_COLOR else ""
    red     = "\x1b[31m"     if USE_COLOR else ""
    green   = "\x1b[32m"     if USE_COLOR else ""
    yellow  = "\x1b[33m"     if USE_COLOR else ""
    blue    = "\x1b[34m"     if USE_COLOR else ""
    magenta = "\x1b[35m"     if USE_COLOR else ""
    cyan    = "\x1b[36m"     if USE_COLOR else ""

# -------------------------- Metrics ---------------------------------------
class Meter:
    __slots__ = ("val", "t0")
    def __init__(self):
        self.val = 0
        self.t0 = time.time()
    def add(self, n=1):
        self.val += n
    def rate(self):
        dt = max(1e-6, time.time() - self.t0)
        return self.val / dt

# ----------------------------- CONFIG ---------------------------------

# --- Store the DB under ./scrapes
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "scrapes"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = str(DATA_DIR / "yellowpages.db")   # ./scrapes/yellowpages.db

# DB tuning
USE_WAL = True                    # keep WAL for speed or set False for single .db file
BATCH_SIZE = 5000                 # bigger batch less commits more speed
FLUSH_SECS = 0.5                  # commit at least twice per second I am SPeed
PER_HOST_CAP = 128                # limit concurrent requests per host; try 32..128 - 1024?

# NEW: WAL checkpoint controls
CHECKPOINT_SECS = 8               # checkpoint every N seconds
WAL_MAX_MB = 512                  # or when WAL reaches this size Send it

# Frontier tuning
FRONTIER_MAXSIZE = 0              # 0 = unbounded queue. For bounded, set example - 200000.

CONCURRENCY = 900                 # yellowpages has no rate limits
CONNECT_TIMEOUT = 20
READ_TIMEOUT = 30
MAX_RETRIES = 4
USE_SELENIUM_FALLBACK = False
SELENIUM_PAGELOAD_TIMEOUT = 25

BASE_DOMAIN = "www.yellowpages.com"
BASE_SCHEME = "https"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
}

# CSS selectors used on SRP pages
SELECTORS = {
    "listing_container": "div.result",
    "name":   ".business-name",
    "phone":  ".phones.phone.primary",
    "street": ".street-address",
    "locality": ".locality",
}

USER_AGENTS = [
    # recent stable-channel UAs (rotate a few)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.6; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/125.0.0.0 Safari/537.36",
]

def pick_ua():
    return random.choice(USER_AGENTS)

# ---------------------------- UTILITIES --------------------------------

def nrm(s: str | None) -> str | None:
    if s is None: return None
    s = s.strip()
    return s if s else None

def css_text(node, selector):
    el = node.css_first(selector)
    if not el: return None
    return nrm(el.text(strip=True))

def phone_clean(s: str | None) -> str | None:
    if not s: return None
    s2 = re.sub(r"[^\d+x]", "", s)
    return s2 or None

def split_locality(locality: str | None):
    # "Fullerton, CA 92832" → ("Fullerton", "CA", "92832")
    if not locality: return (None, None, None)
    city, state, zipc = None, None, None
    m = re.match(r"^(.*?),\s*([A-Z]{2})(?:\s+(\d{5}))?$", locality.strip())
    if m:
        city = nrm(m.group(1))
        state = nrm(m.group(2))
        zipc = nrm(m.group(3))
    else:
        city = nrm(locality)
    return (city, state, zipc)

def _pick(*args):
    """Return the first truthy value (or None). Handy for 'accept if at least X is present'."""
    for a in args:
        if a:
            return a
    return None

def _json_loads_safe(raw: str):
    """
    Lenient JSON loader for noisy <script> blobs:
    - strips whitespace / trailing semicolons
    - tries plain json.loads, else returns None
    """
    if not raw:
        return None
    s = raw.strip()
    if s.endswith(";"):
        s = s[:-1]
    try:
        return json.loads(s)
    except Exception:
        return None

# Toll-free detection & basic listing classification

TOLL_FREE_RE = re.compile(r'^\(?8(00|33|44|55|66|77|88)\)?[-.\s]*\d', re.ASCII)

def is_toll_free(phone: str | None) -> bool:
    if not phone:
        return False
    d = re.sub(r'\D+', '', phone)
    return bool(re.match(r'^8(00|33|44|55|66|77|88)', d))

def classify_listing(listing: dict) -> dict:
    """Return dict of booleans: service_area, nationwide, aggregator, toll_free."""
    omit = (listing.get("omitAddress") or listing.get("omit_address") or "").upper() == "Y"
    phone = (listing.get("phoneNumber") or listing.get("phone") or "").strip()
    content_provider = (listing.get("content_provider") or listing.get("contentProvider") or "")
    return {
        "service_area": omit,
        "nationwide": "nationwide/mip" in (listing.get("_page_path") or "").lower(),
        "aggregator": content_provider in {"IY", "DEX", "YEXT", "SUPERPAGES"},
        "toll_free": is_toll_free(phone),
    }

def accept_record(name, street, city, state, phone, flags) -> bool:
    """
    Prefer full address OR a non-toll-free local phone.
    Keeps quality high; adjust if you want different tolerance.
    """
    has_addr = bool(street and city and state)
    has_local_phone = bool(phone and not flags.get("toll_free"))
    return bool(name and (has_addr or has_local_phone))


def same_domain(u: str) -> bool:
    try:
        p = urlparse(u)
        return p.scheme in ("http","https") and p.netloc.endswith("yellowpages.com")
    except Exception:
        return False

def add_or_replace_page(url: str, page: int) -> str:
    pu = urlparse(url)
    q = parse_qs(pu.query)
    q["page"] = [str(page)]
    nq = urlencode(q, doseq=True)
    return urlunparse((pu.scheme, pu.netloc, pu.path, pu.params, nq, pu.fragment))

def extract_total_and_pagecount(doc: HTMLParser):
    """
    Parse "Showing X-Y of Z" from SRP pages to estimate total pages.
    Returns: (start, end, total, pages)
    """
    txt = doc.text(separator=" ", strip=True)
    m = re.search(r"Showing\s+(\d+)\s*-\s*(\d+)\s*of\s*(\d+)", txt, re.I)
    if not m:
        return (None, None, None, None)
    start, end, total = map(int, m.groups())
    per_page = end - start + 1 if end >= start else 30
    pages = (total + per_page - 1) // per_page
    return (start, end, total, pages)


    # --- simple multi-line status panel ---

def status_stack(lines: list[str]):
    """
    Render a live status panel.
    - In PyCharm (or STATUS_SINGLELINE=1): single-line '\r' refresh (no scrolling).
    - In real TTYs with ANSI: N-line panel (cursor up + clear) with no scrolling.
    """
    if not lines:
        return

    # Prefer single-line in PyCharm (more reliable), or via env.
    SINGLELINE = os.environ.get("STATUS_SINGLELINE", "").lower() in ("1", "true", "yes", "on")

    if SINGLELINE or (not sys.stdout.isatty() and not FORCE_COLOR):
        # --- single-line fallback (no scrolling) ---
        line = "  |  ".join(s.strip() for s in lines)
        try:
            width = shutil.get_terminal_size((120, 20)).columns
        except Exception:
            width = 120
        if len(line) >= width:
            line = line[:max(0, width - 1)]
        prev_len = getattr(status_stack, "_prev_len", 0)
        pad = " " * max(0, prev_len - len(line))
        sys.stdout.write("\r" + line + pad)
        sys.stdout.flush()
        status_stack._prev_len = len(line)
        return

    # --- Real TTY with ANSI: N-line panel (no scrolling) ---
    n = len(lines)
    prev = getattr(status_stack, "_prev_lines", 0)

    if prev == 0:
        sys.stdout.write("\n" * (n - 1))
        status_stack._prev_lines = n
        prev = n
    else:
        if n > prev:
            sys.stdout.write("\n" * (n - prev))
            status_stack._prev_lines = n
            prev = n
        elif n < prev:
            sys.stdout.write(f"\x1b[{prev - 1}F")
            for i in range(prev):
                sys.stdout.write("\x1b[2K")
                if i < prev - 1:
                    sys.stdout.write("\n")
            sys.stdout.write("\n" * (n - 1))
            status_stack._prev_lines = n
            prev = n

    if n > 1:
        sys.stdout.write(f"\x1b[{n - 1}F")
    for i, ln in enumerate(lines):
        sys.stdout.write("\x1b[2K")  # clear line
        sys.stdout.write("\x1b[0G")  # column 0
        sys.stdout.write(ln)
        if i < n - 1:
            sys.stdout.write("\n")
    sys.stdout.flush()

# --- simple multi-line status panel ---
_status_lines = []

def status_panel_init(n: int):
    """Allocate an n-line status panel."""
    global _status_lines
    _status_lines = [""] * n
    status_stack(_status_lines)

def status_set(i: int, text: str):
    """Update line i and redraw the panel."""
    global _status_lines
    if not _status_lines:
        return
    if 0 <= i < len(_status_lines):
        _status_lines[i] = text
        status_stack(_status_lines)

async def sanity_check(seed: str):
    async with Fetcher() as f:
        html = await f.get(seed)
        if not html:
            banner("SANITY", f"First seed failed to fetch: {seed}", C.red)
            return False
        recs, nexts = extract_records_and_links(html, seed)
        banner("SANITY", f"seed ok: recs={len(recs)} next_links={len(nexts)}", C.green)
        if recs:
            ypid,name,phone,street,city,state,zipc,url,category,price,status,source,notes = recs[0]
            banner("SANITY", f"sample: name={name!r} city={city!r} url={url}", C.dim)
        return True

# ---------------------------- DB LAYER ---------------------------------

SCHEMA_SQL = f"""
PRAGMA journal_mode={'WAL' if USE_WAL else 'DELETE'};
PRAGMA synchronous=NORMAL;             -- good speed/safety tradeoff
PRAGMA temp_store=MEMORY;              -- keep temp structures in RAM
PRAGMA page_size=4096;                 -- set before table creation
PRAGMA cache_size=-262144;             -- ~256MB cache (negative = KB units)
PRAGMA mmap_size=268435456;            -- 256MB memory map if supported
PRAGMA wal_autocheckpoint=0;           -- we'll checkpoint ourselves
PRAGMA busy_timeout=5000;              -- wait for locks

CREATE TABLE IF NOT EXISTS businesses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ypid   TEXT,
    name   TEXT,
    phone  TEXT,
    street TEXT,
    city   TEXT,
    state  TEXT,
    zip    TEXT,
    url    TEXT,
    category TEXT,
    price  TEXT,
    status TEXT,
    source_url TEXT,
    notes  TEXT DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS uniq_ypid ON businesses(ypid);
CREATE UNIQUE INDEX IF NOT EXISTS uniq_business_fallback
  ON businesses(name, phone, street, city, state, zip);

CREATE TABLE IF NOT EXISTS seen_urls (
    url TEXT PRIMARY KEY
);
"""

def file_mb(p: str) -> float:
    """Return file size in MB; 0.0 if it doesn't exist."""
    try:
        return os.path.getsize(p) / (1024 * 1024)
    except FileNotFoundError:
        return 0.0
    except OSError:
        return 0.0

class DB:
    INS_SQL = (
        "INSERT OR IGNORE INTO businesses("
        "ypid,name,phone,street,city,state,zip,url,category,price,status,source_url,notes"
        ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
    )

    def __init__(self, path: str, qmax: int = 10_000):
        self.path = path
        self.q = asyncio.Queue(maxsize=qmax)
        self._stop = asyncio.Event()
        self.inserted_total = 0
        self._last_ckpt = time.time()
        self.insert_meter = Meter()
        self.conn = None

    async def init(self):
        self.conn = await aiosqlite.connect(self.path)
        # Speed: row factory off, text factory default
        await self.conn.executescript(SCHEMA_SQL)
        await self.conn.commit()

    async def writer(self):
        """
        High-throughput, low-latency async writer.
        - Batches INSERT OR IGNOREs
        - Flushes by size/time
        - WAL checkpoints by time/size
        - Tracks inserts/sec via insert_meter
        """
        batch = []
        last_flush = time.time()

        while not (self._stop.is_set() and self.q.empty()):
            try:
                item = await asyncio.wait_for(self.q.get(), timeout=0.25)
                if item is not None:
                    batch.append(item)
            except asyncio.TimeoutError:
                item = None  # just reevaluate flush conditions

            need_flush = (
                len(batch) >= BATCH_SIZE
                or (batch and (time.time() - last_flush) > FLUSH_SECS)
                or (item is None and self._stop.is_set())
            )
            if not need_flush:
                continue

            # ---- flush batch ----
            try:
                await self.conn.executemany(self.INS_SQL, batch)
                cur = await self.conn.execute("SELECT changes()")
                (delta,) = await cur.fetchone()
                delta = int(delta)
                if delta:
                    self.inserted_total += delta
                    self.insert_meter.add(delta)
                await self.conn.commit()
            except Exception as e:
                # keep going; we prefer throughput over being precious with one batch
                print(f"\n[DB] executemany error on batch({len(batch)}): {e}", file=sys.stderr)
            finally:
                batch.clear()
                last_flush = time.time()

            # ---- WAL maintenance ----
            now = time.time()
            wal_mb = file_mb(self.path + "-wal")
            if (now - self._last_ckpt) >= CHECKPOINT_SECS or wal_mb >= WAL_MAX_MB:
                await self._maybe_checkpoint()
                self._last_ckpt = now

        # ---- final sweep ----
        if batch:
            try:
                await self.conn.executemany(self.INS_SQL, batch)
                cur = await self.conn.execute("SELECT changes()")
                (delta,) = await cur.fetchone()
                delta = int(delta)
                if delta:
                    self.inserted_total += delta
                    self.insert_meter.add(delta)
                await self.conn.commit()
            except Exception as e:
                print(f"\n[DB] final executemany error: {e}", file=sys.stderr)

    async def _maybe_checkpoint(self):
        # PASSIVE avoids long stalls; if it throws, don’t die
        try:
            await self.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            await self.conn.commit()
        except Exception as e:
            print(f"\n[DB] checkpoint error: {e}", file=sys.stderr)

    async def mark_seen(self, url: str) -> bool:
        try:
            await self.conn.execute("INSERT OR IGNORE INTO seen_urls(url) VALUES (?)", (url,))
            await self.conn.commit()
            return True
        except Exception:
            return False

    async def close(self):
        # Merge WAL → main db; run optimize; close.
        try:
            await self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            await self.conn.execute("PRAGMA optimize")
            await self.conn.commit()
        finally:
            await self.conn.close()

# ------------------------ EXTRACTION LOGIC ------------------------------

def extract_records_and_links(html: str, base_url: str, discover: bool = True):
    """
    Extract from:
      - SRP/listing pages (div.result cards)
      - MIP/business detail pages via:
          a) <script type="application/ld+json"> LocalBusiness
          b) inline JS "YPU = {... listing:{...} ...}"
    Optionally discover next links (SRP pagination + internal links) when discover=True.
    """
    doc = HTMLParser(html)
    recs: list[tuple] = []

    # -------- helpers --------
    def normalize_internal(u: str) -> str | None:
        """Keep only yellowpages.com links and strip fragments & tracking noise."""
        try:
            pu = urlparse(u)
            if pu.scheme not in ("http", "https"):
                return None
            if not pu.netloc.endswith("yellowpages.com"):
                return None
            q = parse_qs(pu.query, keep_blank_values=False)
            # drop common trackers
            for k in list(q.keys()):
                if k.startswith("utm_") or k in {"gclid", "fbclid"}:
                    q.pop(k, None)
            nq = urlencode(q, doseq=True)
            return urlunparse((pu.scheme, pu.netloc, pu.path, pu.params, nq, ""))  # no fragment
        except Exception:
            return None

    # =================== SRP (search results) cards ===================
    try:
        for item in doc.css(SELECTORS["listing_container"]):
            ypid = item.attributes.get("data-ypid")
            name = css_text(item, SELECTORS["name"])
            phone = phone_clean(css_text(item, SELECTORS["phone"]))
            street = css_text(item, SELECTORS["street"])
            locality = css_text(item, SELECTORS["locality"])
            city, state, zipc = split_locality(locality)

            price = None
            pr = item.css_first(".price-range")
            if pr:
                price = nrm(pr.text(strip=True))

            status = None
            st = item.css_first(".open-status")
            if st:
                status = nrm(st.text(strip=True))

            url = None
            a = item.css_first("a.business-name")
            if a:
                href = a.attributes.get("href")
                if href:
                    url = urljoin(base_url, href)

            cats = []
            for c in item.css(".categories a"):
                t = nrm(c.text(strip=True))
                if t:
                    cats.append(t)
            category = ", ".join(cats) if cats else None

            # accept if at least a name or ypid is present (keeps existing behavior)
            if _pick(ypid, name):
                recs.append((
                    ypid, name, phone, street, city, state, zipc, url, category, price, status, base_url, ""
                ))
    except Exception:
        # be forgiving—parsing problems on SRP shouldn't kill the page
        pass

    # =================== MIP via JSON-LD (LocalBusiness) ===================
    ld_blocks = []
    try:
        for s in doc.css("script[type='application/ld+json']"):
            raw = s.text() or ""
            if not raw.strip():
                continue
            j = _json_loads_safe(raw)
            if j is None:
                # some pages stuff multiple JSON objs in one tag; try splitting heuristically
                parts = re.split(r"}\s*{\s*", raw.strip())
                if len(parts) > 1:
                    merged = []
                    for i, p in enumerate(parts):
                        frag = p
                        if i > 0:
                            frag = "{" + frag
                        if i < len(parts) - 1:
                            frag = frag + "}"
                        jj = _json_loads_safe(frag)
                        if jj is not None:
                            merged.append(jj)
                    if merged:
                        ld_blocks.extend(merged)
                continue
            if isinstance(j, list):
                ld_blocks.extend(j)
            else:
                ld_blocks.append(j)
    except Exception:
        pass

    def _as_text(x):
        # JSON-LD fields can be strings or dicts
        if isinstance(x, dict):
            return x.get("@id") or x.get("url") or x.get("name")
        return x

    for lb in ld_blocks:
        try:
            t = lb.get("@type") or lb.get("@type".lower())
            if isinstance(t, list):
                t = [str(x).lower() for x in t]
                is_local = any("localbusiness" in x for x in t)
            else:
                is_local = str(t).lower().endswith("localbusiness")
            if not is_local:
                continue

            ypid = None
            idurl = _as_text(lb.get("@id"))
            if idurl and isinstance(idurl, str):
                m = re.search(r"/mip/[^/]+-(\d+)", idurl)
                if m:
                    ypid = m.group(1)

            name = nrm(lb.get("name"))
            tel = nrm(lb.get("telephone"))
            phone = phone_clean(tel)

            addr = lb.get("address") or {}
            if isinstance(addr, dict):
                street = nrm(addr.get("streetAddress"))
                city = nrm(addr.get("addressLocality"))
                state = nrm(addr.get("addressRegion"))
                zipc = nrm(addr.get("postalCode"))
            else:
                street = city = state = zipc = None

            canon = doc.css_first("link[rel='canonical']")
            detail_url = urljoin(base_url, canon.attributes.get("href")) if canon else base_url

            category = None
            pr = None
            status = None

            if _pick(ypid, name, phone, street, city, state, zipc):
                recs.append((
                    ypid, name, phone, street, city, state, zipc, detail_url, category, pr, status, base_url, ""
                ))
        except Exception:
            pass

    # =================== MIP via inline YPU.listing ===================
    try:
        head_text = html[:200000]  # small prefix for speed
        m = re.search(r"\bYPU\s*=\s*{", head_text)
        if m:
            # capture the whole YPU object by bracket matching
            start = head_text.find("{", m.start())
            depth = 0
            end = -1
            for k, ch in enumerate(head_text[start:], start=start):
                if ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        end = k
                        break
            if end != -1:
                blob = head_text[start:end + 1]
                ypu = _json_loads_safe(blob)
                if isinstance(ypu, dict):
                    listing = ypu.get("listing") or {}
                    if listing:
                        name = nrm(listing.get("displayName"))
                        phone = phone_clean(nrm(listing.get("phoneNumber")))
                        street = nrm(listing.get("addressLine1"))
                        city = nrm(listing.get("city"))
                        state = nrm(listing.get("state"))
                        zipc = nrm(listing.get("zip"))
                        # categories
                        cats = []
                        for c in (listing.get("categories") or []):
                            nm = nrm(c.get("name"))
                            if nm:
                                cats.append(nm)
                        category = ", ".join(cats) if cats else None

                        # annotate path/provider so we can classify
                        listing["_page_path"] = urlparse(base_url).path.lower()
                        node = doc.css_first("#bpp-impression")
                        listing["content_provider"] = node.attributes.get("data-analytics", "") if node else ""
                        flags = classify_listing(listing)

                        # prefer quality rows here
                        if accept_record(name, street, city, state, phone, flags):
                            recs.append((
                                nrm(listing.get("id")), name, phone, street, city, state, zipc,
                                base_url, category, None, None, base_url,
                                "flags=" + ",".join(k for k, v in flags.items() if v)
                            ))
    except Exception:
        pass

    # =================== Link discovery (conditional) ===================
    if not discover:
        return recs, []

    nexts: set[str] = set()

    # Rel/next, "Next"
    try:
        for a in doc.css("a.next, a[rel='next'], .pagination .next a"):
            href = a.attributes.get("href")
            if not href:
                continue
            u = normalize_internal(urljoin(base_url, href))
            if u:
                nexts.add(u)
    except Exception:
        pass

    # Numeric data-page
    try:
        for a in doc.css("a[data-page]"):
            p = a.attributes.get("data-page")
            href = a.attributes.get("href")
            if not p or not p.isdigit():
                continue
            if href:
                u = normalize_internal(urljoin(base_url, href))
            else:
                u = normalize_internal(add_or_replace_page(base_url, int(p)))
            if u:
                nexts.add(u)
    except Exception:
        pass

    # Synthesize ?page= from "Showing X-Y of Z"
    try:
        _s, _e, total, pages = extract_total_and_pagecount(doc)
        if pages and pages > 1:
            try:
                cur = int(parse_qs(urlparse(base_url).query).get("page", ["1"])[0])
            except Exception:
                cur = 1
            for p in range(2, pages + 1):
                if p == cur:
                    continue
                u = normalize_internal(add_or_replace_page(base_url, p))
                if u:
                    nexts.add(u)
    except Exception:
        pass

    # General internal links (keep SRP-like). Cap to avoid explosion.
    try:
        cap = 400  # generous but bounded
        added = 0
        for a in doc.css("a"):
            if added >= cap:
                break
            href = a.attributes.get("href")
            if not href or href.startswith("#"):
                continue
            u = normalize_internal(urljoin(base_url, href))
            if not u:
                continue
            # keep to SRP-like paths (cheap heuristic)
            if re.search(r"/search\?|/state-|/[a-z\-]+/[a-z0-9\-]+", u):
                if u not in nexts:
                    nexts.add(u)
                    added += 1
    except Exception:
        pass

    return recs, list(nexts)

# --------------------------- FETCHING -----------------------------------

class Fetcher:
    def __init__(self):
        # Global concurrency gate
        self.sem = asyncio.Semaphore(CONCURRENCY)
        # Per-host smoothing to reduce 503s
        self.host_sems = defaultdict(lambda: asyncio.Semaphore(PER_HOST_CAP))

        self.session = None
        self.driver = None
        self.local_seen = set()   # quick in-memory URL de-dup in run
        self.http_403s = 0
        self.http_404s = 0
        self.http_429s = 0
        self.http_503s = 0
        self._cooldown_until = 0.0 # global cooldown
        self.pages_ok_meter = Meter()

    async def __aenter__(self):
        # A tolerant cookie jar helps if the site sets slightly non-RFC cookies
        jar = aiohttp.CookieJar(unsafe=True)

        self.session = aiohttp.ClientSession(
            cookie_jar=jar,
            connector=aiohttp.TCPConnector(
                limit=0,  # you rate-limit via semaphores
                limit_per_host=0,
                ttl_dns_cache=300,
                enable_cleanup_closed=True,
                keepalive_timeout=30,  # keep conns warm a bit longer
                # ssl: leave default (proper TLS)
            ),
            timeout=aiohttp.ClientTimeout(
                total=None,
                sock_connect=CONNECT_TIMEOUT,
                sock_read=READ_TIMEOUT
            ),
            # Default headers; you still override per request for UA rotation
            headers=HEADERS | {
                "User-Agent": pick_ua(),
                "Accept-Encoding": "gzip, deflate, br",  # let aiohttp decompress
            },
            trust_env=False,  # avoid accidental system proxy/env
            auto_decompress=True
        )

        # --- tiny concurrent warm-up to get cookies/CDN edges comfy ---
        async def _poke(u):
            try:
                async with self.session.get(u, allow_redirects=True) as r:
                    # read a small chunk to complete handshake; no need to keep body
                    await r.content.readany()
            except Exception:
                pass

        warm_urls = [
            "https://www.yellowpages.com/",
            "https://www.yellowpages.com/state-ca",
            "https://www.yellowpages.com/search?search_terms=pizza&geo_location_terms=Los%20Angeles%2C%20CA",
        ]
        await asyncio.gather(*(_poke(u) for u in warm_urls), return_exceptions=True)
        # ---------------------------------------------------------------

        if USE_SELENIUM_FALLBACK:
            self._init_selenium()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass

    def _init_selenium(self):
        try:
            opts = Options()
            opts.add_argument("--headless")
            opts.add_argument("--disable-gpu")
            opts.add_argument("--no-sandbox")
            opts.set_preference("permissions.default.image", 2)  # faster more Speed
            self.driver = webdriver.Firefox(options=opts)
            self.driver.set_page_load_timeout(SELENIUM_PAGELOAD_TIMEOUT)
        except Exception as e:
            print(f"\n[SELENIUM] init failed: {e}", file=sys.stderr)
            self.driver = None

    async def get(self, url: str, referer: str | None = "https://www.yellowpages.com/") -> str | None:
        host = urlparse(url).netloc
        async with self.sem, self.host_sems[host]:
            # global cooldown if we recently tripped protection
            now = time.time()
            if now < self._cooldown_until:
                await asyncio.sleep(self._cooldown_until - now)

            backoff = 0.2  # was 0.4
            consecutive_soft_blocks = 0  # 403/429/503 or HTML "Access Denied"
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    # rotate UA and referer each attempt
                    req_headers = {
                        "User-Agent": pick_ua(),
                        "Referer": referer or "https://www.yellowpages.com/",
                        "Accept": HEADERS["Accept"],
                        "Accept-Language": HEADERS["Accept-Language"],
                        "Cache-Control": "no-cache",
                        "Pragma": "no-cache",
                        "Connection": "keep-alive",
                    }

                    async with self.session.get(url, allow_redirects=True, headers=req_headers) as resp:
                        ct = (resp.headers.get("Content-Type") or "").lower()
                        retry_after_hdr = resp.headers.get("Retry-After")

                        # ---- hard HTTP classifications ----
                        if resp.status == 404:
                            self.http_404s += 1
                            return None

                        if resp.status == 403:
                            self.http_403s += 1
                            consecutive_soft_blocks += 1
                            # adaptive global cooldown (shorter than 429/503)
                            self._cooldown_until = time.time() + random.uniform(6, 14) * min(3, consecutive_soft_blocks)
                            # brief per-attempt pause then retry
                            await asyncio.sleep(random.uniform(2, 5))
                            if attempt == MAX_RETRIES:
                                return None
                            continue

                        if resp.status == 429:
                            # honor Retry-After if provided
                            self.http_429s += 1
                            consecutive_soft_blocks += 1
                            sleep_s = 0.0
                            if retry_after_hdr:
                                try:
                                    # header can be seconds or an HTTP-date
                                    sleep_s = float(retry_after_hdr)
                                except ValueError:
                                    # if HTTP-date, just pick a sane default
                                    sleep_s = random.uniform(10, 20)
                            else:
                                sleep_s = random.uniform(6, 12)

                            self._cooldown_until = time.time() + sleep_s * min(3, consecutive_soft_blocks)
                            await asyncio.sleep(sleep_s)
                            if attempt == MAX_RETRIES:
                                return None
                            continue

                        if resp.status == 503:
                            self.http_503s += 1
                            consecutive_soft_blocks += 1
                            sleep_s = random.uniform(4, 10)
                            if retry_after_hdr:
                                try:
                                    sleep_s = max(sleep_s, float(retry_after_hdr))
                                except ValueError:
                                    pass
                            self._cooldown_until = time.time() + sleep_s * min(3, consecutive_soft_blocks)
                            await asyncio.sleep(sleep_s)
                            if attempt == MAX_RETRIES:
                                return None
                            continue

                        if resp.status >= 400:
                            # treat other 4xx/5xx as terminal for this URL
                            return None

                        # ---- content checks ----
                        if "text/html" not in ct and "application/xhtml+xml" not in ct:
                            return None

                        text = await resp.text(errors="ignore")
                        if len(text) < 2000:
                            raise ValueError("Short/empty HTML")

                        # detect common block pages (CloudFront/Y P verbiage)
                        blk = False
                        low = text.lower()
                        if ("access denied" in low and "cloudfront" in low) or \
                                ("request blocked" in low) or \
                                ("the owner of this website has banned your ip address" in low):
                            blk = True

                        if blk:
                            # count it as a soft block and retry
                            self.http_403s += 1
                            consecutive_soft_blocks += 1
                            self._cooldown_until = time.time() + random.uniform(8, 20) * min(3, consecutive_soft_blocks)
                            await asyncio.sleep(random.uniform(3, 7))
                            if attempt == MAX_RETRIES:
                                return None
                            continue

                        # success → tiny jitter to smooth burstiness per worker
                        await asyncio.sleep(random.uniform(0.05, 0.25))
                        self.pages_ok_meter.add(1) # counts successful fetches here
                        return text

                except asyncio.CancelledError:
                    raise
                except Exception:
                    if attempt >= MAX_RETRIES:
                        # optional Selenium one-shot
                        if USE_SELENIUM_FALLBACK and self.driver is not None:
                            try:
                                html = await asyncio.get_running_loop().run_in_executor(None,
                                                                                        self._render_with_selenium, url)
                                if html and len(html) > 2000:
                                    return html
                            except Exception:
                                pass
                        return None

                    # exponential backoff with jitter
                    await asyncio.sleep(backoff + random.uniform(0.1, 0.6))
                    backoff = min(backoff * 2, 6.0)


            return None  # should not hit

    def _render_with_selenium(self, url: str) -> str | None:
        if not self.driver:
            return None
        try:
            self.driver.get(url)
            # crude wait: listings container or footer present
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.result, footer, #content"))
            )
            return self.driver.page_source
        except Exception:
            return None

# --------------------------- FRONTIER -----------------------------------

class Crawler:
    def __init__(self, db: DB):
        # Frontier queue: unbounded (0) or very large per config
        self.db = db
        self.q = asyncio.Queue(maxsize=FRONTIER_MAXSIZE)
        self.stop = asyncio.Event()
        self.in_flight = 0
        self.fetcher = None
        self.dropped = 0  # for stats
        self._force_cancel = False  # second Ctrl-C forces cancel

    async def enqueue(self, url: str):
        # no expansion during drain
        if self.stop.is_set():
            return
        if not same_domain(url):
            return
        # in-memory fast dedupe
        if url in self.fetcher.local_seen:
            return
        # mark seen only if we're still accepting URLs
        self.fetcher.local_seen.add(url)
        try:
            self.q.put_nowait(url)
        except asyncio.QueueFull:
            self.dropped += 1

    async def worker(self, wid: int):
        while not (self.stop.is_set() and self.q.empty()):
            try:
                url = await asyncio.wait_for(self.q.get(), timeout=0.25)
            except asyncio.TimeoutError:
                continue

            self.in_flight += 1
            try:
                html = await self.fetcher.get(url)
                if not html:
                    continue  # finally will decrement in_flight

                discover = not self.stop.is_set()
                recs, nexts = extract_records_and_links(html, url, discover=discover)

                # enqueue DB writes for SPEED
                for r in recs:
                    await asyncio.shield(self.db.q.put(r))

                # schedule discovered URLs only if not draining
                if discover:
                    for nurl in nexts:
                        await self.enqueue(nurl)

            except Exception as e:
                print(f"\n[W{wid}] error on {url}: {e}", file=sys.stderr)
            finally:
                self.in_flight -= 1

    async def run(self, seeds: list[str]):
        async with Fetcher() as fetcher:
            self.fetcher = fetcher

            for s in seeds:
                await self.enqueue(s)

            writers = [asyncio.create_task(self.db.writer())]
            workers = [asyncio.create_task(self.worker(i)) for i in range(CONCURRENCY)]

            # signal hooks
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self._shutdown(s)))
                except NotImplementedError:
                    pass

            status_panel_init(4)  # was 3
            last_sizes = time.time()
            crawl_t0 = time.time()
            self.db.insert_meter.t0 = crawl_t0
            self.fetcher.pages_ok_meter.t0 = crawl_t0

            # ...
            while True:
                await asyncio.sleep(1.0)
                qsize = self.q.qsize()
                inflight = self.in_flight

                db_mb = file_mb(self.db.path)
                wal_mb = file_mb(self.db.path + "-wal")
                shm_mb = file_mb(self.db.path + "-shm")

                fetch_rate = self.fetcher.pages_ok_meter.rate()
                ins_rate_s = self.db.insert_meter.rate()
                cur_per_min = ins_rate_s * 60.0
                elapsed = max(1e-6, time.time() - crawl_t0)
                avg_per_min = self.db.inserted_total / (elapsed / 60.0)

                errs = (
                    f"404={C.yellow}{self.fetcher.http_404s}{C.reset} "
                    f"403={C.red}{self.fetcher.http_403s}{C.reset} "
                    f"429={C.magenta}{self.fetcher.http_429s}{C.reset} "
                    f"503={C.magenta}{self.fetcher.http_503s}{C.reset} "
                    f"ok={C.cyan}{self.fetcher.pages_ok_meter.val}{C.reset}"
                    f"({C.cyan}{fetch_rate:,.0f}/s{C.reset})"
                )
                status_set(0, errs)

                # Split metrics across two lines so each can be updated independently
                core1 = (
                    f"{C.cyan}[STAT]{C.reset} "
                    f"q={C.cyan}{qsize}{C.reset} "
                    f"inflight={C.yellow}{inflight}{C.reset} "
                    f"dbq={C.blue}{self.db.q.qsize()}{C.reset} "
                    f"seen={C.blue}{len(self.fetcher.local_seen)}{C.reset} "
                    f"dropped={C.dim}{self.dropped}{C.reset}"
                )
                core2 = (
                    f"{C.cyan}[INS]{C.reset} "
                    f"ins={C.green}{self.db.inserted_total}{C.reset} "
                    f"avg={C.green}{avg_per_min:,.0f}/min{C.reset} "
                    f"cur={C.green}{cur_per_min:,.0f}/min{C.reset}"
                )
                status_set(1, core1)
                status_set(2, core2)

                now = time.time()
                if now - last_sizes > 1.5:
                    status_set(3, f"db={db_mb:.1f}MB wal={wal_mb:.1f}MB shm={shm_mb:.1f}MB")
                    last_sizes = now

                if (qsize == 0 and inflight == 0) or self._force_cancel:
                    break

            # -------- wind down (no data loss) --------
            if self._force_cancel:
                for w in workers:
                    w.cancel()
            else:
                self.stop.set()
                while not (self.q.empty() and self.in_flight == 0):
                    await asyncio.sleep(0.1)

            # stop DB writer AFTER draining its queue
            self.db._stop.set()
            await asyncio.gather(*writers, return_exceptions=True)

            if not self._force_cancel:
                await asyncio.gather(*workers, return_exceptions=True)

            sys.stdout.write("\n")

    async def _shutdown(self, sig):
        if not self.stop.is_set():
            print(f"\n[SHUTDOWN] {sig.name}: entering drain mode…", file=sys.stderr)
            self.stop.set()  # stop expansion; let workers finish
        else:
            print(f"\n[SHUTDOWN] {sig.name}: forcing cancellation…", file=sys.stderr)
            self._force_cancel = True

# ------------------------ SEED GENERATION -------------------------------

def build_city_slugs():
    # Created this array with ChatGPT looks pretty solid
    return [
        "new-york-ny","los-angeles-ca","chicago-il","houston-tx","phoenix-az","philadelphia-pa",
        "san-antonio-tx","san-diego-ca","dallas-tx","san-jose-ca","austin-tx","jacksonville-fl",
        "san-francisco-ca","columbus-oh","fort-worth-tx","charlotte-nc","indianapolis-in","seattle-wa",
        "denver-co","washington-dc","boston-ma","nashville-tn","detroit-mi","oklahoma-city-ok",
        "portland-or","las-vegas-nv","memphis-tn","louisville-ky","baltimore-md","milwaukee-wi",
        "albuquerque-nm","tucson-az","fresno-ca","sacramento-ca","mesa-az","kansas-city-mo",
        "atlanta-ga","miami-fl","omaha-ne","raleigh-nc","long-beach-ca","virginia-beach-va",
        "oakland-ca","minneapolis-mn","tulsa-ok","arlington-tx","new-orleans-la","wichita-ks",
        "tampa-fl","aurora-co","anaheim-ca","cleveland-oh","bakersfield-ca","riverside-ca",
        "stockton-ca","cincinnati-oh","st-louis-mo","pittsburgh-pa","greensboro-nc","henderson-nv",
        "anchorage-ak","plano-tx","lincoln-ne","orlando-fl","irvine-ca","newark-nj","toledo-oh",
        "durham-nc","chula-vista-ca","fort-wayne-in","jersey-city-nj","st-paul-mn","st-petersburg-fl",
        "laredo-tx","buffalo-ny","madison-wi","lubbock-tx","chandler-az","scottsdale-az","reno-nv",
        "glendale-az","gilbert-az","winston-salem-nc","north-las-vegas-nv","norfolk-va","chesapeake-va",
        "garland-tx","irving-tx","hialeah-fl","fremont-ca","boise-id","richmond-va","baton-rouge-la",
        "spokane-wa","des-moines-ia","tacoma-wa","san-bernardino-ca","modesto-ca"
    ]

def build_category_slugs():
    # From YP’s own browse blocks + common high-volume categories
    return [
        # Auto services
        "automobile-body-repairing-painting", "windshield-repair", "automobile-parts-supplies",
        "auto-repair-service", "automobile-detailing", "auto-oil-lube", "automotive-roadside-service",
        "tire-dealers", "towing", "window-tinting",
        # Beauty
        "barbers", "beauty-salons", "beauty-supplies-equipment", "day-spas", "skin-care", "hair-removal",
        "hair-supplies-accessories", "hair-stylists", "massage-therapists", "nail-salons",
        # Home services
        "air-conditioning-service-repair", "major-appliance-refinishing-repair", "carpet-rug-cleaners",
        "electricians", "garage-doors-openers", "movers", "pest-control-services", "plumbers", "self-storage",
        # Insurance
        "boat-marine-insurance", "business-commercial-insurance", "auto-insurance", "dental-insurance",
        "workers-compensation-disability-insurance", "flood-insurance", "homeowners-insurance", "insurance",
        "liability-malpractice-insurance", "life-insurance",
        # Legal services
        "attorneys", "bail-bonds", "bankruptcy-law-attorneys", "automobile-accident-attorneys",
        "divorce-attorneys", "family-law-attorneys", "lie-detection-service", "private-investigators-detectives",
        "process-servers", "stenographers-public", "tax-attorneys",
        # Medical services
        "dentists", "physicians-surgeons-dermatology", "physicians-surgeons",
        "physicians-surgeons-endocrinology-diabetes-metabolism", "physicians-surgeons-gynecology", "hospitals",
        "physicians-surgeons-neurology", "physicians-surgeons-ophthalmology", "optometrists", "physical-therapists",
        "physicians-surgeons-podiatrists",
        # Pet services
        "animal-shelters", "dog-training", "dog-day-care", "veterinarian-emergency-services", "kennels",
        "mobile-pet-grooming", "pet-boarding-kennels", "pet-cemeteries-crematories", "pet-grooming",
        "veterinary-clinics-hospitals",
        # Restaurants
        "breakfast-brunch-lunch-restaurants", "chinese-restaurants", "cuban-restaurants", "italian-restaurants",
        "korean-restaurants", "mexican-restaurants", "seafood-restaurants", "sushi-bars", "thai-restaurants",
        "vegetarian-restaurants", "pizza", "fast-food-restaurants", "steak-houses", "family-style-restaurants",
        "barbecue-restaurants", "take-out-restaurants",
        # High-yield catch-alls often dense:
        "real-estate-agents", "apartment-finder-rental-service", "landscaping-lawn-services",
        "roofing-contractors", "handyman-services", "general-contractors", "heating-contractors-specialties",
        "cleaning-contractors", "security-control-systems-monitoring"
    ]

def build_default_seeds():
    seeds = set()

    # Classic search example
    seeds.add("https://www.yellowpages.com/search?search_terms=burbone&geo_location_terms=Los%20Angeles%2C%20CA")

    # City directory hubs (auto-discover more)
    for city in ("los-angeles-ca", "new-york-ny", "chicago-il", "houston-tx",
                 "phoenix-az", "philadelphia-pa", "san-diego-ca", "dallas-tx",
                 "san-jose-ca", "austin-tx"):
        seeds.add(f"https://www.yellowpages.com/{city}")

    # City × Category fanout (large coverage)
    for city in build_city_slugs():
        for cat in build_category_slugs():
            seeds.add(f"https://www.yellowpages.com/{city}/{cat}")

    # State hubs (optional)
    for st in [
        "ca","tx","fl","ny","il","pa","oh","ga","nc","mi","az","wa","co","tn",
        "in","ma","mo","wi","va","md","mn","al","la","ky","or","ok","ct","sc",
        "ut","ia","nv","ar","ms","ks","nm","ne","wv","id","hi","nh","me","ri",
        "mt","de","sd","nd","ak","vt","wy"
    ]:
        seeds.add(f"https://www.yellowpages.com/state-{st}")

    # Convert to list + shuffle order for distribution
    seeds_list = list(seeds)
    random.shuffle(seeds_list)

    return seeds_list

# ------------------------------ MAIN -----------------------------------

async def main():
    banner("INIT", "opening database…", C.cyan)
    # print(f"[INIT] DB path: {DB_PATH}")
    db = DB(DB_PATH)
    await db.init()

    seeds = build_default_seeds()
    banner("SEEDS", f"{len(seeds)} starting URLs", C.yellow)

    # sanity ping
    ok = await sanity_check(seeds[0])
    if not ok:
        banner("EXIT", "First seed didn’t fetch. Likely a hard 403/CloudFront block or network issue.", C.red)
        await db.close()
        return

    crawler = Crawler(db)
    t0 = time.time()
    await crawler.run(seeds)
    dt = time.time() - t0
    banner("DONE", f"elapsed: {dt:.1f}s  total_inserted={db.inserted_total}", C.green)
    await db.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
