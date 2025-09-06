# YP-Scraper

Ultra-fast Yellow Pages crawler powered by `aiohttp` + `aiosqlite`. It fans out from city/state/category hubs, extracts business data from SRP cards, JSON-LD, and inline objects, and streams inserts to SQLite with a single async writer.

> **Note:** Scraping can violate terms of service and trigger rate-limits/blocks. Use responsibly and only where you have permission.

---

## Features

- High-concurrency HTML fetching (async `aiohttp`)
- Multi-strategy extraction (SRP cards, JSON-LD, inline `YPU`)
- Auto pagination & internal link discovery (capped)
- SQLite WAL pipeline with batched `INSERT OR IGNORE`
- Live status panel (errors, OK/s, queue depth, insert rates, DB/WAL sizes)
- Graceful shutdown: **Ctrl+C** to drain; **Ctrl+C** again to force cancel

---

## Install

~~~bash
git clone https://github.com/havokzero/YP-Scraper.git
cd YP-Scraper
python3 -m venv .venv
source .venv/bin/activate               # Windows: .venv\Scripts\activate
pip install -U pip wheel
pip install aiohttp aiosqlite selectolax beautifulsoup4 colorama uvloop
# Optional (only if you plan to enable Selenium fallback)
pip install selenium
~~~

Playwright is **not** required; this project uses `aiohttp`. Selenium is optional and off by default.

---

## Run

~~~bash
python3 main.py
~~~

- Database: `./scrapes/yellowpages.db`
- Defaults are aggressive (see tuning below).
- Stop with **Ctrl+C** once (drain & flush). Press again to force cancel.

**Sample output**
~~~text
[INIT] opening database…
[SEEDS] 9468 starting URLs
[SANITY] seed ok: recs=60 next_links=216
404=2 403=244 429=0 503=0 ok=628(11/s)
[STAT] q=52912 inflight=900 dbq=0 seen=54442 dropped=0
[INS] ins=13 avg=14/min cur=14/min
db=1695.1MB wal=0.0MB shm=0.0MB
[DONE] elapsed: 58.6s  total_inserted=13
~~~

---

## Configuration (edit `main.py`)

| Key                 | Default | What it does |
|---------------------|---------|--------------|
| `CONCURRENCY`       | `900`   | Global concurrent fetches. Start lower. |
| `PER_HOST_CAP`      | `128`   | Smooths bursts per host. |
| `BATCH_SIZE`        | `5000`  | Rows per `executemany` batch. |
| `FLUSH_SECS`        | `0.5`   | Max seconds between DB flushes. |
| `USE_WAL`           | `True`  | SQLite WAL journaling. |
| `CHECKPOINT_SECS`   | `8`     | Periodic WAL checkpoint. |
| `WAL_MAX_MB`        | `512`   | WAL size trigger for checkpoint. |
| `USE_SELENIUM_FALLBACK` | `False` | Optional last-ditch render on blocks. |

Seeds are built in `build_default_seeds()` (state hubs, city hubs, city×category fanout). Trim/replace for your scope.

---

## Tuning & Tips

- **Start sane:** try `CONCURRENCY=128`–`256`, then scale up. Very high values can overload routers (NAT/conntrack), trip ISP QoS, or get you blocked.
- **Faster inserts:** larger `BATCH_SIZE` (e.g., `50_000`) + higher `FLUSH_SECS` (e.g., `2.0`) reduce commit overhead. For extreme ingest, temporarily set `PRAGMA synchronous=OFF` (trade safety for speed), then restore.
- **Duplicate control:** YP surfaces the same biz many times. Consider a `dedupe_key` (normalized `name|phone|street|city|state|zip` → hash) and make it unique alongside `ypid`.
- **Status panel scrolling in IDEs:** set `STATUS_SINGLELINE=1`:
  ~~~bash
  STATUS_SINGLELINE=1 python3 main.py
  ~~~

---

## Troubleshooting

- **403/429 rising:** lower concurrency, lengthen per-attempt backoff, stagger user-agents, and reduce discovery fanout.
- **Huge `-wal` file after kill:** the script checkpoints on clean shutdown. If needed:
  ~~~sql
  PRAGMA wal_checkpoint(TRUNCATE);
  PRAGMA optimize;
  ~~~

---

## Legal & Ethics

Respect the site’s terms/robots, do not degrade service, and comply with applicable laws and data licenses.

---


MIT — see `LICENSE`.
