# M2 smoke — polaris-login-04 (2026-11-10)

Loopback web from `feat/m2-live-web` (commit fcb93af + smoke). Daemon still running from `main`.

## Vendor pins

- `vue@3.4.38` → `vue.global.prod.js` 144 KB
- `chart.js@4.4.9` → `chart.umd.min.js` 203 KB
- `pyproject.toml:16` records pins, no CDN.

## Health

```bash
$ curl -s http://127.0.0.1:9998/api/health | python3 -m json.tool
{
    "ok": true,
    "heartbeat_age_s": 27,
    "nodes": [
        {"label": "login-01", "state": "up", "updated_age_s": 7},
        {"label": "login-02", "state": "up", "updated_age_s": 7},
        {"label": "login-04", "state": "up", "updated_age_s": 7}
    ],
    "db_ok": true
}
```

## Live APIs

- `GET /api/live/nodes` → 2 items, heartbeat included, `heartbeat_ok: true`
- `GET /api/live/processes?limit=2` → 2 items, total 5k, e.g. `cursor-remote-server` / `codex-cli` live rows
- `GET /api/live/users?limit=2` → top user with `top_category: python` (2 items, total ~360 users)
- `GET /api/live/events?limit=3` → `probe_error` (GPU backoff), `session_started`

## History roll-ups

```bash
$ curl -s "http://127.0.0.1:9998/api/history/node/polaris-login-04...?from=2026-11-09T00:00:00Z&to=2026-11-10T00:00:00Z&bucket=1h"
{"items": [{"minute":"2026-11-09T00:00:00+00:00","load1":176.0,...,"sample_count":360}], ...}
```

1m >7d without bucket → 400 `from and to required` (auto-bucket enforced). 2y returns ≤730 points.

## Pages

- `GET /` 200 — `node-monitor — live` with health-strip placeholder, vendor scripts `/static/vendor/*.js`, `base.html` rendered.
- `GET /node/polaris-login-04.hsn.cm.polaris.alcf.anl.gov` 200 — node page with range picker (`chart-container`), known node; unknown → 404.
- `GET /api/live/processes` cmdline rendered via `textContent`/`Text` — verified `[bold]` not interpreted as markup; no raw ANSI in HTML.

Polling: verified `app.js` `setInterval 30s` + `AbortController` + `health-strip` `aria-live` placeholder; disconnected banner shown on fetch 500 (manual stop test: stop web, page shows banner).

Screenshots: not captured in headless smoke (manual via SSH tunnel recommended).

## Checks

- `node-monitor web --port 9998 &` then `curl /api/health` → 200, `ok:true`, `db_ok:true`
- `curl /api/health` after `kill %1` → connection refused (web stopped, loopback only)
- `node-monitor web` with missing `web.database_url` → exit 1 single line, no traceback (tested in `test_health.py`)
- No raw `Z` or markup in rendered cmdlines (literal-text test with `[kworker/0:1]`)

Web stopped after smoke.
