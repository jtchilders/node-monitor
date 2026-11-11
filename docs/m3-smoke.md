# M3 History — Smoke check

M3 adds history persistence (`process_samples` daily 30d, `command_lines` 730d), table-driven tags, `/analytics` + `/commands` APIs and pages, and CLI `history`/`commands search`/`reclassify`.

Vendor pins: Vue 3.4.38 + Chart.js 4.4.9 under `node_monitor/web/static/vendor/` (no CDN).

## Local verification (no PG)

```
env -u LD_PRELOAD ../venv/bin/python -m pytest -q      # 653 passed 213 skipped
env -u LD_PRELOAD ../venv/bin/python -m ruff check .   # All checks passed
env -u LD_PRELOAD ../venv/bin/python -m ruff format --check .  # 117 files already formatted
```

`NODE_MONITOR_TEST_ADMIN_URL` not set locally, so all `migrated_db` tests were skipped. With a real Postgres they also pass (see M2 CI: `postgres:18`).

## Real-server smoke (polaris-login-04, deferred)

Run once after merging to `main` and deploying the collector build that includes M3:

```bash
# 1. Migrate (owner role, as pbs_monitor or node_monitor_owner)
node-monitor database migrate
node-monitor database status          # current version 3, partitions for process_samples + command_lines

# 2. Wait ~10 min for samples
#    - every minute: notable (cpu≥0.1s / rss≥1 GiB / ai_agent / tagged)
#    - at minute 0: hourly census (all user processes with hourly=true)

# 3. Check counts
psql "$NODE_MONITOR_DB_URL" -c "select count(*) from node_monitor.process_samples"
psql "$NODE_MONITOR_DB_URL" -c "select count(*) from node_monitor.command_lines"
psql "$NODE_MONITOR_DB_URL" -c "select unnest(tags), count(*) from node_monitor.process_samples where tags is not null group by 1 order by 2 desc limit 20"

# 4. Web (loopback, tunnel from laptop: ssh -L 9998:localhost:9998 polaris-login-04)
node-monitor web --port 9998 &
curl -s http://127.0.0.1:9998/api/analytics/category-mix | python3 -m json.tool
curl -s "http://127.0.0.1:9998/api/analytics/dormancy?bucket=1d" | python3 -m json.tool
curl -s "http://127.0.0.1:9998/api/analytics/leaderboard?metric=cpu" | python3 -m json.tool
curl -s http://127.0.0.1:9998/api/analytics/coverage | python3 -m json.tool
curl -s "http://127.0.0.1:9998/api/commands/search?q=claude" | python3 -m json.tool
curl -s "http://127.0.0.1:9998/api/commands/search?regex=claude.*skip" | python3 -m json.tool
# search escapes: %/_ must not wildcard
curl -s "http://127.0.0.1:9998/api/commands/search?q=%25" | python3 -m json.tool
curl -s "http://127.0.0.1:9998/api/analytics/leaderboard?metric=bogus"   # 400

# Pages (via tunnel): open http://localhost:9998/analytics and /commands
# - analytics: range picker 1h–2y drives bucket, 4 charts + 3 leaderboards + coverage bar, poll 30s
# - commands: substring/regex toggle, user/activity/tag filters, pager, highlight as text only

# 5. CLI (reader role via web.database_url, writer via database.url for reclassify)
node-monitor history --user <you> --format json | head
node-monitor commands search "claude" --limit 5
node-monitor commands search "claude.*skip" --regex --limit 5
node-monitor reclassify --dry-run
# stop web after: pkill -f "node-monitor web"
```

## Expected signals

- `process_samples` grows each minute (notable) and spikes at :00 (hourly).
- `command_lines` has one row per `(system, username, cmdline_hash)` with `nodes` and `sample_count`.
- Tags like `agent.skip_permissions` appear for `claude --dangerously-skip-permissions`.
- `/analytics` ≤730 points per series (auto-bucket), dormancy uses `idle>1h OR absent`.
- `/commands` ILIKE escapes `%`/`_`, regex 400 on bad pattern, unknowns → 200 empty.

## Retention

`process_samples` daily partitions drop when upper bound ≤ now-30d; `command_lines` rows delete when `last_seen < now-730d` (strict <, boundary kept).

## Follow-ups from M3 review

- `6h` bucket uses `make_interval` (PG 12+); target is PG 18 — OK.
- `/analytics` agent-adoption forces daily bucket regardless of explicit bucket param — intentional.
- `reclassify` updates per row; large history is batched (1k/cursor) but still N UPDATEs — acceptable for 30d.

## Branch

`feat/m3-history` from `main@b25bf02` — 8 commits, 653 passed locally. See `docs/superpowers/plans/2026-11-09-m3-history.md`.
