# M3 — History and research — Design

**Status:** draft for review (builder check pending)
**Spec parent:** `docs/superpowers/specs/2026-10-28-node-monitor-v2-design.md` §13 M3
**Milestone:** M3 History and research — `process_samples` + hourly census, `command_lines`, tags, `/analytics`, `/commands`, `history`/`commands search`/`reclassify` CLI
**Daemon:** remains running from `main`; M3 is read-mostly over accumulated 1.6 GB → ~130 GB/year dataset

## Intent and the why

M2 answers “what is happening right now.” M3 answers “what has been happening, for whom, and with what commands.” The owner wants general diagnostic ability before specializing: given 8 days of live + 5m/1m history, be able to diagnose capacity and behavior questions (who leaves dormant agents, yolo-mode/tag usage, category mix drift, per-user command patterns) without ad-hoc SQL. Success is an operator opening `/analytics` or running `history`/`commands search` and getting an answer in seconds, not minutes of psql.

## Goals

- Persist **notable** processes each minute (CPU ≥ 0.1s or RSS ≥ 1 GiB or `ai_agent`/tagged) + **hourly census** (every user process at minute 0) into `process_samples` (daily partitions, 30d), and every distinct redacted cmdline into `command_lines` (hash-keyed, 730d from `last_seen`).
- Compute **tags** per process from activity + flags (e.g., `agent.skip_permissions` for `claude-code --dangerously-skip-permissions`) — table-driven, testable, applied in collector.
- Serve **/analytics**: users/processes per node (typical vs peak), category/activity mix over time, AI-agent adoption (users/day, tag share), dormancy trends, leaderboards (CPU-h, mem-h, dormant mem-h), collector coverage/gaps.
- Serve **/commands**: substring/regex search over `command_lines` with filters by user/activity/tag, first/last seen, nodes, count; used to iterate on classification rules.
- CLI: `history` (user or node 5m history, same roll-ups as web), `commands search PATTERN` (same filters), `reclassify --dry-run` (re-label `process_samples`+`command_lines` in a transaction and report delta counts).

## Non-goals (deferred)

- Alerts/notifications (M4), Aurora deployment (M4), per-process continuous tracing, non-redacted cmdlines, ML anomaly detection.

## Anti-goals

- A slow analytics page that scans 20M rows in Python — roll-ups must be SQL-side.
- A tag system that requires code change per new flag — it must be table-driven.
- Exposing raw non-redacted commands or other users’ full cmdlines beyond the redacted `command_lines` table.

## Constraints

- Same as M2 global constraints: `~/node-monitor/node-monitor`, branch `feat/m3-history`, `~/node-monitor/venv` editable, `env -u LD_PRELOAD`, 3-space ruff, Python ≥3.10, no `asyncio.wait_for`.
- Postgres 18, `process_samples` daily partitions, `command_lines` unpartitioned with `last_seen` retention.
- Tests via `migrated_db`/`pg_db` fixtures; API tests via `httpx`; no CDN; redacted cmdlines only.

## How (decided)

- **Collection:** `NodePipeline` emits `process_samples` rows (notable + `hourly=true` at minute 0) and `command_lines` sightings (in-memory dedup per 5m interval, upserted with `first_seen`/`last_seen`/`nodes`/`sample_count`). `Writer` maps them to `process_samples` (by day) and `command_lines` (by hash) — reuse M1c `BufferedWriter` + spool; retention via `DROP PARTITION` for samples and `DELETE WHERE last_seen < now() - 730d` for cmdlines.
- **Tags:** `pipeline/tags.py` — `TAGS = [{activity, flag_pattern, tag}]` applied after `classify.py`; stored as `text[]` on both tables.
- **Web:** extend `node_monitor/web` FastAPI app — `/api/analytics/*` (category mix, agent adoption, dormancy, leaderboards, coverage) and `/api/commands/search` (SQL `LIKE`/`~` over `command_lines` with trigram index `pg_trgm` if needed). Vue pages under `/analytics` and `/commands` reuse `dashboard.css`, Chart.js for trends, paged tables (50/page).
- **CLI:** `node_monitor/cli/history.py` and `cli/commands.py` reuse `database/queries.py` helpers; `reclassify` loads rules and runs in one transaction with `REPORT`.

## What's left to the builder

- Exact tag table entries (seed from v1 tuned rules + new yolo-mode variants).
- Index choices beyond PKs (e.g., `command_lines` GIN trigram vs `ILIKE`).
- Chart color scales and leaderboard window defaults.
- Commit granularity and test fixture shapes.

## Builder calls (pre-approved)

- Pinned vendor versions as in M2 (Vue 3.4.38, Chart.js 4.4.9).
- `reclassify` dry-run first; real write behind `--apply`.

---
*Builder check: dispatched; minor calls above are pre-approved, theirs pile will be asked once.*
