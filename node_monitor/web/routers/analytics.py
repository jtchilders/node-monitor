"""Analytics APIs over history tables."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg
from fastapi import APIRouter, HTTPException, Request
from psycopg.rows import dict_row

from node_monitor.database.connect import DatabaseUnavailable
from node_monitor.web.deps import web_reader

router = APIRouter(prefix="/api/analytics", tags=["analytics"])

# Allowed bucket values (date_trunc style + shorthand used in query params).
_BUCKET_MAP: dict[str, str] = {
   "1h": "hour",
   "6h": "hour",  # handled via custom trunc
   "1d": "day",
   "1w": "week",
   "hour": "hour",
   "day": "day",
   "week": "week",
}
# Buckets that are directly date_trunc values vs those needing custom logic.
_ALLOWED_BUCKETS = {"1h", "6h", "1d", "1w", "hour", "day", "week"}
_ALLOWED_METRICS = {"cpu", "mem", "dormant"}
# Candidate buckets in increasing granularity for auto-bucket.
_CANDIDATES = ["1h", "6h", "1d", "1w"]
_BUCKET_SECONDS: dict[str, float] = {
   "1h": 3600,
   "6h": 6 * 3600,
   "1d": 86400,
   "1w": 7 * 86400,
}


def _parse_ts(s: str) -> datetime:
   try:
      dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
      if dt.tzinfo is None:
         dt = dt.replace(tzinfo=timezone.utc)
      return dt.astimezone(timezone.utc)
   except Exception as exc:
      raise HTTPException(status_code=400, detail=f"invalid timestamp {s!r}") from exc


def _default_range(q) -> tuple[datetime, datetime]:
   from_s = q.get("from")
   to_s = q.get("to")
   now = datetime.now(timezone.utc)
   if from_s is None and to_s is None:
      return now - timedelta(hours=1), now
   if from_s is None or to_s is None:
      raise HTTPException(status_code=400, detail="both from and to required together")
   from_ts = _parse_ts(from_s)
   to_ts = _parse_ts(to_s)
   if from_ts >= to_ts:
      raise HTTPException(status_code=400, detail="from must be before to")
   return from_ts, to_ts


def _infer_bucket(from_ts: datetime, to_ts: datetime) -> str:
   span = (to_ts - from_ts).total_seconds()
   for cand in _CANDIDATES:
      if span / _BUCKET_SECONDS[cand] <= 730:
         return cand
   return "1w"


def _validate_bucket(bucket: str | None, from_ts: datetime, to_ts: datetime) -> str:
   if bucket is None:
      return _infer_bucket(from_ts, to_ts)
   if bucket not in _ALLOWED_BUCKETS:
      raise HTTPException(status_code=400, detail=f"unknown bucket {bucket!r}")
   # Cap to 730 points: if explicit bucket would exceed, keep as-is but caller may get 400
   # Spec says cap via auto-bucketing; explicit bucket is honoured.
   # However, if explicit bucket would produce >730 we auto-upgrade to keep the cap.
   span = (to_ts - from_ts).total_seconds()
   if span / _BUCKET_SECONDS[bucket] > 730:
      # auto-upgrade to a valid bucket
      return _infer_bucket(from_ts, to_ts)
   return bucket


def _trunc_expr(bucket: str) -> str:
   """SQL expression for bucket truncation."""
   # 6h needs custom: truncate to 6h aligned to epoch
   if bucket == "6h":
      return "date_trunc('hour', ts) - make_interval(hours => (EXTRACT(hour FROM ts)::int % 6))"
   trunc = _BUCKET_MAP[bucket]
   return f"date_trunc('{trunc}', ts)"


def _iso(v):
   if isinstance(v, datetime):
      return v.isoformat()
   return v


# ---------------------------------------------------------------------------
# GET /api/analytics/category-mix
# ---------------------------------------------------------------------------


@router.get("/category-mix")
def category_mix(request: Request):
   q = request.query_params
   bucket_s = q.get("bucket")
   cfg = request.app.state.config
   # Parse from/to with defaults
   if q.get("from") or q.get("to"):
      from_ts, to_ts = _default_range(q)
   else:
      now = datetime.now(timezone.utc)
      from_ts, to_ts = now - timedelta(hours=1), now
      # if only bucket given without from/to, use defaults
   # Re-parse if both present (default range logic above)
   if q.get("from") and q.get("to"):
      from_ts, to_ts = _default_range(q)
   elif not q.get("from") and not q.get("to"):
      now = datetime.now(timezone.utc)
      from_ts, to_ts = now - timedelta(hours=1), now

   bucket = _validate_bucket(bucket_s, from_ts, to_ts)
   trunc_sql = _trunc_expr(bucket)

   try:
      with web_reader(cfg) as conn:
         with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
               f"SELECT {trunc_sql} AS bucket, category, count(*)::bigint AS cnt"
               " FROM process_samples"
               " WHERE system=%s AND ts >= %s AND ts < %s"
               " GROUP BY 1, 2 ORDER BY 1, 2",
               (cfg.system, from_ts, to_ts),
            )
            rows = cur.fetchall()
   except (psycopg.Error, DatabaseUnavailable, OSError) as exc:
      raise HTTPException(status_code=503, detail="database unavailable") from exc

   # Reshape to per-bucket category counts
   buckets: dict[str, dict] = {}
   for r in rows:
      b = _iso(r["bucket"])
      cat = r["category"] or "other"
      buckets.setdefault(b, {})[cat] = int(r["cnt"])

   items = [{"bucket": k, "categories": v} for k, v in sorted(buckets.items())]
   return {"items": items, "total": len(items), "bucket": bucket}


# ---------------------------------------------------------------------------
# GET /api/analytics/agent-adoption
# ---------------------------------------------------------------------------


@router.get("/agent-adoption")
def agent_adoption(request: Request):
   q = request.query_params
   cfg = request.app.state.config
   if q.get("from") or q.get("to"):
      from_ts, to_ts = _default_range(q)
   else:
      now = datetime.now(timezone.utc)
      from_ts, to_ts = now - timedelta(hours=1), now

   # Auto-bucket to day for adoption (users/day)
   bucket = _validate_bucket(None, from_ts, to_ts)
   # Force daily for adoption
   if (to_ts - from_ts).total_seconds() > 730 * 86400:
      bucket = "1w"
   else:
      bucket = "1d" if (to_ts - from_ts).total_seconds() <= 730 * 86400 else "1w"

   trunc_sql = _trunc_expr(bucket)

   try:
      with web_reader(cfg) as conn:
         with conn.cursor(row_factory=dict_row) as cur:
            # users with ai_agent per bucket
            cur.execute(
               f"SELECT {trunc_sql} AS bucket,"
               " count(DISTINCT username)::bigint AS ai_users"
               " FROM process_samples"
               " WHERE system=%s AND ts >= %s AND ts < %s AND category='ai_agent'"
               " GROUP BY 1 ORDER BY 1",
               (cfg.system, from_ts, to_ts),
            )
            ai_rows = cur.fetchall()
            # total distinct users per bucket
            cur.execute(
               f"SELECT {trunc_sql} AS bucket,"
               " count(DISTINCT username)::bigint AS total_users"
               " FROM process_samples"
               " WHERE system=%s AND ts >= %s AND ts < %s"
               " GROUP BY 1 ORDER BY 1",
               (cfg.system, from_ts, to_ts),
            )
            total_rows = cur.fetchall()
            # tag share overall
            cur.execute(
               "SELECT unnest(tags) AS tag, count(*)::bigint AS cnt"
               " FROM process_samples"
               " WHERE system=%s AND ts >= %s AND ts < %s"
               " AND tags IS NOT NULL AND array_length(tags,1) > 0"
               " GROUP BY 1 ORDER BY cnt DESC",
               (cfg.system, from_ts, to_ts),
            )
            tag_rows = cur.fetchall()
   except (psycopg.Error, DatabaseUnavailable, OSError) as exc:
      raise HTTPException(status_code=503, detail="database unavailable") from exc

   ai_map = {_iso(r["bucket"]): int(r["ai_users"]) for r in ai_rows}
   total_map = {_iso(r["bucket"]): int(r["total_users"]) for r in total_rows}
   all_buckets = sorted(set(ai_map) | set(total_map))
   items = []
   for b in all_buckets:
      items.append(
         {
            "bucket": b,
            "ai_users": ai_map.get(b, 0),
            "total_users": total_map.get(b, 0),
         }
      )
   tag_share = [{"tag": r["tag"], "count": int(r["cnt"])} for r in tag_rows]
   return {"items": items, "total": len(items), "bucket": bucket, "tag_share": tag_share}


# ---------------------------------------------------------------------------
# GET /api/analytics/dormancy
# ---------------------------------------------------------------------------


@router.get("/dormancy")
def dormancy(request: Request):
   q = request.query_params
   cfg = request.app.state.config
   if q.get("from") or q.get("to"):
      from_ts, to_ts = _default_range(q)
   else:
      now = datetime.now(timezone.utc)
      from_ts, to_ts = now - timedelta(hours=1), now

   bucket = _validate_bucket(q.get("bucket"), from_ts, to_ts)
   trunc_sql = _trunc_expr(bucket)

   try:
      with web_reader(cfg) as conn:
         with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
               f"SELECT {trunc_sql} AS bucket, category,"
               " count(*)::bigint AS cnt, coalesce(sum(rss_kb),0)::bigint AS rss_kb"
               " FROM process_samples"
               " WHERE system=%s AND ts >= %s AND ts < %s"
               " AND ("
               "   (cpu_state='idle' AND coalesce(idle_for_sec,0) > 3600)"
               "   OR owner_presence='absent'"
               " )"
               " GROUP BY 1, 2 ORDER BY 1, 2",
               (cfg.system, from_ts, to_ts),
            )
            rows = cur.fetchall()
   except (psycopg.Error, DatabaseUnavailable, OSError) as exc:
      raise HTTPException(status_code=503, detail="database unavailable") from exc

   buckets: dict[str, dict] = {}
   for r in rows:
      b = _iso(r["bucket"])
      cat = r["category"] or "other"
      entry = buckets.setdefault(b, {})
      entry[cat] = {"count": int(r["cnt"]), "rss_kb": int(r["rss_kb"])}

   items = [{"bucket": k, "categories": v} for k, v in sorted(buckets.items())]
   return {"items": items, "total": len(items), "bucket": bucket}


# ---------------------------------------------------------------------------
# GET /api/analytics/leaderboard
# ---------------------------------------------------------------------------


@router.get("/leaderboard")
def leaderboard(request: Request):
   q = request.query_params
   cfg = request.app.state.config
   metric = q.get("metric", "cpu")
   if metric not in _ALLOWED_METRICS:
      raise HTTPException(
         status_code=400,
         detail=f"unknown metric {metric!r}; expected one of {sorted(_ALLOWED_METRICS)}",
      )
   try:
      limit = int(q.get("limit", "20"))
   except ValueError as exc:
      raise HTTPException(status_code=400, detail="invalid limit") from exc
   if limit < 1 or limit > 100:
      raise HTTPException(status_code=400, detail="limit must be 1..100")

   if q.get("from") or q.get("to"):
      from_ts, to_ts = _default_range(q)
   else:
      now = datetime.now(timezone.utc)
      from_ts, to_ts = now - timedelta(hours=1), now

   # Metric expressions
   if metric == "cpu":
      select_expr = "coalesce(sum(cpu_seconds),0) AS value"
      order_expr = "value DESC"
   elif metric == "mem":
      select_expr = "coalesce(sum(rss_kb),0)::bigint AS value"
      order_expr = "value DESC"
   else:  # dormant
      select_expr = "coalesce(sum(rss_kb),0)::bigint AS value"
      order_expr = "value DESC"

   dormant_filter = ""
   if metric == "dormant":
      dormant_filter = (
         " AND ((cpu_state='idle' AND coalesce(idle_for_sec,0) > 3600) OR owner_presence='absent')"
      )

   try:
      with web_reader(cfg) as conn:
         with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
               f"SELECT username, {select_expr}"
               " FROM process_samples"
               " WHERE system=%s AND ts >= %s AND ts < %s"
               f"{dormant_filter}"
               " GROUP BY username ORDER BY " + order_expr + " LIMIT %s",
               (cfg.system, from_ts, to_ts, limit),
            )
            rows = cur.fetchall()
   except (psycopg.Error, DatabaseUnavailable, OSError) as exc:
      raise HTTPException(status_code=503, detail="database unavailable") from exc

   items = [{"username": r["username"], "value": float(r["value"])} for r in rows]
   return {"items": items, "total": len(items), "metric": metric}


# ---------------------------------------------------------------------------
# GET /api/analytics/coverage
# ---------------------------------------------------------------------------


@router.get("/coverage")
def coverage(request: Request):
   q = request.query_params
   cfg = request.app.state.config
   if q.get("from") or q.get("to"):
      from_ts, to_ts = _default_range(q)
   else:
      now = datetime.now(timezone.utc)
      from_ts, to_ts = now - timedelta(hours=1), now

   try:
      with web_reader(cfg) as conn:
         with conn.cursor(row_factory=dict_row) as cur:
            # collection_events gaps
            cur.execute(
               "SELECT ts, node, event_type, detail"
               " FROM collection_events"
               " WHERE system=%s AND ts >= %s AND ts < %s"
               " AND event_type IN ('gap','session_lost','node_rebooted')"
               " ORDER BY ts",
               (cfg.system, from_ts, to_ts),
            )
            gap_events = cur.fetchall()
            # node_metrics uptime: minutes with expected>0 vs missing
            cur.execute(
               "SELECT minute, node, sample_count, expected_count"
               " FROM node_metrics_1m"
               " WHERE system=%s AND minute >= %s AND minute < %s"
               " ORDER BY minute",
               (cfg.system, from_ts, to_ts),
            )
            metric_rows = cur.fetchall()
            # count distinct minutes covered
            cur.execute(
               "SELECT count(DISTINCT minute)::bigint AS covered_minutes"
               " FROM node_metrics_1m"
               " WHERE system=%s AND minute >= %s AND minute < %s",
               (cfg.system, from_ts, to_ts),
            )
            covered_row = cur.fetchone()
   except (psycopg.Error, DatabaseUnavailable, OSError) as exc:
      raise HTTPException(status_code=503, detail="database unavailable") from exc

   total_minutes = int((to_ts - from_ts).total_seconds() // 60)
   covered_minutes = (
      int(covered_row["covered_minutes"]) if covered_row and covered_row["covered_minutes"] else 0
   )
   # gaps from events
   gaps = []
   for r in gap_events:
      gaps.append(
         {
            "ts": _iso(r["ts"]),
            "node": r["node"],
            "event_type": r["event_type"],
            "detail": r["detail"],
         }
      )
   # also detect metric gaps: any interval where expected_count > sample_count significantly
   # collect minutes where sample_count < expected_count
   metric_gaps = []
   for r in metric_rows:
      exp = r["expected_count"] or 0
      samp = r["sample_count"] or 0
      if exp > 0 and samp < exp:
         metric_gaps.append(
            {
               "minute": _iso(r["minute"]),
               "node": r["node"],
               "sample_count": int(samp),
               "expected_count": int(exp),
            }
         )

   uptime_ratio = covered_minutes / total_minutes if total_minutes else 1.0

   return {
      "from": from_ts.isoformat(),
      "to": to_ts.isoformat(),
      "total_minutes": total_minutes,
      "covered_minutes": covered_minutes,
      "uptime_ratio": uptime_ratio,
      "gaps": gaps,
      "metric_gaps": metric_gaps[:100],
   }
