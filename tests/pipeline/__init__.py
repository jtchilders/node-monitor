from datetime import datetime, timedelta, timezone

UTC = timezone.utc


def at(seconds):
   """A collector receive time, `seconds` after 2026-11-01 12:00:00Z."""
   return datetime(2026, 11, 1, 12, 0, tzinfo=UTC) + timedelta(seconds=seconds)
