"""Receive-time validation shared by the pipeline stages."""

from datetime import datetime


def require_aware(recv: datetime) -> datetime:
   """Return `recv`, or raise ValueError if it has no timezone.

   Intervals are aligned to UTC, and a naive datetime would be read in the host's local zone
   by `timestamp()`, silently landing samples in the wrong interval.
   """
   if recv.tzinfo is None or recv.utcoffset() is None:
      raise ValueError(f"receive time must be timezone-aware, got {recv!r}")
   return recv
