"""Table-driven process tags — activity + flag presence to tag names.

Each rule matches when the process activity equals the rule's activity and the
flag pattern occurs as an exact token in argv. Redaction preserves flags, so
an exact token match is correct.
"""

from __future__ import annotations

from typing import TypedDict


class TagRule(TypedDict):
   activity: str
   pattern: str
   tag: str


TAGS: list[TagRule] = [
   TagRule(
      activity="claude-code",
      pattern="--dangerously-skip-permissions",
      tag="agent.skip_permissions",
   ),
   TagRule(
      activity="claude-code",
      pattern="--dangerously_skip_permissions",
      tag="agent.skip_permissions",
   ),
   TagRule(activity="python-generic", pattern="--pdb", tag="python.debugger"),
   TagRule(activity="python-generic", pattern="pdb", tag="python.debugger"),
   TagRule(activity="pytest", pattern="--pdb", tag="python.debugger"),
]


def tags_for(activity: str | None, argv: list[str]) -> list[str]:
   """Return sorted unique tags for a process with the given activity and argv."""
   matched: set[str] = set()
   for rule in TAGS:
      pat = rule.get("pattern", rule.get("flag_pattern", ""))  # type: ignore[attr-defined]
      if rule.get("activity") != activity:
         continue
      if pat in argv:
         matched.add(rule["tag"])
   return sorted(matched)
