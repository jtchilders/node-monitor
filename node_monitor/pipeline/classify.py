"""Classify a process into a (category, activity) pair."""

import re
from collections import namedtuple

from node_monitor.pipeline.rules import ACTIVITY_RULES, CATEGORY_RULES, DEFAULT_CATEGORY

Classification = namedtuple("Classification", "category activity")

_ACTIVITY = tuple((a, c, re.compile(rx), p) for a, c, rx, p in ACTIVITY_RULES)
_CATEGORY = tuple((c, re.compile(rx), p) for c, rx, p in CATEGORY_RULES)


def classify(comm: str, cmdline: str, uid: int, min_user_uid: int) -> Classification:
   """Highest-priority rule wins; ties go to the earlier rule.

   argv is preferred over comm because comm is a renameable task name. System uids are
   forced to category "system" but keep their activity.
   """
   haystack = cmdline if cmdline else comm
   activity = category = None
   best = None
   for act, cat, pattern, priority in _ACTIVITY:
      if (best is None or priority > best) and pattern.search(haystack):
         activity, category, best = act, cat, priority
   # A category rule outranking the matched activity wins the category, so a python-launched
   # agent (e.g. "python -m claude_code.cli", only python-generic at priority 5) stays ai_agent.
   cat_best = best
   for cat, pattern, priority in _CATEGORY:
      if (cat_best is None or priority > cat_best) and pattern.search(haystack):
         category, cat_best = cat, priority
   if uid < min_user_uid:
      category = "system"
   return Classification(category or DEFAULT_CATEGORY, activity)
