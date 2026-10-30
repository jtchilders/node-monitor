"""Value-only redaction of command lines before they are stored.

Only the values of secret-looking options, assignments, URL passwords, auth headers and
well-known key formats are replaced. Flags and option names are never altered, so a stored
command line still shows what was run.
"""

import re
from collections.abc import Iterable

MASK = "***"

# Keywords must be whole name segments, so `author` does not match `auth` and `oauth` does not
# match `auth`. Segments are separated by `-` or `_`.
_KEY = r"(?:password|passwd|pwd|token|secret|api[-_]?key|auth|credential)"
_NAME = r"(?:[A-Za-z0-9]+[-_])*" + _KEY + r"(?:[-_][A-Za-z0-9]+)*"
# A quoted value is taken whole so that spaces inside it cannot leak the rest of it.
_VALUE = r"""((?:"[^"]*"|'[^']*'|\S+))"""

# `--opt=value` and `--opt value`. A value after a separate option must not itself be a flag.
_OPTION_EQ = re.compile(r"(?<![\w-])--" + _NAME + "=" + _VALUE, re.IGNORECASE)
_OPTION_SPACE = re.compile(r"(?<![\w-])--" + _NAME + r"[ \t]+(?!-)" + _VALUE, re.IGNORECASE)
# `KEY=value` assignments. The lookbehind keeps `--opt=` out of this rule.
_ASSIGNMENT = re.compile(r"(?<![\w-])" + _NAME + "=" + _VALUE, re.IGNORECASE)
# The password in `scheme://user:password@host`.
_URL_PASSWORD = re.compile(r"(?<=://)[^\s/:@]+:([^\s/@]+)(?=@)")
# The whole header value, optionally with a scheme word, up to whitespace or a closing quote.
_AUTH_HEADER = re.compile(r"\bAuthorization:[ \t]*((?:[A-Za-z]+[ \t]+)?[^\s'\"]+)", re.IGNORECASE)
_BEARER = re.compile(r"\bBearer[ \t]+([^\s'\"]+)", re.IGNORECASE)

# Applied last, so they never fire inside text another rule has already masked.
_KNOWN_KEYS = (
   re.compile(r"(?<![A-Za-z0-9])(AKIA[0-9A-Z]{16})"),
   re.compile(r"(?<![A-Za-z0-9])(gh[pousr]_[A-Za-z0-9]{20,})"),
   re.compile(r"(?<![A-Za-z0-9])(github_pat_[A-Za-z0-9_]{20,})"),
   re.compile(r"(?<![A-Za-z0-9])(sk-[A-Za-z0-9_-]{16,})"),
)


def _sub(pattern: re.Pattern, text: str) -> str:
   """Mask the first capture group of each match, or the whole match if there is none."""

   def repl(m: re.Match) -> str:
      whole = m.group(0)
      if pattern.groups == 0 or m.start(1) < 0:
         return MASK
      start = m.start(1) - m.start(0)
      end = m.end(1) - m.start(0)
      return whole[:start] + MASK + whole[end:]

   return pattern.sub(repl, text)


class Redactor:
   def __init__(self, extra_patterns: Iterable[str] = ()) -> None:
      self._extra = tuple(re.compile(p) for p in extra_patterns)

   def redact(self, cmdline: str) -> str:
      text = _sub(_OPTION_EQ, cmdline)
      text = _sub(_OPTION_SPACE, text)
      text = _sub(_ASSIGNMENT, text)
      text = _sub(_URL_PASSWORD, text)
      text = _sub(_AUTH_HEADER, text)
      text = _sub(_BEARER, text)
      for pattern in self._extra:
         text = _sub(pattern, text)
      for pattern in _KNOWN_KEYS:
         text = _sub(pattern, text)
      return text
