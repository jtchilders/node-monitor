"""Value-only redaction of command lines before they are stored.

Only the values of secret-looking options, assignments, URL passwords, auth headers and
well-known key formats are replaced. Flags and option names are never altered, so a stored
command line still shows what was run.
"""

import re
from collections.abc import Iterable

MASK = "***"

# Long keywords are unambiguous enough to end a glued or camelCase name (`PGPASSWORD`,
# `keyStorePassword`, `clientSecret`). They must END the name: `--tokenizer`,
# `TOKENIZERS_PARALLELISM` and `--max-tokens` are not secrets.
_LONG_KEYWORD = (
   r"(?:passphrase|password|passwd|secret|token|(?:api|access|secret|private|ssh)[-_]?key)"
)
# Short keywords are common inside ordinary words (`--author`, `-keyfile`, `classpath`), so
# they count only as the whole name or its last `-`/`_`/`.` segment (`--oauth-token` but not
# `--auth-type`). Bare `key` is absent: `--sort-key name` and `openssl -key file.pem` name
# a field or a file, and the compound secret forms are long keywords.
_SHORT_KEYWORD = r"(?:pass|pat|pwd|auth|credentials?)"
_NAME_CHAR = r"[A-Za-z0-9_.-]"
# Names starting with `no-`/`no_` negate a feature (`--no-auth`) and are never secrets. A name
# starts with a letter or digit, so `--no-auth` cannot be re-read as `-` + `-no-auth`.
_SECRET_NAME = (
   r"(?!no[-_])(?=[A-Za-z0-9])(?:"
   + _NAME_CHAR
   + "*"
   + _LONG_KEYWORD
   + "|(?:"
   + _NAME_CHAR
   + "*[-_.])?"
   + _SHORT_KEYWORD
   + ")"
)
# A match may start only where no name character precedes it. Every name character is in
# this set, so each run of name characters has a single start and matching stays linear.
_NAME_START = r"(?<![\w.-])"
# A quoted value is taken whole so that spaces inside it cannot leak the rest of it.
_VALUE = r"""((?:"[^"]*"|'[^']*'|\S+))"""

# `-D<x>` is a define or Java property, handled by `_DEFINE_EQ`; the generic option rules
# must not read it as a single-dash option named `D<x>`.
_DASHES = r"(?:--|-(?-i:(?!D)))"
# `--opt=value` and `-opt=value`.
_OPTION_EQ = re.compile(_NAME_START + _DASHES + _SECRET_NAME + "=" + _VALUE, re.IGNORECASE)
# `--opt value` and `-opt value`. The value must not itself be a flag.
_OPTION_SPACE = re.compile(
   _NAME_START + _DASHES + _SECRET_NAME + r"[ \t]+(?!-)" + _VALUE, re.IGNORECASE
)
# Java's `-D<prop>=value`. Only a long keyword ending the name counts (`-Ddb.password`,
# `-Djavax.net.ssl.keyStorePassword`): `-Dauth=basic` is a mode, and an ALL_CAPS name such
# as `-DUSE_TOKEN=1` or `-DPASSWORD_LEN=8` is a C/CMake build define, never a secret.
_DEFINE_EQ = re.compile(
   _NAME_START
   + r"(?-i:-D)(?!no[-_])(?=[A-Za-z0-9])(?-i:(?![A-Z0-9_]+=))"
   + _NAME_CHAR
   + "*"
   + _LONG_KEYWORD
   + "="
   + _VALUE,
   re.IGNORECASE,
)
# keytool passwords are single-dash options whose names end in `pass`.
_KEYTOOL_PASSWORD = re.compile(r"(?<!\S)-(?:src|dest)?(?:store|key)pass[ \t]+(?!-)" + _VALUE)
# openssl's `-passin pass:X` / `-passout pass:X`; `env:` and `file:` sources hold no secret.
_OPENSSL_PASSWORD = re.compile(r"(?<!\S)-pass(?:in|out)[ \t]+pass:" + _VALUE)
# `KEY=value` assignments. The name-start lookbehind keeps `--opt=` out of this rule.
_ASSIGNMENT = re.compile(_NAME_START + _SECRET_NAME + "=" + _VALUE, re.IGNORECASE)
# The password in `scheme://user:password@host`. It runs to the last `@` before the host,
# since a password may itself contain `@`; the host part has no `@`, `/` or whitespace.
_URL_PASSWORD = re.compile(r"(?<=://)[^\s/:@]+:([^\s/]+)(?=@[^\s/@]*(?:[/\s]|$))")
# curl/wget style `-u user:pass` and `--user user:pass`. Without a colon there is no password.
_BASIC_AUTH = re.compile(r"(?<![\w-])(?:-u[ \t]+|--user(?:[ \t]+|=))[^\s:/'\"]+:([^\s'\"]+)")
# The whole header value, optionally with a scheme word, up to whitespace or a closing quote.
_AUTH_HEADER = re.compile(r"\bAuthorization:[ \t]*((?:[A-Za-z]+[ \t]+)?[^\s'\"]+)", re.IGNORECASE)
# Outside an Authorization header a bearer token is only recognised by its length, so prose
# such as `Bearer is good` is left alone.
_BEARER = re.compile(r"\bBearer[ \t]+([^\s'\"]{16,})", re.IGNORECASE)

# Only these take a password attached to `-p` (`mysql -pSecret`); elsewhere `-p` is a flag
# such as `mkdir -p` or `ssh -p 22`.
_MYSQL_TOOLS = frozenset({"mysql", "mariadb", "mysqldump", "mysqladmin"})
_MYSQL_PASSWORD = re.compile(r"(?<!\S)-p(\S+)")
# sshpass takes the password as `-pSecret` or `-p Secret`. Only its own leading options are
# searched, since a later `-p` belongs to the wrapped command (`sshpass -e ssh -p 22 host`).
_SSHPASS_OPTIONS = re.compile(r"\s*\S+(?:[ \t]+-[fdPp][ \t]+\S+|[ \t]+-\S*)*")
_SSHPASS_PASSWORD = re.compile(r"(?<!\S)-p[ \t]*" + _VALUE)

# Applied last, so they never fire inside text another rule has already masked.
_KNOWN_KEYS = (
   re.compile(r"(?<![A-Za-z0-9])(AKIA[0-9A-Z]{16})"),
   re.compile(r"(?<![A-Za-z0-9])(gh[pousr]_[A-Za-z0-9]{20,})"),
   re.compile(r"(?<![A-Za-z0-9])(github_pat_[A-Za-z0-9_]{20,})"),
   re.compile(r"(?<![A-Za-z0-9])(sk-[A-Za-z0-9_-]{16,})"),
)


def _sub(pattern: re.Pattern, text: str, count: int = 0) -> str:
   """Mask the first capture group of each match, or the whole match if there is none."""

   def repl(m: re.Match) -> str:
      whole = m.group(0)
      if pattern.groups == 0 or m.start(1) < 0:
         return MASK
      start = m.start(1) - m.start(0)
      end = m.end(1) - m.start(0)
      return whole[:start] + MASK + whole[end:]

   return pattern.sub(repl, text, count=count)


def _argv0_basename(cmdline: str) -> str:
   parts = cmdline.split(None, 1)
   return parts[0].rsplit("/", 1)[-1] if parts else ""


class Redactor:
   def __init__(self, extra_patterns: Iterable[str] = ()) -> None:
      self._extra = tuple(re.compile(p) for p in extra_patterns)

   def redact(self, cmdline: str) -> str:
      text = cmdline
      # First, so that `-pSecret db` is not read as an option ending in `secret` whose
      # value is `db`.
      if _argv0_basename(cmdline) in _MYSQL_TOOLS:
         text = _sub(_MYSQL_PASSWORD, text)
      if _argv0_basename(cmdline) == "sshpass":
         end = _SSHPASS_OPTIONS.match(text).end()
         text = _sub(_SSHPASS_PASSWORD, text[:end], count=1) + text[end:]
      text = _sub(_KEYTOOL_PASSWORD, text)
      text = _sub(_OPENSSL_PASSWORD, text)
      text = _sub(_DEFINE_EQ, text)
      text = _sub(_OPTION_EQ, text)
      text = _sub(_OPTION_SPACE, text)
      text = _sub(_ASSIGNMENT, text)
      text = _sub(_URL_PASSWORD, text)
      text = _sub(_BASIC_AUTH, text)
      text = _sub(_AUTH_HEADER, text)
      text = _sub(_BEARER, text)
      for pattern in self._extra:
         text = _sub(pattern, text)
      for pattern in _KNOWN_KEYS:
         text = _sub(pattern, text)
      return text
