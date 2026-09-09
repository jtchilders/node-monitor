"""Executable fake interpreters for tests/test_transport.py.

`write_fake_probe` materializes a real, executable Python script -- not a
mock -- so transport.py's subprocess machinery (argv, stdin delivery,
environment, process groups, signals) is exercised for real. The same
script doubles as a stand-in for both "the local probe interpreter" and
"the ssh binary" (the tests that need ssh-specific failure semantics --
auth/transport exit 255 with no JSON -- just set FAKE_EXIT_CODE and
FAKE_STDERR_TEXT directly; there is no real network layer to fake).

Every knob is an environment variable so no two tests need their own
script variant, and so the behavior a given test cares about is visible
in that test's own env dict rather than buried in a script file.
"""

import os
import sys


_TEMPLATE = '''#!/usr/bin/env python3
import json
import os
import signal
import subprocess
import sys
import time


def _env(name, default=None):
   return os.environ.get(name, default)


if _env("FAKE_IGNORE_SIGTERM") == "1":
   signal.signal(signal.SIGTERM, signal.SIG_IGN)

if _env("FAKE_SPAWN_MARKER_FILE"):
   child = subprocess.Popen([
      sys.executable, "-c",
      "import os,sys,time;"
      "open(sys.argv[1],'w').write(str(os.getpid()));"
      "time.sleep(float(sys.argv[2]))",
      _env("FAKE_SPAWN_MARKER_FILE"),
      _env("FAKE_SPAWN_SLEEP_SECONDS", "30"),
   ])

if _env("FAKE_CHECK_NO_LD_PRELOAD") == "1" and os.environ.get("LD_PRELOAD"):
   sys.stderr.write("LD_PRELOAD leaked into child\\n")
   sys.exit(9)

if _env("FAKE_READ_STDIN_LEN") == "1" or _env("FAKE_STDIN_DUMP"):
   stdin_bytes = sys.stdin.buffer.read()
   if _env("FAKE_STDIN_DUMP"):
      with open(_env("FAKE_STDIN_DUMP"), "wb") as handle:
         handle.write(stdin_bytes)

sleep_seconds = _env("FAKE_SLEEP_SECONDS")
if sleep_seconds:
   time.sleep(float(sleep_seconds))

stderr_text = _env("FAKE_STDERR_TEXT")
if stderr_text:
   sys.stderr.write(stderr_text)
   sys.stderr.flush()

exit_code = int(_env("FAKE_EXIT_CODE", "0"))

stdout_text = _env("FAKE_STDOUT_TEXT")
if stdout_text is None:
   if _env("FAKE_ECHO_ARGV") == "1":
      payload = {
         "probe_version": int(_env("FAKE_PROBE_VERSION", "4")),
         "loop": _env("FAKE_LOOP", "census"),
         "hostname_fqdn": _env("FAKE_HOSTNAME_FQDN", "testhost.example.org"),
         "argv": sys.argv[1:],
      }
   else:
      payload = {
         "probe_version": int(_env("FAKE_PROBE_VERSION", "4")),
         "loop": _env("FAKE_LOOP", "census"),
         "hostname_fqdn": _env("FAKE_HOSTNAME_FQDN", "testhost.example.org"),
      }
   stdout_text = json.dumps(payload) + "\\n"

sys.stdout.write(stdout_text)
sys.stdout.flush()
sys.exit(exit_code)
'''


def write_fake_probe(path):
   """Write an executable fake probe/ssh stand-in to `path` and return it."""
   with open(path, "w") as handle:
      handle.write(_TEMPLATE)
   os.chmod(path, 0o755)
   return str(path)
