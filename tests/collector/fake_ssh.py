#!/usr/bin/env python3
"""Stand-in for ssh: runs the remote command locally under /bin/sh.

Accepts ssh's shape (`[options] target command...`). Environment knobs:
FAKE_SSH_STDERR_BYTES  write that many bytes of "x" to stderr before running the command;
FAKE_SSH_FAIL=1        print "Permission denied" to stderr and exit 255, as ssh does.
"""

import os
import sys

OPTIONS_WITH_VALUE = ("-F", "-o")


def main(argv):
   i = 0
   while i < len(argv) and argv[i].startswith("-"):
      i += 2 if argv[i] in OPTIONS_WITH_VALUE else 1
   if i >= len(argv):
      sys.stderr.write("fake_ssh: no target given\n")
      return 255
   command = " ".join(argv[i + 1 :])

   flood = int(os.environ.get("FAKE_SSH_STDERR_BYTES") or 0)
   chunk = b"x" * 65536
   while flood > 0:
      sys.stderr.buffer.write(chunk[:flood])
      flood -= len(chunk)
   sys.stderr.flush()

   if os.environ.get("FAKE_SSH_FAIL") == "1":
      sys.stderr.write("Permission denied (publickey).\n")
      sys.stderr.flush()
      return 255

   os.execvp("/bin/sh", ["sh", "-c", command])


if __name__ == "__main__":
   sys.exit(main(sys.argv[1:]))
