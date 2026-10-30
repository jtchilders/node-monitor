import time

import pytest

from node_monitor.pipeline.redact import Redactor

R = Redactor()

MASKED = [
   ("tool --api-key x", "tool --api-key ***"),
   ("tool --client-secret x", "tool --client-secret ***"),
   ("tool --access-token=x", "tool --access-token=***"),
   ("tool --oauth-token x", "tool --oauth-token ***"),
   ("tool --auth x", "tool --auth ***"),
   ("tool --passphrase x", "tool --passphrase ***"),
   ("tool --pat x", "tool --pat ***"),
   ("tool --password 'a b c'", "tool --password ***"),
   ("env GITHUB_TOKEN=x make", "env GITHUB_TOKEN=*** make"),
   ("env AWS_SECRET_ACCESS_KEY=x run", "env AWS_SECRET_ACCESS_KEY=*** run"),
   ("curl -u bob:pw url", "curl -u bob:*** url"),
   ("curl --user bob:pw url", "curl --user bob:*** url"),
   ("mysql -pSecret db", "mysql -p*** db"),
   ("/usr/bin/mariadb -pSecret db", "/usr/bin/mariadb -p*** db"),
   ("echo Bearer abcdefghijklmnopqrstu", "echo Bearer ***"),
]

UNTOUCHED = [
   "tool --auth-type ssh",
   "tool --no-auth foo",
   "tool --no-token x",
   "tool --token-cache-dir /x",
   "tool --token_limit 5",
   "tool --author me",
   "tool --max-tokens 5",
   "env TOKEN_LIMIT=5 make",
   "env NO_TOKEN=x make",
   "tool --password",
   "echo Bearer is good",
   "git clone https://user@host/repo.git",
   "git -c credential.helper=store push",
   "mkdir -p /tmp/x",
   "ssh -p 22 host",
   "ssh -p22 host",
   "mysql -p db",
   "curl -u bob url",
]


@pytest.mark.parametrize(("cmdline", "expected"), MASKED)
def test_masks_secret_values_only(cmdline, expected):
   assert R.redact(cmdline) == expected


@pytest.mark.parametrize("cmdline", UNTOUCHED)
def test_keeps_non_secret_arguments(cmdline):
   assert R.redact(cmdline) == cmdline


def test_redacts_both_option_forms_and_keeps_flags():
   assert (
      R.redact("claude --dangerously-skip-permissions --api-key sk-abc123def456ghi789")
      == "claude --dangerously-skip-permissions --api-key ***"
   )
   assert R.redact("tool --password=hunter2 --verbose") == "tool --password=*** --verbose"
   assert R.redact("tool --token abc --model opus") == "tool --token *** --model opus"


def test_redacts_assignments_urls_headers_and_known_keys():
   assert R.redact("env GITHUB_TOKEN=ghp_" + "a" * 36 + " make") == "env GITHUB_TOKEN=*** make"
   assert R.redact("git clone https://bob:s3cret@host/repo.git") == (
      "git clone https://bob:***@host/repo.git"
   )
   assert R.redact("curl -H 'Authorization: Bearer xyz.abc' url") == (
      "curl -H 'Authorization: ***' url"
   )
   assert R.redact("aws AKIAABCDEFGHIJKLMNOP") == "aws ***"


def test_leaves_ordinary_commands_untouched():
   for c in (
      "python3 train.py --epochs 3 --lr 0.1",
      "ssh -o BatchMode=yes host",
      "make -j32 author=me",
   ):
      assert R.redact(c) == c


def test_extra_patterns_mask_group_or_match():
   r = Redactor([r"--site-key\s+(\S+)", r"SITE[0-9]{6}"])
   assert r.redact("x --site-key k1 SITE123456") == "x --site-key *** ***"


def test_long_and_non_ascii_cmdlines():
   long = "python " + " ".join("--opt%d=v" % i for i in range(10000))  # ~100 KB
   t = time.perf_counter()
   out = R.redact(long)
   assert out == long and time.perf_counter() - t < 0.5
   assert R.redact("runé --password=pässwörd") == "runé --password=***"
