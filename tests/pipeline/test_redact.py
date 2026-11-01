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
   # Long keywords may end a glued or camelCase name.
   ("PGPASSWORD=hunter2 psql", "PGPASSWORD=*** psql"),
   ("tool --secret_key=abc", "tool --secret_key=***"),
   ("tool --secretkey=def", "tool --secretkey=***"),
   ("tool --clientSecret s4", "tool --clientSecret ***"),
   # A single dash before an option name.
   ("tool -password hunter2", "tool -password ***"),
   ("tool -token=abc123", "tool -token=***"),
   ("tool -api-key=XYZ", "tool -api-key=***"),
   # Java -D<prop>=value, where the property may contain dots.
   ("java -Dpassword=hunter2 X", "java -Dpassword=*** X"),
   (
      "java -Djavax.net.ssl.keyStorePassword=pw X",
      "java -Djavax.net.ssl.keyStorePassword=*** X",
   ),
   ("java -Ddb.password=x X", "java -Ddb.password=*** X"),
   ("tool --ssh-key k1", "tool --ssh-key ***"),
   ("tool --private-key k1", "tool --private-key ***"),
   ("tool --access_key=k1", "tool --access_key=***"),
   ("tool --secret-key k1", "tool --secret-key ***"),
   ("keytool -genkey -storepass s3 -keypass k4", "keytool -genkey -storepass *** -keypass ***"),
   (
      "keytool -importkeystore -srcstorepass a -deststorepass b",
      "keytool -importkeystore -srcstorepass *** -deststorepass ***",
   ),
   (
      "openssl pkcs12 -passout pass:hunter2 -in a",
      "openssl pkcs12 -passout pass:*** -in a",
   ),
   ("openssl rsa -passin pass:hunter2 -in a", "openssl rsa -passin pass:*** -in a"),
   ("sshpass -p 'a b' ssh h", "sshpass -p *** ssh h"),
   ('sshpass -p "a b c" ssh h', "sshpass -p *** ssh h"),
   ("sshpass -P prompt -p y ssh h", "sshpass -P prompt -p *** ssh h"),
   ("sshpass -d 3 -p x ssh h", "sshpass -d 3 -p *** ssh h"),
   ("java -Dspring.datasource.password=x X", "java -Dspring.datasource.password=*** X"),
   ("java -DclientSecret=x X", "java -DclientSecret=*** X"),
   ("aws s3 cp --sse-c-key x", "aws s3 cp --sse-c-key ***"),
   ("tool --encryption-key=abc", "tool --encryption-key=***"),
   ("tool --signing_key k", "tool --signing_key ***"),
   ("tool --customer-key k", "tool --customer-key ***"),
   ("tool --sse-customer-key k", "tool --sse-customer-key ***"),
   ("sshpass -pSecret ssh h", "sshpass -p*** ssh h"),
   ("sshpass -p Secret ssh h", "sshpass -p *** ssh h"),
   ("/usr/bin/sshpass -p Secret ssh -p 22 h", "/usr/bin/sshpass -p *** ssh -p 22 h"),
   # The password runs to the last @ before the host.
   ("curl https://bob:p@ss@host/x", "curl https://bob:***@host/x"),
   ("curl https://bob:p@ss@host", "curl https://bob:***@host"),
   # Hugging Face tokens, wherever they appear.
   ("huggingface-cli login hf_AbCdEfGhIjKlMnOpQrStUvWxYz0123", "huggingface-cli login ***"),
   ("env HF=hf_AbCdEfGhIjKlMnOpQrStUvWxYz0123 python", "env HF=*** python"),
   # Any `*-key`/`*_key` option whose value looks like a key: 24+ characters of
   # [A-Za-z0-9_-] with no `/` or `.`.
   ("tool --license-key AbCdEfGhIjKlMnOpQrStUvWx", "tool --license-key ***"),
   ("tool --license_key=AbCdEfGhIjKlMnOpQrStUvWx12", "tool --license_key=***"),
   ("tool -dev-key Ab_Cd-EfGhIjKlMnOpQrStUvWx run", "tool -dev-key *** run"),
   ("tool --WANDB_KEY 0123456789abcdef0123456789abcdef", "tool --WANDB_KEY ***"),
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
   "java -Dfile.encoding=UTF-8 X",
   "java -Dmy.auth=pw X",
   "java -Dauth=pw X",
   "java -DDB_PASSWORD=x X",
   "java -Dpass=x X",
   "tool --sort-key name",
   "tool --key path/obj",
   "openssl req -key file.pem",
   "cmake -DENABLE_AUTH=ON ..",
   "gcc -DUSE_TOKEN=1 x.c",
   "cmake -DPASSWORD_LEN=8 ..",
   "tool --primary-key id",
   "env KEY=value make",
   "openssl rsa -passin env:PW -in a",
   "keytool -list -keystore ks",
   "sshpass -e ssh -p 22 h",
   "tool --tokenizer bert",
   "env TOKENIZERS_PARALLELISM=false python",
   "tool -keyfile x",
   "tool --keyfile x",
   "make -j32 author=me",
   "curl https://host:8080/a@b",
   "pip install hf_transfer",
   "tool --license-key AbCdEfGhIjKlMnOpQrStUvW",
   "tool --cache-key /var/cache/abcdefghijklmnopqrstuvwxyz",
   "tool --model-key org.example.abcdefghijklmnopqrstuvwxyz",
   "tool --sort-key --a-very-long-flag-name-here-indeed",
   "tool --key AbCdEfGhIjKlMnOpQrStUvWxYz012",
   "tool --keys AbCdEfGhIjKlMnOpQrStUvWxYz012",
   "cmake -DAPP_KEY=AbCdEfGhIjKlMnOpQrStUvWxYz ..",
   "tool --sort-key 'AbCdEfGhIjKlMnOp QrStUvWxYz012'",
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


@pytest.mark.parametrize(
   "cmdline",
   [
      "x " + "a." * 50000,  # dotted names: every segment could start a name
      "x -" + "a-" * 50000 + " v",  # one long dashed option name with a value
      "x " + "token" * 20000,  # a keyword repeated without an `=`
      "x https://u:" + "p@" * 50000,  # a URL password full of `@` and no host end
   ],
)
def test_pathological_inputs_stay_fast(cmdline):
   t = time.perf_counter()
   R.redact(cmdline)
   assert time.perf_counter() - t < 0.5


@pytest.mark.parametrize(
   "cmdline",
   [
      "java -D" + "a." * 50000,
      "keytool" + " -storepass " * 10000,
      "sshpass" + " -p " * 25000,
      "sshpass" + " -P '" * 20000,
      "x " + "-passin pass:" * 8000,
      "x --" + "a_" * 50000 + "key " + "b" * 30,
      "x " + "--a-key " * 10000,
      "x " + "hf_" * 30000,
   ],
)
def test_new_patterns_stay_fast(cmdline):
   t = time.perf_counter()
   R.redact(cmdline)
   assert time.perf_counter() - t < 0.5
