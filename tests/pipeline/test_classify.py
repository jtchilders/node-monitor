import pytest

from node_monitor.pipeline.classify import Classification, classify
from node_monitor.pipeline.rules import ACTIVITY_RULES, SPEC_CATEGORIES

VSX = "/home/u/.vscode-server/extensions/"

CASES = [
   # (comm, cmdline, uid, expected_category, expected_activity)
   (
      "node",
      "/home/u/.vscode-server/extensions/anthropic.claude-code-2.1.0/bin/claude",
      31112,
      "ai_agent",
      "claude-code",
   ),
   (
      "node",
      "/home/u/.cursor-server/bin/xyz/node server.js",
      31112,
      "editor_server",
      "cursor-remote-server",
   ),
   ("bash", "/home/u/scripts/my_windsurf_notes.sh", 31112, "other", None),
   ("python3", "python3 train.py --epochs 3", 31112, "python", "python-training"),
   ("sshd", "sshd: alice@pts/3", 0, "system", "ssh-session"),
   ("weird", "", 31112, "other", None),
   ("make", "make -j 32", 500, "system", "build-driver"),
   # comm is only a fallback when cmdline is empty
   ("claude", "", 31112, "ai_agent", "claude-code"),
   # v1 unknown stays other
   ("mysteryd", "/usr/libexec/mysteryd --serve", 31112, "other", None),
   # v1 confidence/path cases: classification of both forms
   ("claude", "/soft/nodejs/bin/claude --resume", 31112, "ai_agent", "claude-code"),
   ("claude", "claude", 31112, "ai_agent", "claude-code"),
   # v1 ai agent beats compute/build
   ("python", "/soft/python/bin/python -m claude_code.cli", 31112, "ai_agent", "python-generic"),
   # v1 extension-path cases
   (
      "claude",
      VSX + "anthropic.claude-code-1.2.3/bin/claude",
      31112,
      "ai_agent",
      "claude-code",
   ),
   ("codex", VSX + "openai.chatgpt-0.9.0/bin/codex", 31112, "ai_agent", "codex-cli"),
   (
      "node",
      "/home/u/.vscode-server/bin/abc/node /home/u/.vscode-server/bin/abc/out/server-main.js",
      31112,
      "editor_server",
      "vscode-remote-server",
   ),
   (
      "MainThread",
      VSX + "anthropic.claude-code-1.2.3/bin/claude",
      31112,
      "ai_agent",
      "claude-code",
   ),
   ("node", VSX + "anthropic.claude-code-1.2.3/bin/claude", 31112, "ai_agent", "claude-code"),
   # windsurf, cursor, hermes, openclaw, roo, continue, copilot
   ("windsurf", "/soft/windsurf/bin/windsurf --headless", 31112, "ai_agent", "windsurf"),
   ("node", VSX + "codeium.windsurf-1.0.0/bin/node", 31112, "ai_agent", "windsurf"),
   ("cursor-agent", "cursor-agent --resume", 31112, "ai_agent", "cursor-cli"),
   (
      "node",
      "/home/u/.cursor-server/bin/xyz/node --max-old-space-size=4096 server.js",
      31112,
      "editor_server",
      "cursor-remote-server",
   ),
   (
      "cursor-agent",
      "/usr/local/bin/cursor-agent exec",
      31112,
      "ai_agent",
      "cursor-cli",
   ),
   ("hermes", "/soft/hermes/bin/hermes --profile coder", 31112, "ai_agent", "hermes"),
   ("hermes-agent", "hermes-agent --task t_1", 31112, "ai_agent", "hermes"),
   (
      "python",
      "/soft/python/bin/python /data/hermes/results/analyze.py",
      31112,
      "python",
      "python-generic",
   ),
   ("openclaw", "/opt/openclaw/bin/openclaw serve", 31112, "ai_agent", "openclaw"),
   ("node", VSX + "rooveterinaryinc.roo-cline-1.2.3/bin/node", 31112, "ai_agent", "roo"),
   ("node", VSX + "continue.continue-0.9.5/bin/node", 31112, "ai_agent", "continue"),
   (
      "bash",
      "/bin/bash -c 'for i in 1 2 3; do continue; done'",
      31112,
      "shell",
      "shell",
   ),
   ("copilot-agent", "copilot-agent --serve", 31112, "ai_agent", "copilot-agent"),
   (
      "python3",
      "/soft/python/bin/python3 train.py --epochs 10",
      31112,
      "python",
      "python-training",
   ),
   (
      "node",
      VSX + "somevendor.some-ai-agent-2.0.0/bin/node",
      31112,
      "ai_agent",
      "agent-like-unclassified",
   ),
   (
      "node",
      VSX + "dbaeumer.vscode-eslint-2.4.0/bin/node",
      31112,
      "editor_server",
      "vscode-remote-server",
   ),
]

# v1 fleet corpus (category only; activity is whatever the rules give).
CORPUS = [
   (
      "/home/u/.vscode-server/bin/abc/node /home/u/.vscode-server/bin/abc/out/server-main.js",
      "editor_server",
   ),
   ("/home/u/.cursor-server/bin/xyz/node --max-old-space-size=4096 server.js", "editor_server"),
   ("claude", "ai_agent"),
   ("/soft/nodejs/bin/node /home/u/.npm/bin/claude --resume", "ai_agent"),
   ("codex exec --model gpt-5", "ai_agent"),
   ("aider --model sonnet", "ai_agent"),
   ("/soft/python/bin/python -m ipykernel_launcher -f /tmp/kernel-1.json", "jupyter"),
   ("/soft/python/bin/jupyter-lab --no-browser", "jupyter"),
   ("qstat -u someone", "scheduler"),
   ("mpiexec -n 4 ./a.out", "scheduler"),
   ("git status", "data_transfer"),
   ("rsync -av /lus/eagle/projects/foo/ dest/", "data_transfer"),
   ("make -j16", "build"),
   ("/opt/cray/pe/gcc/bin/gcc -O3 -c kernel.c", "build"),
   ("cmake --build build", "build"),
   ("find /lus/eagle/projects/foo -name '*.h5'", "fs_scan"),
   ("du -sh /home/u", "fs_scan"),
   ("-bash", "shell"),
   ("sshd: someuser@pts/12", "shell"),
   ("tmux new-session", "shell"),
   ("/usr/libexec/some-unknown-daemon --flag", "other"),
   ("windsurf --headless", "ai_agent"),
   ("cursor-agent --resume", "ai_agent"),
   ("hermes-agent run", "ai_agent"),
   ("/opt/openclaw/bin/openclaw serve", "ai_agent"),
   (VSX + "rooveterinaryinc.roo-cline-1.0.0/bin/node", "ai_agent"),
   (VSX + "continue.continue-0.9.0/bin/node", "ai_agent"),
   ("gh-copilot suggest", "ai_agent"),
]


@pytest.mark.parametrize("comm,cmdline,uid,cat,act", CASES)
def test_classify_cases(comm, cmdline, uid, cat, act):
   assert classify(comm, cmdline, uid, 1000) == (cat, act)


@pytest.mark.parametrize("cmdline,cat", CORPUS)
def test_classify_v1_corpus(cmdline, cat):
   assert classify("x", cmdline, 31112, 1000).category == cat


def test_classify_returns_namedtuple():
   result = classify("make", "make", 31112, 1000)
   assert isinstance(result, Classification)
   assert result.category == "build"
   assert result.activity == "build-driver"


def test_bare_python_falls_back_to_python_category():
   assert classify("python", "python", 31112, 1000) == ("python", "python-generic")


def test_category_fallback_when_no_activity_matches():
   assert classify("x", "tar xf a.tar", 31112, 1000) == ("data_transfer", None)


def test_system_uid_keeps_activity_and_boundary_is_inclusive():
   assert classify("make", "make", 999, 1000) == ("system", "build-driver")
   assert classify("make", "make", 1000, 1000) == ("build", "build-driver")


def test_every_activity_maps_to_a_spec_category():
   assert {c for _, c, _, _ in ACTIVITY_RULES} <= SPEC_CATEGORIES
   assert len(ACTIVITY_RULES) == 42


def test_narrow_agent_rule_beats_broad_editor_rule():
   prio = {a: p for a, _, _, p in ACTIVITY_RULES}
   assert prio["claude-code"] > prio["vscode-remote-server"]


def test_residual_agent_priority_between_editor_and_named_agents():
   prio = {a: p for a, _, _, p in ACTIVITY_RULES}
   assert prio["vscode-remote-server"] < prio["agent-like-unclassified"] < prio["claude-code"]
