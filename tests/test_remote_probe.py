"""Tests for the remote probe.

The probe runs unattended on shared login nodes where a parse bug produces a
plausible-looking wrong number rather than a crash. These tests therefore
focus on the failure modes that are silent: comm-field parsing, permission
denial, coverage accounting, and the JSON contract the daemon depends on.
"""

import errno
import json
import os
import re
import shutil
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "fixtures"))

from node_monitor.collector import remote_probe as probe  # noqa: E402
import fake_proc  # noqa: E402


PROBE_PATH = os.path.join(
   os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
   "node_monitor", "collector", "remote_probe.py")


@pytest.fixture
def proc_root(tmp_path, monkeypatch):
   """Point the probe at a synthetic /proc for the duration of one test."""
   root = str(tmp_path / "proc")

   def _build(pids, **kwargs):
      fake_proc.write_proc(root, pids, **kwargs)
      monkeypatch.setattr(probe, "PROC_ROOT", root)
      return root

   return _build


@pytest.fixture
def fake_proc_env(tmp_path):
   """Build a synthetic /proc tree plus the env var to point a real
   subprocess invocation of the probe at it.

   The in-process `proc_root` fixture above monkeypatches the module
   attribute, which a child process started by `subprocess.run` cannot see.
   `NODE_MONITOR_PROC_ROOT` is the only channel that reaches a subprocess,
   which is exactly why it exists (see remote_probe.py's module docstring).
   This is what lets TestExitCodes and TestPayloadContract run on macOS,
   which has no real /proc, while still exercising the real subprocess exit
   codes and stdout contract -- not a monkeypatched in-process call.

   Populated with enough processes to give TestPayloadContract a non-empty
   `processes` list: an interactive shell, a vscode-server node process (a
   daemon, no tty), an sshd session, and a kernel thread (absent cmdline)
   to prove exclusion survives the real CLI path too.
   """
   root = str(tmp_path / "proc")
   fake_proc.write_proc(root, [
      {"pid": 2, "comm": "kthreadd", "cmdline": None},
      {"pid": 100, "comm": "bash", "cmdline": "-bash", "tty_nr": 34816},
      {"pid": 200, "comm": "node", "tty_nr": 0, "ppid": 1,
       "cmdline": "/home/u/.vscode-server/bin/a/node server-main.js"},
      {"pid": 300, "comm": "sshd", "cmdline": "sshd: someuser@pts/12"},
   ])
   env = dict(os.environ)
   env["NODE_MONITOR_PROC_ROOT"] = root
   return env


# --------------------------------------------------------------------------
# /proc/<pid>/stat parsing -- the highest-risk parser in the probe
# --------------------------------------------------------------------------

class TestParseStat:
   def test_plain_comm(self):
      comm, tail = probe._parse_stat(fake_proc.make_stat(42, "bash"))
      assert comm == "bash"
      assert tail[1] == "1"

   def test_comm_containing_spaces(self):
      """Real fleet processes have spaces in comm.

      Splitting /proc/<pid>/stat on whitespace shifts every subsequent field
      by one, so utime lands in the tty slot. The result is not a crash --
      it is a plausible wrong CPU number, which is worse.
      """
      comm, tail = probe._parse_stat(
         fake_proc.make_stat(42, "my proc name", utime=777))
      assert comm == "my proc name"
      assert tail[11] == "777"

   def test_comm_containing_parens(self):
      """'(sd-pam)' is present on every Polaris login node.

      Bounding comm with the FIRST ')' instead of the last truncates the
      name and shifts the fields.
      """
      comm, tail = probe._parse_stat(
         fake_proc.make_stat(42, "(sd-pam)", stime=333))
      assert comm == "(sd-pam)"
      assert tail[12] == "333"

   def test_comm_with_parens_and_spaces(self):
      comm, _ = probe._parse_stat(fake_proc.make_stat(42, "weird (x) name"))
      assert comm == "weird (x) name"

   def test_malformed_returns_none(self):
      assert probe._parse_stat("garbage with no parens") is None
      assert probe._parse_stat("") is None

   def test_truncated_tail_returns_none(self):
      assert probe._parse_stat("42 (bash) S 1 2 3") is None


# --------------------------------------------------------------------------
# Classification
# --------------------------------------------------------------------------

class TestClassification:
   @pytest.mark.parametrize("cmdline,expected", fake_proc.CMDLINE_CORPUS)
   def test_corpus(self, cmdline, expected):
      category, _, _ = probe._classify("x", cmdline)
      assert category == expected, "%r -> %s, expected %s" % (
         cmdline, category, expected)

   def test_unknown_stays_other(self):
      """An unrecognized process must not be forced into a category.

      Every misfiled process inflates one category and deflates 'other',
      which is exactly the number an ALCF-4 sizing argument would rest on.
      """
      category, activity, confidence = probe._classify(
         "mysteryd", "/usr/libexec/mysteryd --serve")
      assert category == "other"
      assert activity is None
      assert confidence == "unknown"

   def test_confidence_path_qualified_vs_heuristic(self):
      _, _, absolute = probe._classify(
         "claude", "/soft/nodejs/bin/claude --resume")
      _, _, bare = probe._classify("claude", "claude")
      assert absolute == "path_qualified"
      assert bare == "argv_heuristic"

   def test_ai_agent_beats_compute_build(self):
      """Ordering hazard: an agent launched via python must stay an agent.

      Both rules match 'python ... claude'; category order decides. If
      compute/build won, the fleet's ai-coding-agent count would silently
      lose every python-launched agent.
      """
      category, _, _ = probe._classify(
         "python", "/soft/python/bin/python -m claude_code.cli")
      assert category == "ai-coding-agent"

   def test_project_path_hint(self):
      assert probe._project_from_path(
         "/lus/eagle/projects/datascience/run.sh") == "datascience"
      assert probe._project_from_path("/home/u/run.sh") is None


class TestClassificationPriority:
   """Overlapping-rule precedence must be explicit priority data, not
   implicit source-line order.

   Regression: a Claude Code VS Code extension path such as
   .vscode-server/extensions/anthropic.claude-code-*/bin/claude matched the
   broad ide-remote / vscode-remote-server rules first because they simply
   appeared earlier in the list. Live Polaris impact measured 2026-09-04:
   about 24% undercount of AI-agent processes, 9 distinct users reported
   instead of 12.
   """

   def test_vscode_claude_code_extension_path_is_ai_coding_agent(self):
      """The exact path shape from the live undercount."""
      cmdline = (
         "/home/u/.vscode-server/extensions/anthropic.claude-code-1.2.3/"
         "bin/claude")
      category, activity, _ = probe._classify("claude", cmdline)
      assert category == "ai-coding-agent"
      assert activity == "claude-code"

   def test_vscode_codex_extension_path_is_ai_coding_agent(self):
      cmdline = (
         "/home/u/.vscode-server/extensions/openai.chatgpt-0.9.0/bin/codex")
      category, activity, _ = probe._classify("codex", cmdline)
      assert category == "ai-coding-agent"
      assert activity == "codex-cli"

   def test_ordinary_vscode_server_path_stays_ide_remote(self):
      """A broad VS Code server path with no AI-agent marker must be
      unaffected by the priority fix."""
      cmdline = (
         "/home/u/.vscode-server/bin/abc/node "
         "/home/u/.vscode-server/bin/abc/out/server-main.js")
      category, activity, _ = probe._classify("node", cmdline)
      assert category == "ide-remote"
      assert activity == "vscode-remote-server"

   def test_misleading_comm_cannot_defeat_argv_classification(self):
      """comm is a renameable 15-char task name (MainThread, node, ...).

      argv identifying Claude Code must win regardless of what comm says.
      """
      cmdline = (
         "/home/u/.vscode-server/extensions/anthropic.claude-code-1.2.3/"
         "bin/claude")
      for misleading_comm in ("MainThread", "node"):
         category, activity, _ = probe._classify(misleading_comm, cmdline)
         assert category == "ai-coding-agent"
         assert activity == "claude-code"

   def test_priority_is_data_highest_wins_regardless_of_list_order(self):
      """Priority must be resolved by an explicit numeric field, not by
      which rule happens to appear first in the source list.

      Builds a tiny reversed/shuffled rule sequence -- deliberately putting
      the low-priority rule ahead of the high-priority one in list order --
      and shows the resolver still returns the higher-priority match. A
      resolver that just returns the first regex to match this sequence
      would fail here even though it might pass against the real
      _CATEGORY_RULES, because real list order happens to already agree
      with priority for the untouched cases.
      """
      broad = ("broad-low-priority", re.compile(r"marker"), 10)
      specific = ("specific-high-priority", re.compile(r"special-marker"), 90)
      # Broad, lower-priority rule listed FIRST -- source order disagrees
      # with priority order on purpose.
      rules = [broad, specific]
      label = probe._resolve_rule(rules, "special-marker-here")
      assert label == "specific-high-priority"

      # Shuffle again the other way to prove it is not an accident of
      # which position happens to be checked first.
      rules_reversed = [specific, broad]
      label_reversed = probe._resolve_rule(rules_reversed, "special-marker-here")
      assert label_reversed == "specific-high-priority"

   def test_category_rules_carry_explicit_priority(self):
      """Every category rule must declare priority as data (a 3-tuple),
      not rely on its position in the list."""
      for rule in probe._CATEGORY_RULES:
         assert len(rule) == 3, (
            "%r must be a (label, pattern, priority) tuple" % (rule,))
         label, pattern, priority = rule
         assert isinstance(priority, int)

   def test_activity_rules_carry_explicit_priority(self):
      for rule in probe._ACTIVITY_RULES:
         assert len(rule) == 3, (
            "%r must be a (label, pattern, priority) tuple" % (rule,))
         label, pattern, priority = rule
         assert isinstance(priority, int)

   def test_claude_code_priority_exceeds_vscode_remote_server(self):
      """Direct data assertion backing up the behavioral tests above:
      the claude-code activity rule's priority must exceed
      vscode-remote-server's, and the ai-coding-agent category rule's
      priority must exceed ide-remote's."""
      cat_priority = {label: pr for label, _, pr in probe._CATEGORY_RULES}
      act_priority = {label: pr for label, _, pr in probe._ACTIVITY_RULES}
      assert cat_priority["ai-coding-agent"] > cat_priority["ide-remote"]
      assert act_priority["claude-code"] > act_priority["vscode-remote-server"]


class TestBehavior:
   def test_tty_implies_interactive(self):
      assert probe._behavior(34816, 100, "bash") == "interactive"

   def test_no_tty_parent_init_is_daemon(self):
      assert probe._behavior(0, 1, "sshd") == "daemon"

   def test_no_tty_live_parent_is_batch(self):
      assert probe._behavior(0, 4242, "python") == "batch"


# --------------------------------------------------------------------------
# Census over a synthetic /proc
# --------------------------------------------------------------------------

class TestCensus:
   def test_basic_census(self, proc_root):
      proc_root([
         {"pid": 100, "comm": "bash", "cmdline": "-bash", "tty_nr": 34816},
         {"pid": 200, "comm": "node", "tty_nr": 0,
          "cmdline": "/home/u/.vscode-server/bin/a/node server-main.js"},
      ])
      rows, coverage, _tools = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      assert coverage["pids_seen"] == 2
      by_pid = {row["pid"]: row for row in rows}
      assert by_pid[100]["category"] == "shell/session"
      assert by_pid[100]["interactive"] is True
      assert by_pid[200]["category"] == "ide-remote"
      assert by_pid[200]["behavior"] == "daemon"

   def test_kernel_threads_excluded(self, proc_root):
      """Absent cmdline = kernel thread. Not user behavior; must not appear.

      Counting them would add hundreds of phantom 'other' processes per node.
      """
      proc_root([
         {"pid": 2, "comm": "kthreadd", "cmdline": None},
         {"pid": 100, "comm": "bash", "cmdline": "-bash"},
      ])
      rows, coverage, _tools = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      assert [row["pid"] for row in rows] == [100]
      assert coverage["kernel_thread"] == 1
      assert coverage["pids_seen"] == 2

   def test_empty_cmdline_file_also_excluded(self, proc_root):
      """cmdline file present but empty is the same signal as file absent.

      On a real Linux node a kernel thread's cmdline file exists and reads
      empty; the test fixture's 'absent' case simulates the same thing a
      different way. Both must be excluded, and this project keeps them in
      separate coverage buckets (kernel_thread vs cmdline_empty) only
      because the fixture can express both shapes -- not because an analyst
      is meant to draw a distinction between them.
      """
      proc_root([
         {"pid": 3, "comm": "kworker", "cmdline": ""},
         {"pid": 100, "comm": "bash", "cmdline": "-bash"},
      ])
      rows, coverage, _tools = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      assert [row["pid"] for row in rows] == [100]
      assert coverage["cmdline_empty"] == 1
      assert coverage["pids_seen"] == 2

   def test_denied_cmdline_treated_as_kernel_thread(self, proc_root, monkeypatch):
      """EACCES on cmdline is indistinguishable from absent here -- and rare.

      /proc/<pid>/cmdline is normally world-readable on Linux, so a real
      denial would be unusual. _read_text collapses denial and absence into
      the same None; rather than invent a way to tell them apart, the probe
      picks the conservative reading (exclude, count as kernel_thread).
      """
      root = proc_root([
         {"pid": 100, "comm": "bash", "cmdline": "-bash"},
         {"pid": 300, "comm": "secret", "cmdline": "secret"},
      ])
      real_read = probe._read_text

      def denying_read(path):
         if path.endswith(os.path.join("300", "cmdline")):
            return None
         return real_read(path)

      monkeypatch.setattr(probe, "_read_text", denying_read)
      rows, coverage, _tools = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      assert [row["pid"] for row in rows] == [100]
      assert coverage["kernel_thread"] == 1
      assert coverage["pids_seen"] == 2

   def test_raw_args_dropped_by_default(self, proc_root):
      proc_root([{"pid": 100, "comm": "git",
                  "cmdline": "git clone https://user:secret@host/r.git"}])
      rows, _, _tools = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      assert "cmdline" not in rows[0]
      assert "secret" not in json.dumps(rows[0])

   def test_raw_args_kept_when_requested(self, proc_root):
      proc_root([{"pid": 100, "comm": "git", "cmdline": "git status"}])
      rows, _, _tools = probe._collect_processes(
         {0: "root"}, drop_raw_args=False, deadline=None)
      assert rows[0]["cmdline"] == "git status"

   def test_unreadable_stat_counted_not_fatal(self, proc_root, monkeypatch):
      """Another user's process denies reads. Expected, must be counted."""
      root = proc_root([
         {"pid": 100, "comm": "bash", "cmdline": "-bash"},
         {"pid": 300, "comm": "secret", "cmdline": "secret"},
      ])
      real_read = probe._read_text

      def denying_read(path):
         if path.endswith(os.path.join("300", "stat")):
            return None
         return real_read(path)

      monkeypatch.setattr(probe, "_read_text", denying_read)
      rows, coverage, _tools = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      assert coverage["stat_unreadable"] == 1
      assert coverage["pids_seen"] == 2
      assert len(rows) == 1

   def test_username_null_when_unresolvable(self, proc_root):
      """An unresolvable uid must stay null, never be invented."""
      proc_root([{"pid": 100, "comm": "bash", "cmdline": "-bash"}])
      rows, coverage, _tools = probe._collect_processes(
         {}, drop_raw_args=True, deadline=None)
      assert rows[0]["username"] is None
      assert coverage["owner_unresolved"] == 1

   def test_rss_converted_from_pages(self, proc_root):
      proc_root([{"pid": 100, "comm": "bash", "cmdline": "-bash",
                  "rss_pages": 2048}])
      rows, _, _tools = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      page_kb = os.sysconf("SC_PAGESIZE") // 1024
      assert rows[0]["rss_kb"] == 2048 * page_kb


# --------------------------------------------------------------------------
# On-node tool-instance aggregates (privacy-preserving; computed while argv
# is still available, before drop_raw_args strips cmdline from the rows).
# --------------------------------------------------------------------------

class TestToolAggregates:
   def test_tools_present_default_rows_have_no_cmdline(self, proc_root):
      proc_root([
         {"pid": 100, "comm": "claude", "cmdline": "claude", "ppid": 1},
      ])
      rows, coverage, tools = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      assert "cmdline" not in rows[0]
      assert tools == [
         {"tool": "claude-code", "username": rows[0]["username"],
          "proc_count": 1, "tree_root_count": 1, "install_count": None,
          "rss_kb_total": rows[0]["rss_kb"], "nested_in": []},
      ]

   def test_proc_count_vs_tree_root_count_same_tool_parent_child(
         self, proc_root):
      """Same-tool parent+child must count as 2 processes but 1 tree root."""
      proc_root([
         {"pid": 100, "comm": "claude", "cmdline": "claude", "ppid": 1},
         {"pid": 101, "comm": "claude", "cmdline": "claude --resume",
          "ppid": 100},
      ])
      rows, coverage, tools = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      entry = next(t for t in tools if t["tool"] == "claude-code")
      assert entry["proc_count"] == 2
      assert entry["tree_root_count"] == 1

   def test_install_id_dedup_and_null_for_non_install_tools(self, proc_root):
      install_id = "a" * 40
      proc_root([
         {"pid": 200, "comm": "node", "ppid": 1,
          "cmdline": "/home/u/.vscode-server/bin/Stable-%s/node "
                     "server.js" % install_id},
         {"pid": 201, "comm": "node", "ppid": 1,
          "cmdline": "/home/u/.vscode-server/bin/Stable-%s/node "
                     "worker.js" % install_id},
         {"pid": 300, "comm": "claude", "ppid": 1, "cmdline": "claude"},
      ])
      rows, coverage, tools = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      vscode = next(t for t in tools if t["tool"] == "vscode-server")
      assert vscode["proc_count"] == 2
      assert vscode["install_count"] == 1
      claude = next(t for t in tools if t["tool"] == "claude-code")
      assert claude["install_count"] is None

   def test_install_id_dedup_is_case_insensitive(self, proc_root):
      """Stable-<HEX> vs Stable-<HEX-uppercase> must count as one install."""
      hex_id = "abc123def456abc123def456abc123def456abcd"[:40]
      proc_root([
         {"pid": 200, "comm": "node", "ppid": 1,
          "cmdline": "/home/u/.vscode-server/bin/Stable-%s/node "
                     "server.js" % hex_id},
         {"pid": 201, "comm": "node", "ppid": 1,
          "cmdline": "/home/u/.vscode-server/bin/Stable-%s/node "
                     "worker.js" % hex_id.upper()},
      ])
      rows, coverage, tools = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      vscode = next(t for t in tools if t["tool"] == "vscode-server")
      assert vscode["proc_count"] == 2
      assert vscode["install_count"] == 1

   def test_rss_totals_sum_exactly(self, proc_root):
      proc_root([
         {"pid": 100, "comm": "claude", "cmdline": "claude", "ppid": 1,
          "rss_pages": 100},
         {"pid": 101, "comm": "claude", "cmdline": "claude", "ppid": 1,
          "rss_pages": 200},
      ])
      rows, coverage, tools = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      entry = next(t for t in tools if t["tool"] == "claude-code")
      page_kb = os.sysconf("SC_PAGESIZE") // 1024
      assert entry["rss_kb_total"] == 300 * page_kb

   def test_claude_under_vscode_nested_in_and_primary_category_preserved(
         self, proc_root):
      proc_root([
         {"pid": 200, "comm": "node", "ppid": 1,
          "cmdline": "/home/u/.vscode-server/bin/abc/node server-main.js"},
         {"pid": 201, "comm": "claude", "ppid": 200,
          "cmdline": "/home/u/.vscode-server/extensions/"
                     "anthropic.claude-code-1.2.3/bin/claude"},
      ])
      rows, coverage, tools = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      claude_row = next(r for r in rows if r["pid"] == 201)
      assert claude_row["category"] == "ai-coding-agent"
      claude_tool = next(t for t in tools if t["tool"] == "claude-code")
      assert claude_tool["nested_in"] == ["vscode-server"]

   def test_parent_chain_bounded_and_cycle_safe(self, proc_root):
      """A PPID cycle among ancestors must not hang or crash the walk.

      pids 300..311 form a 12-node ring (each one's parent is the previous
      one in the ring, wrapping around); pid 400 (claude) hangs off pid 300.
      Walking claude's ancestry must terminate at the 8-hop bound.
      """
      pids = []
      for i in range(12):
         pid = 300 + i
         ppid = 300 + (i - 1) if i > 0 else 300 + 11
         pids.append({"pid": pid, "comm": "sh", "ppid": ppid,
                      "cmdline": "sh -c foo"})
      pids.append(
         {"pid": 400, "comm": "claude", "ppid": 300, "cmdline": "claude"})
      proc_root(pids)
      rows, coverage, tools = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      entry = next(t for t in tools if t["tool"] == "claude-code")
      assert entry["proc_count"] == 1
      assert entry["nested_in"] == []

   def test_one_process_contributes_to_two_tools_not_summed(self, proc_root):
      """A Claude Code VS Code extension process legitimately matches both
      claude-code and vscode-server aggregates. Each tool's proc_count must
      independently reflect that one process; a caller summing proc_count
      across tools would double count it, which is why callers must not.
      """
      proc_root([
         {"pid": 500, "comm": "claude", "ppid": 1,
          "cmdline": "/home/u/.vscode-server/extensions/"
                     "anthropic.claude-code-1.2.3/bin/claude"},
      ])
      rows, coverage, tools = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      claude_tool = next(t for t in tools if t["tool"] == "claude-code")
      vscode_tool = next(t for t in tools if t["tool"] == "vscode-server")
      assert claude_tool["proc_count"] == 1
      assert vscode_tool["proc_count"] == 1

   def test_unknown_process_produces_no_tool_row(self, proc_root):
      proc_root([
         {"pid": 100, "comm": "bash", "cmdline": "-bash", "ppid": 1},
      ])
      rows, coverage, tools = probe._collect_processes(
         {0: "root"}, drop_raw_args=True, deadline=None)
      assert tools == []

   def test_match_tools_boundary_positives_and_false_positives(self):
      """Every tool family: a legitimate match plus a substring near-miss.

      Review round 1 finding: only the bare-name rules were boundary
      anchored; the longer marker strings (claude-code, jupyter,
      vscode-server, cursor-server) matched as plain substrings, so
      'myclaude-code-helper' and 'myjupyterhelper' falsely matched.
      """
      assert probe._match_tools("claude") == ["claude-code"]
      assert probe._match_tools("myclaude-code-helper") == []
      assert probe._match_tools("/x/notclaude-code-wrapper") == []

      assert probe._match_tools("codex exec --model gpt-5") == ["codex"]
      assert probe._match_tools("mycodexhelper --run") == []
      assert probe._match_tools("notcodex") == []

      assert probe._match_tools(
         "/home/u/.vscode-server/bin/abc/node server-main.js"
      ) == ["vscode-server"]
      assert probe._match_tools("notvscode-serverhelper") == []

      assert probe._match_tools(
         "/home/u/.cursor-server/bin/xyz/node server.js"
      ) == ["cursor-server"]
      assert probe._match_tools("notcursor-serverhelper") == []

      assert probe._match_tools(
         "/soft/python/bin/python -m ipykernel_launcher -f k.json"
      ) == ["jupyter"]
      assert probe._match_tools("myjupyterhelper") == []
      assert probe._match_tools("/x/myipykernelhelper") == []

   def test_match_tools_underscore_and_dotted_component_near_misses(self):
      """Review round 2 finding: `_` was already boundary-excluded, but the
      bare-word alternatives still treated `.` as a valid boundary
      character, so a `.` glued directly onto a marker (e.g. the unrelated
      path component "foo.codex") satisfied the old boundary check even
      though the tool-specific dotted markers (`.codex/`, `.vscode-server`,
      `.cursor-server`) were correctly anchored to a real path component.
      Also covers the underscore near-miss for every tool, not just
      claude-code, and confirms `ipykernel_launcher` still matches as the
      explicit suffix exception.
      """
      # Underscore near-misses across all five tool families.
      assert probe._match_tools("my_claude_helper") == []
      assert probe._match_tools("my_codex_helper") == []
      assert probe._match_tools("my_vscode_server_helper") == []
      assert probe._match_tools("my_cursor_server_helper") == []
      assert probe._match_tools("my_jupyter_helper") == []
      assert probe._match_tools("my_ipykernel_helper") == []

      # Dotted-component near-misses: the marker sits inside an unrelated
      # path component ("foo.codex", not a "/.codex/" component), so none
      # of these should match even though the bare marker (codex,
      # vscode-server, cursor-server) is present in the string.
      assert probe._match_tools("/opt/foo.codex/bin/node") == []
      assert probe._match_tools("/opt/foo.vscode-server/bin/node") == []
      assert probe._match_tools("/opt/foo.cursor-server/bin/node") == []

      # Real dotted path components must still match.
      assert probe._match_tools("/home/u/.codex/bin/node") == ["codex"]
      assert probe._match_tools(
         "/home/u/.vscode-server/bin/node") == ["vscode-server"]
      assert probe._match_tools(
         "/home/u/.cursor-server/bin/node") == ["cursor-server"]

      # Markers that legitimately contain an internal "." must still match
      # -- the boundary exclusion only applies at the marker's edges.
      assert probe._match_tools(
         "anthropic.claude-code extension.js") == ["claude-code"]
      assert probe._match_tools("openai.chatgpt cli") == ["codex"]

      # ipykernel_launcher remains the one explicit underscore exception.
      assert probe._match_tools(
         "ipykernel_launcher -f k.json") == ["jupyter"]


class TestInstallIdBoundaries:
   """Review round 1 finding: install-id extraction had no path-component
   anchoring, so a prefix glued to another word, a 41-hex near-miss, and a
   trailing extra character after the hex all falsely extracted an id.
   """

   def _hex40(self, ch="a"):
      return ch * 40

   def test_rejects_prefix_not_at_path_boundary(self):
      cmdline = "/x/fooStable-%s/node" % self._hex40()
      assert probe._extract_install_ids(cmdline) == []

   def test_rejects_41_hex_near_miss(self):
      cmdline = "Stable-%s" % ("a" * 41)
      assert probe._extract_install_ids(cmdline) == []

   def test_rejects_trailing_suffix_after_hex(self):
      cmdline = "code-%sz" % self._hex40()
      assert probe._extract_install_ids(cmdline) == []

   def test_accepts_valid_stable_and_code_forms(self):
      hex_id = self._hex40("b")
      assert probe._extract_install_ids(
         "/home/u/.vscode-server/bin/Stable-%s/node" % hex_id) == [hex_id]
      assert probe._extract_install_ids(
         "/home/u/.cursor-server/bin/code-%s/node" % hex_id) == [hex_id]

   def test_accepts_case_insensitive_and_dedupes(self):
      hex_id = self._hex40("c")
      cmdline = "/x/Stable-%s/node worker.js" % hex_id.upper()
      assert probe._extract_install_ids(cmdline) == [hex_id]


class TestToolAggregatesSubprocess:
   def test_census_subprocess_emits_tools_without_raw_args(self, tmp_path):
      root = str(tmp_path / "proc")
      fake_proc.write_proc(root, [
         {"pid": 100, "comm": "claude", "cmdline": "claude", "ppid": 1},
         {"pid": 200, "comm": "bash", "cmdline": "-bash"},
      ])
      env = dict(os.environ)
      env["NODE_MONITOR_PROC_ROOT"] = root
      result = subprocess.run(
         [sys.executable, PROBE_PATH, "--loop", "census"],
         capture_output=True, text=True, timeout=60, env=env)
      assert result.returncode == 0, result.stderr
      payload = json.loads(result.stdout)
      assert "tools" in payload
      assert {t["tool"] for t in payload["tools"]} == {"claude-code"}
      for row in payload["processes"]:
         assert "cmdline" not in row


# --------------------------------------------------------------------------
# Node counters
# --------------------------------------------------------------------------

class TestCounters:
   def test_counters_parse(self, proc_root):
      proc_root([])
      counters = probe._collect_node_counters()
      assert counters["load1"] == 53.92
      assert counters["procs_running"] == 42
      assert counters["procs_total"] == 6616
      assert counters["cpu_jiffies"]["user"] == 24771747
      assert counters["cpu_jiffies"]["idle"] == 8264041757
      assert counters["mem"]["total_kb"] == 527954108
      assert counters["mem"]["available_kb"] == 403844552
      assert counters["socket_count"] == 1234

   def test_loopback_excluded_from_net(self, proc_root):
      proc_root([])
      net = probe._collect_node_counters()["net"]
      assert "lo" not in net
      assert net["pub0"]["rx_bytes"] == 5000
      assert net["pub0"]["tx_bytes"] == 7000

   def test_clk_tck_reported_not_assumed(self, proc_root):
      """Ticks-per-second must be reported, never hardcoded to 100.

      Every CPU-seconds figure downstream divides by this.
      """
      proc_root([])
      assert probe._collect_node_counters()["clk_tck"] == os.sysconf("SC_CLK_TCK")

   def test_missing_lustre_is_not_an_error(self, proc_root):
      proc_root([])
      assert probe._collect_node_counters()["md_ops"] == {}


# --------------------------------------------------------------------------
# Argument parsing and exit codes
# --------------------------------------------------------------------------

class TestArgs:
   def test_defaults(self):
      opts = probe._parse_args(["probe"])
      assert opts["loop"] == "census"
      assert opts["drop_raw_args"] is True

   def test_counter_loop(self):
      assert probe._parse_args(["probe", "--loop", "counter"])["loop"] == "counter"

   def test_bad_loop_rejected(self):
      with pytest.raises(ValueError):
         probe._parse_args(["probe", "--loop", "nonsense"])

   def test_unknown_arg_rejected(self):
      with pytest.raises(ValueError):
         probe._parse_args(["probe", "--wat"])


class TestExitCodes:
   def _run(self, args, stdin="", env=None):
      return subprocess.run(
         [sys.executable, PROBE_PATH] + args,
         input=stdin, capture_output=True, text=True, timeout=60, env=env)

   def test_success_emits_one_json_line(self, fake_proc_env):
      result = self._run(["--loop", "counter"], env=fake_proc_env)
      assert result.returncode == 0, result.stderr
      lines = [line for line in result.stdout.split("\n") if line.strip()]
      assert len(lines) == 1, "probe must emit exactly one line"
      json.loads(lines[0])

   def test_bad_args_exit_2(self):
      assert self._run(["--loop", "bogus"]).returncode == 2

   def test_bad_proc_root_exit_3(self, tmp_path):
      """Confirms the exit-3 contract without depending on a real /proc.

      Points the probe at a directory that exists but has no uptime file --
      the same failure mode a denied/unmounted /proc produces on Linux.
      """
      env = dict(os.environ)
      env["NODE_MONITOR_PROC_ROOT"] = str(tmp_path / "empty")
      os.makedirs(env["NODE_MONITOR_PROC_ROOT"])
      result = self._run(["--loop", "counter"], env=env)
      assert result.returncode == 3

   def test_version_flag(self):
      result = self._run(["--version"])
      assert result.returncode == 0
      assert int(result.stdout.strip()) == probe.PROBE_VERSION


# --------------------------------------------------------------------------
# The JSON contract the daemon parses. Changing any of this must be a
# deliberate probe_version bump, not an accident.
# --------------------------------------------------------------------------

class TestPayloadContract:
   def _payload(self, loop, env):
      result = subprocess.run(
         [sys.executable, PROBE_PATH, "--loop", loop],
         capture_output=True, text=True, timeout=60, env=env)
      assert result.returncode == 0, result.stderr
      return json.loads(result.stdout)

   def test_counter_payload_shape(self, fake_proc_env):
      payload = self._payload("counter", fake_proc_env)
      for key in ("probe_version", "loop", "hostname_fqdn", "uptime_sec",
                  "monotonic_sec", "wall_clock_utc", "probe_self_seconds",
                  "counters", "python_version", "drop_raw_args"):
         assert key in payload, "missing %s" % key
      assert payload["loop"] == "counter"
      assert payload["probe_version"] == probe.PROBE_VERSION

   def test_counter_loop_emits_no_processes(self, fake_proc_env):
      """The counter loop must stay cheap.

      If it ever starts walking /proc, the two loops cost the same and the
      whole two-cadence design is pointless.
      """
      payload = self._payload("counter", fake_proc_env)
      assert "processes" not in payload
      assert "census_coverage" not in payload

   def test_census_payload_has_processes_and_coverage(self, fake_proc_env):
      payload = self._payload("census", fake_proc_env)
      assert isinstance(payload["processes"], list)
      assert payload["processes"], "census on the fake proc found no processes"
      coverage = payload["census_coverage"]
      assert coverage["pids_seen"] >= len(payload["processes"])

   def test_process_row_fields(self, fake_proc_env):
      payload = self._payload("census", fake_proc_env)
      required = {
         "pid", "ppid", "uid", "username", "comm", "category", "behavior",
         "activity", "activity_confidence", "project_path_hint",
         "utime_ticks", "stime_ticks", "rss_kb", "state",
         "start_time_ticks", "interactive",
      }
      for row in payload["processes"][:20]:
         missing = required - set(row)
         assert not missing, "row missing %s" % missing

   def test_identity_pair_present(self, fake_proc_env):
      """Delta math keys on (pid, start_time_ticks), never pid alone.

      A reused PID with lower cumulative CPU than its predecessor yields a
      negative delta that gets silently clamped to zero.
      """
      payload = self._payload("census", fake_proc_env)
      for row in payload["processes"][:20]:
         assert isinstance(row["pid"], int)
         assert isinstance(row["start_time_ticks"], int)

   def test_no_raw_cmdline_by_default(self, fake_proc_env):
      payload = self._payload("census", fake_proc_env)
      assert payload["drop_raw_args"] is True
      for row in payload["processes"]:
         assert "cmdline" not in row

   def test_stdout_is_pure_json(self, fake_proc_env):
      """Anything else on stdout makes the daemon's parse fail.

      Lmod and XALT both print to stdout on some ALCF shells.
      """
      result = subprocess.run(
         [sys.executable, PROBE_PATH, "--loop", "counter"],
         capture_output=True, text=True, timeout=60, env=fake_proc_env)
      assert result.stdout.count("\n") == 1
      json.loads(result.stdout)


# --------------------------------------------------------------------------
# hwinfo loop -- the static hardware inventory
# --------------------------------------------------------------------------

class TestHwinfoLoop:
   def _hwinfo_env(self, tmp_path, cpu_max_freq_khz=2000000,
                    nvidia_gpus=None, no_sys=False, net_speeds=None):
      proc_root = str(tmp_path / "proc")
      sys_root = str(tmp_path / "sys")
      fake_proc.write_proc(proc_root, [])
      fake_proc.write_proc_hwinfo(proc_root, nvidia_gpus=nvidia_gpus)
      env = dict(os.environ)
      env["NODE_MONITOR_PROC_ROOT"] = proc_root
      if not no_sys:
         fake_proc.write_sys_hwinfo(
            sys_root, cpu_max_freq_khz=cpu_max_freq_khz,
            net_speeds=net_speeds)
         env["NODE_MONITOR_SYS_ROOT"] = sys_root
      else:
         env["NODE_MONITOR_SYS_ROOT"] = str(tmp_path / "sys-does-not-exist")
      return env

   def _run(self, env):
      result = subprocess.run(
         [sys.executable, PROBE_PATH, "--loop", "hwinfo"],
         capture_output=True, text=True, timeout=60, env=env)
      assert result.returncode == 0, result.stderr
      lines = [line for line in result.stdout.split("\n") if line.strip()]
      assert len(lines) == 1, "probe must emit exactly one line"
      return json.loads(lines[0])

   def test_hwinfo_payload_shape(self, tmp_path):
      env = self._hwinfo_env(tmp_path)
      payload = self._run(env)
      for key in ("probe_version", "loop", "hostname_fqdn",
                  "wall_clock_utc", "probe_self_seconds", "python_version"):
         assert key in payload, "missing %s" % key
      assert payload["loop"] == "hwinfo"
      assert "hardware" in payload
      hw = payload["hardware"]
      required = {
         "cpu_model", "cpu_logical", "sockets", "cores_per_socket",
         "cpu_max_freq_khz", "numa_nodes", "mem_total_kb", "swap_total_kb",
         "hugepage_size_kb", "kernel_release", "os_pretty_name",
         "net_fs_mounts", "net_ifaces", "boot_id", "btime", "gpus",
      }
      missing = required - set(hw)
      assert not missing, "hardware payload missing %s" % missing

   def test_hwinfo_loop_has_no_counters_or_processes(self, tmp_path):
      """hwinfo must stay cheap -- no per-60s counters, no process walk."""
      env = self._hwinfo_env(tmp_path)
      payload = self._run(env)
      assert "counters" not in payload
      assert "processes" not in payload
      assert "census_coverage" not in payload

   def test_hwinfo_values(self, tmp_path):
      env = self._hwinfo_env(tmp_path)
      hw = self._run(env)["hardware"]
      assert hw["cpu_model"] == "AMD EPYC 7713 64-Core Processor"
      assert hw["cpu_logical"] == 2
      assert hw["sockets"] == 2
      assert hw["cores_per_socket"] == 64
      assert hw["numa_nodes"] == 2
      assert hw["net_fs_mounts"] == 2
      assert hw["net_ifaces"] == {"bond0": 1000, "ens10f0": 1000}
      assert hw["boot_id"] == "abc-123"
      assert hw["btime"] == 1700000000
      assert hw["kernel_release"] == "6.4.0"

   def test_cpu_max_freq_khz_read_from_sysfs(self, tmp_path):
      env = self._hwinfo_env(tmp_path, cpu_max_freq_khz=2098489)
      hw = self._run(env)["hardware"]
      assert hw["cpu_max_freq_khz"] == 2098489

   def test_cpu_max_freq_khz_null_when_sysfs_absent(self, tmp_path):
      """Absent sysfs file -> NULL. Must NEVER fall back to /proc/cpuinfo
      MHz -- that field is a live DVFS reading, not a hardware fact."""
      env = self._hwinfo_env(tmp_path, no_sys=True)
      hw = self._run(env)["hardware"]
      assert hw["cpu_max_freq_khz"] is None

   def test_missing_nvidia_smi_yields_empty_gpus(self, tmp_path, monkeypatch):
      monkeypatch.setenv("PATH", str(tmp_path / "empty-bin"))
      os.makedirs(str(tmp_path / "empty-bin"), exist_ok=True)
      env = self._hwinfo_env(tmp_path)
      env["PATH"] = str(tmp_path / "empty-bin")
      result = subprocess.run(
         [sys.executable, PROBE_PATH, "--loop", "hwinfo"],
         capture_output=True, text=True, timeout=60, env=env)
      assert result.returncode == 0, result.stderr
      payload = json.loads(result.stdout)
      assert payload["hardware"]["gpus"] == []

   def test_nvidia_gpus_from_proc_driver(self, tmp_path):
      env = self._hwinfo_env(tmp_path, nvidia_gpus=["0000:01:00.0"])
      hw = self._run(env)["hardware"]
      assert hw["gpus"] == ["0000:01:00.0"]

   def test_no_instantaneous_frequency_field_anywhere(self, tmp_path):
      """The specific mistake this task exists to prevent: cpu MHz is a
      live per-core DVFS reading (measured on the real fleet: 133/110/97
      distinct values across 256 cores in samples 2s apart) and must never
      appear in the static hardware payload under any name."""
      env = self._hwinfo_env(tmp_path)
      payload = self._run(env)
      raw = json.dumps(payload).lower()
      for forbidden in ("cpu_mhz", "\"mhz\"", "cur_freq", "curfreq"):
         assert forbidden not in raw, (
            "forbidden instantaneous-frequency field found: %s" % forbidden)
      # Confirm the allowed static field is exactly the one we expect.
      assert "cpu_max_freq_khz" in payload["hardware"]

   def test_net_ifaces_survives_bonding_masters_and_unreadable_speed(
         self, tmp_path):
      """Regression for the crash observed on all 3 reachable Polaris login
      nodes: `_collect_net_ifaces()` assumed every /sys/class/net entry is
      an interface directory containing a readable `speed` file. Neither
      holds on the real fleet:

      - `bonding_masters` is a plain FILE living alongside the interface
        dirs, not an interface -- opening `<it>/speed` raised
        NotADirectoryError and crashed the whole probe (exit non-zero,
        traceback on stdout instead of the required JSON line).
      - `lo` (and other down/unsupported interfaces) has a `speed` file
        that EXISTS but raises OSError on read; `os.path.exists()` alone
        cannot distinguish "readable" from "present but errors".

      Fails on unpatched `_collect_net_ifaces()` with the same
      NotADirectoryError traceback that killed the probe on
      polaris-login-02; must pass after the fix.
      """
      env = self._hwinfo_env(tmp_path, net_speeds={
         "hsn0": 200000,
         "bonding_masters": fake_proc.IFACE_AS_FILE,
         "lo": fake_proc.IFACE_UNREADABLE_SPEED,
         "ens10f1": None,
      })
      result = subprocess.run(
         [sys.executable, PROBE_PATH, "--loop", "hwinfo"],
         capture_output=True, text=True, timeout=60, env=env)
      assert result.returncode == 0, (
         "probe crashed instead of emitting JSON; stderr=%r" % result.stderr)
      lines = [line for line in result.stdout.split("\n") if line.strip()]
      assert len(lines) == 1, "probe must emit exactly one line"
      net_ifaces = json.loads(lines[0])["hardware"]["net_ifaces"]
      assert net_ifaces["hsn0"] == 200000
      assert net_ifaces["lo"] is None
      assert net_ifaces["ens10f1"] is None
      assert "bonding_masters" not in net_ifaces, (
         "bonding_masters is a control file, not an interface -- it must "
         "not appear in net_ifaces at all")

   def test_hanging_nvidia_smi_self_aborts_exit_4_not_0(self, tmp_path):
      """A wedged nvidia-smi must not let the probe report success past
      its own SIGALRM budget.

      Regression for review run #14: the broad `except Exception` around
      the nvidia-smi subprocess call also caught the probe's own
      ProbeTimeout, so a hanging nvidia-smi under a short --max-seconds
      returned exit 0 with a full JSON line instead of the required
      exit-4 self-abort. Reproduced here with a fake nvidia-smi that
      sleeps far longer than the global budget.
      """
      bin_dir = tmp_path / "fake-bin"
      bin_dir.mkdir()
      fake_nvidia_smi = bin_dir / "nvidia-smi"
      # Absolute path to the real sleep(1) -- replacing PATH wholesale with
      # bin_dir (matching the "PATH has only nvidia-smi on it" fixture
      # convention used elsewhere in this class) means a bare "sleep" in the
      # script would fail to resolve and exit immediately instead of hanging.
      sleep_bin = shutil.which("sleep") or "/bin/sleep"
      fake_nvidia_smi.write_text("#!/bin/sh\n%s 10\n" % sleep_bin)
      fake_nvidia_smi.chmod(0o755)
      env = self._hwinfo_env(tmp_path)
      env["PATH"] = str(bin_dir)
      result = subprocess.run(
         [sys.executable, PROBE_PATH, "--loop", "hwinfo",
          "--max-seconds", "0.1"],
         capture_output=True, text=True, timeout=60, env=env)
      assert result.returncode == 4, (
         "expected exit 4 (self-abort) got %r; stdout=%r stderr=%r"
         % (result.returncode, result.stdout, result.stderr))
      assert result.stdout == ""


# --------------------------------------------------------------------------
# In-process hwinfo collectors (proc_root fixture, no subprocess)
# --------------------------------------------------------------------------

class TestHwinfoCollectors:
   def test_collect_cpuinfo(self, proc_root):
      root = proc_root([])
      fake_proc.write_proc_hwinfo(root)
      cpuinfo = probe._collect_cpuinfo()
      assert cpuinfo["cpu_model"] == "AMD EPYC 7713 64-Core Processor"
      assert cpuinfo["cpu_logical"] == 2
      assert cpuinfo["sockets"] == 2
      assert cpuinfo["cores_per_socket"] == 64

   def test_collect_cpuinfo_absent_sockets_is_none(self, proc_root):
      root = proc_root([])
      fake_proc.write_proc_hwinfo(
         root, cpuinfo="processor\t: 0\nmodel name\t: Some CPU\n")
      cpuinfo = probe._collect_cpuinfo()
      assert cpuinfo["sockets"] is None
      assert cpuinfo["cores_per_socket"] is None

   def test_collect_net_fs_mounts_counts_only_network_fs(self, proc_root):
      root = proc_root([])
      fake_proc.write_proc_hwinfo(root, mounts=(
         "/dev/sda1 / ext4 rw 0 0\n"
         "tmpfs /tmp tmpfs rw 0 0\n"
         "srv:/a /a nfs rw 0 0\n"
         "srv:/b /b nfs4 rw 0 0\n"
         "mds@o2ib:/fs /lus lustre rw 0 0\n"))
      assert probe._collect_net_fs_mounts() == 3

   def test_collect_boot_id_strips_newline(self, proc_root):
      root = proc_root([])
      fake_proc.write_proc_hwinfo(root, boot_id="deadbeef-1234\n")
      assert probe._collect_boot_id() == "deadbeef-1234"

   def test_collect_btime(self, proc_root):
      root = proc_root([])
      fake_proc.write_proc_hwinfo(
         root, stat_line="cpu  1 2 3 4\nbtime 1699999999\n")
      assert probe._collect_btime() == 1699999999

   def test_collect_os_pretty_name(self, proc_root, tmp_path, monkeypatch):
      root = proc_root([])
      fake_proc.write_proc_hwinfo(root)
      os_release = tmp_path / "os-release"
      os_release.write_text(
         'NAME="SLES"\nPRETTY_NAME="SUSE Linux Enterprise Server 15 SP7"\n')
      monkeypatch.setattr(probe, "OS_RELEASE_PATH", str(os_release))
      assert probe._collect_os_pretty_name() == (
         "SUSE Linux Enterprise Server 15 SP7")

   def test_collect_os_pretty_name_null_when_absent(
         self, proc_root, tmp_path, monkeypatch):
      root = proc_root([])
      fake_proc.write_proc_hwinfo(root)
      monkeypatch.setattr(
         probe, "OS_RELEASE_PATH", str(tmp_path / "does-not-exist"))
      assert probe._collect_os_pretty_name() is None

   def test_collect_gpus_empty_without_nvidia(self, proc_root, monkeypatch):
      root = proc_root([])
      fake_proc.write_proc_hwinfo(root)
      monkeypatch.setattr(
         probe, "_collect_gpus_nvidia_smi", lambda: [])
      assert probe._collect_gpus() == []

   def test_collect_net_ifaces_survives_unallowlisted_oserror(
         self, tmp_path, monkeypatch):
      """The broad catch in _collect_net_ifaces() must turn ANY read
      failure into null, not just the errno values _read_text() already
      allowlists (EACCES/EPERM/ENOENT/ESRCH/EINVAL/EIO/ENXIO). Forces
      _read_text() to raise an OSError outside that allowlist (EBUSY) to
      prove the handler here -- not _read_text()'s own allowlist -- is
      what protects this call site.
      """
      sys_root = str(tmp_path / "sys")
      fake_proc.write_sys_hwinfo(
         sys_root, net_speeds={"hsn0": 200000, "ens10f1": None})
      monkeypatch.setattr(probe, "SYS_ROOT", sys_root)
      real_read_text = probe._read_text

      def _raising_read_text(path):
         if path.endswith(os.path.join("hsn0", "speed")):
            raise OSError(errno.EBUSY, "device or resource busy")
         return real_read_text(path)

      monkeypatch.setattr(probe, "_read_text", _raising_read_text)
      net_ifaces = probe._collect_net_ifaces()
      assert net_ifaces["hsn0"] is None
      assert net_ifaces["ens10f1"] is None

   def test_collect_net_ifaces_reraises_probe_timeout(
         self, tmp_path, monkeypatch):
      """ProbeTimeout must win over the broad "read failed -> null" catch,
      same pattern required of _collect_gpus_nvidia_smi (remote_probe.py
      :556) -- a hung/erroring speed read must not swallow the probe's own
      SIGALRM self-abort.
      """
      sys_root = str(tmp_path / "sys")
      fake_proc.write_sys_hwinfo(sys_root, net_speeds={"hsn0": 200000})
      monkeypatch.setattr(probe, "SYS_ROOT", sys_root)

      def _timing_out_read_text(path):
         raise probe.ProbeTimeout()

      monkeypatch.setattr(probe, "_read_text", _timing_out_read_text)
      with pytest.raises(probe.ProbeTimeout):
         probe._collect_net_ifaces()

   def test_collect_gpus_nvidia_smi_hang_yields_empty(self, monkeypatch):
      """A hanging/failing nvidia-smi must never propagate -- [] always."""
      import subprocess as sp

      def _raising_run(*args, **kwargs):
         raise sp.TimeoutExpired(cmd="nvidia-smi", timeout=3)

      monkeypatch.setattr("shutil.which", lambda name: "/usr/bin/nvidia-smi")
      monkeypatch.setattr(sp, "run", _raising_run)
      assert probe._collect_gpus_nvidia_smi() == []
