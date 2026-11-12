from node_monitor.pipeline.tags import TAGS, TagRule, tags_for


def test_tags_match_flag_variants():
   # Seed must contain claude-code --dangerously-skip-permissions -> agent.skip_permissions.
   assert any(
      r["activity"] == "claude-code"
      and r["pattern"] == "--dangerously-skip-permissions"
      and r["tag"] == "agent.skip_permissions"
      for r in TAGS
   )
   # Exact token match: flag present in different positions still matches.
   for argv in [
      ["claude", "--dangerously-skip-permissions"],
      ["claude", "--model", "opus", "--dangerously-skip-permissions", "--verbose"],
      ["--dangerously-skip-permissions", "claude"],
   ]:
      assert "agent.skip_permissions" in tags_for("claude-code", argv)
   # Variants: at least one other pattern maps to the same tag.
   variants = [r for r in TAGS if r["tag"] == "agent.skip_permissions"]
   assert len(variants) >= 2
   assert len({r["pattern"] for r in variants}) >= 2
   # Exact token: substring or extended flag must not match.
   assert tags_for("claude-code", ["claude", "--dangerously-skip-permissions-extra"]) == []
   assert tags_for("claude-code", ["claude", "prefix--dangerously-skip-permissions"]) == []
   # Activity mismatch must not match even if flag present.
   assert tags_for("other", ["claude", "--dangerously-skip-permissions"]) == []
   assert tags_for(None, ["claude", "--dangerously-skip-permissions"]) == []


def test_tags_sorted_unique():
   # Find a case with multiple tags or duplicate tag via duplicate patterns.
   # Inject two distinct rules that both match and produce distinct tags, verify sorted unique.
   base = list(TAGS)
   try:
      TAGS.append(TagRule(activity="claude-code", pattern="--flag-a", tag="zzz.tag"))
      TAGS.append(TagRule(activity="claude-code", pattern="--flag-b", tag="aaa.tag"))
      # Duplicate tag via two patterns mapping to same tag should dedup.
      TAGS.append(TagRule(activity="claude-code", pattern="--flag-c", tag="aaa.tag"))
      result = tags_for("claude-code", ["claude", "--flag-a", "--flag-b", "--flag-c"])
      assert result == sorted(set(result))
      assert result == ["aaa.tag", "zzz.tag"]
   finally:
      TAGS[:] = base
   # Seed TAGS itself should be deduped and sorted when matched.
   # claude-code with its seeded flag appears once even if multiple variants map to same tag.
   result = tags_for("claude-code", ["claude", "--dangerously-skip-permissions"])
   assert result == sorted(set(result))


def test_tags_no_match_returns_empty():
   assert tags_for("claude-code", ["claude", "--verbose"]) == []
   assert tags_for("claude-code", []) == []
   assert tags_for(None, ["claude", "--dangerously-skip-permissions"]) == []
   assert tags_for("python-generic", ["python", "train.py"]) == []
   # Flag for other activity must not match.
   assert tags_for("python-generic", ["claude", "--dangerously-skip-permissions"]) == []


def test_tags_new_flag_added_via_table_without_code_change():
   base = list(TAGS)
   try:
      TAGS.append(TagRule(activity="test-activity", pattern="--new-test-flag", tag="test.new_flag"))
      assert tags_for("test-activity", ["tool", "--new-test-flag"]) == ["test.new_flag"]
      assert tags_for("test-activity", ["tool", "--other"]) == []
      assert tags_for("other-activity", ["tool", "--new-test-flag"]) == []
   finally:
      TAGS[:] = base
   # After removal the temporary rule no longer matches.
   assert tags_for("test-activity", ["tool", "--new-test-flag"]) == []
