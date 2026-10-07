"""Task 8 aggregation correctness tests: multi-grain-per-interval fixture.

RED tests first: these prove the production bug where grains sharing the same
interval_end are mapped independently instead of being aggregated.

Fixture has FOUR grains across TWO intervals:
  Interval A (11:30): interactive/shell (cpu=120.5, dState=0.08/alice,
                       countMax=12/alice, rssMax=307200/alice)
              and batch/compute (cpu=300.2, dState=0.12/bob,
                       countMax=10/bob, rssMax=256000/bob)
     -> cpu SUM = 420.7; dState MAX = 0.12/bob; countMax = 12/alice; rssMax = 307200/alice

  Interval B (11:45): other/shell (cpu=50.0, dState=0.05/charlie,
                       countMax=5/charlie, rssMax=100000/charlie)
              and other/io (cpu=80.0, dState=0.20/diana,
                       countMax=3/diana, rssMax=200000/diana)
     -> cpu SUM = 130.0; dState MAX = 0.20/diana; countMax = 5/charlie; rssMax = 200000/diana

Note: countMax and rssMax winners can differ (independently attributed).
"""
import copy

import pytest


# Sentinel fixture: 4 grains, 2 per interval, with conflicting winner usernames
MULTI_GRAIN_SNAPSHOT = {
    "server_utc_now": "2026-10-03T12:00:00+00:00",
    "node": "login-04",
    "range_hours": 1,
    "hardware": {
        "system": "polaris",
        "source_hostname": "login-04",
        "mem_total_kb": 131072000,
        "cpu_model": "Intel Xeon",
        "cpu_logical": 96,
        "os_pretty_name": "RHEL 8",
    },
    "counters": {
        "rows": [
            {
                "window_end": "2026-10-03T11:57:00+00:00",
                "sample_count": 6,
                "expected_count": 6,
                "coverage": 1.0,
                "complete": True,
                "mem_available_kb": 65000000,
                "load1": 1.2,
                "load5": 0.9,
                "load15": 0.7,
                "procs_running": 3,
                "procs_total": 300,
                "cpu_busy_pct": {"p50": 20.0, "p95": 50.0, "max": 80.0},
                "network_rates": {
                    "eth0": {
                        "rx_bytes_per_sec": {"p50": 1200.5, "p95": 3000, "max": 5000},
                        "tx_bytes_per_sec": {"p50": 800.0, "p95": 1500, "max": 2500},
                    },
                },
                "lustre_md_summary": {
                    "read": {
                        "p50_sum": 120.5, "p95_sum": 300.0,
                        "max_sum": 500.0, "target_count": 2,
                    },
                },
            },
        ],
        "newest_window_end": "2026-10-03T11:57:00+00:00",
        "is_fresh": True,
        "status": "complete",
        "gaps": {"missing_count": 0},
        "latest": {
            "mem_used_physical_kb": 67072000,
            "load1": 1.2,
            "load5": 0.9,
            "load15": 0.7,
            "procs_running": 3,
            "procs_total": 300,
            "cpu_busy_pct": {"p50": 20.0, "p95": 50.0, "max": 80.0},
        },
    },
    "usage": {
        "grains": [
            # --- Interval A: two grains sharing the same interval_end ---
            {
                "interval_end": "2026-10-03T11:30:00+00:00",
                "category": "interactive",
                "activity": "shell",
                "cpu_seconds": 120.5,
                "complete": True,
                "process_count_p50": 6,
                "process_count_p50_username": "alice",
                "process_count_p95": 10,
                "process_count_p95_username": "alice",
                "process_count_max": 12,
                "process_count_max_username": "alice",
                "rss_p50_kb": 102400,
                "rss_p50_username": "alice",
                "rss_p95_kb": 204800,
                "rss_p95_username": "alice",
                "rss_max_kb": 307200,
                "rss_max_username": "alice",
                "d_state_fraction": 0.08,
                "d_state_username": "alice",
                "interactivity_fraction": 0.15,
                "interactivity_username": "alice",
            },
            {
                "interval_end": "2026-10-03T11:30:00+00:00",  # SAME interval as above
                "category": "batch",
                "activity": "compute",
                "cpu_seconds": 300.2,
                "complete": True,
                "process_count_p50": 2,
                "process_count_p50_username": "bob",
                "process_count_p95": 5,
                "process_count_p95_username": "bob",
                "process_count_max": 10,
                "process_count_max_username": "bob",
                "rss_p50_kb": 51200,
                "rss_p50_username": "bob",
                "rss_p95_kb": 153600,
                "rss_p95_username": "bob",
                "rss_max_kb": 256000,
                "rss_max_username": "bob",
                "d_state_fraction": 0.12,
                "d_state_username": "bob",
                "interactivity_fraction": 0.05,
                "interactivity_username": "bob",
            },
            # --- Interval B: two grains sharing a different interval_end ---
            {
                "interval_end": "2026-10-03T11:45:00+00:00",
                "category": "other",
                "activity": "shell",
                "cpu_seconds": 50.0,
                "complete": True,
                "process_count_p50": 3,
                "process_count_p50_username": "charlie",
                "process_count_p95": 4,
                "process_count_p95_username": "charlie",
                "process_count_max": 5,
                "process_count_max_username": "charlie",
                "rss_p50_kb": 40960,
                "rss_p50_username": "charlie",
                "rss_p95_kb": 81920,
                "rss_p95_username": "charlie",
                "rss_max_kb": 100000,
                "rss_max_username": "charlie",
                "d_state_fraction": 0.05,
                "d_state_username": "charlie",
                "interactivity_fraction": 0.30,
                "interactivity_username": "charlie",
            },
            {
                "interval_end": "2026-10-03T11:45:00+00:00",  # SAME interval as above
                "category": "other",
                "activity": "io",
                "cpu_seconds": 80.0,
                "complete": True,
                "process_count_p50": 1,
                "process_count_p50_username": "diana",
                "process_count_p95": 2,
                "process_count_p95_username": "diana",
                "process_count_max": 3,
                "process_count_max_username": "diana",
                "rss_p50_kb": 20480,
                "rss_p50_username": "diana",
                "rss_p95_kb": 61440,
                "rss_p95_username": "diana",
                "rss_max_kb": 200000,
                "rss_max_username": "diana",
                "d_state_fraction": 0.20,
                "d_state_username": "diana",
                "interactivity_fraction": 0.10,
                "interactivity_username": "diana",
            },
        ],
        "newest_interval_end": "2026-10-03T11:45:00+00:00",
        "is_fresh": True,
        "status": "complete",
        "gaps": {"missing_count": 0},
    },
    "poll_failures": [],
    "collection_log": [],
}

CONNECTED_TIMEOUT = 10000


def wait_connected(page):
    from playwright.sync_api import expect
    expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
        "Connected", timeout=CONNECTED_TIMEOUT
    )


def wait_chart_data(page):
    page.wait_for_function(
        "() => window.__nodeMonitorTest && window.__nodeMonitorTest.chartData "
        "&& window.__nodeMonitorTest.chartData.cpu "
        "&& window.__nodeMonitorTest.chartData.cpu.labels.length > 0",
        timeout=CONNECTED_TIMEOUT,
    )


def wait_lifecycle(page):
    page.wait_for_function(
        "() => window.__nodeMonitorTest && window.__nodeMonitorTest.getChartLifecycle "
        "&& window.__nodeMonitorTest.getChartLifecycle().createCount > 0",
        timeout=CONNECTED_TIMEOUT,
    )


@pytest.fixture
def multi_grain_web(browser_page):
    """Starts a LiveWeb server with the multi-grain (4-grain, 2-interval) fixture."""
    from tests.browser.conftest import LiveWeb
    snap = copy.deepcopy(MULTI_GRAIN_SNAPSHOT)
    web = LiveWeb(snap).start()
    try:
        yield web, browser_page
    finally:
        web.close()


# ===========================================================================
# Test 1: processCPU has ONE entry per distinct interval = SUM of cpu_seconds
# ===========================================================================

def test_process_cpu_one_entry_per_interval_is_sum(multi_grain_web):
    """RED: processCPU must have 2 entries (one per interval), each = SUM of grains.

    With 4 grains across 2 intervals:
      Interval A (11:30): 120.5 + 300.2 = 420.7
      Interval B (11:45): 50.0  + 80.0  = 130.0

    Bug: current code emits one entry per grain (4 entries), not one per interval.
    """
    web, (page, errors, _external) = multi_grain_web
    page.goto(web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")
    cpu_sum = data["processCPU"]
    labels = data["processLabels"]

    # Must have exactly 2 entries (one per distinct interval_end)
    assert len(cpu_sum) == 2, (
        f"processCPU must have 2 entries (one per interval), got {len(cpu_sum)}: {cpu_sum}"
    )

    # Interval A sum: 120.5 + 300.2 = 420.7
    assert abs(cpu_sum[0] - 420.7) < 0.001, (
        f"Interval A cpu_seconds SUM should be 420.7, got {cpu_sum[0]}"
    )

    # Interval B sum: 50.0 + 80.0 = 130.0
    assert abs(cpu_sum[1] - 130.0) < 0.001, (
        f"Interval B cpu_seconds SUM should be 130.0, got {cpu_sum[1]}"
    )

    # Labels must show interval_end, not category/activity (2 labels for 2 intervals)
    assert len(labels) == 2, (
        f"processLabels must have 2 entries (one per interval), got {len(labels)}: {labels}"
    )
    # Labels are local-time HH:MM; minutes are timezone-invariant
    assert labels[0].endswith(":30"), f"Interval A label must end with :30: {labels[0]}"
    assert labels[1].endswith(":45"), f"Interval B label must end with :45: {labels[1]}"
    # Labels must NOT contain category/activity as if per-grain
    assert "interactive" not in labels[0] and "batch" not in labels[0], (
        f"Label must not contain per-grain category, got: {labels[0]}"
    )

    assert errors == []


# ===========================================================================
# Test 2: D-state has ONE point per interval = MAX fraction + exact username
# ===========================================================================

def test_d_state_one_point_per_interval_is_max(multi_grain_web):
    """RED: dStatePoints must have 2 entries (one per interval), fraction = max grain, correct username.

    Interval A (11:30): grains dState = 0.08/alice, 0.12/bob -> max = 0.12/bob
    Interval B (11:45): grains dState = 0.05/charlie, 0.20/diana -> max = 0.20/diana

    Bug: current code emits one point per grain (4 points), not max per interval.
    """
    web, (page, errors, _external) = multi_grain_web
    page.goto(web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")
    d_points = data["dStatePoints"]

    # Must have exactly 2 points (one per distinct interval_end)
    assert len(d_points) == 2, (
        f"dStatePoints must have 2 entries (one per interval), got {len(d_points)}: {d_points}"
    )

    # Interval A: max dState = 0.12 (bob wins over alice's 0.08)
    assert abs(d_points[0]["value"] - 0.12) < 0.001, (
        f"Interval A d_state_fraction should be max=0.12, got {d_points[0]['value']}"
    )
    assert d_points[0]["username"] == "bob", (
        f"Interval A d_state winning username should be 'bob', got {d_points[0]['username']!r}"
    )
    assert "11:30" in d_points[0]["interval_end"], (
        f"Interval A interval_end should contain 11:30, got {d_points[0]['interval_end']!r}"
    )

    # Interval B: max dState = 0.20 (diana wins over charlie's 0.05)
    assert abs(d_points[1]["value"] - 0.20) < 0.001, (
        f"Interval B d_state_fraction should be max=0.20, got {d_points[1]['value']}"
    )
    assert d_points[1]["username"] == "diana", (
        f"Interval B d_state winning username should be 'diana', got {d_points[1]['username']!r}"
    )
    assert "11:45" in d_points[1]["interval_end"], (
        f"Interval B interval_end should contain 11:45, got {d_points[1]['interval_end']!r}"
    )

    assert errors == []


# ===========================================================================
# Test 3: processCountMax has ONE entry per interval = MAX hotspot + correct username
# ===========================================================================

def test_process_count_max_one_per_interval_is_hotspot(multi_grain_web):
    """RED: processCountMax must have 2 entries, each = max grain's process_count_max + username.

    Interval A (11:30): grains countMax = 12/alice, 10/bob -> max = 12/alice
    Interval B (11:45): grains countMax = 5/charlie, 3/diana -> max = 5/charlie

    Bug: current code emits one value per grain (4 values), not max per interval.
    """
    web, (page, errors, _external) = multi_grain_web
    page.goto(web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")
    count_max = data["processCountMax"]
    usernames = data["processCountMaxUsername"]

    assert len(count_max) == 2, (
        f"processCountMax must have 2 entries (one per interval), got {len(count_max)}: {count_max}"
    )
    assert count_max[0] == 12, (
        f"Interval A processCountMax should be 12 (alice wins), got {count_max[0]}"
    )
    assert usernames[0] == "alice", (
        f"Interval A count max username should be 'alice', got {usernames[0]!r}"
    )

    assert count_max[1] == 5, (
        f"Interval B processCountMax should be 5 (charlie wins), got {count_max[1]}"
    )
    assert usernames[1] == "charlie", (
        f"Interval B count max username should be 'charlie', got {usernames[1]!r}"
    )

    assert errors == []


# ===========================================================================
# Test 4: processRSSMax has ONE entry per interval = MAX hotspot + correct username
#         (winner may differ from processCountMax winner)
# ===========================================================================

def test_process_rss_max_one_per_interval_is_hotspot_independent(multi_grain_web):
    """RED: processRSSMax must have 2 entries, max rss_max_kb + independently attributed username.

    Interval A (11:30): grains rssMax = 307200/alice, 256000/bob -> max = 307200/alice
    Interval B (11:45): grains rssMax = 100000/charlie, 200000/diana -> max = 200000/diana

    Note for Interval B: countMax winner=charlie but rssMax winner=diana (independent attribution).

    Bug: current code emits one value per grain (4 values).
    """
    web, (page, errors, _external) = multi_grain_web
    page.goto(web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")
    rss_max = data["processRSSMax"]
    rss_usernames = data["processRSSMaxUsername"]

    assert len(rss_max) == 2, (
        f"processRSSMax must have 2 entries (one per interval), got {len(rss_max)}: {rss_max}"
    )
    assert rss_max[0] == 307200, (
        f"Interval A processRSSMax should be 307200 (alice wins), got {rss_max[0]}"
    )
    assert rss_usernames[0] == "alice", (
        f"Interval A rss max username should be 'alice', got {rss_usernames[0]!r}"
    )

    assert rss_max[1] == 200000, (
        f"Interval B processRSSMax should be 200000 (diana wins), got {rss_max[1]}"
    )
    assert rss_usernames[1] == "diana", (
        f"Interval B rss max username should be 'diana', got {rss_usernames[1]!r}"
    )

    # The RSS winner in Interval B (diana) differs from count winner (charlie) -- correct
    count_usernames = data["processCountMaxUsername"]
    assert count_usernames[1] == "charlie", (
        f"Interval B count username should be 'charlie' (independent of RSS), "
        f"got {count_usernames[1]!r}"
    )

    assert errors == []


# ===========================================================================
# Test 5: Labels show interval_end, NOT category/activity from individual grains
# ===========================================================================

def test_labels_show_interval_not_category_activity(multi_grain_web):
    """RED: processLabels must show interval_end (one per interval), not grain category/activity.

    With 4 grains across 2 intervals, labels should be 2 entries referencing the
    interval timestamp, not 4 entries containing 'interactive', 'batch', 'other'.
    """
    web, (page, errors, _external) = multi_grain_web
    page.goto(web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")
    labels = data["processLabels"]

    assert len(labels) == 2, (
        f"processLabels must have 2 entries (one per interval), got {len(labels)}: {labels}"
    )

    # Labels must be HH:MM local-time strings matching the interval minutes
    for lbl in labels:
        assert len(lbl) == 5 and ':' in lbl, f"Label should be HH:MM format, got: {lbl!r}"
    assert labels[0].endswith(":30"), f"First label should end with :30, got: {labels[0]!r}"
    assert labels[1].endswith(":45"), f"Second label should end with :45, got: {labels[1]!r}"

    # Labels must NOT be polluted with individual grain categories/activities
    for lbl in labels:
        assert "interactive" not in lbl, f"Label must not contain grain category: {lbl!r}"
        assert "batch" not in lbl, f"Label must not contain grain category: {lbl!r}"
        assert "compute" not in lbl, f"Label must not contain grain activity: {lbl!r}"

    assert errors == []


# ===========================================================================
# Test 6: processCountP50/P95 and processRSSP50/P95 not summed/averaged;
#         if exposed, must be max-per-interval with correct attribution
# ===========================================================================

def test_process_percentiles_are_max_per_interval_not_sum(multi_grain_web):
    """RED: processCountP50/P95 and processRSSP50/P95 must not be summed or averaged.

    If these arrays are exposed, they should have exactly 2 entries (one per interval)
    and represent the MAX-grain value per interval (with correct attribution).
    If they are not exposed or are empty, that is also acceptable (unused arrays removed).

    Interval A (11:30): countP50 grains = 6/alice, 2/bob -> max = 6/alice
    Interval A (11:30): rssP50 grains = 102400/alice, 51200/bob -> max = 102400/alice
    Interval B (11:45): countP50 grains = 3/charlie, 1/diana -> max = 3/charlie
    """
    web, (page, errors, _external) = multi_grain_web
    page.goto(web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")

    # If processCountP50 is exposed in chartData, it must have 2 entries (one per interval)
    if "processCountP50" in data:
        p50 = data["processCountP50"]
        assert len(p50) == 2, (
            f"processCountP50 must have 2 entries (one per interval), "
            f"got {len(p50)}: {p50}"
        )
        # Must not be a sum (6+2=8 would be wrong; max=6 is correct)
        assert p50[0] != 8, (
            f"processCountP50[0] must not be sum (8), it should be max (6): {p50[0]}"
        )
        assert p50[0] == 6, f"Interval A processCountP50 max should be 6, got {p50[0]}"

    if "processRSSP50" in data:
        rss_p50 = data["processRSSP50"]
        assert len(rss_p50) == 2, (
            f"processRSSP50 must have 2 entries (one per interval), "
            f"got {len(rss_p50)}: {rss_p50}"
        )
        # Must not be summed (102400+51200=153600 would be wrong; max=102400 is correct)
        assert rss_p50[0] != 153600, (
            f"processRSSP50[0] must not be sum (153600): {rss_p50[0]}"
        )
        assert rss_p50[0] == 102400, (
            f"Interval A processRSSP50 max should be 102400, got {rss_p50[0]}"
        )

    assert errors == []


# ===========================================================================
# Test 7: D-state observation-weighted note and interactivity max semantics
# ===========================================================================

def test_interactivity_max_semantics_per_interval(multi_grain_web):
    """RED: interactivity (if exposed) should follow max-grain semantics per interval.

    Interval A (11:30): interactivity grains = 0.15/alice, 0.05/bob -> max = 0.15/alice
    Interval B (11:45): interactivity grains = 0.30/charlie, 0.10/diana -> max = 0.30/charlie

    If interactivity is not exposed in chartData, test that dStatePoints observation
    note is accessible in the DOM.
    """
    web, (page, errors, _external) = multi_grain_web
    page.goto(web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")

    # Check interactivity fields if exposed
    if "interactivityPoints" in data:
        points = data["interactivityPoints"]
        assert len(points) == 2, (
            f"interactivityPoints must have 2 entries (one per interval), "
            f"got {len(points)}: {points}"
        )
        assert abs(points[0]["value"] - 0.15) < 0.001, (
            f"Interval A interactivity max should be 0.15, got {points[0]['value']}"
        )
        assert points[0]["username"] == "alice", (
            f"Interval A interactivity winner should be 'alice', got {points[0]['username']!r}"
        )
        assert abs(points[1]["value"] - 0.30) < 0.001, (
            f"Interval B interactivity max should be 0.30, got {points[1]['value']}"
        )
        assert points[1]["username"] == "charlie", (
            f"Interval B interactivity winner should be 'charlie', got {points[1]['username']!r}"
        )

    # The CPU/load chart must have an observation-weighted note in its accessible element
    # (this verifies D-state points are represented, not silently omitted)
    obs_note = page.locator('[aria-label="CPU and load chart"]').inner_text()
    assert "observation-weighted" in obs_note.lower(), (
        f"CPU/load summary table must contain 'observation-weighted' note. "
        f"Got: {obs_note!r}"
    )

    assert errors == []


# ===========================================================================
# Test 8: Memory Percent button is actually disabled when mem_total_kb is null
# ===========================================================================

def test_memory_percent_button_disabled_when_total_null(browser_page, live_web):
    """RED: mem-mode-percent button must have disabled attribute when mem_total_kb is null.

    Current code only guards the isPercent logic (skips computing values) but does
    not set the disabled attribute on the button, so users can click Percent and see
    nothing useful. The button should be disabled=true when total is null/zero.
    """
    from tests.browser.conftest import LiveWeb
    snap = {
        "server_utc_now": "2026-10-03T12:00:00+00:00",
        "node": "login-04",
        "range_hours": 1,
        "hardware": {
            "system": "polaris",
            "source_hostname": "login-04",
            "mem_total_kb": None,  # null total
            "cpu_model": "Intel Xeon",
            "cpu_logical": 96,
            "os_pretty_name": "RHEL 8",
        },
        "counters": {
            "rows": [
                {
                    "window_end": "2026-10-03T11:57:00+00:00",
                    "sample_count": 6, "expected_count": 6,
                    "coverage": 1.0, "complete": True,
                    "mem_available_kb": 65000000,
                    "load1": 1.0, "load5": 0.9, "load15": 0.8,
                    "procs_running": 2, "procs_total": 200,
                    "cpu_busy_pct": {"p50": 10.0, "p95": 20.0, "max": 30.0},
                    "network_rates": {},
                    "lustre_md_summary": {},
                },
            ],
            "newest_window_end": "2026-10-03T11:57:00+00:00",
            "is_fresh": True, "status": "complete",
            "gaps": {"missing_count": 0},
            "latest": {"load1": 1.0},
        },
        "usage": {
            "grains": [],
            "newest_interval_end": None,
            "is_fresh": False, "status": "empty",
            "gaps": {"missing_count": 0},
        },
        "poll_failures": [],
        "collection_log": [],
    }

    live_web.state.set_snapshot(snap)

    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    # Wait for memory controls to appear
    page.wait_for_function(
        "() => document.querySelector('[data-testid=\"mem-mode-percent\"]') !== null",
        timeout=CONNECTED_TIMEOUT,
    )

    percent_btn = page.locator('[data-testid="mem-mode-percent"]')
    # The button must be disabled when mem_total_kb is null
    is_disabled = percent_btn.evaluate("el => el.disabled === true")
    assert is_disabled, (
        "mem-mode-percent button must be disabled (disabled=true) when mem_total_kb is null; "
        "current code only guards isPercent logic but doesn't set disabled attribute"
    )

    assert errors == []


def test_memory_percent_button_disabled_when_total_zero(browser_page, live_web):
    """RED: mem-mode-percent button must have disabled attribute when mem_total_kb is 0."""
    from tests.browser.conftest import LiveWeb
    snap = {
        "server_utc_now": "2026-10-03T12:00:00+00:00",
        "node": "login-04",
        "range_hours": 1,
        "hardware": {
            "system": "polaris",
            "source_hostname": "login-04",
            "mem_total_kb": 0,  # zero total
            "cpu_model": "Intel Xeon",
            "cpu_logical": 96,
            "os_pretty_name": "RHEL 8",
        },
        "counters": {
            "rows": [
                {
                    "window_end": "2026-10-03T11:57:00+00:00",
                    "sample_count": 6, "expected_count": 6,
                    "coverage": 1.0, "complete": True,
                    "mem_available_kb": 65000000,
                    "load1": 1.0, "load5": 0.9, "load15": 0.8,
                    "procs_running": 2, "procs_total": 200,
                    "cpu_busy_pct": {"p50": 10.0, "p95": 20.0, "max": 30.0},
                    "network_rates": {},
                    "lustre_md_summary": {},
                },
            ],
            "newest_window_end": "2026-10-03T11:57:00+00:00",
            "is_fresh": True, "status": "complete",
            "gaps": {"missing_count": 0},
            "latest": {"load1": 1.0},
        },
        "usage": {
            "grains": [],
            "newest_interval_end": None,
            "is_fresh": False, "status": "empty",
            "gaps": {"missing_count": 0},
        },
        "poll_failures": [],
        "collection_log": [],
    }

    live_web.state.set_snapshot(snap)

    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    page.wait_for_function(
        "() => document.querySelector('[data-testid=\"mem-mode-percent\"]') !== null",
        timeout=CONNECTED_TIMEOUT,
    )

    percent_btn = page.locator('[data-testid="mem-mode-percent"]')
    is_disabled = percent_btn.evaluate("el => el.disabled === true")
    assert is_disabled, (
        "mem-mode-percent button must be disabled when mem_total_kb is 0"
    )

    assert errors == []


# ===========================================================================
# Test 9: Control button clicks cause lifecycle chart replacement (not just state change)
# ===========================================================================

def test_proc_mode_button_click_causes_chart_update(browser_page, live_web):
    """Controls: clicking proc-mode-max updates charts in-place (no destroy+create)."""
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    lc_before = page.evaluate("() => window.__nodeMonitorTest.getChartLifecycle()")
    assert lc_before["createCount"] == 4, f"Expected 4 initial creates, got {lc_before}"

    # Click 'Process max' button
    page.locator('[data-testid="proc-mode-max"]').click()
    # Wait for render count to increase (in-place update)
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartLifecycle().renders.process >= 2",
        timeout=CONNECTED_TIMEOUT,
    )

    lc_after = page.evaluate("() => window.__nodeMonitorTest.getChartLifecycle()")
    assert lc_after["createCount"] == 4, (
        f"Clicking Process max should not create new charts (in-place update), "
        f"got createCount={lc_after['createCount']}"
    )
    assert lc_after["destroyCount"] == 0, (
        f"Clicking Process max should not destroy charts (in-place update), "
        f"got destroyCount={lc_after['destroyCount']}"
    )

    # aria-pressed state must be updated correctly
    max_btn = page.locator('[data-testid="proc-mode-max"]')
    cpusum_btn = page.locator('[data-testid="proc-mode-cpusum"]')
    assert max_btn.get_attribute("aria-pressed") == "true", \
        "proc-mode-max must have aria-pressed=true after click"
    assert cpusum_btn.get_attribute("aria-pressed") == "false", \
        "proc-mode-cpusum must have aria-pressed=false after switching to max"

    assert errors == []


def test_mem_mode_gib_click_causes_chart_update(browser_page, live_web):
    """Controls: clicking mem-mode-gib triggers in-place chart update."""
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    lc_before = page.evaluate("() => window.__nodeMonitorTest.getChartLifecycle()")
    assert lc_before["createCount"] == 4

    # Click GiB button (already selected, but should still re-render to verify binding)
    page.locator('[data-testid="mem-mode-gib"]').click()
    # Wait for render count to increase (in-place update)
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartLifecycle().renders.memory >= 2",
        timeout=CONNECTED_TIMEOUT,
    )

    lc_after = page.evaluate("() => window.__nodeMonitorTest.getChartLifecycle()")
    assert lc_after["createCount"] == 4, (
        f"Clicking mem-mode-gib should not create new charts (in-place update), "
        f"got createCount={lc_after['createCount']}"
    )

    assert errors == []


# ===========================================================================
# Test 10: D-state points visibly represented in CPU chart or accessible table
# ===========================================================================

def test_d_state_points_exposed_in_chartdata(multi_grain_web):
    """D-state must be present in chartData.dStatePoints after aggregation.

    With 4 grains across 2 intervals, dStatePoints must have exactly 2 entries
    (not 4 per-grain entries and not 0).
    This confirms the chart rendering can use them and the accessible alt table
    can display them.
    """
    web, (page, errors, _external) = multi_grain_web
    page.goto(web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")

    assert "dStatePoints" in data, "chartData must expose dStatePoints"
    d_points = data["dStatePoints"]
    assert len(d_points) > 0, "dStatePoints must be non-empty when grains have d_state_fraction"
    assert len(d_points) == 2, (
        f"dStatePoints must have 2 entries (one per interval, max-aggregated), "
        f"got {len(d_points)}: {d_points}"
    )

    # Verify values and usernames are the MAX per interval
    values = [p["value"] for p in d_points]
    assert 0.12 in values, f"Interval A max d_state (0.12/bob) missing: {values}"
    assert 0.20 in values, f"Interval B max d_state (0.20/diana) missing: {values}"

    assert errors == []


# ===========================================================================
# Test 11: Network/Lustre control button clicks cause chart redraw
# ===========================================================================

def test_nl_lustre_p50_button_click_causes_chart_update(browser_page, live_web):
    """Controls: clicking Lustre p50-sum button updates chart in-place and updates aria-pressed."""
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    lc_before = page.evaluate("() => window.__nodeMonitorTest.getChartLifecycle()")
    assert lc_before["createCount"] == 4

    page.locator('[data-testid="nl-mode-lustre-p50"]').click()
    # Wait for render count to increase (in-place update)
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartLifecycle().renders.networkLustre >= 2",
        timeout=CONNECTED_TIMEOUT,
    )

    lc_after = page.evaluate("() => window.__nodeMonitorTest.getChartLifecycle()")
    assert lc_after["createCount"] == 4, (
        f"Clicking Lustre p50-sum should not create new charts (in-place update), "
        f"got createCount={lc_after['createCount']}"
    )

    # aria-pressed must update
    assert page.locator('[data-testid="nl-mode-lustre-p50"]').get_attribute("aria-pressed") == "true"
    assert page.locator('[data-testid="nl-network-p50"]').get_attribute("aria-pressed") == "false"

    assert errors == []
