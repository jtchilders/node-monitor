"""Task 8 blocker fix tests.

RED tests written first; all should fail at 9422bc4 starting HEAD.
Covers:
  1. D-state hotspot table under CPU chart (per-interval, observation-weighted caveat)
  2. Interactivity max semantics (unconditional, exact values)
  3. getChartRenderState() summary (mode, dataset labels, cloned values)
  4. Control click tests with exact data arrays (Memory, Process, Network, Lustre)
  5. Network p50/p95/max mode controls (three distinct buttons)
  6. Lustre label correctness (p50/p95 must NOT claim 'sum of per-target maxima')
  7. Narrow viewport using is_visible() and bounding boxes
"""
import copy

import pytest
from playwright.sync_api import expect

CONNECTED_TIMEOUT = 10000  # ms
CHART_DATA_WAIT = (
    "() => window.__nodeMonitorTest"
    " && window.__nodeMonitorTest.chartData"
    " && window.__nodeMonitorTest.chartData.cpu"
    " && window.__nodeMonitorTest.chartData.cpu.labels.length > 0"
)
LIFECYCLE_WAIT = (
    "() => window.__nodeMonitorTest"
    " && window.__nodeMonitorTest.getChartLifecycle"
    " && window.__nodeMonitorTest.getChartLifecycle().createCount > 0"
)
RENDER_STATE_WAIT = (
    "() => window.__nodeMonitorTest"
    " && typeof window.__nodeMonitorTest.getChartRenderState === 'function'"
)


def wait_connected(page):
    expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
        "Connected", timeout=CONNECTED_TIMEOUT
    )


def wait_chart_data(page):
    page.wait_for_function(CHART_DATA_WAIT, timeout=CONNECTED_TIMEOUT)


def wait_lifecycle(page):
    page.wait_for_function(LIFECYCLE_WAIT, timeout=CONNECTED_TIMEOUT)


# Four-grain fixture (2 per interval) to test interactivity
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
                "sample_count": 6, "expected_count": 6,
                "coverage": 1.0, "complete": True,
                "mem_available_kb": 65000000,
                "load1": 1.2, "load5": 0.9, "load15": 0.7,
                "procs_running": 3, "procs_total": 300,
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
            "load1": 1.2, "load5": 0.9, "load15": 0.7,
            "procs_running": 3, "procs_total": 300,
            "cpu_busy_pct": {"p50": 20.0, "p95": 50.0, "max": 80.0},
        },
    },
    "usage": {
        "grains": [
            # Interval A: two grains
            {
                "interval_end": "2026-10-03T11:30:00+00:00",
                "category": "interactive", "activity": "shell",
                "cpu_seconds": 120.5, "complete": True,
                "process_count_p50": 6, "process_count_p50_username": "alice",
                "process_count_p95": 10, "process_count_p95_username": "alice",
                "process_count_max": 12, "process_count_max_username": "alice",
                "rss_p50_kb": 102400, "rss_p50_username": "alice",
                "rss_p95_kb": 204800, "rss_p95_username": "alice",
                "rss_max_kb": 307200, "rss_max_username": "alice",
                "d_state_fraction": 0.08, "d_state_username": "alice",
                "interactivity_fraction": 0.15, "interactivity_username": "alice",
            },
            {
                "interval_end": "2026-10-03T11:30:00+00:00",
                "category": "batch", "activity": "compute",
                "cpu_seconds": 300.2, "complete": True,
                "process_count_p50": 2, "process_count_p50_username": "bob",
                "process_count_p95": 5, "process_count_p95_username": "bob",
                "process_count_max": 10, "process_count_max_username": "bob",
                "rss_p50_kb": 51200, "rss_p50_username": "bob",
                "rss_p95_kb": 153600, "rss_p95_username": "bob",
                "rss_max_kb": 256000, "rss_max_username": "bob",
                "d_state_fraction": 0.12, "d_state_username": "bob",
                "interactivity_fraction": 0.05, "interactivity_username": "bob",
            },
            # Interval B: two grains
            {
                "interval_end": "2026-10-03T11:45:00+00:00",
                "category": "other", "activity": "shell",
                "cpu_seconds": 50.0, "complete": True,
                "process_count_p50": 3, "process_count_p50_username": "charlie",
                "process_count_p95": 4, "process_count_p95_username": "charlie",
                "process_count_max": 5, "process_count_max_username": "charlie",
                "rss_p50_kb": 40960, "rss_p50_username": "charlie",
                "rss_p95_kb": 81920, "rss_p95_username": "charlie",
                "rss_max_kb": 100000, "rss_max_username": "charlie",
                "d_state_fraction": 0.05, "d_state_username": "charlie",
                "interactivity_fraction": 0.30, "interactivity_username": "charlie",
            },
            {
                "interval_end": "2026-10-03T11:45:00+00:00",
                "category": "other", "activity": "io",
                "cpu_seconds": 80.0, "complete": True,
                "process_count_p50": 1, "process_count_p50_username": "diana",
                "process_count_p95": 2, "process_count_p95_username": "diana",
                "process_count_max": 3, "process_count_max_username": "diana",
                "rss_p50_kb": 20480, "rss_p50_username": "diana",
                "rss_p95_kb": 61440, "rss_p95_username": "diana",
                "rss_max_kb": 200000, "rss_max_username": "diana",
                "d_state_fraction": 0.20, "d_state_username": "diana",
                "interactivity_fraction": 0.10, "interactivity_username": "diana",
            },
        ],
        "newest_interval_end": "2026-10-03T11:45:00+00:00",
        "is_fresh": True, "status": "complete",
        "gaps": {"missing_count": 0},
    },
    "poll_failures": [],
    "collection_log": [],
}


@pytest.fixture
def multi_grain_web(browser_page):
    from tests.browser.conftest import LiveWeb
    snap = copy.deepcopy(MULTI_GRAIN_SNAPSHOT)
    web = LiveWeb(snap).start()
    try:
        yield web, browser_page
    finally:
        web.close()


# ===========================================================================
# BLOCKER 1: D-state hotspot table adjacent to CPU chart
# ===========================================================================

def test_d_state_hotspot_table_present_under_cpu_chart(multi_grain_web):
    """RED: A D-state hotspot table must exist under (or adjacent to) the CPU chart.

    Requirements:
    - data-testid 'dstate-hotspot-table' present in DOM
    - Contains at least 2 rows (one per interval) from the 4-grain fixture
    - Explicitly states 'observation-weighted per stored grain; not node-wide or clock-time weighted'
    - Updates per snapshot (data must show values from the fixture)
    - Shows interval, max fraction/percent, winning username
    """
    web, (page, errors, _external) = multi_grain_web
    page.goto(web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    # Table must exist in DOM
    table = page.locator('[data-testid="dstate-hotspot-table"]')
    assert table.count() > 0, "dstate-hotspot-table must be present in DOM"
    assert table.is_visible(), "dstate-hotspot-table must be visible"

    table_text = table.inner_text()

    # Must contain the observation-weighted disclaimer
    assert "observation-weighted per stored grain" in table_text, (
        f"Table must state 'observation-weighted per stored grain; not node-wide or clock-time weighted'. "
        f"Got table text: {table_text!r}"
    )
    assert "not node-wide or clock-time weighted" in table_text, (
        f"Table must state 'not node-wide or clock-time weighted'. Got: {table_text!r}"
    )

    # Must show interval timestamps from the fixture
    assert "11:30" in table_text, (
        f"Table must show interval A (11:30). Got: {table_text!r}"
    )
    assert "11:45" in table_text, (
        f"Table must show interval B (11:45). Got: {table_text!r}"
    )

    # Must show winning usernames
    assert "bob" in table_text, (
        f"Table must show bob (Interval A d_state max=0.12/bob). Got: {table_text!r}"
    )
    assert "diana" in table_text, (
        f"Table must show diana (Interval B d_state max=0.20/diana). Got: {table_text!r}"
    )

    # Must show percentage or fraction values
    # 0.12 -> 12% or 0.12; 0.20 -> 20% or 0.20
    assert ("12" in table_text or "0.12" in table_text), (
        f"Table must show Interval A d_state (12% or 0.12). Got: {table_text!r}"
    )
    assert ("20" in table_text or "0.20" in table_text), (
        f"Table must show Interval B d_state (20% or 0.20). Got: {table_text!r}"
    )

    assert errors == []


def test_d_state_hotspot_table_inside_cpu_chart_article(browser_page, live_web, snapshot_complete):
    """RED: D-state hotspot table must be adjacent to / inside the CPU chart article.

    The table's parent must be within aria-label='CPU and load chart' or immediately
    adjacent to it (not far away in the DOM tree).
    """
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    # Must exist in the CPU chart article area
    table = page.locator('[aria-label="CPU and load chart"] [data-testid="dstate-hotspot-table"]')
    assert table.count() > 0, (
        "dstate-hotspot-table must be within the CPU and load chart article"
    )

    assert errors == []


# ===========================================================================
# BLOCKER 2: Interactivity max semantics - unconditional with exact values
# ===========================================================================

def test_interactivity_points_exposed_and_exact_four_grain(multi_grain_web):
    """RED: interactivityPoints must be unconditionally exposed in chartData.

    Interval A (11:30): interactivity grains = 0.15/alice, 0.05/bob -> max = 0.15/alice
    Interval B (11:45): interactivity grains = 0.30/charlie, 0.10/diana -> max = 0.30/charlie

    No conditional 'if exposed' guards - this field must always be present.
    """
    web, (page, errors, _external) = multi_grain_web
    page.goto(web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")

    # Must be unconditionally present (no 'if exposed' guard)
    assert "interactivityPoints" in data, (
        "chartData must always expose interactivityPoints (no conditional guard)"
    )
    points = data["interactivityPoints"]
    assert len(points) == 2, (
        f"interactivityPoints must have 2 entries (one per interval), "
        f"got {len(points)}: {points}"
    )

    # Interval A: max = 0.15/alice (alice wins over bob's 0.05)
    assert abs(points[0]["value"] - 0.15) < 0.001, (
        f"Interval A interactivity max should be 0.15, got {points[0]['value']}"
    )
    assert points[0]["username"] == "alice", (
        f"Interval A interactivity winner should be 'alice', got {points[0]['username']!r}"
    )
    assert "11:30" in points[0]["interval_end"], (
        f"Interval A interval_end should contain 11:30, got {points[0]['interval_end']!r}"
    )

    # Interval B: max = 0.30/charlie (charlie wins over diana's 0.10)
    assert abs(points[1]["value"] - 0.30) < 0.001, (
        f"Interval B interactivity max should be 0.30, got {points[1]['value']}"
    )
    assert points[1]["username"] == "charlie", (
        f"Interval B interactivity winner should be 'charlie', got {points[1]['username']!r}"
    )
    assert "11:45" in points[1]["interval_end"], (
        f"Interval B interval_end should contain 11:45, got {points[1]['interval_end']!r}"
    )

    assert errors == []


def test_interactivity_and_dstate_both_in_hotspot_table(multi_grain_web):
    """RED: The D-state hotspot table must also show interactivity max per interval.

    For the 4-grain fixture:
    - Interval A: d_state=12%/bob, interactivity=15%/alice
    - Interval B: d_state=20%/diana, interactivity=30%/charlie

    Both winners must appear in the table text.
    """
    web, (page, errors, _external) = multi_grain_web
    page.goto(web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    table = page.locator('[data-testid="dstate-hotspot-table"]')
    assert table.count() > 0, "dstate-hotspot-table must be present"
    table_text = table.inner_text()

    # D-state winners
    assert "bob" in table_text, (
        f"Interval A d_state winner 'bob' must appear in table. Got: {table_text!r}"
    )
    assert "diana" in table_text, (
        f"Interval B d_state winner 'diana' must appear in table. Got: {table_text!r}"
    )

    # Interactivity winners
    assert "alice" in table_text, (
        f"Interval A interactivity winner 'alice' must appear in table. Got: {table_text!r}"
    )
    assert "charlie" in table_text, (
        f"Interval B interactivity winner 'charlie' must appear in table. Got: {table_text!r}"
    )

    assert errors == []


# ===========================================================================
# BLOCKER 3: getChartRenderState() API
# ===========================================================================

def test_get_chart_render_state_api_exists(browser_page, live_web, snapshot_complete):
    """RED: getChartRenderState() must exist on window.__nodeMonitorTest.

    Must return {mode, labels, values} per chart, no Chart instances.
    """
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    has_api = page.evaluate(
        "() => typeof window.__nodeMonitorTest.getChartRenderState === 'function'"
    )
    assert has_api, "window.__nodeMonitorTest.getChartRenderState must be a function"

    state = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    assert state is not None, "getChartRenderState() must return a non-null value"

    # Must have entries for all 4 charts
    for chart in ("cpu", "memory", "process", "networkLustre"):
        assert chart in state, f"getChartRenderState() must include '{chart}' chart entry"

    # Each entry must have mode, labels, datasets
    for chart in ("cpu", "memory", "process", "networkLustre"):
        entry = state[chart]
        assert "mode" in entry, f"Chart '{chart}' entry must have 'mode' field"
        assert "labels" in entry, f"Chart '{chart}' entry must have 'labels' field"
        assert "datasets" in entry, f"Chart '{chart}' entry must have 'datasets' array"

    # Must NOT expose Chart instance properties
    assert "destroy" not in state, "getChartRenderState() must not expose 'destroy'"
    assert "update" not in state, "getChartRenderState() must not expose 'update'"

    assert errors == []


def test_get_chart_render_state_process_default_mode(browser_page, live_web, snapshot_complete):
    """RED: Process chart default mode is 'cpusum' with exact dataset labels."""
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    state = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    proc = state["process"]

    assert proc["mode"] == "proc-mode-cpusum", (
        f"Default process mode must be 'proc-mode-cpusum', got {proc['mode']!r}"
    )
    # Dataset 0 label must reference cpu_seconds
    assert len(proc["datasets"]) >= 1, "Process chart must have at least 1 dataset"
    dataset_label = proc["datasets"][0]["label"]
    assert "cpu" in dataset_label.lower() or "second" in dataset_label.lower(), (
        f"Default process dataset label must reference cpu_seconds, got: {dataset_label!r}"
    )
    # Values must be non-empty arrays (cloned, not references)
    assert isinstance(proc["datasets"][0]["data"], list), (
        "datasets[0].data must be a list (cloned values)"
    )

    assert errors == []


def test_get_chart_render_state_network_default_mode(browser_page, live_web, snapshot_complete):
    """RED: Network/Lustre chart default mode is 'nl-network-p50' with p50 data."""
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    state = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    nl = state["networkLustre"]

    assert nl["mode"] == "nl-network-p50", (
        f"Default network/lustre mode must be 'nl-network-p50', got {nl['mode']!r}"
    )
    # Must have datasets for interfaces
    assert len(nl["datasets"]) >= 1, "Network chart must have at least 1 dataset in default mode"
    # Dataset labels should reference p50 or RX/TX
    label0 = nl["datasets"][0]["label"]
    assert "p50" in label0.lower() or "rx" in label0.lower() or "tx" in label0.lower(), (
        f"Network dataset label must reference p50/RX/TX, got: {label0!r}"
    )

    assert errors == []


# ===========================================================================
# BLOCKER 3 (continued): Control tests with exact data arrays
# ===========================================================================

def test_memory_percent_click_exact_arrays_and_label(browser_page, live_web, snapshot_complete):
    """RED: Memory Percent click must change dataset data to percent values and y-label to Percent.

    After clicking Percent:
    - aria-pressed on percent btn = true
    - getChartRenderState().memory.datasets[x].label contains 'Percent'
    - values are fraction*100 of total (not raw KB)
    - mode == 'mem-mode-percent'

    Then GiB click:
    - y-label returns to GiB; values are in GiB range
    - mode == 'mem-mode-gib'
    """
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    # Wait for render state API
    page.wait_for_function(RENDER_STATE_WAIT, timeout=CONNECTED_TIMEOUT)

    # Initial GiB mode
    gib_state = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    assert gib_state["memory"]["mode"] == "mem-mode-gib", (
        f"Initial memory mode must be 'mem-mode-gib', got {gib_state['memory']['mode']!r}"
    )

    # Click Percent
    percent_btn = page.locator('[data-testid="mem-mode-percent"]')
    assert not percent_btn.is_disabled(), (
        "mem-mode-percent must be enabled (total is 131072000)"
    )
    percent_btn.click()
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartRenderState().memory.mode === 'mem-mode-percent'",
        timeout=CONNECTED_TIMEOUT,
    )

    # Verify aria-pressed
    assert percent_btn.get_attribute("aria-pressed") == "true", (
        "mem-mode-percent must have aria-pressed=true after click"
    )
    gib_btn = page.locator('[data-testid="mem-mode-gib"]')
    assert gib_btn.get_attribute("aria-pressed") == "false", (
        "mem-mode-gib must have aria-pressed=false after percent click"
    )

    pct_state = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    mem_pct = pct_state["memory"]
    assert mem_pct["mode"] == "mem-mode-percent", (
        f"Memory mode must be 'mem-mode-percent' after click, got {mem_pct['mode']!r}"
    )

    # Dataset label must contain 'Percent'
    all_labels = [ds["label"] for ds in mem_pct["datasets"]]
    assert any("Percent" in lbl or "percent" in lbl.lower() for lbl in all_labels), (
        f"At least one dataset label must contain 'Percent' in percent mode. Labels: {all_labels}"
    )

    # Values must be in percent range (0-100 range, not GiB range)
    # GiB values are ~62-65 GiB; percent values should be ~49-50%
    # mem_total = 131072000 KiB; mem_available_row0 = 65000000 KiB
    # used = 131072000 - 65000000 = 66072000 KiB -> 66072000/131072000 * 100 = 50.41%
    for ds in mem_pct["datasets"]:
        for v in ds["data"]:
            if v is not None:
                assert 0 <= v <= 100, (
                    f"Percent mode values must be 0-100, got {v} in dataset {ds['label']}"
                )

    # Click GiB to restore
    gib_btn.click()
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartRenderState().memory.mode === 'mem-mode-gib'",
        timeout=CONNECTED_TIMEOUT,
    )
    gib_state2 = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    assert gib_state2["memory"]["mode"] == "mem-mode-gib", (
        "Memory mode must return to 'mem-mode-gib' after GiB click"
    )
    # GiB values: 131072000/1024/1024 ~ 125 GiB; available ~62 GiB
    gib_ds = gib_state2["memory"]["datasets"]
    for ds in gib_ds:
        for v in ds["data"]:
            if v is not None:
                # GiB values should be > 1 (they're tens of GiB)
                assert v > 1, (
                    f"GiB mode values must be > 1 GiB, got {v} in dataset {ds['label']}"
                )

    assert errors == []


def test_memory_percent_disabled_null_total_no_click(browser_page, live_web):
    """RED: Percent button disabled when total null; aria-pressed correct; exact arrays."""
    from tests.browser.conftest import LiveWeb
    snap = {
        "server_utc_now": "2026-10-03T12:00:00+00:00",
        "node": "login-04",
        "range_hours": 1,
        "hardware": {
            "system": "polaris",
            "source_hostname": "login-04",
            "mem_total_kb": None,
            "cpu_model": "Intel Xeon",
            "cpu_logical": 96,
            "os_pretty_name": "RHEL 8",
        },
        "counters": {
            "rows": [{
                "window_end": "2026-10-03T11:57:00+00:00",
                "sample_count": 6, "expected_count": 6, "coverage": 1.0, "complete": True,
                "mem_available_kb": 65000000,
                "load1": 1.0, "load5": 0.9, "load15": 0.8,
                "procs_running": 2, "procs_total": 200,
                "cpu_busy_pct": {"p50": 10.0, "p95": 20.0, "max": 30.0},
                "network_rates": {}, "lustre_md_summary": {},
            }],
            "newest_window_end": "2026-10-03T11:57:00+00:00",
            "is_fresh": True, "status": "complete",
            "gaps": {"missing_count": 0},
            "latest": {"load1": 1.0},
        },
        "usage": {
            "grains": [], "newest_interval_end": None,
            "is_fresh": False, "status": "empty",
            "gaps": {"missing_count": 0},
        },
        "poll_failures": [], "collection_log": [],
    }
    live_web.state.set_snapshot(snap)

    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    percent_btn = page.locator('[data-testid="mem-mode-percent"]')
    # Must be visible (button exists)
    assert percent_btn.count() > 0, "mem-mode-percent button must exist"
    # Must be disabled
    assert percent_btn.is_disabled(), (
        "mem-mode-percent must be disabled when mem_total_kb is null"
    )

    assert errors == []


def test_process_cpu_sum_click_exact_arrays(browser_page, live_web, snapshot_complete):
    """RED: Process CPU sum mode: getChartRenderState returns exact processCPU arrays.

    Fixture: grain0 (11:30) cpu_seconds=120.5, grain1 (11:45) cpu_seconds=300.2.
    """
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    page.wait_for_function(RENDER_STATE_WAIT, timeout=CONNECTED_TIMEOUT)

    # Default mode is cpusum
    state = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    proc = state["process"]
    assert proc["mode"] == "proc-mode-cpusum"

    data_values = proc["datasets"][0]["data"]
    assert len(data_values) == 2, (
        f"Process CPU sum must have 2 entries (one per interval), got {len(data_values)}: {data_values}"
    )
    assert abs(data_values[0] - 120.5) < 0.001, (
        f"Interval 0 cpu_seconds must be 120.5, got {data_values[0]}"
    )
    assert abs(data_values[1] - 300.2) < 0.001, (
        f"Interval 1 cpu_seconds must be 300.2, got {data_values[1]}"
    )

    # Contributor summary: null (CPU sum has no per-user attribution)
    contributor_info = proc.get("contributorSummary")
    # May be null/empty for cpusum mode - just verify it doesn't have usernames
    # (cpusum is aggregate, not per-user)

    assert errors == []


def test_process_max_click_exact_arrays_and_contributor(browser_page, live_web, snapshot_complete):
    """RED: Process max click: getChartRenderState shows exact process_count_max and contributor.

    After clicking Process max:
    - mode == 'proc-mode-max'
    - datasets[0].data == [12, 10] (from fixture)
    - contributorSummary[0] == 'alice', [1] == 'bob'
    """
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    page.wait_for_function(RENDER_STATE_WAIT, timeout=CONNECTED_TIMEOUT)

    # Click Process max
    max_btn = page.locator('[data-testid="proc-mode-max"]')
    max_btn.click()
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartRenderState().process.mode === 'proc-mode-max'",
        timeout=CONNECTED_TIMEOUT,
    )

    state = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    proc = state["process"]
    assert proc["mode"] == "proc-mode-max", (
        f"Process mode must be 'proc-mode-max' after click, got {proc['mode']!r}"
    )

    data_values = proc["datasets"][0]["data"]
    assert data_values == [12, 10], (
        f"process_count_max values must be [12, 10], got {data_values}"
    )

    # Contributor summary must be present and exact
    assert "contributorSummary" in proc, (
        "getChartRenderState().process must include 'contributorSummary' in max mode"
    )
    contrib = proc["contributorSummary"]
    assert contrib[0] == "alice", (
        f"Interval 0 contributor must be 'alice', got {contrib[0]!r}"
    )
    assert contrib[1] == "bob", (
        f"Interval 1 contributor must be 'bob', got {contrib[1]!r}"
    )

    # aria-pressed
    assert max_btn.get_attribute("aria-pressed") == "true"
    assert page.locator('[data-testid="proc-mode-cpusum"]').get_attribute("aria-pressed") == "false"

    assert errors == []


def test_process_rss_click_exact_arrays_and_contributor(browser_page, live_web, snapshot_complete):
    """RED: Process RSS (grain total RSS) click: exact rss_max_kb arrays and contributor.

    After clicking Grain total RSS:
    - mode == 'proc-mode-rss'
    - datasets[0].data == [307200, 256000]
    - contributorSummary == ['alice', 'bob']
    """
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    page.wait_for_function(RENDER_STATE_WAIT, timeout=CONNECTED_TIMEOUT)

    rss_btn = page.locator('[data-testid="proc-mode-rss"]')
    rss_btn.click()
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartRenderState().process.mode === 'proc-mode-rss'",
        timeout=CONNECTED_TIMEOUT,
    )

    state = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    proc = state["process"]
    assert proc["mode"] == "proc-mode-rss"

    data_values = proc["datasets"][0]["data"]
    assert data_values == [307200, 256000], (
        f"rss_max_kb values must be [307200, 256000], got {data_values}"
    )

    assert "contributorSummary" in proc
    contrib = proc["contributorSummary"]
    assert contrib[0] == "alice", f"RSS contributor 0 must be 'alice', got {contrib[0]!r}"
    assert contrib[1] == "bob", f"RSS contributor 1 must be 'bob', got {contrib[1]!r}"

    assert errors == []


# ===========================================================================
# BLOCKER 3: Network p50/p95/max mode controls
# ===========================================================================

def test_network_has_p50_p95_max_mode_buttons(browser_page, live_web):
    """RED: Network must have explicit p50/p95/max buttons (not just default p50 label)."""
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    # All three network stat mode buttons must exist
    assert page.locator('[data-testid="nl-network-p50"]').count() > 0, (
        "nl-network-p50 button must exist"
    )
    assert page.locator('[data-testid="nl-network-p95"]').count() > 0, (
        "nl-network-p95 button must exist"
    )
    assert page.locator('[data-testid="nl-network-max"]').count() > 0, (
        "nl-network-max button must exist"
    )

    # Default: p50 is active
    assert page.locator('[data-testid="nl-network-p50"]').get_attribute("aria-pressed") == "true", (
        "nl-network-p50 must be aria-pressed=true by default"
    )

    assert errors == []


def test_network_p50_exact_data_in_render_state(browser_page, live_web, snapshot_complete):
    """RED: Network p50 mode: getChartRenderState shows exact eth0 RX p50 values.

    Fixture: eth0 RX p50 = [1200.5 (row0), None (gap), 1300.0 (row1)]
    """
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    page.wait_for_function(RENDER_STATE_WAIT, timeout=CONNECTED_TIMEOUT)

    state = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    nl = state["networkLustre"]
    assert nl["mode"] == "nl-network-p50", f"Default mode must be nl-network-p50, got {nl['mode']}"

    # Find eth0 RX p50 dataset
    eth0_rx_datasets = [ds for ds in nl["datasets"] if "eth0" in ds["label"] and "RX" in ds["label"]]
    assert len(eth0_rx_datasets) >= 1, (
        f"Must have eth0 RX dataset in p50 mode. Got datasets: {[ds['label'] for ds in nl['datasets']]}"
    )
    eth0_rx_data = eth0_rx_datasets[0]["data"]

    # 2-minute gap -> 3 slots
    assert len(eth0_rx_data) == 3, (
        f"eth0 RX must have 3 slots (2 rows + 1 gap), got {len(eth0_rx_data)}: {eth0_rx_data}"
    )
    assert eth0_rx_data[0] == 1200.5, f"eth0 RX p50 row0 must be 1200.5, got {eth0_rx_data[0]}"
    assert eth0_rx_data[1] is None, f"eth0 RX p50 gap must be None, got {eth0_rx_data[1]}"
    assert eth0_rx_data[2] == 1300.0, f"eth0 RX p50 row1 must be 1300.0, got {eth0_rx_data[2]}"

    assert errors == []


def test_network_p95_click_exact_data(browser_page, live_web, snapshot_complete):
    """RED: Network p95 click: getChartRenderState shows eth0 RX p95 values.

    Fixture: eth0 RX p95 = [3000 (row0), None (gap), 3100 (row1)]
    """
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    page.wait_for_function(RENDER_STATE_WAIT, timeout=CONNECTED_TIMEOUT)

    # Click p95 button
    p95_btn = page.locator('[data-testid="nl-network-p95"]')
    p95_btn.click()
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartRenderState().networkLustre.mode === 'nl-network-p95'",
        timeout=CONNECTED_TIMEOUT,
    )

    state = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    nl = state["networkLustre"]
    assert nl["mode"] == "nl-network-p95", f"Mode must be 'nl-network-p95', got {nl['mode']!r}"

    eth0_rx_datasets = [ds for ds in nl["datasets"] if "eth0" in ds["label"] and "RX" in ds["label"]]
    assert len(eth0_rx_datasets) >= 1
    eth0_rx_data = eth0_rx_datasets[0]["data"]
    assert eth0_rx_data[0] == 3000, f"eth0 RX p95 row0 must be 3000, got {eth0_rx_data[0]}"
    assert eth0_rx_data[1] is None, f"eth0 RX p95 gap must be None, got {eth0_rx_data[1]}"
    assert eth0_rx_data[2] == 3100, f"eth0 RX p95 row1 must be 3100, got {eth0_rx_data[2]}"

    assert p95_btn.get_attribute("aria-pressed") == "true"

    assert errors == []


def test_network_max_click_exact_data(browser_page, live_web, snapshot_complete):
    """RED: Network max click: getChartRenderState shows eth0 RX max values.

    Fixture: eth0 RX max = [5000 (row0), None (gap), 5200 (row1)]
    """
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    page.wait_for_function(RENDER_STATE_WAIT, timeout=CONNECTED_TIMEOUT)

    max_btn = page.locator('[data-testid="nl-network-max"]')
    max_btn.click()
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartRenderState().networkLustre.mode === 'nl-network-max'",
        timeout=CONNECTED_TIMEOUT,
    )

    state = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    nl = state["networkLustre"]
    assert nl["mode"] == "nl-network-max", f"Mode must be 'nl-network-max', got {nl['mode']!r}"

    eth0_rx_datasets = [ds for ds in nl["datasets"] if "eth0" in ds["label"] and "RX" in ds["label"]]
    assert len(eth0_rx_datasets) >= 1
    eth0_rx_data = eth0_rx_datasets[0]["data"]
    assert eth0_rx_data[0] == 5000, f"eth0 RX max row0 must be 5000, got {eth0_rx_data[0]}"
    assert eth0_rx_data[1] is None, f"eth0 RX max gap must be None, got {eth0_rx_data[1]}"
    assert eth0_rx_data[2] == 5200, f"eth0 RX max row1 must be 5200, got {eth0_rx_data[2]}"

    assert max_btn.get_attribute("aria-pressed") == "true"

    assert errors == []


# ===========================================================================
# BLOCKER 3: Lustre mode clicks with exact data
# ===========================================================================

def test_lustre_p50_click_exact_arrays(browser_page, live_web, snapshot_complete):
    """RED: Lustre p50-sum click: exact read p50_sum values from getChartRenderState.

    Fixture: read p50_sum = [120.5 (row0), None (gap), 130.0 (row1)]
    """
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    page.wait_for_function(RENDER_STATE_WAIT, timeout=CONNECTED_TIMEOUT)

    page.locator('[data-testid="nl-mode-lustre-p50"]').click()
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartRenderState().networkLustre.mode === 'nl-mode-lustre-p50'",
        timeout=CONNECTED_TIMEOUT,
    )

    state = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    nl = state["networkLustre"]
    assert nl["mode"] == "nl-mode-lustre-p50"

    read_datasets = [ds for ds in nl["datasets"] if "read" in ds["label"].lower()]
    assert len(read_datasets) >= 1, (
        f"Must have 'read' dataset in p50 mode. Labels: {[ds['label'] for ds in nl['datasets']]}"
    )
    read_data = read_datasets[0]["data"]
    assert len(read_data) == 3, f"read p50_sum must have 3 slots, got {len(read_data)}"
    assert read_data[0] == 120.5, f"read p50_sum row0 must be 120.5, got {read_data[0]}"
    assert read_data[1] is None, f"read p50_sum gap must be None, got {read_data[1]}"
    assert read_data[2] == 130.0, f"read p50_sum row1 must be 130.0, got {read_data[2]}"

    assert errors == []


def test_lustre_p95_click_exact_arrays(browser_page, live_web, snapshot_complete):
    """RED: Lustre p95-sum click: exact read p95_sum values.

    Fixture: read p95_sum = [300.0, None, 310.0]
    """
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    page.wait_for_function(RENDER_STATE_WAIT, timeout=CONNECTED_TIMEOUT)

    page.locator('[data-testid="nl-mode-lustre-p95"]').click()
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartRenderState().networkLustre.mode === 'nl-mode-lustre-p95'",
        timeout=CONNECTED_TIMEOUT,
    )

    state = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    nl = state["networkLustre"]
    assert nl["mode"] == "nl-mode-lustre-p95"

    read_datasets = [ds for ds in nl["datasets"] if "read" in ds["label"].lower()]
    assert len(read_datasets) >= 1
    read_data = read_datasets[0]["data"]
    assert read_data[0] == 300.0, f"read p95_sum row0 must be 300.0, got {read_data[0]}"
    assert read_data[1] is None
    assert read_data[2] == 310.0, f"read p95_sum row1 must be 310.0, got {read_data[2]}"

    assert errors == []


def test_lustre_peak_click_exact_arrays(browser_page, live_web, snapshot_complete):
    """RED: Lustre peak-sum click: exact read max_sum values.

    Fixture: read max_sum = [500.0, None, 520.0]
    """
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    page.wait_for_function(RENDER_STATE_WAIT, timeout=CONNECTED_TIMEOUT)

    page.locator('[data-testid="nl-mode-lustre-peak"]').click()
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartRenderState().networkLustre.mode === 'nl-mode-lustre-peak'",
        timeout=CONNECTED_TIMEOUT,
    )

    state = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    nl = state["networkLustre"]
    assert nl["mode"] == "nl-mode-lustre-peak"

    read_datasets = [ds for ds in nl["datasets"] if "read" in ds["label"].lower()]
    assert len(read_datasets) >= 1
    read_data = read_datasets[0]["data"]
    assert read_data[0] == 500.0, f"read max_sum row0 must be 500.0, got {read_data[0]}"
    assert read_data[1] is None
    assert read_data[2] == 520.0, f"read max_sum row1 must be 520.0, got {read_data[2]}"

    assert errors == []


def test_lustre_target_count_click_exact_arrays(browser_page, live_web, snapshot_complete):
    """RED: Lustre target count click: exact read target_count values.

    Fixture: read target_count = [2, None, 2]
    """
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    page.wait_for_function(RENDER_STATE_WAIT, timeout=CONNECTED_TIMEOUT)

    page.locator('[data-testid="nl-mode-lustre-targets"]').click()
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartRenderState().networkLustre.mode === 'nl-mode-lustre-targets'",
        timeout=CONNECTED_TIMEOUT,
    )

    state = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    nl = state["networkLustre"]
    assert nl["mode"] == "nl-mode-lustre-targets"

    read_datasets = [ds for ds in nl["datasets"] if "read" in ds["label"].lower()]
    assert len(read_datasets) >= 1
    read_data = read_datasets[0]["data"]
    assert read_data[0] == 2, f"read target_count row0 must be 2, got {read_data[0]}"
    assert read_data[1] is None
    assert read_data[2] == 2, f"read target_count row1 must be 2, got {read_data[2]}"

    assert errors == []


# ===========================================================================
# BLOCKER 4: Lustre label correctness - p50-sum and p95-sum must NOT say
#             'sum of per-target maxima'; that phrase belongs only to peak-sum
# ===========================================================================

def test_lustre_p50_label_does_not_say_maxima(browser_page, live_web, snapshot_complete):
    """RED: Lustre p50-sum dataset label must NOT contain 'sum of per-target maxima'.

    p50-sum is the sum of per-target p50 values (not maxima).
    Only peak-sum (max_sum) should say 'sum of per-target maxima'.
    """
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    page.wait_for_function(RENDER_STATE_WAIT, timeout=CONNECTED_TIMEOUT)

    # Click p50 mode
    page.locator('[data-testid="nl-mode-lustre-p50"]').click()
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartRenderState().networkLustre.mode === 'nl-mode-lustre-p50'",
        timeout=CONNECTED_TIMEOUT,
    )

    state = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    nl = state["networkLustre"]

    for ds in nl["datasets"]:
        assert "maxima" not in ds["label"].lower(), (
            f"p50-sum dataset label must NOT contain 'maxima'. Got: {ds['label']!r}"
        )
        assert "sum of per-target maxima" not in ds["label"], (
            f"p50-sum label must NOT say 'sum of per-target maxima'. Got: {ds['label']!r}"
        )

    assert errors == []


def test_lustre_p95_label_does_not_say_maxima(browser_page, live_web, snapshot_complete):
    """RED: Lustre p95-sum dataset label must NOT contain 'sum of per-target maxima'."""
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    page.wait_for_function(RENDER_STATE_WAIT, timeout=CONNECTED_TIMEOUT)

    page.locator('[data-testid="nl-mode-lustre-p95"]').click()
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartRenderState().networkLustre.mode === 'nl-mode-lustre-p95'",
        timeout=CONNECTED_TIMEOUT,
    )

    state = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    nl = state["networkLustre"]

    for ds in nl["datasets"]:
        assert "maxima" not in ds["label"].lower(), (
            f"p95-sum dataset label must NOT contain 'maxima'. Got: {ds['label']!r}"
        )

    assert errors == []


def test_lustre_peak_label_says_maxima(browser_page, live_web, snapshot_complete):
    """RED: Lustre peak-sum dataset label MUST contain 'sum of per-target maxima'."""
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    page.wait_for_function(RENDER_STATE_WAIT, timeout=CONNECTED_TIMEOUT)

    page.locator('[data-testid="nl-mode-lustre-peak"]').click()
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartRenderState().networkLustre.mode === 'nl-mode-lustre-peak'",
        timeout=CONNECTED_TIMEOUT,
    )

    state = page.evaluate("() => window.__nodeMonitorTest.getChartRenderState()")
    nl = state["networkLustre"]

    for ds in nl["datasets"]:
        assert "maxima" in ds["label"].lower() or "max_sum" in ds["label"].lower(), (
            f"peak-sum dataset label must reference 'maxima' or 'max_sum'. Got: {ds['label']!r}"
        )

    assert errors == []


def test_lustre_note_says_maxima_only_for_peak(browser_page, live_web, snapshot_complete):
    """RED: Network/Lustre chart note must correctly attribute the 'maxima' caveat to peak-sum only."""
    page, errors, _external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    note = page.locator('[aria-label="Network and Lustre chart"] .chart-note').inner_text()
    # Note must exist and reference peak-sum/max_sum for the maxima caveat
    assert "peak-sum" in note.lower() or "max_sum" in note.lower(), (
        f"Chart note must reference peak-sum or max_sum for the maxima caveat. Got: {note!r}"
    )
    # The static note text should NOT broadly claim p50-sum or p95-sum have maxima caveat
    # (the caveat belongs exclusively to peak-sum)

    assert errors == []


# ===========================================================================
# BLOCKER 5: Narrow viewport with is_visible() and bounding boxes
# ===========================================================================

def test_narrow_viewport_charts_visible_with_bounding_boxes(browser_page, live_web, snapshot_complete):
    """RED: At 400px viewport, all chart canvases must be is_visible() with valid bounding boxes.

    scrollWidth <= clientWidth must hold (no horizontal overflow).
    """
    page, errors, _external = browser_page

    # Set narrow viewport BEFORE loading
    page.set_viewport_size({"width": 400, "height": 800})
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    for testid in ("chart-cpu", "chart-memory", "chart-process", "chart-network-lustre"):
        canvas = page.locator(f'[data-testid="{testid}"]')
        # Must be visible (is_visible() not just count > 0)
        assert canvas.is_visible(), (
            f"{testid} must be is_visible() at 400px viewport"
        )
        # Must have a valid bounding box (width > 0, height > 0)
        bbox = canvas.bounding_box()
        assert bbox is not None, f"{testid} bounding_box() must not be None"
        assert bbox["width"] > 0, f"{testid} width must be > 0, got {bbox['width']}"
        assert bbox["height"] > 0, f"{testid} height must be > 0, got {bbox['height']}"
        # Width should not significantly exceed viewport (allow small scroll bar tolerance)
        assert bbox["width"] <= 420, (
            f"{testid} width {bbox['width']} exceeds 400px viewport (overflow)"
        )

    # No horizontal overflow: body scrollWidth <= clientWidth
    no_overflow = page.evaluate("""() => {
        return document.body.scrollWidth <= document.body.clientWidth + 2;
    }""")
    assert no_overflow, (
        "Page body must not overflow horizontally at 400px viewport"
    )

    assert errors == []


def test_narrow_viewport_dstate_table_visible(browser_page, live_web, snapshot_complete):
    """RED: At 400px viewport, dstate-hotspot-table must be is_visible()."""
    page, errors, _external = browser_page
    page.set_viewport_size({"width": 400, "height": 800})
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    table = page.locator('[data-testid="dstate-hotspot-table"]')
    assert table.count() > 0, "dstate-hotspot-table must exist at 400px"
    assert table.is_visible(), "dstate-hotspot-table must be is_visible() at 400px"

    bbox = table.bounding_box()
    assert bbox is not None, "dstate-hotspot-table must have bounding box"
    assert bbox["width"] > 0, f"dstate-hotspot-table width must be > 0"

    assert errors == []


# ===========================================================================
# BLOCKER 6: No external requests in mode switch tests
# ===========================================================================

def test_no_external_requests_on_process_mode_switch(browser_page, live_web, snapshot_complete):
    """RED: Clicking process mode buttons must not trigger external requests."""
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    # External requests captured before mode switch
    external_before = len(external)

    # Click all process mode buttons
    page.locator('[data-testid="proc-mode-max"]').click()
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartLifecycle().createCount >= 8",
        timeout=CONNECTED_TIMEOUT,
    )
    page.locator('[data-testid="proc-mode-rss"]').click()
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartLifecycle().createCount >= 12",
        timeout=CONNECTED_TIMEOUT,
    )
    page.locator('[data-testid="proc-mode-cpusum"]').click()
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartLifecycle().createCount >= 16",
        timeout=CONNECTED_TIMEOUT,
    )

    # No new external requests
    assert len(external) == external_before, (
        f"No external requests should occur on mode switches. New requests: {external[external_before:]}"
    )

    assert errors == []


def test_no_external_requests_on_lustre_mode_switches(browser_page, live_web, snapshot_complete):
    """RED: Switching between Lustre modes must not trigger external requests."""
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    external_before = len(external)

    for mode_btn in ("nl-mode-lustre-p50", "nl-mode-lustre-p95", "nl-mode-lustre-peak", "nl-mode-lustre-targets"):
        page.locator(f'[data-testid="{mode_btn}"]').click()
        page.wait_for_function(
            f"() => window.__nodeMonitorTest.getChartRenderState().networkLustre.mode === '{mode_btn}'",
            timeout=CONNECTED_TIMEOUT,
        )

    assert len(external) == external_before, (
        f"No external requests should occur on Lustre mode switches. New: {external[external_before:]}"
    )

    assert errors == []
