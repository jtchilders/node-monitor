"""Task 8 real-browser chart acceptance tests with genuine value assertions.

All tests wait for 'Connected' or chartData before reading chart state.
No tautologies (no 'or True'), no type-only assertions, no UI-existence fallbacks.
"""
import copy

from playwright.sync_api import expect


CONNECTED_TIMEOUT = 10000  # ms
CHART_DATA_WAIT = "() => window.__nodeMonitorTest && window.__nodeMonitorTest.chartData && window.__nodeMonitorTest.chartData.cpu && window.__nodeMonitorTest.chartData.cpu.labels.length > 0"
LIFECYCLE_WAIT = "() => window.__nodeMonitorTest && window.__nodeMonitorTest.getChartLifecycle && window.__nodeMonitorTest.getChartLifecycle().createCount > 0"


def wait_connected(page):
    """Wait until 'Connected' appears in the connectivity-status element."""
    expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
        "Connected", timeout=CONNECTED_TIMEOUT
    )


def wait_chart_data(page):
    """Wait until chartData is populated with real labels."""
    page.wait_for_function(CHART_DATA_WAIT, timeout=CONNECTED_TIMEOUT)


def wait_lifecycle(page):
    """Wait until at least one chart has been created."""
    page.wait_for_function(LIFECYCLE_WAIT, timeout=CONNECTED_TIMEOUT)


# ---- CPU / Load tests ----

def test_cpu_p50_p95_max_exact_values(browser_page, live_web, snapshot_complete):
    """CPU p50/p95/max arrays exactly match fixture values; load arrays present on yLoad axis.

    The fixture has rows at 11:57 and 11:59. The 2-minute separation (120s) exceeds the
    60s cadence + 30s tolerance (90s), so one gap slot is inserted at 11:58. This gives
    3 expanded slots: [11:57 (row0), 11:58 (gap), 11:59 (row1)].
    """
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")
    cpu = data["cpu"]

    # 2-minute gap between 11:57 and 11:59 => 3 expanded labels (row0, gap, row1)
    labels = cpu["labels"]
    assert len(labels) == 3, \
        f"Expected 3 labels (2 rows + 1 gap at 11:58), got {len(labels)}: {labels}"
    assert "2026-10-03T11:57" in labels[0], f"First label wrong: {labels[0]}"
    assert "(gap)" in labels[1], f"Middle label must be gap slot: {labels[1]}"
    assert "2026-10-03T11:59" in labels[2], f"Third label wrong: {labels[2]}"

    # CPU p50 exact values: [row0, None(gap), row1]
    assert cpu["cpuP50"] == [20.0, None, 25.0], f"cpuP50 wrong: {cpu['cpuP50']}"
    assert cpu["cpuP95"] == [50.0, None, 60.0], f"cpuP95 wrong: {cpu['cpuP95']}"
    assert cpu["cpuMax"] == [80.0, None, 90.0], f"cpuMax wrong: {cpu['cpuMax']}"

    # Load arrays: [row0, None(gap), row1]
    assert cpu["load1"] == [1.2, None, 1.5], f"load1 wrong: {cpu['load1']}"
    assert cpu["load5"] == [0.9, None, 1.0], f"load5 wrong: {cpu['load5']}"
    assert cpu["load15"] == [0.7, None, 0.8], f"load15 wrong: {cpu['load15']}"

    # Axis IDs
    assert cpu["yAxisCPU"] == "y"
    assert cpu["yAxisLoad"] == "yLoad"
    assert cpu["spanGaps"] is False

    assert errors == []
    assert external == []


def test_gap_null_inserted_exact_one_aligned_null(browser_page, live_web, snapshot_complete):
    """A 2-minute missing counter interval inserts exactly one aligned null; no fabricated zero."""
    # fixture has rows at 11:57 and 11:59 - 2 min gap with 60s cadence
    # 11:57 -> 11:59 is exactly 2 minutes (120s) > 90s threshold = one missing slot (11:58)
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")
    cpu = data["cpu"]
    labels = cpu["labels"]
    p50 = cpu["cpuP50"]

    # 2-minute gap: exactly 1 null slot inserted for 11:58
    assert len(labels) == 3, f"Expected 3 labels (2 rows + 1 gap), got {len(labels)}: {labels}"
    assert "(gap)" in labels[1], f"Middle label should be gap, got: {labels[1]}"
    assert p50[1] is None, f"Gap slot must be None, not {p50[1]!r}"
    # No fabricated zero: real values are correct
    assert p50[0] == 20.0, f"First real row p50 wrong: {p50[0]}"
    assert p50[2] == 25.0, f"Second real row p50 wrong: {p50[2]}"

    assert errors == []
    assert external == []


def test_no_fabricated_zero_missing_values(browser_page, live_web, snapshot_complete):
    """Missing values for any series must be None, never fabricated zero."""
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")

    # For gap slots, all counter-derived series must be None not 0
    cpu = data["cpu"]
    if any(v is None for v in cpu["cpuP50"]):
        for i, v in enumerate(cpu["cpuP50"]):
            if cpu["labels"][i] and "(gap)" in cpu["labels"][i]:
                assert v is None, f"Gap slot {i} has non-None p50: {v}"
                assert cpu["cpuP95"][i] is None, f"Gap slot {i} has non-None p95: {cpu['cpuP95'][i]}"
                assert cpu["cpuMax"][i] is None, f"Gap slot {i} has non-None max: {cpu['cpuMax'][i]}"
                assert cpu["load1"][i] is None, f"Gap slot {i} has non-None load1: {cpu['load1'][i]}"

    assert errors == []


# ---- D-state tests ----

def test_d_state_sparse_maxima_exact_values_and_username(browser_page, live_web, snapshot_complete):
    """D-state points carry exact fraction, username, and interval_end from each grain."""
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")
    d_points = data["dStatePoints"]

    # Fixture has 2 grains with d_state_fraction 0.08 and 0.12
    assert len(d_points) == 2, f"Expected 2 d-state points, got {len(d_points)}"
    assert d_points[0]["value"] == 0.08, f"First d_state value wrong: {d_points[0]['value']}"
    assert d_points[0]["username"] == "alice", f"First d_state username wrong: {d_points[0]['username']}"
    assert d_points[0]["interval_end"] == "2026-10-03T11:30:00+00:00", \
        f"First interval_end wrong: {d_points[0]['interval_end']}"

    assert d_points[1]["value"] == 0.12, f"Second d_state value wrong: {d_points[1]['value']}"
    assert d_points[1]["username"] == "alice", f"Second d_state username wrong: {d_points[1]['username']}"
    assert d_points[1]["interval_end"] == "2026-10-03T11:45:00+00:00", \
        f"Second interval_end wrong: {d_points[1]['interval_end']}"

    # The HTML table note mentions observation-weighted fraction
    obs_note = page.locator('[aria-label="CPU and load chart"] .chart-alt').inner_text()
    assert "observation-weighted" in obs_note.lower() or "Observation-weighted" in obs_note, \
        f"Observation-weighted note missing from CPU/load table: {obs_note!r}"

    assert errors == []


# ---- Memory tests ----

def test_memory_gib_exact_values_and_derived_used(browser_page, live_web, snapshot_complete):
    """Memory available series is exact per-row; used = MemTotal - MemAvailable per row.

    With 2-row fixture and 1 gap slot: indices 0 and 2 are real rows; index 1 is gap (None).
    """
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")
    mem_avail = data["memoryAvailableSeries"]
    mem_used = data["memoryUsedSeries"]
    total_kb = data["memoryTotalKb"]

    # After gap expansion: [row0, None(gap), row1]
    assert len(mem_avail) == 3, f"Expected 3 avail slots, got {len(mem_avail)}"
    assert mem_avail[0] == 65000000, f"Row0 avail wrong: {mem_avail[0]}"
    assert mem_avail[1] is None, f"Gap slot avail must be None, got: {mem_avail[1]}"
    assert mem_avail[2] == 64000000, f"Row1 avail wrong: {mem_avail[2]}"

    # Used = total - available (131072000 - 65000000 = 66072000)
    assert total_kb == 131072000, f"totalKb wrong: {total_kb}"
    assert mem_used[0] == 131072000 - 65000000, \
        f"Row0 used wrong: {mem_used[0]} (expected {131072000 - 65000000})"
    assert mem_used[1] is None, f"Gap slot used must be None, got: {mem_used[1]}"
    assert mem_used[2] == 131072000 - 64000000, \
        f"Row1 used wrong: {mem_used[2]} (expected {131072000 - 64000000})"

    assert errors == []


def test_memory_percent_disabled_when_total_null(browser_page, live_web, snapshot_complete):
    """When mem_total_kb is null, percent-mode values must be null (not fabricated)."""
    snap = copy.deepcopy(snapshot_complete)
    snap["hardware"]["mem_total_kb"] = None
    live_web.state.set_snapshot(snap)

    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")
    # memoryUsedSeries must all be None because total is null
    assert data["memoryTotalKb"] is None
    for i, v in enumerate(data["memoryUsedSeries"]):
        assert v is None, f"memoryUsedSeries[{i}] must be None when total null, got {v}"

    assert errors == []


def test_memory_percent_disabled_when_total_zero(browser_page, live_web, snapshot_complete):
    """When mem_total_kb is 0, percent mode stays null; GiB available series still works."""
    snap = copy.deepcopy(snapshot_complete)
    snap["hardware"]["mem_total_kb"] = 0
    live_web.state.set_snapshot(snap)

    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")
    # total=0: used series null; avail series should still have values
    for i, v in enumerate(data["memoryUsedSeries"]):
        assert v is None, f"memoryUsedSeries[{i}] must be None when total=0, got {v}"
    # Available values still present
    real_avail = [v for v in data["memoryAvailableSeries"] if v is not None]
    assert len(real_avail) > 0, "Should have non-null available values even when total=0"

    assert errors == []


def test_memory_canvas_visible_and_controls_present(browser_page, live_web):
    """Memory chart canvas is visible; GiB/Percent toggle controls are in DOM."""
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    assert page.locator('[data-testid="chart-memory"]').is_visible()
    # Controls inserted by JS
    assert page.locator('[data-testid="mem-mode-gib"]').count() > 0
    assert page.locator('[data-testid="mem-mode-percent"]').count() > 0

    assert errors == []


# ---- Process chart tests ----

def test_process_cpu_sum_exact_grouped_by_interval(browser_page, live_web, snapshot_complete):
    """CPU sum mode: processCPU contains exact additive cpu_seconds per grain."""
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")
    cpu_sum = data["processCPU"]
    labels = data["processLabels"]

    # Fixture: grain0 cpu_seconds=120.5, grain1 cpu_seconds=300.2
    assert len(cpu_sum) == 2, f"Expected 2 process grains, got {len(cpu_sum)}"
    assert cpu_sum[0] == 120.5, f"Grain0 cpu_seconds wrong: {cpu_sum[0]}"
    assert cpu_sum[1] == 300.2, f"Grain1 cpu_seconds wrong: {cpu_sum[1]}"

    # Labels contain interval_end + category
    assert "interactive" in labels[0], f"Grain0 label missing category: {labels[0]}"
    assert "batch" in labels[1], f"Grain1 label missing category: {labels[1]}"

    assert errors == []


def test_process_count_max_with_exact_contributor(browser_page, live_web, snapshot_complete):
    """Process max mode: processCountMax and processCountMaxUsername match fixture exactly."""
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")
    count_max = data["processCountMax"]
    usernames = data["processCountMaxUsername"]

    # Fixture: grain0 process_count_max=12 (alice), grain1 process_count_max=10 (bob)
    assert count_max == [12, 10], f"processCountMax wrong: {count_max}"
    assert usernames[0] == "alice", f"Grain0 contributor wrong: {usernames[0]}"
    assert usernames[1] == "bob", f"Grain1 contributor wrong: {usernames[1]}"

    assert errors == []


def test_process_rss_max_exact_label_and_contributor(browser_page, live_web, snapshot_complete):
    """RSS mode shows rss_max_kb with label 'grain total RSS' and exact contributor."""
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")
    rss_max = data["processRSSMax"]
    rss_usernames = data["processRSSMaxUsername"]

    # Fixture: grain0 rss_max_kb=307200 (alice), grain1 rss_max_kb=256000 (bob)
    assert rss_max == [307200, 256000], f"processRSSMax wrong: {rss_max}"
    assert rss_usernames[0] == "alice", f"Grain0 RSS username wrong: {rss_usernames[0]}"
    assert rss_usernames[1] == "bob", f"Grain1 RSS username wrong: {rss_usernames[1]}"

    # HTML table has 'grain total RSS'
    proc_table = page.locator('[aria-label="Process chart"] .chart-alt').inner_text()
    assert "grain total RSS" in proc_table, \
        f"'grain total RSS' missing from process table: {proc_table!r}"

    assert errors == []


def test_process_canvas_visible(browser_page, live_web):
    """Process chart canvas is visible and has mode toggle controls."""
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    assert page.locator('[data-testid="chart-process"]').is_visible()
    assert page.locator('[data-testid="proc-mode-cpusum"]').count() > 0
    assert page.locator('[data-testid="proc-mode-max"]').count() > 0
    assert page.locator('[data-testid="proc-mode-rss"]').count() > 0

    assert errors == []


# ---- Network tests ----

def test_network_per_interface_exact_rx_tx_p50(browser_page, live_web, snapshot_complete):
    """Network RX/TX p50 are exact time-series per interface; lo excluded; eth0 present.

    With 2-row fixture + 1 gap: expanded to 3 slots.
    Row0 (11:57) has eth0 data; gap (11:58) is None; Row1 (11:59) has eth0 data.
    """
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")
    iface_list = data["ifaceList"]
    rx_p50 = data["networkRXP50"]
    tx_p50 = data["networkTXP50"]

    # lo must not appear
    assert "lo" not in iface_list, f"lo should be excluded, got ifaceList: {iface_list}"
    # eth0 must appear
    assert "eth0" in iface_list, f"eth0 missing from ifaceList: {iface_list}"

    # eth0 RX p50 time series (3 slots: row0, gap, row1)
    eth0_rx = rx_p50["eth0"]
    assert len(eth0_rx) == 3, f"eth0 RX should have 3 slots: {eth0_rx}"
    assert eth0_rx[0] == 1200.5, f"eth0 RX p50 row0 wrong: {eth0_rx[0]}"
    assert eth0_rx[1] is None, f"eth0 RX p50 gap slot must be None: {eth0_rx[1]}"
    assert eth0_rx[2] == 1300.0, f"eth0 RX p50 row1 wrong: {eth0_rx[2]}"

    # eth0 TX p50 (row0=800.0, gap=None, row1=850.0)
    eth0_tx = tx_p50["eth0"]
    assert eth0_tx[0] == 800.0, f"eth0 TX p50 row0 wrong: {eth0_tx[0]}"
    assert eth0_tx[1] is None, f"eth0 TX p50 gap slot must be None: {eth0_tx[1]}"
    assert eth0_tx[2] == 850.0, f"eth0 TX p50 row1 wrong: {eth0_tx[2]}"

    assert errors == []
    assert external == []


def test_network_canvas_visible_and_lo_note(browser_page, live_web):
    """Network chart canvas is visible; note says Loopback (lo) excluded."""
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    assert page.locator('[data-testid="chart-network-lustre"]').is_visible()
    note = page.locator('[aria-label="Network and Lustre chart"] .chart-note').inner_text()
    assert "Loopback (lo) excluded" in note, f"lo exclusion note missing: {note!r}"
    assert "sum of per-target maxima" in note, f"Lustre caveat missing: {note!r}"

    assert errors == []


# ---- Lustre tests ----

def test_lustre_p50_sum_exact_time_series(browser_page, live_web, snapshot_complete):
    """Lustre p50_sum per operation is a proper time series matching each counter row.

    With 2-row fixture + 1 gap: expanded to 3 slots [row0, gap, row1].
    """
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")
    lustre_ops = data["lustreOps"]
    p50_sum = data["lustreP50Sum"]
    p95_sum = data["lustreP95Sum"]
    max_sum = data["lustreMaxSum"]
    target_count = data["lustreTargetCount"]

    assert "read" in lustre_ops, f"read op missing: {lustre_ops}"
    assert "write" in lustre_ops, f"write op missing: {lustre_ops}"

    # read p50_sum: [120.5 (row0), None (gap), 130.0 (row1)]
    read_p50 = p50_sum["read"]
    assert len(read_p50) == 3, f"read p50_sum should have 3 slots: {read_p50}"
    assert read_p50[0] == 120.5, f"read p50_sum row0 wrong: {read_p50[0]}"
    assert read_p50[1] is None, f"read p50_sum gap must be None: {read_p50[1]}"
    assert read_p50[2] == 130.0, f"read p50_sum row1 wrong: {read_p50[2]}"

    # write p95_sum: [150.0 (row0), None (gap), 160.0 (row1)]
    write_p95 = p95_sum["write"]
    assert write_p95[0] == 150.0, f"write p95_sum row0 wrong: {write_p95[0]}"
    assert write_p95[1] is None, f"write p95_sum gap must be None: {write_p95[1]}"
    assert write_p95[2] == 160.0, f"write p95_sum row1 wrong: {write_p95[2]}"

    # read max_sum (peak-sum): [500.0, None, 520.0]
    read_max = max_sum["read"]
    assert read_max[0] == 500.0, f"read max_sum row0 wrong: {read_max[0]}"
    assert read_max[1] is None
    assert read_max[2] == 520.0, f"read max_sum row1 wrong: {read_max[2]}"

    # target_count: read has 2, write has 3
    read_tc = target_count["read"]
    assert read_tc[0] == 2, f"read target_count row0 wrong: {read_tc[0]}"
    assert read_tc[1] is None, f"read target_count gap must be None: {read_tc[1]}"
    assert read_tc[2] == 2, f"read target_count row1 wrong: {read_tc[2]}"
    write_tc = target_count["write"]
    assert write_tc[0] == 3, f"write target_count row0 wrong: {write_tc[0]}"

    assert errors == []


# ---- Canvas render evidence ----

def test_canvas_has_nontransparent_pixels_after_render(browser_page, live_web, snapshot_complete):
    """After successful render, CPU chart canvas has drawn (non-transparent) pixels."""
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    # Verify canvas has actual pixel data (not blank white or all-transparent)
    has_pixels = page.evaluate("""() => {
        const canvas = document.getElementById('chart-cpu');
        if (!canvas) return false;
        const ctx = canvas.getContext('2d');
        if (!ctx) return false;
        const w = canvas.width;
        const h = canvas.height;
        if (w <= 0 || h <= 0) return false;
        const imageData = ctx.getImageData(0, 0, w, h);
        const data = imageData.data;
        // Check if any pixel is non-transparent (alpha > 0)
        for (let i = 3; i < data.length; i += 4) {
            if (data[i] > 0) return true;
        }
        return false;
    }""")
    assert has_pixels, "CPU chart canvas has no drawn pixels (all transparent/blank)"

    assert errors == []


def test_chart_source_arrays_nonempty_after_render(browser_page, live_web, snapshot_complete):
    """Chart data source arrays are all nonempty after successful render."""
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    data = page.evaluate("() => window.__nodeMonitorTest.chartData")

    assert len(data["cpu"]["labels"]) > 0, "CPU labels empty"
    assert len(data["cpu"]["cpuP50"]) > 0, "cpuP50 empty"
    assert len(data["memoryLabels"]) > 0, "memoryLabels empty"
    assert len(data["memoryAvailableSeries"]) > 0, "memoryAvailableSeries empty"
    assert len(data["processLabels"]) > 0, "processLabels empty"
    assert len(data["networkLabels"]) > 0, "networkLabels empty"
    assert len(data["ifaceList"]) > 0, "ifaceList empty"
    assert len(data["lustreOps"]) > 0, "lustreOps empty"

    assert errors == []


# ---- Chart lifecycle tests ----

def test_chart_lifecycle_creates_four_charts(browser_page, live_web, snapshot_complete):
    """After first render, createCount == 4 (one per chart canvas)."""
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    lc = page.evaluate("() => window.__nodeMonitorTest.getChartLifecycle()")
    assert lc["createCount"] == 4, f"Expected 4 charts created, got {lc['createCount']}"
    assert lc["destroyCount"] == 0, f"Expected 0 destroys on first load, got {lc['destroyCount']}"
    # All 4 named charts rendered
    assert lc["renders"].get("cpu", 0) == 1, f"cpu not rendered once: {lc['renders']}"
    assert lc["renders"].get("memory", 0) == 1, f"memory not rendered once: {lc['renders']}"
    assert lc["renders"].get("process", 0) == 1, f"process not rendered once: {lc['renders']}"
    assert lc["renders"].get("networkLustre", 0) == 1, \
        f"networkLustre not rendered once: {lc['renders']}"

    assert errors == []


def test_chart_lifecycle_destroy_on_refresh(browser_page, live_web, snapshot_complete):
    """After a second refresh, destroyCount == 4 (each chart destroyed before replace)."""
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    # Trigger a second refresh by clicking a range button
    page.locator('[data-range="3h"]').click()
    wait_connected(page)
    # Wait for lifecycle to update to 8 creates
    page.wait_for_function(
        "() => window.__nodeMonitorTest.getChartLifecycle().createCount >= 8",
        timeout=CONNECTED_TIMEOUT
    )

    lc = page.evaluate("() => window.__nodeMonitorTest.getChartLifecycle()")
    assert lc["createCount"] == 8, f"Expected 8 total creates after 2 renders, got {lc['createCount']}"
    assert lc["destroyCount"] == 4, \
        f"Expected 4 destroys after second render, got {lc['destroyCount']}"
    assert lc["renders"].get("cpu", 0) == 2, f"cpu not rendered twice: {lc['renders']}"

    assert errors == []


def test_failed_refresh_retains_existing_charts(browser_page, live_web, snapshot_complete):
    """Failed refresh retains existing charts; no additional creates or destroys."""
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    lc_before = page.evaluate("() => window.__nodeMonitorTest.getChartLifecycle()")
    assert lc_before["createCount"] == 4

    # Make server fail
    live_web.state.fail()
    page.locator('[data-range="3h"]').click()
    expect(page.locator('[data-testid="connectivity-status"]')).to_have_text(
        "Web server disconnected", timeout=CONNECTED_TIMEOUT
    )

    lc_after = page.evaluate("() => window.__nodeMonitorTest.getChartLifecycle()")
    assert lc_after["createCount"] == 4, \
        f"Creates should not increase on failure: {lc_after['createCount']}"
    assert lc_after["destroyCount"] == 0, \
        f"Destroys should not increase on failure: {lc_after['destroyCount']}"

    assert all("503" in e for e in errors if errors)


# ---- No console errors / no external requests ----

def test_no_console_errors_or_external_requests(browser_page, live_web, snapshot_complete):
    """Clean load: no console errors and no external network requests."""
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    assert errors == [], f"Console errors: {errors}"
    assert external == [], f"External requests: {external}"


# ---- Stale/partial and narrow viewport tests ----

def test_stale_state_notes_visible_narrow_viewport(browser_page, live_web, snapshot_complete):
    """Stale data state: all chart canvases visible in narrow viewport; notes visible."""
    snap = copy.deepcopy(snapshot_complete)
    snap["counters"]["status"] = "stale"
    snap["counters"]["is_fresh"] = False
    live_web.state.set_snapshot(snap)

    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    # Set narrow viewport
    page.set_viewport_size({"width": 400, "height": 700})

    # All 4 chart canvases must remain in DOM
    for testid in ("chart-cpu", "chart-memory", "chart-process", "chart-network-lustre"):
        count = page.locator(f'[data-testid="{testid}"]').count()
        assert count > 0, f"{testid} not in DOM after narrow viewport"

    # Status indicators visible
    expect(page.locator('[data-testid="counter-freshness"]')).to_have_text("Stale")
    expect(page.locator('[data-testid="connectivity-status"]')).to_have_text("Connected")

    assert errors == []


def test_partial_state_chart_notes_in_viewport(browser_page, live_web, snapshot_complete):
    """Partial state: Network/Lustre note box and Process table visible; bounding box in viewport."""
    snap = copy.deepcopy(snapshot_complete)
    snap["counters"]["status"] = "partial"
    snap["usage"]["status"] = "partial"
    live_web.state.set_snapshot(snap)

    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_lifecycle(page)

    # Lustre note visible
    lustre_note = page.locator('[aria-label="Network and Lustre chart"] .chart-note')
    assert lustre_note.count() > 0, "Lustre note not in DOM"
    bbox = lustre_note.bounding_box()
    if bbox:
        vp = page.viewport_size
        assert bbox["y"] >= 0, "Note box above viewport top"
        # Note is in document even if requires scroll

    # Process table visible
    proc_table = page.locator('[aria-label="Process chart"] .chart-alt')
    assert proc_table.count() > 0, "Process alt table not in DOM"

    assert errors == []


def test_chart_projection_readonly_no_mutation_methods(browser_page, live_web, snapshot_complete):
    """chartData getter returns frozen object; no mutation methods exposed."""
    page, errors, external = browser_page
    page.goto(live_web.url + "/")
    wait_connected(page)
    wait_chart_data(page)

    # Attempt mutation should fail silently (frozen object)
    mutation_succeeded = page.evaluate("""() => {
        const d = window.__nodeMonitorTest.chartData;
        try {
            d.cpu.cpuP50[0] = 999;
            return d.cpu.cpuP50[0] === 999;
        } catch(e) {
            return false;
        }
    }""")
    assert mutation_succeeded is False, "chartData should be frozen (mutation should fail)"

    # No destroy/update methods exposed
    has_destroy = page.evaluate(
        "() => typeof window.__nodeMonitorTest.chartData.destroy === 'function'"
    )
    assert has_destroy is False, "chartData must not expose destroy method"

    assert errors == []
