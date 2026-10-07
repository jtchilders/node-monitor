(function() {
   'use strict';

   // ---- PBS Monitor color palette ----
   const PALETTE = [
      '#3b82f6', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4',
      '#10b981', '#f43f5e', '#eab308', '#14b8a6', '#ec4899',
   ];
   const GRID_COLOR = '#2d3748';
   const TICK_COLOR = '#94a3b8';
   const LEGEND_COLOR = '#e0e0e0';

   // ---- Legend hover descriptions ----
   // Maps dataset label text (or the start of it) to a plain-English explanation.
   // Dynamic labels (interface names, Lustre ops, categories) are matched by suffix.
   const SERIES_HELP = {
      // CPU / Load
      'CPU p50 %':   'Median CPU busy percentage across all logical cores in each 1-minute window.',
      'CPU p95 %':   '95th-percentile CPU busy % — only 5% of samples were higher.',
      'CPU max %':   'Peak CPU busy % seen in any sample within each 1-minute window.',
      'Load1':       '1-minute load average: mean number of processes in runnable or uninterruptible (D-state/IO-wait) state. Spiky — reacts within seconds to bursts. Values above the logical CPU count signal saturation.',
      'Load5':       '5-minute load average: same metric exponentially smoothed over 5 minutes. Filters out short spikes; sustained elevation means persistent demand.',
      'Load15':      '15-minute load average: longest smoothing window. Shows the background trend — if load15 is high while load1 is low, a burst just ended; if load1 >> load15, a burst is in progress.',
      'CPU count':   'Saturation threshold: the number of logical CPUs on this node. Load above this line means processes are queuing for CPU time.',
      // Memory (labels include units that change with toggle)
      'System used':  'Physical memory in use (MemTotal − MemAvailable) at each 1-minute sample. Includes kernel buffers and page cache.',
      'Total memory': 'Total installed physical memory (constant reference line).',
      // Process
      'cpu_seconds (additive, 15-min interval)':   'Total CPU seconds consumed by all matched processes in each 15-minute usage interval.',
      'process count max per grain':               'Highest concurrent process count observed in any single grain within each 15-minute interval.',
      'grain total RSS (rss_max_kb)':              'Maximum total resident set size (KB) of the largest-contributing grain in each 15-minute interval.',
   };

   // Patterns checked by suffix / contains for dynamic labels
   const SERIES_HELP_PATTERNS = [
      // Category RSS (memory chart)
      { match: 'RSS p50 (non-additive)',
        help: 'Median RSS for this process category — non-additive hotspot grain; may double-count shared pages.' },
      // Network
      { match: 'RX p50 (B/s)',   help: 'Median receive throughput (bytes/sec) in each 1-minute window.' },
      { match: 'TX p50 (B/s)',   help: 'Median transmit throughput (bytes/sec) in each 1-minute window.' },
      { match: 'RX p95 (B/s)',   help: '95th-percentile receive throughput (bytes/sec).' },
      { match: 'TX p95 (B/s)',   help: '95th-percentile transmit throughput (bytes/sec).' },
      { match: 'RX max (B/s)',   help: 'Peak receive throughput (bytes/sec) seen in any sample.' },
      { match: 'TX max (B/s)',   help: 'Peak transmit throughput (bytes/sec) seen in any sample.' },
      // Lustre
      { match: 'p50-sum (sum of per-target p50)',          help: 'Sum of per-target median rates for this Lustre metadata operation.' },
      { match: 'p95-sum (sum of per-target p95)',          help: 'Sum of per-target 95th-percentile rates for this Lustre operation.' },
      { match: 'peak-sum/max_sum (sum of per-target maxima)', help: 'Sum of per-target peak rates — not an instantaneous global peak.' },
      { match: 'target_count',   help: 'Number of Lustre OST/MDT targets seen for this operation.' },
   ];

   function seriesDescription(label) {
      if (!label) return null;
      // Exact match first (static labels)
      if (SERIES_HELP[label]) return SERIES_HELP[label];
      // Prefix match for labels that include dynamic units, e.g. "System used (GiB)"
      var keys = Object.keys(SERIES_HELP);
      for (var i = 0; i < keys.length; i++) {
         if (label.indexOf(keys[i]) === 0) return SERIES_HELP[keys[i]];
      }
      // Suffix / contains match for dynamic interface/op/category names
      for (var j = 0; j < SERIES_HELP_PATTERNS.length; j++) {
         if (label.indexOf(SERIES_HELP_PATTERNS[j].match) !== -1) {
            return SERIES_HELP_PATTERNS[j].help;
         }
      }
      return null;
   }

   // Floating tooltip element for legend hovers — created once
   var legendTip = document.createElement('div');
   legendTip.className = 'legend-tip';
   legendTip.setAttribute('role', 'tooltip');
   legendTip.style.display = 'none';
   document.body.appendChild(legendTip);

   // Format an ISO timestamp or epoch-ms as local HH:MM for chart labels.
   // Range is 1–24 h, so the date is always implicit.
   function formatTimeLabel(value) {
      if (!value) return '';
      const d = (typeof value === 'number') ? new Date(value) : new Date(value);
      if (isNaN(d.getTime())) return '';
      const h = d.getHours();
      const m = d.getMinutes();
      return (h < 10 ? '0' : '') + h + ':' + (m < 10 ? '0' : '') + m;
   }

   // ---- State ----
   const state = {
      snapshot: null,
      connected: false,
      receivedMonotonicMs: 0,
      counterAgeAtReceiptSec: null,
      usageAgeAtReceiptSec: null,
      currentNode: null,
      currentRange: '1h',
      currentUsername: null,
      refreshTimer: null,
      ageTimer: null,
   };

   const endpoint = '/api/dashboard';
   const qs = function(selector) { return document.querySelector(selector); };
   const qsa = function(selector) { return document.querySelectorAll(selector); };

   function finiteNumber(value) {
      const number = Number(value);
      return Number.isFinite(number) ? number : null;
   }

   function formatAge(seconds) {
      const value = finiteNumber(seconds);
      return value === null ? '-' : Math.max(0, Math.floor(value)) + 's';
   }

   function ageAtReceipt(serverNow, timestamp) {
      if (!serverNow || !timestamp) return null;
      const serverMs = Date.parse(serverNow);
      const valueMs = Date.parse(timestamp);
      if (!Number.isFinite(serverMs) || !Number.isFinite(valueMs)) return null;
      return Math.max(0, (serverMs - valueMs) / 1000);
   }

   function freshnessLabel(section) {
      const status = section && section.status ? section.status : 'empty';
      if (status === 'empty') return 'Empty';
      if (status === 'partial') return 'Partial';
      if (status === 'stale' || !section.is_fresh) return 'Stale';
      return 'Current';
   }

   // Narrow presentation-state helpers for operational-state styling.
   // Derive exclusively from response semantics; never parse text.
   const ALLOWED_CARD_STATES = ['current', 'partial', 'stale', 'empty'];

   function normalizedSectionState(section) {
      const status = section && section.status ? section.status : 'empty';
      if (status === 'empty') return 'empty';
      if (status === 'partial') return 'partial';
      if (status === 'stale') return 'stale';
      if (section && section.is_fresh === false) return 'stale';
      return 'current';
   }

   function renderGapWarning(counters) {
      var strip = qs('#gap-warning-strip');
      var text = qs('[data-testid="gap-warning-text"]');
      if (!strip || !text) return;
      var gaps = counters && counters.gaps ? counters.gaps : {};
      var missing = gaps.missing_count || 0;
      var maxGap = gaps.max_gap_minutes || 0;
      if (missing > 0) {
         strip.hidden = false;
         text.textContent = missing + ' missing one-minute counter window(s) in the selected range (maximum gap ' + maxGap + ' minutes)';
      } else {
         strip.hidden = true;
         text.textContent = '—';
      }
   }

   function setExclusiveStateClass(el, stateName) {
      if (!el) return;
      ALLOWED_CARD_STATES.forEach(function(s) {
         el.classList.remove('state-' + s);
      });
      if (ALLOWED_CARD_STATES.indexOf(stateName) >= 0) {
         el.classList.add('state-' + stateName);
      }
   }

   function renderPresentationState(counters, usage, connected) {
      const header = qs('.header-bar');
      if (header) {
         header.classList.remove('is-connected', 'is-disconnected');
         header.classList.add(connected ? 'is-connected' : 'is-disconnected');
      }
      setExclusiveStateClass(qs('[data-testid="counter-card"]'), normalizedSectionState(counters || {}));
      setExclusiveStateClass(qs('[data-testid="usage-card"]'), normalizedSectionState(usage || {}));
   }

   function setActiveRange() {
      qsa('[data-testid="range-btn"]').forEach(function(button) {
         const active = button.getAttribute('data-range') === state.currentRange;
         button.setAttribute('aria-pressed', active ? 'true' : 'false');
      });
   }

   function dashboardUrl() {
      const url = new URL(endpoint, location.origin);
      url.searchParams.set('node', state.currentNode);
      url.searchParams.set('range', state.currentRange);
      if (state.currentUsername) url.searchParams.set('username', state.currentUsername);
      return url.toString();
   }

   async function fetchDashboard() {
      const response = await fetch(dashboardUrl(), {
         cache: 'no-store',
         signal: AbortSignal.timeout(15000),
      });
      if (!response.ok) {
         const error = new Error('refresh failed');
         error.status = response.status;
         throw error;
      }
      return response.json();
   }

   function latestCounter(counters) {
      return counters && counters.latest ? counters.latest : null;
   }

   function renderCounterQuality(counters) {
      const rows = counters && Array.isArray(counters.rows) ? counters.rows : [];
      const latest = rows.length ? rows[rows.length - 1] : null;
      if (!latest) {
         qs('[data-testid="counter-quality"]').textContent = 'No counter samples in range';
         return;
      }
      const coverage = finiteNumber(latest.coverage);
      const coverageText = coverage === null ? '-' : (coverage * 100).toFixed(1) + '%';
      const samples = latest.sample_count == null ? '-' : String(latest.sample_count);
      const expected = latest.expected_count == null ? '-' : String(latest.expected_count);
      const gaps = counters.gaps && counters.gaps.missing_count != null
         ? String(counters.gaps.missing_count) : '-';
      qs('[data-testid="counter-quality"]').textContent =
         'Coverage ' + coverageText + '; samples ' + samples + '/' + expected
         + '; missing windows ' + gaps;
   }

   function renderMemory(data, latest) {
      const usedKb = latest ? finiteNumber(latest.mem_used_physical_kb) : null;
      const totalKb = data.hardware ? finiteNumber(data.hardware.mem_total_kb) : null;
      let text = '-';
      if (usedKb !== null) {
         text = (usedKb / (1024 * 1024)).toFixed(2) + ' GiB';
         if (totalKb !== null && totalKb > 0) {
            text += ' / ' + (totalKb / (1024 * 1024)).toFixed(2) + ' GiB ('
               + ((usedKb / totalKb) * 100).toFixed(1) + '%)';
         } else {
            text += ' (percentage unavailable)';
         }
      }
      qs('[data-testid="mem-used"]').textContent = text;
   }

   function renderUsage(data) {
      const usage = data.usage || {};
      const grains = Array.isArray(usage.grains) ? usage.grains : [];
      // Per-interval maximum d_state with exact username and interval_end
      let hotspot = null;
      grains.forEach(function(grain) {
         const value = finiteNumber(grain.d_state_fraction);
         if (value !== null && (!hotspot || value > hotspot.value)) {
            hotspot = { value: value, grain: grain };
         }
      });
      qs('[data-testid="d-state"]').textContent = hotspot
         ? (hotspot.value * 100).toFixed(1) + '%' : '-';
      qs('[data-testid="usage-timestamp"]').textContent = hotspot
         ? (hotspot.grain.interval_end || '-') : '-';
      qs('[data-testid="usage-username"]').textContent = hotspot
         ? (hotspot.grain.d_state_username || '-') : '-';

      let matchText = 'All usernames';
      if (state.currentUsername) {
         matchText = grains.length ? 'Username matched' : 'No matching usage';
      }
      qs('[data-testid="username-result"]').textContent = matchText;
   }

   function renderFailures(data) {
      const failures = Array.isArray(data.poll_failures)
         ? data.poll_failures.slice(0, 10) : [];
      qs('[data-testid="poll-failures"]').textContent = String(failures.length);
      qs('[data-testid="poll-breaker"]').textContent = failures.length
         ? (failures[0].breaker_state || '-') : '-';
      qs('[data-testid="poll-failures-text"]').textContent = failures.length
         ? failures.map(function(failure) {
            return (failure.recorded_at || '-') + ' '
               + (failure.loop || '-') + ' ' + (failure.failure_type || '-')
               + (failure.detail ? ': ' + failure.detail : '');
         }).join('; ')
         : 'No recent poll failures';
   }

   function renderAges() {
      if (!state.snapshot || !state.receivedMonotonicMs) return;
      const elapsed = (performance.now() - state.receivedMonotonicMs) / 1000;
      const counterAge = state.counterAgeAtReceiptSec === null
         ? null : state.counterAgeAtReceiptSec + elapsed;
      const usageAge = state.usageAgeAtReceiptSec === null
         ? null : state.usageAgeAtReceiptSec + elapsed;
      const ageElement = qs('[data-testid="data-age-seconds"]');
      ageElement.textContent = formatAge(counterAge);
      ageElement.setAttribute('data-age-seconds', counterAge === null
         ? '' : String(Math.floor(counterAge)));
      qs('[data-testid="counter-age"]').textContent = 'Counter age ' + formatAge(counterAge);
      qs('[data-testid="usage-age"]').textContent = 'Usage age ' + formatAge(usageAge);
   }

   function render(data) {
      const counters = data.counters || {};
      const usage = data.usage || {};
      const latest = latestCounter(counters);
      state.snapshot = data;
      state.connected = true;
      state.receivedMonotonicMs = performance.now();
      state.counterAgeAtReceiptSec = ageAtReceipt(
         data.server_utc_now, counters.newest_window_end);
      state.usageAgeAtReceiptSec = ageAtReceipt(
         data.server_utc_now, usage.newest_interval_end);

      qs('[data-testid="connectivity-status"]').textContent = 'Connected';
      qs('[data-testid="counter-status"]').textContent = counters.status || 'empty';
      qs('[data-testid="counter-freshness"]').textContent = freshnessLabel(counters);
      qs('[data-testid="usage-status"]').textContent = usage.status || 'empty';
      qs('[data-testid="usage-freshness"]').textContent = freshnessLabel(usage);
      qs('[data-testid="data-age-value"]').textContent =
         counters.newest_window_end || '-';
      qs('[data-testid="newest-timestamp"]').textContent =
         counters.newest_window_end || '-';

      qs('[data-testid="cpu-busy"]').textContent = latest
         && latest.cpu_busy_pct && finiteNumber(latest.cpu_busy_pct.p50) !== null
         ? finiteNumber(latest.cpu_busy_pct.p50).toFixed(1) + '%' : '-';
      ['load1', 'load5', 'load15', 'procs_running', 'procs_total'].forEach(
         function(name) {
            qs('[data-testid="' + name.replace('_', '-') + '"]').textContent =
               latest && latest[name] != null ? String(latest[name]) : '-';
         });

      renderCounterQuality(counters);
      renderMemory(data, latest);
      renderUsage(data);
      renderFailures(data);

      const hardware = data.hardware || {};
      qs('[data-testid="system-name"]').textContent = hardware.system || '-';
      qs('[data-testid="node-name"]').textContent = data.node || state.currentNode;
      qs('[data-testid="current-node"]').textContent = data.node || state.currentNode;
      qs('[data-testid="current-range"]').textContent = state.currentRange;
      qs('[data-testid="current-username"]').textContent = state.currentUsername || '-';
      qs('[data-testid="hardware-context"]').textContent =
         (hardware.cpu_model || 'CPU unavailable') + '; '
         + (hardware.cpu_logical == null ? '-' : hardware.cpu_logical)
         + ' logical CPUs; ' + (hardware.os_pretty_name || 'OS unavailable')
         + '; memory total ' + (hardware.mem_total_kb == null
            ? 'unavailable' : String(hardware.mem_total_kb) + ' KiB');
      renderCharts(data);
      setActiveRange();
      renderAges();
      qs('#retry-section').hidden = true;
      renderPresentationState(data.counters || {}, data.usage || {}, true);
      renderGapWarning(data.counters || {});
   }

   function renderFailure(firstLoad) {
      state.connected = false;
      const header = qs('.header-bar');
      if (header) {
         header.classList.remove('is-connected');
         header.classList.add('is-disconnected');
      }
      // NEVER recompute or erase retained card presentation classes
      qs('[data-testid="connectivity-status"]').textContent = firstLoad
         ? 'Connection failure' : 'Web server disconnected';
      qs('#retry-section').hidden = !firstLoad;
      const preservedCounters = (state.snapshot && state.snapshot.counters) ? state.snapshot.counters : {};
      const preservedUsage = (state.snapshot && state.snapshot.usage) ? state.snapshot.usage : {};
      renderPresentationState(preservedCounters, preservedUsage, false);
      renderGapWarning(preservedCounters);
   }

   async function refreshDashboard(recoverUnavailable) {
      if (!state.currentNode) return false;
      const mayRecover = recoverUnavailable !== false;
      try {
         render(await fetchDashboard());
         return true;
      } catch (error) {
         if (error.status === 422 && mayRecover) {
            state.currentNode = null;
            await initInventorySelection(false);
            return state.connected;
         }
         if (error.status === 422) {
            qs('#node-status').textContent = 'Node unavailable';
            state.connected = false;
            qs('[data-testid="connectivity-status"]').textContent =
               'Node unavailable';
            return false;
         }
         renderFailure(state.snapshot === null);
         return false;
      }
   }

   function updateSelection(node, rangeName, username) {
      if (node) state.currentNode = node;
      if (rangeName) state.currentRange = rangeName;
      state.currentUsername = username ? username : null;
      setActiveRange();
   }

   function startTimers() {
      if (state.ageTimer === null) {
         state.ageTimer = setInterval(renderAges, 1000);
      }
      if (state.refreshTimer === null) {
         state.refreshTimer = setInterval(refreshDashboard, 60000);
      }
   }

   qsa('[data-testid="range-btn"]').forEach(function(button) {
      button.addEventListener('click', async function() {
         updateSelection(null, button.getAttribute('data-range'),
                         qs('#user-input').value);
         await refreshDashboard();
      });
   });

   qs('#user-form').addEventListener('submit', async function(event) {
      event.preventDefault();
      updateSelection(null, null, qs('#user-input').value);
      await refreshDashboard();
   });

   qs('#retry-btn').addEventListener('click', async function() {
      if (state.currentNode) {
         await refreshDashboard();
      } else {
         await initInventorySelection();
      }
   });

   // ---- Chart registry with lifecycle tracking ----
   // Maps chart name -> Chart instance
   const chartRegistry = {};
   // Lifecycle counters exposed to tests
   const chartLifecycle = {
      createCount: 0,
      destroyCount: 0,
      // Per-name render counts
      renders: {},
   };
   // Current render state: mode and last rendered datasets per chart
   // Used by getChartRenderState() test API
   const chartRenderState = {
      cpu: { mode: 'cpu', labels: [], datasets: [] },
      memory: { mode: 'mem-mode-gib', labels: [], datasets: [] },
      process: { mode: 'proc-mode-cpusum', labels: [], datasets: [], contributorSummary: null },
      networkLustre: { mode: 'nl-network-p50', labels: [], datasets: [] },
   };

   // ---- Chart lifecycle: create once, update in-place ----
   function renderOrUpdateChart(name, canvasId, config) {
      const canvas = document.getElementById(canvasId);
      if (!canvas) return null;

      if (chartRegistry[name]) {
         // In-place update: replace data + options, call update()
         const chart = chartRegistry[name];
         chart.data = config.data;
         chart.options = config.options;
         chart.update();
         chartLifecycle.renders[name] = (chartLifecycle.renders[name] || 0) + 1;
         return chart;
      }

      // First render: create the chart
      const chart = new Chart(canvas, config);
      chartRegistry[name] = chart;
      chartLifecycle.createCount++;
      chartLifecycle.renders[name] = (chartLifecycle.renders[name] || 0) + 1;
      return chart;
   }

   // ---- PBS-themed scale defaults ----
   function pbsScaleDefaults(overrides) {
      const base = {
         grid: { color: GRID_COLOR },
         ticks: { color: TICK_COLOR },
      };
      return Object.assign(base, overrides);
   }

   function pbsChartOptions(scales, tooltipLabelCb) {
      return {
         responsive: true,
         maintainAspectRatio: false,
         plugins: {
            legend: {
               position: 'bottom',
               labels: { color: LEGEND_COLOR, boxWidth: 24, boxHeight: 2 },
               onHover: function(evt, legendItem) {
                  var desc = seriesDescription(legendItem.text);
                  if (!desc) { legendTip.style.display = 'none'; return; }
                  legendTip.textContent = desc;
                  legendTip.style.display = 'block';
                  // Position near cursor; stay on-screen
                  var x = evt.x != null ? evt.x : (evt.native ? evt.native.clientX : 0);
                  var y = evt.y != null ? evt.y : (evt.native ? evt.native.clientY : 0);
                  // evt.x/y are canvas-relative; convert to page coords
                  var canvas = evt.chart ? evt.chart.canvas : null;
                  if (canvas) {
                     var rect = canvas.getBoundingClientRect();
                     x = rect.left + x;
                     y = rect.top + y;
                  }
                  var tipW = legendTip.offsetWidth || 200;
                  var tipH = legendTip.offsetHeight || 30;
                  var maxX = window.innerWidth - tipW - 12;
                  legendTip.style.left = Math.max(4, Math.min(x + 12, maxX)) + 'px';
                  legendTip.style.top = Math.max(4, y - tipH - 8) + 'px';
               },
               onLeave: function() {
                  legendTip.style.display = 'none';
               },
            },
            tooltip: {
               mode: 'index',
               intersect: false,
               callbacks: tooltipLabelCb ? { label: tooltipLabelCb } : {},
            },
         },
         scales: scales,
      };
   }

   // ---- Aligned gap expansion for counter time series ----
   // Input: sorted rows with window_end ISO strings, cadence 60s
   // Output: { labels, indices } where indices map expanded positions to original row positions (null = gap)
   // Bound: between first and last row timestamp; max 1440 insertions (24h at 1min cadence)
   function buildExpandedCounterSeries(sortedRows) {
      if (!sortedRows || !sortedRows.length) {
         return { labels: [], rowIndices: [] };
      }
      const CADENCE_MS = 60000;
      const MAX_INSERTIONS = 1440; // 24h * 60min - bounds for range queries
      const labels = [];
      const rowIndices = []; // null = gap insertion, integer = original sortedRows index

      labels.push(formatTimeLabel(sortedRows[0].window_end));
      rowIndices.push(0);

      let totalInserted = 0;

      for (let i = 1; i < sortedRows.length; i++) {
         const prevTime = sortedRows[i - 1].window_end
            ? new Date(sortedRows[i - 1].window_end).getTime() : null;
         const currTime = sortedRows[i].window_end
            ? new Date(sortedRows[i].window_end).getTime() : null;

         if (prevTime !== null && currTime !== null && currTime > prevTime + CADENCE_MS + 30000) {
            // Gap detected: insert null slots for each missing aligned minute
            // Align to cadence from prevTime
            let nextAligned = prevTime + CADENCE_MS;
            while (nextAligned < currTime - 30000 && totalInserted < MAX_INSERTIONS) {
               const gapLabel = formatTimeLabel(nextAligned) + ' (gap)';
               labels.push(gapLabel);
               rowIndices.push(null);
               nextAligned += CADENCE_MS;
               totalInserted++;
            }
         }
         labels.push(formatTimeLabel(sortedRows[i].window_end));
         rowIndices.push(i);
      }

      return { labels: labels, rowIndices: rowIndices };
   }

   // ---- Build complete chart data from a snapshot ----
   function buildChartData(snapshot) {
      const counters = snapshot && snapshot.counters ? snapshot.counters : {};
      const usage = snapshot && snapshot.usage ? snapshot.usage : {};
      const rows = counters.rows || [];
      const grains = usage.grains || [];
      const hardware = snapshot && snapshot.hardware ? snapshot.hardware : {};
      const memTotalKb = hardware.mem_total_kb != null ? hardware.mem_total_kb : null;

      // Sort counter rows by window_end ascending
      const sortedRows = rows.slice().sort(function(a, b) {
         const ta = a.window_end ? new Date(a.window_end).getTime() : 0;
         const tb = b.window_end ? new Date(b.window_end).getTime() : 0;
         return ta - tb;
      });

      // Build gap-expanded label/index mapping
      const expanded = buildExpandedCounterSeries(sortedRows);
      const expandedLabels = expanded.labels;
      const rowIndices = expanded.rowIndices;

      // Extract scalar series from expanded mapping (null for gap slots)
      function extractSeries(extractor) {
         return rowIndices.map(function(idx) {
            if (idx === null) return null; // gap: null not zero
            return extractor(sortedRows[idx]);
         });
      }

      const cpuP50 = extractSeries(function(r) {
         return (r.cpu_busy_pct && r.cpu_busy_pct.p50 != null) ? r.cpu_busy_pct.p50 : null;
      });
      const cpuP95 = extractSeries(function(r) {
         return (r.cpu_busy_pct && r.cpu_busy_pct.p95 != null) ? r.cpu_busy_pct.p95 : null;
      });
      const cpuMax = extractSeries(function(r) {
         return (r.cpu_busy_pct && r.cpu_busy_pct.max != null) ? r.cpu_busy_pct.max : null;
      });
      const load1 = extractSeries(function(r) { return r.load1 != null ? r.load1 : null; });
      const load5 = extractSeries(function(r) { return r.load5 != null ? r.load5 : null; });
      const load15 = extractSeries(function(r) { return r.load15 != null ? r.load15 : null; });

      // Memory: per-row MemAvailable; derived used = MemTotal - MemAvailable (null if total missing)
      const memTotalSeries = extractSeries(function() {
         return memTotalKb != null && memTotalKb > 0 ? memTotalKb : null;
      });
      const memUsedSeries = extractSeries(function(r) {
         // Used = MemTotal_kb - MemAvailable_kb; null if total missing, zero, or avail missing
         if (memTotalKb == null || memTotalKb <= 0 || r.mem_available_kb == null) return null;
         return memTotalKb - r.mem_available_kb;
      });

      // ---- Aggregate grains by interval_end ----
      // Collect distinct interval_end values in sorted order
      const intervalEndSet = {};
      grains.forEach(function(g) {
         if (g.interval_end) intervalEndSet[g.interval_end] = true;
      });
      const sortedIntervalEnds = Object.keys(intervalEndSet).sort();

      // Build aggregated interval objects: one per distinct interval_end
      // processCPU: SUM of cpu_seconds (additive)
      // dState, interactivity, countMax, rssMax, countP50, countP95, rssP50, rssP95:
      //   MAX-grain selection with independently attributed username
      const aggregatedIntervals = sortedIntervalEnds.map(function(iend) {
         var cpuSum = null;
         var dStateFrac = null, dStateUser = null;
         var interactFrac = null, interactUser = null;
         var cntMax = null, cntMaxUser = null;
         var cntP50 = null, cntP50User = null;
         var cntP95 = null, cntP95User = null;
         var rssMax = null, rssMaxUser = null;
         var rssP50 = null, rssP50User = null;
         var rssP95 = null, rssP95User = null;

         grains.forEach(function(g) {
            if (g.interval_end !== iend) return;

            // cpu_seconds: additive sum
            if (g.cpu_seconds != null) {
               cpuSum = (cpuSum === null ? 0 : cpuSum) + g.cpu_seconds;
            }

            // d_state_fraction: maximum grain wins, carry that grain's username
            if (g.d_state_fraction != null &&
                (dStateFrac === null || g.d_state_fraction > dStateFrac)) {
               dStateFrac = g.d_state_fraction;
               dStateUser = g.d_state_username || null;
            }

            // interactivity_fraction: maximum grain wins (observation-weighted per grain)
            if (g.interactivity_fraction != null &&
                (interactFrac === null || g.interactivity_fraction > interactFrac)) {
               interactFrac = g.interactivity_fraction;
               interactUser = g.interactivity_username || null;
            }

            // process_count_max: hotspot grain wins independently
            if (g.process_count_max != null &&
                (cntMax === null || g.process_count_max > cntMax)) {
               cntMax = g.process_count_max;
               cntMaxUser = g.process_count_max_username || null;
            }

            // process_count_p50: max-grain selection
            if (g.process_count_p50 != null &&
                (cntP50 === null || g.process_count_p50 > cntP50)) {
               cntP50 = g.process_count_p50;
               cntP50User = g.process_count_p50_username || null;
            }

            // process_count_p95: max-grain selection
            if (g.process_count_p95 != null &&
                (cntP95 === null || g.process_count_p95 > cntP95)) {
               cntP95 = g.process_count_p95;
               cntP95User = g.process_count_p95_username || null;
            }

            // rss_max_kb: independently attributed max
            if (g.rss_max_kb != null &&
                (rssMax === null || g.rss_max_kb > rssMax)) {
               rssMax = g.rss_max_kb;
               rssMaxUser = g.rss_max_username || null;
            }

            // rss_p50_kb: max-grain selection
            if (g.rss_p50_kb != null &&
                (rssP50 === null || g.rss_p50_kb > rssP50)) {
               rssP50 = g.rss_p50_kb;
               rssP50User = g.rss_p50_username || null;
            }

            // rss_p95_kb: max-grain selection
            if (g.rss_p95_kb != null &&
                (rssP95 === null || g.rss_p95_kb > rssP95)) {
               rssP95 = g.rss_p95_kb;
               rssP95User = g.rss_p95_username || null;
            }
         });

         return {
            interval_end: iend,
            cpuSum: cpuSum,
            dStateFrac: dStateFrac, dStateUser: dStateUser,
            interactFrac: interactFrac, interactUser: interactUser,
            cntMax: cntMax, cntMaxUser: cntMaxUser,
            cntP50: cntP50, cntP50User: cntP50User,
            cntP95: cntP95, cntP95User: cntP95User,
            rssMax: rssMax, rssMaxUser: rssMaxUser,
            rssP50: rssP50, rssP50User: rssP50User,
            rssP95: rssP95, rssP95User: rssP95User,
         };
      });

      // D-state: one point per distinct interval, max fraction + winning username
      const dStatePoints = aggregatedIntervals.map(function(iv) {
         return {
            interval_end: iv.interval_end,
            value: iv.dStateFrac,
            username: iv.dStateUser,
         };
      });

      // Interactivity: one point per distinct interval, max fraction + winning username
      // observation-weighted per stored grain; not node-wide or clock-time weighted
      const interactivityPoints = aggregatedIntervals.map(function(iv) {
         return {
            interval_end: iv.interval_end,
            value: iv.interactFrac,
            username: iv.interactUser,
         };
      });

      // Process series: one entry per distinct interval (aggregated)
      const processLabels = aggregatedIntervals.map(function(iv) {
         return iv.interval_end ? formatTimeLabel(iv.interval_end) : '-';
      });
      const processCPU = aggregatedIntervals.map(function(iv) {
         return iv.cpuSum;
      });
      const processCountP50 = aggregatedIntervals.map(function(iv) {
         return iv.cntP50;
      });
      const processCountMax = aggregatedIntervals.map(function(iv) {
         return iv.cntMax;
      });
      const processCountP95 = aggregatedIntervals.map(function(iv) {
         return iv.cntP95;
      });
      const processRSSP50 = aggregatedIntervals.map(function(iv) {
         return iv.rssP50;
      });
      const processRSSMax = aggregatedIntervals.map(function(iv) {
         return iv.rssMax;
      });
      const processRSSP95 = aggregatedIntervals.map(function(iv) {
         return iv.rssP95;
      });

      // RSS percentiles are not additive.  Select the largest activity grain
      // for each (category, interval_end), then align it to the one-minute
      // counter timeline without interpolation or zero-filling.
      const memoryCategoryRSSByTime = {};
      grains.forEach(function(g) {
         if (!g.category || !g.interval_end || g.rss_p50_kb == null) return;
         const timestamp = new Date(g.interval_end).getTime();
         if (!Number.isFinite(timestamp)) return;
         const key = String(timestamp);
         if (!memoryCategoryRSSByTime[g.category]) {
            memoryCategoryRSSByTime[g.category] = {};
         }
         const current = memoryCategoryRSSByTime[g.category][key];
         if (current == null || g.rss_p50_kb > current) {
            memoryCategoryRSSByTime[g.category][key] = g.rss_p50_kb;
         }
      });
      const memoryCategoryRSSP50 = {};
      const categoryAlignmentToleranceMs = 30000;
      Object.keys(memoryCategoryRSSByTime).sort().forEach(function(category) {
         const categoryPoints = Object.keys(memoryCategoryRSSByTime[category]).map(function(key) {
            return {
               timestamp: Number(key),
               value: memoryCategoryRSSByTime[category][key],
            };
         });
         memoryCategoryRSSP50[category] = rowIndices.map(function(idx) {
            if (idx === null) return null;
            const counterTimestamp = new Date(sortedRows[idx].window_end).getTime();
            let nearest = null;
            let nearestDistance = categoryAlignmentToleranceMs + 1;
            categoryPoints.forEach(function(point) {
               const distance = Math.abs(point.timestamp - counterTimestamp);
               if (distance < nearestDistance) {
                  nearest = point.value;
                  nearestDistance = distance;
               }
            });
            return nearestDistance <= categoryAlignmentToleranceMs ? nearest : null;
         });
      });
      // Per-interval contributor usernames (independently attributed)
      const processCountMaxUsername = aggregatedIntervals.map(function(iv) {
         return iv.cntMaxUser;
      });
      const processRSSMaxUsername = aggregatedIntervals.map(function(iv) {
         return iv.rssMaxUser;
      });

      // Network: collect all non-lo interfaces across all rows
      // For each interface, build a time series (one value per expanded counter slot)
      const ifaceSet = {};
      sortedRows.forEach(function(r) {
         if (r.network_rates) {
            Object.keys(r.network_rates).forEach(function(iface) {
               if (iface !== 'lo') ifaceSet[iface] = true;
            });
         }
      });
      const ifaceList = Object.keys(ifaceSet).sort();

      // Build per-interface RX/TX p50/p95/max time series over expanded labels
      const networkRXP50 = {};
      const networkTXP50 = {};
      const networkRXP95 = {};
      const networkTXP95 = {};
      const networkRXMax = {};
      const networkTXMax = {};
      ifaceList.forEach(function(iface) {
         networkRXP50[iface] = extractSeries(function(r) {
            return (r.network_rates && r.network_rates[iface] &&
                    r.network_rates[iface].rx_bytes_per_sec &&
                    r.network_rates[iface].rx_bytes_per_sec.p50 != null)
               ? r.network_rates[iface].rx_bytes_per_sec.p50 : null;
         });
         networkTXP50[iface] = extractSeries(function(r) {
            return (r.network_rates && r.network_rates[iface] &&
                    r.network_rates[iface].tx_bytes_per_sec &&
                    r.network_rates[iface].tx_bytes_per_sec.p50 != null)
               ? r.network_rates[iface].tx_bytes_per_sec.p50 : null;
         });
         networkRXP95[iface] = extractSeries(function(r) {
            return (r.network_rates && r.network_rates[iface] &&
                    r.network_rates[iface].rx_bytes_per_sec &&
                    r.network_rates[iface].rx_bytes_per_sec.p95 != null)
               ? r.network_rates[iface].rx_bytes_per_sec.p95 : null;
         });
         networkTXP95[iface] = extractSeries(function(r) {
            return (r.network_rates && r.network_rates[iface] &&
                    r.network_rates[iface].tx_bytes_per_sec &&
                    r.network_rates[iface].tx_bytes_per_sec.p95 != null)
               ? r.network_rates[iface].tx_bytes_per_sec.p95 : null;
         });
         networkRXMax[iface] = extractSeries(function(r) {
            return (r.network_rates && r.network_rates[iface] &&
                    r.network_rates[iface].rx_bytes_per_sec &&
                    r.network_rates[iface].rx_bytes_per_sec.max != null)
               ? r.network_rates[iface].rx_bytes_per_sec.max : null;
         });
         networkTXMax[iface] = extractSeries(function(r) {
            return (r.network_rates && r.network_rates[iface] &&
                    r.network_rates[iface].tx_bytes_per_sec &&
                    r.network_rates[iface].tx_bytes_per_sec.max != null)
               ? r.network_rates[iface].tx_bytes_per_sec.max : null;
         });
      });

      // Lustre: collect all operations across all rows
      const lustreOpSet = {};
      sortedRows.forEach(function(r) {
         if (r.lustre_md_summary) {
            Object.keys(r.lustre_md_summary).forEach(function(op) {
               lustreOpSet[op] = true;
            });
         }
      });
      const lustreOps = Object.keys(lustreOpSet).sort();

      // Build per-operation time series for p50_sum, p95_sum, max_sum, target_count
      const lustreP50Sum = {};
      const lustreP95Sum = {};
      const lustreMaxSum = {};
      const lustreTargetCount = {};
      lustreOps.forEach(function(op) {
         lustreP50Sum[op] = extractSeries(function(r) {
            return (r.lustre_md_summary && r.lustre_md_summary[op] &&
                    r.lustre_md_summary[op].p50_sum != null)
               ? r.lustre_md_summary[op].p50_sum : null;
         });
         lustreP95Sum[op] = extractSeries(function(r) {
            return (r.lustre_md_summary && r.lustre_md_summary[op] &&
                    r.lustre_md_summary[op].p95_sum != null)
               ? r.lustre_md_summary[op].p95_sum : null;
         });
         lustreMaxSum[op] = extractSeries(function(r) {
            return (r.lustre_md_summary && r.lustre_md_summary[op] &&
                    r.lustre_md_summary[op].max_sum != null)
               ? r.lustre_md_summary[op].max_sum : null;
         });
         lustreTargetCount[op] = extractSeries(function(r) {
            return (r.lustre_md_summary && r.lustre_md_summary[op] &&
                    r.lustre_md_summary[op].target_count != null)
               ? r.lustre_md_summary[op].target_count : null;
         });
      });

      // memoryUsedKb from counters.latest (snapshot-level, not per-row - for scalar displays)
      const memoryUsedKbLatest = (counters.latest && counters.latest.mem_used_physical_kb != null)
         ? counters.latest.mem_used_physical_kb : null;

      return {
         cpu: {
            labels: expandedLabels,
            cpuP50: cpuP50,
            cpuP95: cpuP95,
            cpuMax: cpuMax,
            load1: load1,
            load5: load5,
            load15: load15,
            units: 'percent_and_load',
            yAxisCPU: 'y',
            yAxisLoad: 'yLoad',
            spanGaps: false,
         },
         memoryLabels: expandedLabels,
         memoryTotalSeries: memTotalSeries,
         memoryUsedSeries: memUsedSeries,
         memoryCategoryRSSP50: memoryCategoryRSSP50,
         memoryTotalKb: memTotalKb,
         memoryUsedKbLatest: memoryUsedKbLatest,
         dStatePoints: dStatePoints,
         dStateGrains: grains,
         interactivityPoints: interactivityPoints,
         processGrains: grains,
         processLabels: processLabels,
         processCPU: processCPU,
         processCountP50: processCountP50,
         processCountMax: processCountMax,
         processCountP95: processCountP95,
         processRSSP50: processRSSP50,
         processRSSP95: processRSSP95,
         processRSSMax: processRSSMax,
         processCountMaxUsername: processCountMaxUsername,
         processRSSMaxUsername: processRSSMaxUsername,
         networkLabels: expandedLabels,
         ifaceList: ifaceList,
         networkRXP50: networkRXP50,
         networkTXP50: networkTXP50,
         networkRXP95: networkRXP95,
         networkTXP95: networkTXP95,
         networkRXMax: networkRXMax,
         networkTXMax: networkTXMax,
         lustreOps: lustreOps,
         lustreP50Sum: lustreP50Sum,
         lustreP95Sum: lustreP95Sum,
         lustreMaxSum: lustreMaxSum,
         lustreTargetCount: lustreTargetCount,
         counterGaps: counters.gaps || {},
         usageGaps: usage.gaps || {},
         // Lifecycle snapshot at build time (read-only copy)
         _lifecycle: {
            createCount: chartLifecycle.createCount,
            destroyCount: chartLifecycle.destroyCount,
            renders: Object.assign({}, chartLifecycle.renders),
         },
      };
   }

   // ---- D-state/Interactivity hotspot table update ----
   // Renders/updates the table inside the CPU chart article
   function updateDStateHotspotTable(dStatePoints, interactivityPoints) {
      var table = document.querySelector('[data-testid="dstate-hotspot-table"]');
      if (!table) return;

      // Build rows HTML
      var rows = '';
      var points = dStatePoints || [];
      var iPoints = interactivityPoints || [];
      // Align by index (both arrays have same length - one per interval)
      var len = Math.max(points.length, iPoints.length);
      for (var i = 0; i < len; i++) {
         var dp = points[i] || {};
         var ip = iPoints[i] || {};
         var interval = dp.interval_end || ip.interval_end || '-';
         var dsVal = dp.value != null ? (dp.value * 100).toFixed(1) + '%' : '-';
         var dsUser = dp.username || '-';
         var intVal = ip.value != null ? (ip.value * 100).toFixed(1) + '%' : '-';
         var intUser = ip.username || '-';
         rows += '<tr>'
            + '<td>' + formatTimeLabel(interval) + '</td>'
            + '<td>' + dsVal + '</td>'
            + '<td>' + dsUser + '</td>'
            + '<td>' + intVal + '</td>'
            + '<td>' + intUser + '</td>'
            + '</tr>';
      }
      if (len === 0) {
         rows = '<tr><td colspan="5">No D-state/interactivity data in range</td></tr>';
      }

      var tbody = table.querySelector('tbody');
      if (tbody) {
         tbody.innerHTML = rows;
      }
   }

   // ---- Chart rendering ----

   // One-time toggle handler binding (prevents listener leak)
   let controlsBound = false;

   function ensureControlHandlers() {
      if (controlsBound) return;
      controlsBound = true;

      // Memory toggle
      const memControls = document.getElementById('mem-controls');
      if (memControls) {
         memControls.querySelectorAll('button').forEach(function(btn) {
            btn.addEventListener('click', function() {
               memControls.querySelectorAll('button').forEach(function(b) {
                  b.setAttribute('aria-pressed', 'false');
               });
               btn.setAttribute('aria-pressed', 'true');
               if (state.snapshot) renderCharts(state.snapshot);
            });
         });
      }

      // Process toggle
      const procControls = document.getElementById('proc-controls');
      if (procControls) {
         procControls.querySelectorAll('button').forEach(function(btn) {
            btn.addEventListener('click', function() {
               procControls.querySelectorAll('button').forEach(function(b) {
                  b.setAttribute('aria-pressed', 'false');
               });
               btn.setAttribute('aria-pressed', 'true');
               if (state.snapshot) renderCharts(state.snapshot);
            });
         });
      }

      // Network/Lustre toggle
      const nlControls = document.getElementById('nl-controls');
      if (nlControls) {
         nlControls.querySelectorAll('button').forEach(function(btn) {
            btn.addEventListener('click', function() {
               nlControls.querySelectorAll('button').forEach(function(b) {
                  b.setAttribute('aria-pressed', 'false');
               });
               btn.setAttribute('aria-pressed', 'true');
               if (state.snapshot) renderCharts(state.snapshot);
            });
         });
      }
   }

   function renderCharts(snapshot) {
      const input = buildChartData(snapshot);
      const totalKb = input.memoryTotalKb;

      // Bind toggle handlers once
      ensureControlHandlers();

      // Update D-state hotspot table
      updateDStateHotspotTable(input.dStatePoints, input.interactivityPoints);

      // 1. CPU / Load chart
      const hardware = snapshot && snapshot.hardware ? snapshot.hardware : {};
      const cpuLogical = hardware.cpu_logical != null ? hardware.cpu_logical : null;

      // Show logical CPU count in panel header
      const cpuLogicalLabel = qs('[data-testid="cpu-logical-label"]');
      if (cpuLogicalLabel) {
         cpuLogicalLabel.textContent = cpuLogical != null
            ? cpuLogical + ' logical CPUs' : '';
      }

      // Build CPU datasets; add a saturation reference line when cpu_logical is known
      const cpuDatasets = [
               {
                  label: 'CPU p50 %',
                  data: input.cpu.cpuP50,
                  borderColor: PALETTE[0],
                  backgroundColor: 'rgba(59,130,246,0.08)',
                  spanGaps: false,
                  pointStyle: 'circle',
                  pointRadius: 2,
                  borderWidth: 2,
                  tension: 0.2,
                  yAxisID: 'y',
               },
               {
                  label: 'CPU p95 %',
                  data: input.cpu.cpuP95,
                  borderColor: PALETTE[1],
                  backgroundColor: 'rgba(245,158,11,0.08)',
                  spanGaps: false,
                  pointStyle: 'rectRot',
                  pointRadius: 2,
                  borderWidth: 2,
                  tension: 0.2,
                  yAxisID: 'y',
               },
               {
                  label: 'CPU max %',
                  data: input.cpu.cpuMax,
                  borderColor: PALETTE[2],
                  backgroundColor: 'rgba(239,68,68,0.08)',
                  spanGaps: false,
                  pointStyle: 'triangle',
                  pointRadius: 2,
                  borderWidth: 2,
                  tension: 0.2,
                  yAxisID: 'y',
               },
               {
                  label: 'Load1',
                  data: input.cpu.load1,
                  borderColor: PALETTE[3],
                  backgroundColor: 'rgba(139,92,246,0.08)',
                  borderDash: [4, 3],
                  spanGaps: false,
                  pointStyle: 'crossRot',
                  pointRadius: 2,
                  borderWidth: 1.5,
                  tension: 0.2,
                  yAxisID: 'yLoad',
               },
               {
                  label: 'Load5',
                  data: input.cpu.load5,
                  borderColor: PALETTE[5],
                  backgroundColor: 'rgba(16,185,129,0.08)',
                  borderDash: [4, 3],
                  spanGaps: false,
                  pointStyle: 'star',
                  pointRadius: 2,
                  borderWidth: 1.5,
                  tension: 0.2,
                  yAxisID: 'yLoad',
               },
               {
                  label: 'Load15',
                  data: input.cpu.load15,
                  borderColor: PALETTE[7],
                  backgroundColor: 'rgba(234,179,8,0.08)',
                  borderDash: [4, 3],
                  spanGaps: false,
                  pointStyle: 'diamond',
                  pointRadius: 2,
                  borderWidth: 1.5,
                  tension: 0.2,
                  yAxisID: 'yLoad',
               },
      ];

      // Saturation reference line: flat line at cpu_logical on the load axis
      if (cpuLogical != null && input.cpu.labels.length > 0) {
         cpuDatasets.push({
            label: 'CPU count (' + cpuLogical + ')',
            data: input.cpu.labels.map(function() { return cpuLogical; }),
            borderColor: 'rgba(239,68,68,0.45)',
            backgroundColor: 'rgba(0,0,0,0)',
            borderDash: [10, 5],
            borderWidth: 1.5,
            pointRadius: 0,
            pointHoverRadius: 0,
            spanGaps: true,
            tension: 0,
            yAxisID: 'yLoad',
            fill: false,
         });
      }

      const loadAxisTitle = cpuLogical != null
         ? 'Load avg (' + cpuLogical + ' CPUs = saturation)'
         : 'Load avg';

      renderOrUpdateChart('cpu', 'chart-cpu', {
         type: 'line',
         data: {
            labels: input.cpu.labels,
            datasets: cpuDatasets,
         },
         options: pbsChartOptions(
            {
               y: pbsScaleDefaults({
                  beginAtZero: true,
                  type: 'linear',
                  display: true,
                  position: 'left',
                  title: { display: true, text: 'CPU %', color: TICK_COLOR },
               }),
               yLoad: pbsScaleDefaults({
                  beginAtZero: true,
                  type: 'linear',
                  display: true,
                  position: 'right',
                  grid: { drawOnChartArea: false },
                  title: { display: true, text: loadAxisTitle, color: TICK_COLOR },
               }),
               x: pbsScaleDefaults({
                  ticks: { color: TICK_COLOR, maxRotation: 45 },
               }),
            },
            function(c) {
               if (c.raw === null) return c.dataset.label + ': (gap/null)';
               const unit = c.dataset.yAxisID === 'yLoad' ? '' : '%';
               return c.dataset.label + ': ' + c.raw + unit;
            }
         ),
      });
      chartRenderState.cpu = {
         mode: 'cpu',
         labels: input.cpu.labels.slice(),
         datasets: [],
      };

      // 2. Memory chart (GiB / Percent toggle)
      (function() {
         const memControls = document.getElementById('mem-controls');
         // Disable Percent button when total memory is unavailable
         const percentBtn = memControls ? memControls.querySelector('[data-testid="mem-mode-percent"]') : null;
         const percentAvailable = totalKb != null && totalKb > 0;
         if (percentBtn) {
            percentBtn.disabled = !percentAvailable;
         }

         // Determine current mode
         const activeBtn = memControls ? memControls.querySelector('button[aria-pressed="true"]') : null;
         const mode = activeBtn ? activeBtn.getAttribute('data-testid') : 'mem-mode-gib';
         const isPercent = mode === 'mem-mode-percent' && percentAvailable;

         function toDisplayVal(kb) {
            if (kb == null) return null;
            if (isPercent) {
               return totalKb != null && totalKb > 0 ? (kb / totalKb) * 100 : null;
            }
            return kb / (1024 * 1024); // GiB
         }

         const memUsedDisplay = input.memoryUsedSeries.map(toDisplayVal);
         const memTotalDisplay = input.memoryTotalSeries.map(toDisplayVal);
         const yLabel = isPercent ? 'Percent (%)' : 'GiB';

         const categoryColors = [PALETTE[5], PALETTE[1], PALETTE[3], PALETTE[7], PALETTE[0], PALETTE[9]];

         const memDatasets = [
            {
               label: 'System used (' + yLabel + ')',
               data: memUsedDisplay,
               borderColor: PALETTE[2],
               backgroundColor: 'rgba(239,68,68,0.1)',
               spanGaps: false,
               pointStyle: 'circle',
               pointRadius: 2,
               borderWidth: 2,
               yAxisID: 'y',
               fill: false,
            },
            {
               label: 'Total memory (' + yLabel + ')',
               data: memTotalDisplay,
               borderColor: '#666666',
               backgroundColor: 'rgba(102,102,102,0.05)',
               borderDash: [4, 4],
               spanGaps: false,
               pointStyle: false,
               pointRadius: 0,
               borderWidth: 1.5,
               yAxisID: 'y',
               fill: false,
            },
         ];
         Object.keys(input.memoryCategoryRSSP50).sort().forEach(function(category, index) {
            memDatasets.push({
               label: category + ' RSS p50 (non-additive)',
               data: input.memoryCategoryRSSP50[category].map(toDisplayVal),
               borderColor: categoryColors[index % categoryColors.length],
               backgroundColor: 'rgba(0,0,0,0)',
               spanGaps: false,
               pointStyle: 'circle',
               pointRadius: 3,
               borderWidth: 1.5,
               yAxisID: 'yCategory',
               fill: false,
            });
         });

         renderOrUpdateChart('memory', 'chart-memory', {
            type: 'line',
            data: {
               labels: input.memoryLabels,
               datasets: memDatasets,
            },
            options: pbsChartOptions(
               {
                  y: pbsScaleDefaults({
                     beginAtZero: true,
                     position: 'left',
                     title: { display: true, text: 'System memory (' + yLabel + ')', color: TICK_COLOR },
                  }),
                  yCategory: pbsScaleDefaults({
                     beginAtZero: true,
                     position: 'right',
                     grid: { drawOnChartArea: false },
                     title: {
                        display: true,
                        text: 'Category RSS p50 (' + yLabel + ', non-additive)',
                        color: TICK_COLOR,
                     },
                  }),
                  x: pbsScaleDefaults({
                     ticks: { color: TICK_COLOR, maxRotation: 45 },
                  }),
               },
               function(c) {
                  if (c.raw === null) return c.dataset.label + ': (null/missing)';
                  return c.dataset.label + ': '
                     + (isPercent ? c.raw.toFixed(1) + '%' : c.raw.toFixed(3) + ' GiB');
               }
            ),
         });
         chartRenderState.memory = {
            mode: mode,
            labels: input.memoryLabels.slice(),
            datasets: memDatasets.map(function(ds) {
               return {
                  label: ds.label,
                  data: ds.data.slice(),
                  borderDash: (ds.borderDash || []).slice(),
                  pointStyle: ds.pointStyle,
                  pointRadius: ds.pointRadius == null ? null : ds.pointRadius,
                  yAxisID: ds.yAxisID,
                  fill: ds.fill,
                  spanGaps: ds.spanGaps,
               };
            }),
            axes: {
               y: {
                  title: 'System memory (' + yLabel + ')',
                  position: 'left',
               },
               yCategory: {
                  title: 'Category RSS p50 (' + yLabel + ', non-additive)',
                  position: 'right',
               },
            },
         };
      })();

      // 3. Process chart (mode toggle: CPU sum / process max / grain total RSS)
      (function() {
         const procControls = document.getElementById('proc-controls');
         const activeBtn = procControls ? procControls.querySelector('button[aria-pressed="true"]') : null;
         const mode = activeBtn ? activeBtn.getAttribute('data-testid') : 'proc-mode-cpusum';

         let dataArr, yText, usernameArr;
         if (mode === 'proc-mode-rss') {
            dataArr = input.processRSSMax;
            yText = 'grain total RSS (rss_max_kb)';
            usernameArr = input.processRSSMaxUsername;
         } else if (mode === 'proc-mode-max') {
            dataArr = input.processCountMax;
            yText = 'process count max per grain';
            usernameArr = input.processCountMaxUsername;
         } else {
            // CPU sum (default)
            dataArr = input.processCPU;
            yText = 'cpu_seconds (additive, 15-min interval)';
            usernameArr = null;
         }

         const procDatasets = [
            {
               label: yText,
               data: dataArr,
               backgroundColor: PALETTE[0],
               borderColor: PALETTE[0],
               spanGaps: false,
            },
         ];

         renderOrUpdateChart('process', 'chart-process', {
            type: 'bar',
            data: {
               labels: input.processLabels,
               datasets: procDatasets,
            },
            options: pbsChartOptions(
               {
                  y: pbsScaleDefaults({
                     beginAtZero: true,
                     title: { display: true, text: yText, color: TICK_COLOR },
                  }),
                  x: pbsScaleDefaults({
                     ticks: { color: TICK_COLOR, maxRotation: 45 },
                  }),
               },
               function(c) {
                  const val = c.raw === null ? 'null' : c.raw;
                  if (!usernameArr) return yText + ': ' + val;
                  const user = usernameArr[c.dataIndex] || '-';
                  return yText + ': ' + val + ' (contributor: ' + user + ')';
               }
            ),
         });
         chartRenderState.process = {
            mode: mode,
            labels: input.processLabels.slice(),
            datasets: procDatasets.map(function(ds) {
               return { label: ds.label, data: ds.data ? ds.data.slice() : [] };
            }),
            contributorSummary: usernameArr ? usernameArr.slice() : null,
         };
      })();

      // 4. Network / Lustre chart (mode toggle)
      (function() {
         const nlControls = document.getElementById('nl-controls');
         const activeBtn = nlControls ? nlControls.querySelector('button[aria-pressed="true"]') : null;
         // Determine active mode using data-testid
         const mode = activeBtn ? activeBtn.getAttribute('data-testid') : 'nl-network-p50';

         // Also support legacy nl-mode-network as alias for nl-network-p50
         const effectiveMode = mode === 'nl-mode-network' ? 'nl-network-p50' : mode;

         const nlLabels = input.networkLabels.length ? input.networkLabels : ['—'];
         const datasets = [];

         // PBS palette with distinct line styles
         const dashes = [[], [6, 4], [2, 2], [8, 2, 2, 2], [4, 4], [10, 2]];

         if (effectiveMode === 'nl-network-p50') {
            input.ifaceList.forEach(function(iface, idx) {
               const rxColor = PALETTE[idx * 2 % PALETTE.length];
               const txColor = PALETTE[(idx * 2 + 1) % PALETTE.length];
               datasets.push({
                  label: iface + ' RX p50 (B/s)',
                  data: input.networkRXP50[iface],
                  borderColor: rxColor,
                  backgroundColor: rxColor,
                  borderDash: dashes[idx % dashes.length],
                  borderWidth: 2,
                  pointRadius: 2,
                  spanGaps: false,
                  pointStyle: 'circle',
               });
               datasets.push({
                  label: iface + ' TX p50 (B/s)',
                  data: input.networkTXP50[iface],
                  borderColor: txColor,
                  backgroundColor: txColor,
                  borderDash: dashes[(idx + 1) % dashes.length],
                  borderWidth: 2,
                  pointRadius: 2,
                  spanGaps: false,
                  pointStyle: 'rectRot',
               });
            });
         } else if (effectiveMode === 'nl-network-p95') {
            input.ifaceList.forEach(function(iface, idx) {
               const rxColor = PALETTE[idx * 2 % PALETTE.length];
               const txColor = PALETTE[(idx * 2 + 1) % PALETTE.length];
               datasets.push({
                  label: iface + ' RX p95 (B/s)',
                  data: input.networkRXP95[iface],
                  borderColor: rxColor,
                  backgroundColor: rxColor,
                  borderDash: dashes[idx % dashes.length],
                  borderWidth: 2,
                  pointRadius: 2,
                  spanGaps: false,
                  pointStyle: 'circle',
               });
               datasets.push({
                  label: iface + ' TX p95 (B/s)',
                  data: input.networkTXP95[iface],
                  borderColor: txColor,
                  backgroundColor: txColor,
                  borderDash: dashes[(idx + 1) % dashes.length],
                  borderWidth: 2,
                  pointRadius: 2,
                  spanGaps: false,
                  pointStyle: 'rectRot',
               });
            });
         } else if (effectiveMode === 'nl-network-max') {
            input.ifaceList.forEach(function(iface, idx) {
               const rxColor = PALETTE[idx * 2 % PALETTE.length];
               const txColor = PALETTE[(idx * 2 + 1) % PALETTE.length];
               datasets.push({
                  label: iface + ' RX max (B/s)',
                  data: input.networkRXMax[iface],
                  borderColor: rxColor,
                  backgroundColor: rxColor,
                  borderDash: dashes[idx % dashes.length],
                  borderWidth: 2,
                  pointRadius: 2,
                  spanGaps: false,
                  pointStyle: 'circle',
               });
               datasets.push({
                  label: iface + ' TX max (B/s)',
                  data: input.networkTXMax[iface],
                  borderColor: txColor,
                  backgroundColor: txColor,
                  borderDash: dashes[(idx + 1) % dashes.length],
                  borderWidth: 2,
                  pointRadius: 2,
                  spanGaps: false,
                  pointStyle: 'rectRot',
               });
            });
         } else if (effectiveMode === 'nl-mode-lustre-p50') {
            input.lustreOps.forEach(function(op, idx) {
               datasets.push({
                  label: op + ' p50-sum (sum of per-target p50)',
                  data: input.lustreP50Sum[op],
                  borderColor: PALETTE[idx % PALETTE.length],
                  backgroundColor: PALETTE[idx % PALETTE.length],
                  borderDash: dashes[idx % dashes.length],
                  borderWidth: 2,
                  pointRadius: 2,
                  spanGaps: false,
                  pointStyle: 'circle',
               });
            });
         } else if (effectiveMode === 'nl-mode-lustre-p95') {
            input.lustreOps.forEach(function(op, idx) {
               datasets.push({
                  label: op + ' p95-sum (sum of per-target p95)',
                  data: input.lustreP95Sum[op],
                  borderColor: PALETTE[idx % PALETTE.length],
                  backgroundColor: PALETTE[idx % PALETTE.length],
                  borderDash: dashes[idx % dashes.length],
                  borderWidth: 2,
                  pointRadius: 2,
                  spanGaps: false,
                  pointStyle: 'rectRot',
               });
            });
         } else if (effectiveMode === 'nl-mode-lustre-peak') {
            input.lustreOps.forEach(function(op, idx) {
               datasets.push({
                  label: op + ' peak-sum/max_sum (sum of per-target maxima)',
                  data: input.lustreMaxSum[op],
                  borderColor: PALETTE[idx % PALETTE.length],
                  backgroundColor: PALETTE[idx % PALETTE.length],
                  borderDash: dashes[idx % dashes.length],
                  borderWidth: 2,
                  pointRadius: 2,
                  spanGaps: false,
                  pointStyle: 'triangle',
               });
            });
         } else if (effectiveMode === 'nl-mode-lustre-targets') {
            input.lustreOps.forEach(function(op, idx) {
               datasets.push({
                  label: op + ' target_count',
                  data: input.lustreTargetCount[op],
                  borderColor: PALETTE[idx % PALETTE.length],
                  backgroundColor: PALETTE[idx % PALETTE.length],
                  borderDash: dashes[idx % dashes.length],
                  borderWidth: 2,
                  pointRadius: 2,
                  spanGaps: false,
                  pointStyle: 'diamond',
               });
            });
         }

         const nlYText = (effectiveMode === 'nl-network-p50' || effectiveMode === 'nl-network-p95' || effectiveMode === 'nl-network-max')
            ? 'Bytes/sec (' + effectiveMode.replace('nl-network-', '') + ')'
            : 'Sum / count';

         renderOrUpdateChart('networkLustre', 'chart-network-lustre', {
            type: 'line',
            data: {
               labels: nlLabels,
               datasets: datasets,
            },
            options: pbsChartOptions(
               {
                  y: pbsScaleDefaults({
                     beginAtZero: true,
                     title: { display: true, text: nlYText, color: TICK_COLOR },
                  }),
                  x: pbsScaleDefaults({
                     ticks: { color: TICK_COLOR, maxRotation: 45 },
                  }),
               },
               function(c) {
                  if (c.raw === null) return c.dataset.label + ': (null/gap)';
                  return c.dataset.label + ': ' + c.raw;
               }
            ),
         });
         // Store effective mode for getChartRenderState
         chartRenderState.networkLustre = {
            mode: effectiveMode,
            labels: nlLabels.slice(),
            datasets: datasets.map(function(ds) {
               return { label: ds.label, data: ds.data ? ds.data.slice() : [] };
            }),
         };
      })();
   }

   // ---- Read-only test projection ----
   (function() {
      function deepFreeze(obj) {
         if (obj === null || typeof obj !== 'object') return obj;
         Object.freeze(obj);
         Object.keys(obj).forEach(function(k) {
            const v = obj[k];
            if (v !== null && typeof v === 'object' && !Object.isFrozen(v)) {
               deepFreeze(v);
            }
         });
         return obj;
      }

      function deepClone(obj) {
         if (obj === null || typeof obj !== 'object') return obj;
         if (Array.isArray(obj)) return obj.map(deepClone);
         const clone = {};
         Object.keys(obj).forEach(function(k) { clone[k] = deepClone(obj[k]); });
         return clone;
      }

      const testAPI = {
         getTimerState: function() {
            return Object.freeze({
               refreshIntervals: state.refreshTimer === null ? 0 : 1,
               ageIntervals: state.ageTimer === null ? 0 : 1,
            });
         },
         getState: function() {
            return Object.freeze({
               node: state.currentNode,
               range: state.currentRange,
               username: state.currentUsername,
               connected: state.connected,
               receivedMonotonicMs: state.receivedMonotonicMs,
               counterAgeAtReceiptSec: state.counterAgeAtReceiptSec,
               usageAgeAtReceiptSec: state.usageAgeAtReceiptSec,
            });
         },
         get chartData() {
            const data = buildChartData(state.snapshot);
            return deepFreeze(deepClone(data));
         },
         getChartLifecycle: function() {
            return Object.freeze({
               createCount: chartLifecycle.createCount,
               destroyCount: chartLifecycle.destroyCount,
               renders: Object.freeze(Object.assign({}, chartLifecycle.renders)),
            });
         },
         // Read-only render state: current mode, dataset labels, cloned dataset values
         // No Chart instances exposed; no mutation methods.
         getChartRenderState: function() {
            return deepClone({
               cpu: {
                  mode: chartRenderState.cpu.mode,
                  labels: chartRenderState.cpu.labels,
                  datasets: chartRenderState.cpu.datasets,
               },
               memory: {
                  mode: chartRenderState.memory.mode,
                  labels: chartRenderState.memory.labels,
                  datasets: chartRenderState.memory.datasets,
                  axes: chartRenderState.memory.axes,
               },
               process: {
                  mode: chartRenderState.process.mode,
                  labels: chartRenderState.process.labels,
                  datasets: chartRenderState.process.datasets,
                  contributorSummary: chartRenderState.process.contributorSummary,
               },
               networkLustre: {
                  mode: chartRenderState.networkLustre.mode,
                  labels: chartRenderState.networkLustre.labels,
                  datasets: chartRenderState.networkLustre.datasets,
               },
            });
         },
      };

      window.__nodeMonitorTest = Object.freeze(testAPI);
   })();

   // Inventory-first node selection (design spec 194+)
   async function fetchNodes() {
      try {
         const resp = await fetch('/api/nodes', { cache: 'no-store' });
         if (!resp.ok) throw new Error('inventory failed');
         const data = await resp.json();
         if (!data || !Array.isArray(data.nodes)) return null;
         for (let i = 0; i < data.nodes.length; i++) {
            if (!data.nodes[i] || typeof data.nodes[i].id !== 'string'
                  || data.nodes[i].id.length === 0) return null;
         }
         return data.nodes;
      } catch (e) { return null; }
   }

   function renderNodeButtons(nodes) {
      const group = qs('#node-group');
      if (!group) return;
      group.innerHTML = '';
      if (!nodes || nodes.length === 0) {
         group.innerHTML = '<span>No monitored nodes available</span>';
         return;
      }
      nodes.forEach(function(node) {
         const btn = document.createElement('button');
         btn.type = 'button';
         btn.textContent = node.label || node.id;
         btn.setAttribute('data-testid', 'node-btn');
         btn.setAttribute('data-node-id', node.id);
         btn.setAttribute('aria-pressed', node.id === state.currentNode ? 'true' : 'false');
         btn.addEventListener('click', async function() {
            state.currentNode = node.id;
            qsa('#node-group button').forEach(function(b) {
               b.setAttribute('aria-pressed', 'false');
            });
            btn.setAttribute('aria-pressed', 'true');
            await refreshDashboard();
         });
         group.appendChild(btn);
      });
   }

   async function initInventorySelection(recoverUnavailable) {
      const nodes = await fetchNodes();
      if (nodes === null) {
         qs('#node-status').textContent = 'Connection failure';
         return;
      }
      if (nodes.length === 0) {
         qs('#node-status').textContent = 'No monitored nodes available';
         renderNodeButtons([]);
         return;
      }
      renderNodeButtons(nodes);
      // Precedence: current if still available, configured local, first returned
      let selection = null;
      if (state.currentNode) {
         for (let i = 0; i < nodes.length; i++) {
            if (nodes[i].id === state.currentNode) { selection = state.currentNode; break; }
         }
      }
      if (!selection) {
         for (let i = 0; i < nodes.length; i++) {
            if (nodes[i].configured && nodes[i].role === 'local') { selection = nodes[i].id; break; }
         }
      }
      if (!selection) { selection = nodes[0].id; }
      state.currentNode = selection;
      renderNodeButtons(nodes); // refresh aria-pressed
      startTimers();
      await refreshDashboard(recoverUnavailable);
   }

   initInventorySelection(true);
})();
