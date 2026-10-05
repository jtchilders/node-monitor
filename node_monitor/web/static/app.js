(function() {
   'use strict';

   const state = {
      snapshot: null,
      connected: false,
      receivedMonotonicMs: 0,
      counterAgeAtReceiptSec: null,
      usageAgeAtReceiptSec: null,
      currentNode: 'login-04',
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
      // Header connection state from connected flag (not text)
      const header = qs('.dashboard-header');
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
      if (!response.ok) throw new Error('refresh failed');
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
   }

   function renderFailure(firstLoad) {
      state.connected = false;
      const header = qs('.dashboard-header');
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
   }

   async function refreshDashboard() {
      try {
         render(await fetchDashboard());
         return true;
      } catch (_error) {
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

   qs('#node-form').addEventListener('submit', async function(event) {
      event.preventDefault();
      updateSelection(qs('#node-input').value, null, qs('#user-input').value);
      await refreshDashboard();
   });

   qs('#user-form').addEventListener('submit', async function(event) {
      event.preventDefault();
      updateSelection(qs('#node-input').value, null, qs('#user-input').value);
      await refreshDashboard();
   });

   qs('#retry-btn').addEventListener('click', refreshDashboard);

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
      networkLustre: { mode: 'nl-mode-network', labels: [], datasets: [] },
   };

   function replaceChart(name, canvasId, config) {
      const canvas = document.getElementById(canvasId);
      if (!canvas) return null;
      if (chartRegistry[name]) {
         chartRegistry[name].destroy();
         chartLifecycle.destroyCount++;
      }
      const chart = new Chart(canvas, config);
      chartRegistry[name] = chart;
      chartLifecycle.createCount++;
      chartLifecycle.renders[name] = (chartLifecycle.renders[name] || 0) + 1;
      return chart;
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

      labels.push(sortedRows[0].window_end ? sortedRows[0].window_end.slice(0, 16) : '');
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
               const gapLabel = new Date(nextAligned).toISOString().slice(0, 16) + ' (gap)';
               labels.push(gapLabel);
               rowIndices.push(null);
               nextAligned += CADENCE_MS;
               totalInserted++;
            }
         }
         labels.push(sortedRows[i].window_end ? sortedRows[i].window_end.slice(0, 16) : '');
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
      const memAvailableSeries = extractSeries(function(r) {
         return r.mem_available_kb != null ? r.mem_available_kb : null;
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
         return iv.interval_end ? iv.interval_end.slice(0, 16) : '-';
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
         memoryAvailableSeries: memAvailableSeries,
         memoryUsedSeries: memUsedSeries,
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
            + '<td>' + interval.slice(0, 16) + '</td>'
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

   function renderCharts(snapshot) {
      const input = buildChartData(snapshot);
      const totalKb = input.memoryTotalKb;

      // Update D-state hotspot table
      updateDStateHotspotTable(input.dStatePoints, input.interactivityPoints);

      // 1. CPU / Load chart
      replaceChart('cpu', 'chart-cpu', {
         type: 'line',
         data: {
            labels: input.cpu.labels,
            datasets: [
               {
                  label: 'CPU p50 %',
                  data: input.cpu.cpuP50,
                  borderColor: '#005fcc',
                  backgroundColor: 'rgba(0,95,204,0.08)',
                  spanGaps: false,
                  pointStyle: 'circle',
                  tension: 0.2,
                  yAxisID: 'y',
               },
               {
                  label: 'CPU p95 %',
                  data: input.cpu.cpuP95,
                  borderColor: '#cc6600',
                  backgroundColor: 'rgba(204,102,0,0.08)',
                  borderDash: [6, 4],
                  spanGaps: false,
                  pointStyle: 'rectRot',
                  tension: 0.2,
                  yAxisID: 'y',
               },
               {
                  label: 'CPU max %',
                  data: input.cpu.cpuMax,
                  borderColor: '#880000',
                  backgroundColor: 'rgba(136,0,0,0.08)',
                  borderDash: [2, 2],
                  spanGaps: false,
                  pointStyle: 'triangle',
                  tension: 0.2,
                  yAxisID: 'y',
               },
               {
                  label: 'Load1',
                  data: input.cpu.load1,
                  borderColor: '#660099',
                  backgroundColor: 'rgba(102,0,153,0.08)',
                  spanGaps: false,
                  pointStyle: 'crossRot',
                  tension: 0.2,
                  yAxisID: 'yLoad',
               },
               {
                  label: 'Load5',
                  data: input.cpu.load5,
                  borderColor: '#228833',
                  backgroundColor: 'rgba(34,136,51,0.08)',
                  spanGaps: false,
                  pointStyle: 'star',
                  tension: 0.2,
                  yAxisID: 'yLoad',
               },
               {
                  label: 'Load15',
                  data: input.cpu.load15,
                  borderColor: '#aa8800',
                  backgroundColor: 'rgba(170,136,0,0.08)',
                  spanGaps: false,
                  pointStyle: 'diamond',
                  tension: 0.2,
                  yAxisID: 'yLoad',
               },
            ],
         },
         options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
               y: {
                  beginAtZero: true,
                  type: 'linear',
                  display: true,
                  position: 'left',
                  title: { display: true, text: 'CPU %' },
               },
               yLoad: {
                  beginAtZero: true,
                  type: 'linear',
                  display: true,
                  position: 'right',
                  grid: { drawOnChartArea: false },
                  title: { display: true, text: 'Load avg' },
               },
            },
            plugins: {
               tooltip: {
                  callbacks: {
                     label: function(c) {
                        if (c.raw === null) return c.dataset.label + ': (gap/null)';
                        const unit = c.dataset.yAxisID === 'yLoad' ? '' : '%';
                        return c.dataset.label + ': ' + c.raw + unit;
                     },
                  },
               },
            },
         },
      });
      chartRenderState.cpu = {
         mode: 'cpu',
         labels: input.cpu.labels.slice(),
         datasets: [],
      };

      // 2. Memory chart (GiB, with optional Percent toggle)
      (function() {
         // Ensure controls exist
         let memControls = document.getElementById('mem-controls');
         if (!memControls) {
            memControls = document.createElement('div');
            memControls.id = 'mem-controls';
            memControls.setAttribute('data-testid', 'mem-controls');
            memControls.innerHTML =
               '<button type="button" data-testid="mem-mode-gib" aria-pressed="true">GiB</button>'
               + '<button type="button" data-testid="mem-mode-percent" aria-pressed="false">Percent</button>';
            const canvas = document.getElementById('chart-memory');
            if (canvas && canvas.parentElement) {
               canvas.parentElement.insertBefore(memControls, canvas);
            }
         }
         // Disable Percent button when total memory is unavailable
         const percentBtn = memControls.querySelector('[data-testid="mem-mode-percent"]');
         const percentAvailable = totalKb != null && totalKb > 0;
         if (percentBtn) {
            percentBtn.disabled = !percentAvailable;
         }

         // Determine current mode
         const activeBtn = memControls.querySelector('button[aria-pressed="true"]');
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
         const memAvailDisplay = input.memoryAvailableSeries.map(toDisplayVal);
         const yLabel = isPercent ? 'Percent (%)' : 'GiB';

         const memDatasets = [
            {
               label: 'Used (' + yLabel + ')',
               data: memUsedDisplay,
               borderColor: '#880000',
               backgroundColor: 'rgba(136,0,0,0.1)',
               spanGaps: false,
               pointStyle: 'rect',
               fill: false,
            },
            {
               label: 'Available (' + yLabel + ')',
               data: memAvailDisplay,
               borderColor: '#005fcc',
               backgroundColor: 'rgba(0,95,204,0.1)',
               spanGaps: false,
               pointStyle: 'circle',
               fill: false,
            },
         ];

         replaceChart('memory', 'chart-memory', {
            type: 'line',
            data: {
               labels: input.memoryLabels,
               datasets: memDatasets,
            },
            options: {
               responsive: true,
               maintainAspectRatio: false,
               scales: {
                  y: {
                     beginAtZero: true,
                     title: { display: true, text: yLabel },
                  },
               },
               plugins: {
                  tooltip: {
                     callbacks: {
                        label: function(c) {
                           if (c.raw === null) return c.dataset.label + ': (null/missing)';
                           return c.dataset.label + ': '
                              + (isPercent ? c.raw.toFixed(1) + '%' : c.raw.toFixed(3) + ' GiB');
                        },
                     },
                  },
               },
            },
         });
         chartRenderState.memory = {
            mode: mode,
            labels: input.memoryLabels.slice(),
            datasets: memDatasets.map(function(ds) {
               return { label: ds.label, data: ds.data.slice() };
            }),
         };

         // Re-bind toggle handlers each time (buttons may be recreated)
         memControls.querySelectorAll('button').forEach(function(btn) {
            btn.addEventListener('click', function() {
               memControls.querySelectorAll('button').forEach(function(b) {
                  b.setAttribute('aria-pressed', 'false');
               });
               btn.setAttribute('aria-pressed', 'true');
               // Re-render with new mode using stored snapshot
               if (state.snapshot) renderCharts(state.snapshot);
            });
         });
      })();

      // 3. Process chart (mode toggle: CPU sum / process max / grain total RSS)
      (function() {
         let procControls = document.getElementById('proc-controls');
         if (!procControls) {
            procControls = document.createElement('div');
            procControls.id = 'proc-controls';
            procControls.setAttribute('data-testid', 'proc-controls');
            procControls.innerHTML =
               '<button type="button" data-testid="proc-mode-cpusum" aria-pressed="true">CPU sum</button>'
               + '<button type="button" data-testid="proc-mode-max" aria-pressed="false">Process max</button>'
               + '<button type="button" data-testid="proc-mode-rss" aria-pressed="false">Grain total RSS</button>';
            const canvas = document.getElementById('chart-process');
            if (canvas && canvas.parentElement) {
               canvas.parentElement.insertBefore(procControls, canvas);
            }
         }

         const activeBtn = procControls.querySelector('button[aria-pressed="true"]');
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
               backgroundColor: '#005fcc',
               spanGaps: false,
            },
         ];

         replaceChart('process', 'chart-process', {
            type: 'bar',
            data: {
               labels: input.processLabels,
               datasets: procDatasets,
            },
            options: {
               responsive: true,
               maintainAspectRatio: false,
               scales: {
                  y: {
                     beginAtZero: true,
                     title: { display: true, text: yText },
                  },
               },
               plugins: {
                  tooltip: {
                     callbacks: {
                        label: function(c) {
                           const val = c.raw === null ? 'null' : c.raw;
                           if (!usernameArr) return yText + ': ' + val;
                           const user = usernameArr[c.dataIndex] || '-';
                           return yText + ': ' + val + ' (contributor: ' + user + ')';
                        },
                     },
                  },
               },
            },
         });
         chartRenderState.process = {
            mode: mode,
            labels: input.processLabels.slice(),
            datasets: procDatasets.map(function(ds) {
               return { label: ds.label, data: ds.data ? ds.data.slice() : [] };
            }),
            contributorSummary: usernameArr ? usernameArr.slice() : null,
         };

         procControls.querySelectorAll('button').forEach(function(btn) {
            btn.addEventListener('click', function() {
               procControls.querySelectorAll('button').forEach(function(b) {
                  b.setAttribute('aria-pressed', 'false');
               });
               btn.setAttribute('aria-pressed', 'true');
               if (state.snapshot) renderCharts(state.snapshot);
            });
         });
      })();

      // 4. Network / Lustre chart (mode toggle)
      // Network has three statistic sub-modes: p50, p95, max
      // Lustre sub-modes: p50-sum, p95-sum, peak-sum, target-count
      (function() {
         let nlControls = document.getElementById('nl-controls');
         if (!nlControls) {
            nlControls = document.createElement('div');
            nlControls.id = 'nl-controls';
            nlControls.setAttribute('data-testid', 'nl-controls');
            nlControls.innerHTML =
               // Network family (three statistic modes)
               '<button type="button" data-testid="nl-network-p50" aria-pressed="true">Network (p50)</button>'
               + '<button type="button" data-testid="nl-network-p95" aria-pressed="false">Network (p95)</button>'
               + '<button type="button" data-testid="nl-network-max" aria-pressed="false">Network (max)</button>'
               // Lustre family
               + '<button type="button" data-testid="nl-mode-lustre-p50" aria-pressed="false">Lustre p50-sum</button>'
               + '<button type="button" data-testid="nl-mode-lustre-p95" aria-pressed="false">Lustre p95-sum</button>'
               + '<button type="button" data-testid="nl-mode-lustre-peak" aria-pressed="false">Lustre peak-sum</button>'
               + '<button type="button" data-testid="nl-mode-lustre-targets" aria-pressed="false">Lustre target count</button>';
            const canvas = document.getElementById('chart-network-lustre');
            if (canvas && canvas.parentElement) {
               canvas.parentElement.insertBefore(nlControls, canvas);
            }

            // --- BACKWARD COMPAT: keep nl-mode-network button functional ---
            // Tests using nl-mode-network testid still work via the button group logic below
         }

         const activeBtn = nlControls.querySelector('button[aria-pressed="true"]');
         // Determine active mode using data-testid
         const mode = activeBtn ? activeBtn.getAttribute('data-testid') : 'nl-network-p50';

         // Also support legacy nl-mode-network as alias for nl-network-p50
         const effectiveMode = mode === 'nl-mode-network' ? 'nl-network-p50' : mode;

         const nlLabels = input.networkLabels.length ? input.networkLabels : ['—'];
         const datasets = [];

         // Color-blind-safe palette with distinct line styles
         const colors = ['#005fcc', '#cc6600', '#228833', '#880000', '#660099', '#aa8800'];
         const dashes = [[], [6, 4], [2, 2], [8, 2, 2, 2], [4, 4], [10, 2]];

         if (effectiveMode === 'nl-network-p50') {
            // Per-interface RX/TX p50 time series (default)
            input.ifaceList.forEach(function(iface, idx) {
               const rxColor = colors[idx * 2 % colors.length];
               const txColor = colors[(idx * 2 + 1) % colors.length];
               datasets.push({
                  label: iface + ' RX p50 (B/s)',
                  data: input.networkRXP50[iface],
                  borderColor: rxColor,
                  backgroundColor: rxColor,
                  borderDash: dashes[idx % dashes.length],
                  spanGaps: false,
                  pointStyle: 'circle',
               });
               datasets.push({
                  label: iface + ' TX p50 (B/s)',
                  data: input.networkTXP50[iface],
                  borderColor: txColor,
                  backgroundColor: txColor,
                  borderDash: dashes[(idx + 1) % dashes.length],
                  spanGaps: false,
                  pointStyle: 'rectRot',
               });
            });
         } else if (effectiveMode === 'nl-network-p95') {
            // Per-interface RX/TX p95 time series
            input.ifaceList.forEach(function(iface, idx) {
               const rxColor = colors[idx * 2 % colors.length];
               const txColor = colors[(idx * 2 + 1) % colors.length];
               datasets.push({
                  label: iface + ' RX p95 (B/s)',
                  data: input.networkRXP95[iface],
                  borderColor: rxColor,
                  backgroundColor: rxColor,
                  borderDash: dashes[idx % dashes.length],
                  spanGaps: false,
                  pointStyle: 'circle',
               });
               datasets.push({
                  label: iface + ' TX p95 (B/s)',
                  data: input.networkTXP95[iface],
                  borderColor: txColor,
                  backgroundColor: txColor,
                  borderDash: dashes[(idx + 1) % dashes.length],
                  spanGaps: false,
                  pointStyle: 'rectRot',
               });
            });
         } else if (effectiveMode === 'nl-network-max') {
            // Per-interface RX/TX max time series
            input.ifaceList.forEach(function(iface, idx) {
               const rxColor = colors[idx * 2 % colors.length];
               const txColor = colors[(idx * 2 + 1) % colors.length];
               datasets.push({
                  label: iface + ' RX max (B/s)',
                  data: input.networkRXMax[iface],
                  borderColor: rxColor,
                  backgroundColor: rxColor,
                  borderDash: dashes[idx % dashes.length],
                  spanGaps: false,
                  pointStyle: 'circle',
               });
               datasets.push({
                  label: iface + ' TX max (B/s)',
                  data: input.networkTXMax[iface],
                  borderColor: txColor,
                  backgroundColor: txColor,
                  borderDash: dashes[(idx + 1) % dashes.length],
                  spanGaps: false,
                  pointStyle: 'rectRot',
               });
            });
         } else if (effectiveMode === 'nl-mode-lustre-p50') {
            // p50-sum is sum of per-target p50 values (NOT maxima)
            input.lustreOps.forEach(function(op, idx) {
               datasets.push({
                  label: op + ' p50-sum (sum of per-target p50)',
                  data: input.lustreP50Sum[op],
                  borderColor: colors[idx % colors.length],
                  backgroundColor: colors[idx % colors.length],
                  borderDash: dashes[idx % dashes.length],
                  spanGaps: false,
                  pointStyle: 'circle',
               });
            });
         } else if (effectiveMode === 'nl-mode-lustre-p95') {
            // p95-sum is sum of per-target p95 values (NOT maxima)
            input.lustreOps.forEach(function(op, idx) {
               datasets.push({
                  label: op + ' p95-sum (sum of per-target p95)',
                  data: input.lustreP95Sum[op],
                  borderColor: colors[idx % colors.length],
                  backgroundColor: colors[idx % colors.length],
                  borderDash: dashes[idx % dashes.length],
                  spanGaps: false,
                  pointStyle: 'rectRot',
               });
            });
         } else if (effectiveMode === 'nl-mode-lustre-peak') {
            // peak-sum/max_sum: sum of per-target maxima (correct - this is where maxima caveat belongs)
            input.lustreOps.forEach(function(op, idx) {
               datasets.push({
                  label: op + ' peak-sum/max_sum (sum of per-target maxima)',
                  data: input.lustreMaxSum[op],
                  borderColor: colors[idx % colors.length],
                  backgroundColor: colors[idx % colors.length],
                  borderDash: dashes[idx % dashes.length],
                  spanGaps: false,
                  pointStyle: 'triangle',
               });
            });
         } else if (effectiveMode === 'nl-mode-lustre-targets') {
            input.lustreOps.forEach(function(op, idx) {
               datasets.push({
                  label: op + ' target_count',
                  data: input.lustreTargetCount[op],
                  borderColor: colors[idx % colors.length],
                  backgroundColor: colors[idx % colors.length],
                  borderDash: dashes[idx % dashes.length],
                  spanGaps: false,
                  pointStyle: 'diamond',
               });
            });
         }

         const nlYText = (effectiveMode === 'nl-network-p50' || effectiveMode === 'nl-network-p95' || effectiveMode === 'nl-network-max')
            ? 'Bytes/sec (' + effectiveMode.replace('nl-network-', '') + ')'
            : 'Sum / count';

         replaceChart('networkLustre', 'chart-network-lustre', {
            type: 'line',
            data: {
               labels: nlLabels,
               datasets: datasets,
            },
            options: {
               responsive: true,
               maintainAspectRatio: false,
               scales: {
                  y: {
                     beginAtZero: true,
                     title: {
                        display: true,
                        text: nlYText,
                     },
                  },
               },
               plugins: {
                  tooltip: {
                     callbacks: {
                        label: function(c) {
                           if (c.raw === null) return c.dataset.label + ': (null/gap)';
                           return c.dataset.label + ': ' + c.raw;
                        },
                     },
                  },
               },
            },
         });
         // Store effective mode for getChartRenderState
         chartRenderState.networkLustre = {
            mode: effectiveMode,
            labels: nlLabels.slice(),
            datasets: datasets.map(function(ds) {
               return { label: ds.label, data: ds.data ? ds.data.slice() : [] };
            }),
         };

         nlControls.querySelectorAll('button').forEach(function(btn) {
            btn.addEventListener('click', function() {
               nlControls.querySelectorAll('button').forEach(function(b) {
                  b.setAttribute('aria-pressed', 'false');
               });
               btn.setAttribute('aria-pressed', 'true');
               if (state.snapshot) renderCharts(state.snapshot);
            });
         });
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

   startTimers();
   refreshDashboard();
})();
