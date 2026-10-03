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
      let hotspot = null;
      grains.forEach(function(grain) {
         const value = finiteNumber(grain.d_state_fraction);
         if (value !== null && (!hotspot || value > hotspot.value)) {
            hotspot = {value: value, grain: grain};
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
      setActiveRange();
      renderAges();
      qs('#retry-section').hidden = true;
   }

   function renderFailure(firstLoad) {
      state.connected = false;
      qs('[data-testid="connectivity-status"]').textContent = firstLoad
         ? 'Connection failure' : 'Web server disconnected';
      qs('#retry-section').hidden = !firstLoad;
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

   window.__nodeMonitorTest = Object.freeze({
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
   });

   startTimers();
   refreshDashboard();
})();
