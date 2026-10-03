(function() {
   'use strict';

   // State
   let snapshot = null;
   let connected = false;
   let receivedMonotonicMs = 0;
   let serverAgeAtReceiptSec = 0;
   let refreshTimer = null;

   const endpoint = '/api/dashboard';

   function qs(sel) { return document.querySelector(sel); }
   function qsa(sel) { return document.querySelectorAll(sel); }

   function formatAge(sec) {
      if (sec == null || isNaN(sec)) return '—';
      return Math.max(0, Math.round(sec)) + 's';
   }

   async function fetchDashboard(node, rangeName, username) {
      const url = new URL(endpoint, location.origin);
      url.searchParams.set('node', node || 'login-04');
      url.searchParams.set('range', rangeName || '1h');
      if (username) url.searchParams.set('username', username);
      const resp = await fetch(url.toString(), { cache: 'no-store', signal: AbortSignal.timeout(15000) });
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      return await resp.json();
   }

   function applySnapshot(data, nodeName, rangeName) {
      snapshot = data;
      connected = true;
      receivedMonotonicMs = performance.now();

      const newestEnd = data.counters && data.counters.newest_window_end ? new Date(data.counters.newest_window_end) : null;
      const serverNowStr = data.server_utc_now || null;
      const serverNow = serverNowStr ? new Date(serverNowStr) : null;

      let baseAgeSec = 0;
      if (newestEnd && serverNow) {
         baseAgeSec = Math.max(0, (serverNow - newestEnd) / 1000);
      }
      serverAgeAtReceiptSec = baseAgeSec;

      // Update cards
      qs('[data-testid="counter-status"]').textContent = (data.counters && data.counters.status) ? data.counters.status : '—';
      qs('[data-testid="counter-freshness"]').textContent = 'Age: ' + formatAge(baseAgeSec);

      qs('[data-testid="usage-status"]').textContent = (data.usage && data.usage.status) ? data.usage.status : '—';
      qs('[data-testid="usage-freshness"]').textContent = 'Usage age: —';

      qs('[data-testid="poll-failures"]').textContent = (data.poll_failures && data.poll_failures.length >= 0) ? String(data.poll_failures.length) : '—';
      qs('[data-testid="poll-breaker"]').textContent = data.poll_failures && data.poll_failures[0] ? (data.poll_failures[0].breaker_state || '—') : '—';

      // Telemetry
      if (data.counters && data.counters.latest && data.counters.latest.cpu_busy_pct) {
         qs('[data-testid="cpu-busy"]').textContent = String(data.counters.latest.cpu_busy_pct.p50 !== null ? data.counters.latest.cpu_busy_pct.p50 : '—');
      } else {
         qs('[data-testid="cpu-busy"]').textContent = '—';
      }
      qs('[data-testid="load1"]').textContent = (data.counters && data.counters.latest && data.counters.latest.load1 != null) ? String(data.counters.latest.load1) : '—';
      qs('[data-testid="load5"]').textContent = (data.counters && data.counters.latest && data.counters.latest.load5 != null) ? String(data.counters.latest.load5) : '—';
      qs('[data-testid="load15"]').textContent = (data.counters && data.counters.latest && data.counters.latest.load15 != null) ? String(data.counters.latest.load15) : '—';

      // Memory
      const memUsed = (data.counters && data.counters.latest && data.counters.latest.mem_used_physical_kb != null) ? data.counters.latest.mem_used_physical_kb : null;
      const totalKb = (data.hardware && data.hardware.mem_total_kb != null) ? data.hardware.mem_total_kb : null;
      qs('[data-testid="mem-used"]').textContent = (memUsed != null && totalKb != null && totalKb > 0) ? (memUsed / (1024*1024)).toFixed(2) + ' GiB / ' + (totalKb / (1024*1024)).toFixed(2) + ' GiB' : (memUsed != null ? String(memUsed) + ' KiB' : '—');

      qs('[data-testid="procs-running"]').textContent = (data.counters && data.counters.latest && data.counters.latest.procs_running != null) ? String(data.counters.latest.procs_running) : '—';
      qs('[data-testid="procs-total"]').textContent = (data.counters && data.counters.latest && data.counters.latest.procs_total != null) ? String(data.counters.latest.procs_total) : '—';

      // Usage hotspot
      const grains = (data.usage && data.usage.grains) ? data.usage.grains : [];
      const dStateGrain = grains.find(function(g) { return g.d_state_fraction != null; }) || null;
      qs('[data-testid="d-state"]').textContent = dStateGrain ? String(dStateGrain.d_state_fraction) : '—';
      qs('[data-testid="usage-timestamp"]').textContent = dStateGrain ? (dStateGrain.interval_end || '—') : '—';
      qs('[data-testid="usage-username"]').textContent = dStateGrain ? (dStateGrain.d_state_username || '—') : '—';

      // Age tick uses server-reported base + monotonic elapsed
      qs('[data-testid="data-age-value"]').textContent = new Date(newestEnd ? newestEnd.toISOString() : '').toISOString().split('T')[1] ? 'timestamp' : '—';
      qs('[data-testid="data-age-seconds"]').textContent = 'age seconds: ' + formatAge(baseAgeSec + (performance.now() - receivedMonotonicMs) / 1000);

      qs('#retry-section').hidden = true;
      qs('[data-testid="connectivity-status"]').textContent = 'Connected';
   }

   function showDisconnected(msg) {
      connected = false;
      qs('[data-testid="connectivity-status"]').textContent = msg || 'Web server disconnected';
      qs('#retry-section').hidden = false;
   }

   async function load(node, rangeName, username) {
      try {
         const data = await fetchDashboard(node, rangeName, username);
         applySnapshot(data, node, rangeName);
         return true;
      } catch (e) {
         if (snapshot === null) {
            showDisconnected('Connection failure');
         } else {
            showDisconnected('Web server disconnected');
         }
         return false;
      }
   }

   // One-second local display timer updates age only; no fetch
   setInterval(function() {
      if (snapshot && receivedMonotonicMs) {
         const elapsedSec = (performance.now() - receivedMonotonicMs) / 1000;
         const totalAge = serverAgeAtReceiptSec + elapsedSec;
         const valEl = qs('[data-testid="data-age-seconds"]');
         if (valEl) valEl.textContent = 'age seconds: ' + formatAge(totalAge);
         const freshEl = qs('[data-testid="counter-freshness"]');
         if (freshEl && freshEl.textContent && freshEl.textContent.indexOf('Age:') === 0) {
            freshEl.textContent = 'Age: ' + formatAge(totalAge);
         }
      }
   }, 1000);

   // 60-second refresh interval — started once after setup
   function startRefresh(node, rangeName, username) {
      if (refreshTimer) return;
      refreshTimer = setInterval(async function() {
         await load(node, rangeName, username);
      }, 60000);
   }

   // Controls
   qsa('[data-testid="range-btn"]').forEach(function(btn) {
      btn.addEventListener('click', async function() {
         await load(qs('#node-input').value || 'login-04', btn.getAttribute('data-range'), qs('#user-input').value || null);
         startRefresh(qs('#node-input').value || 'login-04', btn.getAttribute('data-range'), qs('#user-input').value || null);
      });
   });

   document.getElementById('node-form').addEventListener('submit', async function(e) {
      e.preventDefault();
      await load(qs('#node-input').value || 'login-04', '1h', qs('#user-input').value || null);
      startRefresh(qs('#node-input').value || 'login-04', '1h', qs('#user-input').value || null);
   });

   document.getElementById('user-form').addEventListener('submit', async function(e) {
      e.preventDefault();
      await load(qs('#node-input').value || 'login-04', '1h', qs('#user-input').value || null);
      startRefresh(qs('#node-input').value || 'login-04', '1h', qs('#user-input').value || null);
   });

   qs('#retry-btn').addEventListener('click', async function() {
      await load(qs('#node-input').value || 'login-04', '1h', qs('#user-input').value || null);
      startRefresh(qs('#node-input').value || 'login-04', '1h', qs('#user-input').value || null);
   });

   // Initial load
   (async function init() {
      await load('login-04', '1h', null);
      startRefresh('login-04', '1h', null);
   })();
})();
