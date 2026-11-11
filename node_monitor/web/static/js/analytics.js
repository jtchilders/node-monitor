/**
 * analytics.js — /analytics page.
 * Chart.js stacked areas for category-mix / agent-adoption / dormancy,
 * paged leaderboards, coverage bars. Range picker 1h–2y drives bucket.
 * All dynamic values via textContent (XSS-safe). Polls every 30s.
 */

const rangeToParams = {
   "1h": { hours: 1, bucket: "1h" },
   "24h": { hours: 24, bucket: "1h" },
   "7d": { hours: 7 * 24, bucket: "1d" },
   "30d": { hours: 30 * 24, bucket: "1d" },
   "1y": { hours: 365 * 24, bucket: "1w" },
   "2y": { hours: 2 * 365 * 24, bucket: "1w" },
};

let currentRange = "7d";
let currentBucket = "1d";
let pollTimer = null;

const CATEGORY_COLORS = {
   ai_agent: "#8b5cf6",
   editor_server: "#06b6d4",
   jupyter: "#f59e0b",
   build: "#ef4444",
   python: "#3b82f6",
   shell: "#6b7280",
   data_transfer: "#10b981",
   fs_scan: "#a3a3a3",
   scheduler: "#ec4899",
   workflow: "#14b8a6",
   system: "#52525b",
   other: "#9ca3af",
};

const CHARTS = {};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function fmtDate(d) {
   return d.toISOString();
}

function fmtRange() {
   const cfg = rangeToParams[currentRange] || rangeToParams["7d"];
   const now = new Date();
   const from = new Date(now.getTime() - cfg.hours * 3600 * 1000);
   currentBucket = cfg.bucket;
   return { from: fmtDate(from), to: fmtDate(now), bucket: currentBucket };
}

async function fetchJson(url) {
   const r = await fetch(url);
   if (!r.ok) throw new Error("fetch failed " + r.status);
   return r.json();
}

function colorFor(cat) {
   return CATEGORY_COLORS[cat] || "#9ca3af";
}

function getChartLib() {
   // vendored Chart.js exposes window.Chart
   return typeof window !== "undefined" ? window.Chart : null;
}

// ---------------------------------------------------------------------------
// chart rendering (Chart.js stacked areas)
// ---------------------------------------------------------------------------

function ensureChart(canvasId, config) {
   const canvas = document.getElementById(canvasId);
   if (!canvas) return null;
   const ChartLib = getChartLib();
   if (!ChartLib) {
      canvas.dataset.loaded = "1";
      return null;
   }
   const existing = CHARTS[canvasId];
   if (existing) {
      existing.data = config.data;
      if (config.options) existing.options = { ...existing.options, ...config.options };
      existing.update();
      return existing;
   }
   const chart = new ChartLib(canvas, config);
   CHARTS[canvasId] = chart;
   return chart;
}

function renderCategoryMix(items) {
   const labels = items.map((it) => it.bucket);
   const allCats = [...new Set(items.flatMap((it) => Object.keys(it.categories || {})))].sort();
   const datasets = allCats.map((cat) => ({
      label: cat,
      data: items.map((it) => (it.categories && it.categories[cat]) || 0),
      backgroundColor: colorFor(cat),
      borderColor: colorFor(cat),
      fill: true,
      tension: 0.2,
      pointRadius: 0,
   }));
   ensureChart("category-mix-chart", {
      type: "line",
      data: { labels, datasets },
      options: {
         responsive: true,
         maintainAspectRatio: false,
         interaction: { mode: "index", intersect: false },
         scales: { x: { stacked: true }, y: { stacked: true, beginAtZero: true } },
         plugins: { legend: { labels: { color: "#94a3b8", boxWidth: 12 } } },
      },
   });
   const el = document.getElementById("category-mix-chart");
   if (el) el.dataset.loaded = String(items.length);
}

function renderAgentAdoption(payload) {
   const items = payload.items || [];
   const labels = items.map((it) => it.bucket);
   ensureChart("agent-adoption-chart", {
      type: "line",
      data: {
         labels,
         datasets: [
            {
               label: "AI users",
               data: items.map((it) => it.ai_users || 0),
               backgroundColor: "rgba(139,92,246,0.35)",
               borderColor: "#8b5cf6",
               fill: true,
               tension: 0.2,
               pointRadius: 0,
            },
            {
               label: "Total users",
               data: items.map((it) => it.total_users || 0),
               backgroundColor: "rgba(59,130,246,0.20)",
               borderColor: "#3b82f6",
               fill: true,
               tension: 0.2,
               pointRadius: 0,
            },
         ],
      },
      options: {
         responsive: true,
         maintainAspectRatio: false,
         scales: { y: { beginAtZero: true } },
         plugins: { legend: { labels: { color: "#94a3b8", boxWidth: 12 } } },
      },
   });
   const el = document.getElementById("agent-adoption-chart");
   if (el) el.dataset.loaded = String(items.length);

   // tag share bar (horizontal)
   const tags = payload.tag_share || [];
   if (tags.length) {
      ensureChart("tag-share-chart", {
         type: "bar",
         data: {
            labels: tags.slice(0, 10).map((t) => t.tag),
            datasets: [{ label: "count", data: tags.slice(0, 10).map((t) => t.count), backgroundColor: "#3b82f6" }],
         },
         options: {
            indexAxis: "y",
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { x: { beginAtZero: true } },
         },
      });
   }
   const tagEl = document.getElementById("tag-share-chart");
   if (tagEl) tagEl.dataset.loaded = String((payload.tag_share || []).length);
}

function renderDormancy(items) {
   const labels = items.map((it) => it.bucket);
   const allCats = [...new Set(items.flatMap((it) => Object.keys(it.categories || {})))].sort();
   const datasets = allCats.map((cat) => ({
      label: cat,
      data: items.map((it) => (it.categories && it.categories[cat] ? it.categories[cat].count : 0)),
      backgroundColor: colorFor(cat),
      borderColor: colorFor(cat),
      fill: true,
      tension: 0.2,
      pointRadius: 0,
   }));
   ensureChart("dormancy-chart", {
      type: "line",
      data: { labels, datasets },
      options: {
         responsive: true,
         maintainAspectRatio: false,
         interaction: { mode: "index", intersect: false },
         scales: { x: { stacked: true }, y: { stacked: true, beginAtZero: true } },
         plugins: { legend: { labels: { color: "#94a3b8", boxWidth: 12 } } },
      },
   });
   const el = document.getElementById("dormancy-chart");
   if (el) el.dataset.loaded = String(items.length);
}

// ---------------------------------------------------------------------------
// leaderboards — paged, textContent only
// ---------------------------------------------------------------------------

const LB_PAGE_SIZE = 10;
const lbState = {
   cpu: { page: 1, items: [], metric: "cpu" },
   mem: { page: 1, items: [], metric: "mem" },
   dormant: { page: 1, items: [], metric: "dormant" },
};

function renderLeaderboard(metric) {
   const state = lbState[metric];
   if (!state) return;
   const body = document.querySelector(`[data-lb-body="${metric}"]`);
   if (!body) return;
   body.textContent = "";
   const page = state.page;
   const start = (page - 1) * LB_PAGE_SIZE;
   const slice = state.items.slice(start, start + LB_PAGE_SIZE);

   if (slice.length === 0) {
      const empty = document.createElement("div");
      empty.textContent = "No data";
      empty.style.cssText = "color:var(--text-dim); text-align:center; padding:1rem;";
      body.appendChild(empty);
   } else {
      const table = document.createElement("table");
      table.className = "lb-table";
      const thead = document.createElement("thead");
      const hr = document.createElement("tr");
      for (const lbl of ["#", "User", "Value"]) {
         const th = document.createElement("th");
         th.textContent = lbl;
         hr.appendChild(th);
      }
      thead.appendChild(hr);
      table.appendChild(thead);
      const tbody = document.createElement("tbody");
      slice.forEach((row, idx) => {
         const tr = document.createElement("tr");
         const rank = document.createElement("td");
         rank.textContent = String(start + idx + 1);
         tr.appendChild(rank);
         const user = document.createElement("td");
         user.textContent = row.username || "";
         tr.appendChild(user);
         const val = document.createElement("td");
         val.textContent = String(row.value != null ? row.value : "");
         tr.appendChild(val);
         tbody.appendChild(tr);
      });
      table.appendChild(tbody);
      body.appendChild(table);
   }

   const info = document.querySelector(`[data-lb-pageinfo="${metric}"]`);
   if (info) {
      const totalPages = Math.max(1, Math.ceil(state.items.length / LB_PAGE_SIZE));
      info.textContent = "page " + page + " / " + totalPages;
   }
   const prev = document.querySelector(`[data-lb-prev="${metric}"]`);
   const next = document.querySelector(`[data-lb-next="${metric}"]`);
   const totalPages = Math.max(1, Math.ceil(state.items.length / LB_PAGE_SIZE));
   if (prev) prev.disabled = page <= 1;
   if (next) next.disabled = page >= totalPages;
}

function bindLeaderboardPager() {
   for (const metric of Object.keys(lbState)) {
      const prev = document.querySelector(`[data-lb-prev="${metric}"]`);
      const next = document.querySelector(`[data-lb-next="${metric}"]`);
      if (prev) {
         prev.addEventListener("click", () => {
            if (lbState[metric].page > 1) {
               lbState[metric].page -= 1;
               renderLeaderboard(metric);
            }
         });
      }
      if (next) {
         next.addEventListener("click", () => {
            const totalPages = Math.max(1, Math.ceil(lbState[metric].items.length / LB_PAGE_SIZE));
            if (lbState[metric].page < totalPages) {
               lbState[metric].page += 1;
               renderLeaderboard(metric);
            }
         });
      }
   }
}

// ---------------------------------------------------------------------------
// coverage bars — textContent only
// ---------------------------------------------------------------------------

function renderCoverage(data) {
   const fill = document.querySelector("[data-coverage-fill]");
   if (fill) {
      const ratio = data.uptime_ratio != null ? data.uptime_ratio : 0;
      fill.style.width = (ratio * 100).toFixed(1) + "%";
   }
   const uptime = document.querySelector("[data-coverage-uptime]");
   if (uptime) {
      const pct = data.uptime_ratio != null ? (data.uptime_ratio * 100).toFixed(1) + "%" : "—";
      uptime.textContent = "uptime " + pct;
   }
   const mins = document.querySelector("[data-coverage-minutes]");
   if (mins) {
      mins.textContent = (data.covered_minutes != null ? data.covered_minutes : "—") + " / " + (data.total_minutes != null ? data.total_minutes : "—") + " minutes";
   }
   const gapsEl = document.querySelector("[data-coverage-gaps]");
   if (gapsEl) {
      gapsEl.textContent = "";
      const countEl = document.querySelector("[data-coverage-gaps-count]");
      if (countEl) countEl.textContent = String((data.gaps || []).length);
      const summaryEl = document.querySelector("[data-coverage-gaps]");
      // use the data-coverage-gaps list element we already have
      const gaps = data.gaps || [];
      const summary = document.querySelector("[data-coverage-gaps]");
      // summary in stats line
      const gapsStat = document.querySelector("[data-coverage-gaps]");
      void gapsStat;
      // find the stat line's gaps span (third span)
      const statGaps = document.querySelector("[data-coverage-stats] [data-coverage-gaps]");
      if (statGaps) statGaps.textContent = gaps.length + " gaps";
      // the dedicated gaps list container is data-coverage-gaps div — but stats also uses same
      // So render list into the container that is NOT inside coverage-stats
      const card = document.querySelector("[data-coverage-card]");
      const listContainer = card ? card.querySelector(".coverage-gaps") : null;
      const target = listContainer || gapsEl;
      if (target && target !== document.querySelector("[data-coverage-stats]")) {
         target.textContent = "";
         for (const g of gaps.slice(0, 50)) {
            const row = document.createElement("div");
            row.className = "coverage-gap";
            const left = document.createElement("span");
            left.textContent = (g.event_type || "") + " " + (g.ts || g.minute || "");
            row.appendChild(left);
            const right = document.createElement("span");
            right.textContent = g.node || "";
            row.appendChild(right);
            target.appendChild(row);
         }
      }
   }
   // also render metric_gaps as coverage bars if present
   const mGaps = data.metric_gaps || [];
   if (mGaps.length) {
      const card = document.querySelector("[data-coverage-card]");
      const listContainer = card ? card.querySelector(".coverage-gaps") : null;
      if (listContainer) {
         for (const mg of mGaps.slice(0, 20)) {
            const row = document.createElement("div");
            row.className = "coverage-gap";
            const left = document.createElement("span");
            left.textContent = "metric gap " + (mg.minute || "");
            row.appendChild(left);
            const right = document.createElement("span");
            right.textContent = (mg.node || "") + " " + mg.sample_count + "/" + mg.expected_count;
            row.appendChild(right);
            listContainer.appendChild(row);
         }
      }
   }
}

// ---------------------------------------------------------------------------
// main fetch
// ---------------------------------------------------------------------------

async function refresh() {
   const { from, to, bucket } = fmtRange();
   const q = "from=" + encodeURIComponent(from) + "&to=" + encodeURIComponent(to) + "&bucket=" + encodeURIComponent(bucket);

   // Update picker dataset + label
   const picker = document.querySelector("[data-range-picker]");
   if (picker) {
      picker.dataset.bucket = bucket;
      picker.dataset.range = currentRange;
   }
   const bucketLabel = document.querySelector("[data-bucket-label]");
   if (bucketLabel) bucketLabel.textContent = bucket;

   // Parallel fetches — tolerate partial failure
   const results = await Promise.allSettled([
      fetchJson("/api/analytics/category-mix?" + q),
      fetchJson("/api/analytics/agent-adoption?" + q),
      fetchJson("/api/analytics/dormancy?" + q),
      fetchJson("/api/analytics/leaderboard?metric=cpu&limit=50&" + q.replace("bucket=" + bucket, "")),
      fetchJson("/api/analytics/leaderboard?metric=mem&limit=50&" + q.replace("bucket=" + bucket, "")),
      fetchJson("/api/analytics/leaderboard?metric=dormant&limit=50&" + q.replace("bucket=" + bucket, "")),
      fetchJson("/api/analytics/coverage?" + q.replace("&bucket=" + bucket, "").replace("bucket=" + bucket + "&", "")),
   ]);

   if (results[0].status === "fulfilled") renderCategoryMix(results[0].value.items || []);
   if (results[1].status === "fulfilled") renderAgentAdoption(results[1].value);
   if (results[2].status === "fulfilled") renderDormancy(results[2].value.items || []);

   if (results[3].status === "fulfilled") {
      lbState.cpu.items = results[3].value.items || [];
      lbState.cpu.page = 1;
      renderLeaderboard("cpu");
   }
   if (results[4].status === "fulfilled") {
      lbState.mem.items = results[4].value.items || [];
      lbState.mem.page = 1;
      renderLeaderboard("mem");
   }
   if (results[5].status === "fulfilled") {
      lbState.dormant.items = results[5].value.items || [];
      lbState.dormant.page = 1;
      renderLeaderboard("dormant");
   }
   if (results[6].status === "fulfilled") renderCoverage(results[6].value);
}

function initRangePicker() {
   const picker = document.querySelector("[data-range-picker]");
   if (!picker) return;
   // mark initial active
   picker.querySelectorAll("button[data-range]").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.range === currentRange);
   });
   picker.addEventListener("click", (e) => {
      const btn = e.target.closest("button[data-range]");
      if (!btn) return;
      currentRange = btn.dataset.range;
      picker.querySelectorAll("button[data-range]").forEach((b) => {
         b.classList.toggle("active", b === btn);
      });
      const cfg = rangeToParams[currentRange];
      if (cfg) {
         currentBucket = cfg.bucket;
         picker.dataset.bucket = currentBucket;
         picker.dataset.range = currentRange;
         const lbl = document.querySelector("[data-bucket-label]");
         if (lbl) lbl.textContent = currentBucket;
      }
      refresh();
   });
}

function init() {
   initRangePicker();
   bindLeaderboardPager();
   refresh();
   pollTimer = setInterval(refresh, 30000);
}

if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
else init();

export { refresh, renderCategoryMix, renderAgentAdoption, renderDormancy, renderLeaderboard, renderCoverage };
