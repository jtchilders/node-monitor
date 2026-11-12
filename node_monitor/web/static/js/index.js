import { ProcessModal } from "./modal.js";
import { renderProcessTable, filterProcesses } from "./list.js";
import { GROUP_DEFS, CATEGORY_COLORS, aggregateForGroup, createPieChart, updatePieChart, createBarChart, updateBarChart, colorFor, buildResourcePieDataset, formatResourceLabel, createResourcePieChart, updateResourcePieChart } from "./live_charts.js";

let allProcesses = [];
let filtered = [];
let mode = "process";
let selectedNode = "__all";
let group = "category";
let pieChart = null;
let rssChart = null;
let cpuChart = null;
let pollTimer = null;
let abortCtrl = null;
let nodeCaps = {};

function debounce(fn, ms) {
   let t = null;
   return (...args) => {
      clearTimeout(t);
      t = setTimeout(() => fn(...args), ms);
   };
}

function showDisconnected(visible) {
   const banner = document.getElementById("disconnected-banner");
   if (!banner) return;
   banner.classList.toggle("visible", visible);
}

async function fetchJson(url, signal) {
   const resp = await fetch(url, { signal });
   if (!resp.ok) throw new Error("fetch failed " + resp.status);
   return resp.json();
}

async function refresh() {
   if (abortCtrl) abortCtrl.abort();
   abortCtrl = new AbortController();
   const signal = abortCtrl.signal;
   try {
      const [health, procs, usersData, eventsData, nodesData] = await Promise.all([
         fetchJson("/api/health", signal),
         fetchJson("/api/live/processes", signal),
         fetchJson("/api/live/users", signal),
         fetchJson("/api/live/events?limit=20", signal),
         fetchJson("/api/live/nodes", signal).catch(() => ({items: []})),
      ]);
      if (nodesData && nodesData.items) {
         nodeCaps = {};
         for (const n of nodesData.items) {
            const lbl = n.label || n.node;
            if (lbl) nodeCaps[lbl] = n;
         }
      }
      // also hydrate from SSR per-node-cards if no nodesData (fallback)
      if (Object.keys(nodeCaps).length === 0) {
         const cards = document.querySelectorAll("#per-node-cards [data-node]");
         for (const c of cards) {
            const lbl = c.dataset.node;
            if (lbl && !nodeCaps[lbl]) nodeCaps[lbl] = {label: lbl, mem_total_kb: null};
         }
      }
      if (signal.aborted) return;
      const strip = document.getElementById("health-strip");
      if (strip && health) {
         const age = health.heartbeat_age_s;
         let cls = "freshness-indicator ";
         if (!health.ok || (age != null && age > 120)) cls += "red";
         else if (age != null && age >= 60) cls += "yellow";
         else cls += "green";
         strip.className = cls;
         strip.textContent = "";
         const dot = document.createElement("span");
         dot.className = "dot";
         strip.appendChild(dot);
         const hbEl = document.createElement("span");
         hbEl.textContent = health.ok ? "ok heartbeat " + (age ?? "") + "s" : "degraded heartbeat " + (age ?? "none") + "s";
         strip.appendChild(hbEl);
         for (const n of health.nodes || []) {
            const el = document.createElement("span");
            el.textContent = " " + n.label + ":" + n.state + " " + n.updated_age_s + "s";
            el.dataset.label = n.label;
            el.dataset.state = n.state;
            strip.appendChild(el);
         }
      }
      allProcesses = procs.items || [];
      if (signal.aborted) return;
      applyFilter();
      showDisconnected(false);
      const evEl = document.getElementById("recent-events");
      if (evEl && eventsData) {
         const ul = evEl.querySelector("ul");
         if (ul) {
            ul.textContent = "";
            for (const ev of eventsData.items || []) {
               const li = document.createElement("li");
               li.textContent = ev.event_type + " " + ev.ts;
               ul.appendChild(li);
            }
         }
      }
   } catch (e) {
      if (e.name !== "AbortError") {
         showDisconnected(true);
         const strip = document.getElementById("health-strip");
         if (strip) strip.className = "freshness-indicator disconnected";
      }
   }
}

function applyFilter() {
   const q = document.getElementById("process-filter")?.value || "";
   let base = allProcesses;
   if (selectedNode !== "__all") {
      base = base.filter(p => (p.label || p.node) === selectedNode || p.node === selectedNode);
   }
   filtered = filterProcesses(base, q);
   const tableEl = document.getElementById("process-table-container");
   if (tableEl) {
      renderProcessTable(tableEl, filtered, {
         mode,
         onRowClick: (proc) => ProcessModal.open({ system: proc.system || "polaris", node: proc.node, pid: proc.pid, start_time: proc.start_time }),
      });
   }
   const agg = aggregateForGroup(filtered, group);
   const bar = document.getElementById("category-mix");
   if (bar) bar.dataset.total = String(filtered.length);
   const legend = document.getElementById("category-legend");
   if (legend) {
      legend.textContent = "";
      if (agg.groups.length === 0) {
         const span = document.createElement("span");
         span.className = "legend-item";
         span.textContent = "—";
         legend.appendChild(span);
      } else {
         for (const g of agg.groups) {
            const span = document.createElement("span");
            span.className = "legend-item";
            const box = document.createElement("i");
            box.className = "color-box";
            box.style.background = colorFor(group, g.key);
            span.appendChild(box);
            span.appendChild(document.createTextNode(" " + String(g.key) + " " + String(g.count)));
            legend.appendChild(span);
         }
      }
      const catBar = bar?.querySelector(".category-bar");
      if (catBar) {
         catBar.textContent = "";
         if (agg.groups.length === 0) {
            catBar.style.width = "0%";
         } else {
            const total = filtered.length || 1;
            for (const g of agg.groups) {
               const seg = document.createElement("div");
               seg.className = "category-seg";
               seg.dataset.category = String(g.key);
               seg.dataset.count = String(g.count);
               seg.style.width = ((g.count * 100 / total) + "%");
               seg.style.background = colorFor(group, g.key);
               catBar.appendChild(seg);
            }
         }
      }
   }
   function onSliceClick(key) {
      const input = document.getElementById("process-filter");
      if (!input) return;
      if (key === "Other") {
         input.value = "";
      } else {
         input.value = key;
      }
      applyFilter();
   }
   const pieTotalEl = document.getElementById("pie-total");
   if (pieTotalEl) {
      if (filtered.length === 0) pieTotalEl.textContent = "No processes";
      else if (selectedNode === "__all") pieTotalEl.textContent = String(filtered.length) + " processes";
      else pieTotalEl.textContent = String(filtered.length) + " processes on " + selectedNode;
   }
   const canvasEl = document.getElementById("pie-canvas");
   if (canvasEl) {
      if (!pieChart) {
         pieChart = createPieChart(canvasEl, agg, group, onSliceClick);
      } else {
         updatePieChart(pieChart, agg, group);
      }
   }
   // node capacity totals for labels
   let totalAvailMem = 0;
   let hasAvail = false;
   if (selectedNode === "__all") {
      for (const lbl of Object.keys(nodeCaps)) {
         const v = nodeCaps[lbl] && nodeCaps[lbl].mem_total_kb;
         if (v) { totalAvailMem += v; hasAvail = true; }
      }
   } else {
      const cap = nodeCaps[selectedNode];
      if (cap && cap.mem_total_kb) { totalAvailMem = cap.mem_total_kb; hasAvail = true; }
   }
   const totalUsedMem = filtered.reduce((s, p) => s + (p.rss_kb || 0), 0);
   const rssTotalEl = document.getElementById("rss-total");
   if (rssTotalEl) {
      if (hasAvail) rssTotalEl.textContent = formatResourceLabel(totalUsedMem, totalAvailMem, "memory");
      else if (filtered.length > 0) rssTotalEl.textContent = formatResourceLabel(totalUsedMem, 0, "memory");
      else rssTotalEl.textContent = "No processes";
   }
   const totalCpu = filtered.reduce((s, p) => s + (p.cpu_seconds || 0), 0);
   const cpuTotalEl = document.getElementById("cpu-total");
   if (cpuTotalEl) {
      if (filtered.length === 0) cpuTotalEl.textContent = "No processes";
      else cpuTotalEl.textContent = formatResourceLabel(totalCpu, 0, "cpu");
   }
   const rssCanvasEl = document.getElementById("rss-canvas");
   const rssEmptyEl = document.getElementById("rss-empty");
   const cpuCanvasEl = document.getElementById("cpu-canvas");
   const cpuEmptyEl = document.getElementById("cpu-empty");
   const rssEmpty = !agg.groups || agg.groups.length === 0;
   const cpuEmpty = rssEmpty;
   if (rssCanvasEl) {
      if (rssEmpty) { rssCanvasEl.hidden = true; if (rssEmptyEl) rssEmptyEl.hidden = false; }
      else { rssCanvasEl.hidden = false; if (rssEmptyEl) rssEmptyEl.hidden = true; }
      if (!rssChart) {
         rssChart = createResourcePieChart(rssCanvasEl, agg, group, "rss", onSliceClick);
      } else {
         updateResourcePieChart(rssChart, agg, group, "rss");
      }
   }
   if (cpuCanvasEl) {
      if (cpuEmpty) { cpuCanvasEl.hidden = true; if (cpuEmptyEl) cpuEmptyEl.hidden = false; }
      else { cpuCanvasEl.hidden = false; if (cpuEmptyEl) cpuEmptyEl.hidden = true; }
      if (!cpuChart) {
         cpuChart = createResourcePieChart(cpuCanvasEl, agg, group, "cpu", onSliceClick);
      } else {
         updateResourcePieChart(cpuChart, agg, group, "cpu");
      }
   }
}

function init() {
   try {
      const saved = sessionStorage.getItem("nm:live:group");
      if (saved && GROUP_DEFS[saved]) group = saved;
   } catch (_e) {}
   try {
      const savedNode = sessionStorage.getItem("nm:live:node");
      if (savedNode) {
         if (savedNode === "__all") selectedNode = "__all";
         else {
            const tabsEl = document.getElementById("node-tabs");
            const valid = tabsEl && tabsEl.querySelector('[data-node="' + savedNode.replace(/"/g, '\\"') + '"]');
            if (valid) selectedNode = savedNode;
            else selectedNode = "__all";
         }
      }
   } catch (_e) {}
   const groupSelect = document.getElementById("group-select");
   if (groupSelect) {
      groupSelect.value = group;
      if (groupSelect.value !== group) {
         group = "category";
         groupSelect.value = "category";
      }
      groupSelect.addEventListener("change", () => {
         group = groupSelect.value;
         try { sessionStorage.setItem("nm:live:group", group); } catch (_e) {}
         applyFilter();
      });
   }
   const filterInput = document.getElementById("process-filter");
   if (filterInput) {
      filterInput.addEventListener("input", debounce(() => {
         applyFilter();
      }, 200));
   }
   function setMode(newMode) {
      mode = newMode;
      document.querySelectorAll("[data-mode]").forEach(b => b.classList.toggle("active", b.dataset.mode === mode));
      applyFilter();
   }
   document.getElementById("toggle-mode-process")?.addEventListener("click", () => setMode("process"));
   document.getElementById("toggle-mode-user")?.addEventListener("click", () => setMode("user"));
   const tabs = document.getElementById("node-tabs");
   if (tabs) {
      if (selectedNode !== "__all") {
         tabs.querySelectorAll(".tab-btn").forEach(b => b.classList.toggle("active", b.dataset.node === selectedNode));
      }
      tabs.addEventListener("click", (ev) => {
         const btn = ev.target.closest("[data-node]");
         if (!btn) return;
         selectedNode = btn.dataset.node;
         try { sessionStorage.setItem("nm:live:node", selectedNode); } catch (_e) {}
         tabs.querySelectorAll(".tab-btn").forEach(b => b.classList.toggle("active", b === btn));
         applyFilter();
      });
      tabs.addEventListener("dblclick", (ev) => {
         const btn = ev.target.closest("[data-node]");
         if (!btn || btn.dataset.node === "__all") return;
         window.location.href = "/node/" + encodeURIComponent(btn.dataset.node);
      });
   }
   document.getElementById("per-node-cards")?.addEventListener("click", (ev) => {
      const card = ev.target.closest("[data-node]");
      if (card) window.location.href = "/node/" + encodeURIComponent(card.dataset.node);
   });
   if (window.__INITIAL_DATA__ && window.__INITIAL_DATA__.processes) {
      allProcesses = window.__INITIAL_DATA__.processes;
      applyFilter();
   }
   pollTimer = setInterval(refresh, 30000);
   refresh();
}

if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
else init();

export { applyFilter, refresh, showDisconnected };
