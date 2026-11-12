import { ProcessModal } from "./modal.js";
import { renderProcessTable, filterProcesses } from "./list.js";
import { GROUP_DEFS, aggregateForGroup, createPieChart, updatePieChart, createBarChart, updateBarChart, colorFor } from "./live_charts.js";

const CATEGORY_COLORS = { ai_agent: "#8b5cf6", editor_server: "#06b6d4", jupyter: "#f59e0b", build: "#ef4444", python: "#3b82f6", shell: "#6b7280", data_transfer: "#10b981", fs_scan: "#a3a3a3", scheduler: "#ec4899", workflow: "#14b8a6", system: "#52525b", other: "#9ca3af" };

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
      const [health, procs, usersData, eventsData] = await Promise.all([
         fetchJson("/api/health", signal),
         fetchJson("/api/live/processes", signal),
         fetchJson("/api/live/users", signal),
         fetchJson("/api/live/events?limit=20", signal),
      ]);
      if (signal.aborted) return;
      const strip = document.getElementById("health-strip");
      if (strip && health) {
         strip.textContent = "";
         const hbEl = document.createElement("span");
         hbEl.textContent = health.ok ? "ok heartbeat " + (health.heartbeat_age_s ?? "") + "s" : "degraded heartbeat " + (health.heartbeat_age_s ?? "none") + "s";
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
      void usersData;
   } catch (e) {
      if (e.name !== "AbortError") showDisconnected(true);
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
   void CATEGORY_COLORS;
   function onSliceClick(key) {
      const input = document.getElementById("process-filter");
      if (!input) return;
      if (key === "Other") {
         input.value = "";
      } else {
         input.value = key;
      }
      input.dispatchEvent(new Event("input", {bubbles: true}));
      applyFilter();
   }
   const canvasEl = document.getElementById("pie-canvas");
   if (canvasEl) {
      if (!pieChart) {
         pieChart = createPieChart(canvasEl, agg, group, onSliceClick);
      } else {
         updatePieChart(pieChart, agg, group);
      }
   }
   const rssCanvasEl = document.getElementById("rss-canvas");
   if (rssCanvasEl) {
      if (!rssChart) {
         rssChart = createBarChart(rssCanvasEl, "rss", onSliceClick);
         updateBarChart(rssChart, agg, group, "rss");
      } else {
         updateBarChart(rssChart, agg, group, "rss");
      }
   }
   const cpuCanvasEl = document.getElementById("cpu-canvas");
   if (cpuCanvasEl) {
      if (!cpuChart) {
         cpuChart = createBarChart(cpuCanvasEl, "cpu", onSliceClick);
         updateBarChart(cpuChart, agg, group, "cpu");
      } else {
         updateBarChart(cpuChart, agg, group, "cpu");
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
      groupSelect.addEventListener("change", () => {
         group = groupSelect.value;
         try { sessionStorage.setItem("nm:live:group", group); } catch (_e) {}
         applyFilter();
      });
   }
   const filterInput = document.getElementById("process-filter");
   if (filterInput) {
      filterInput.addEventListener("input", debounce((e) => {
         void e;
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
   void pollTimer;
}

if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
else init();

export { applyFilter, refresh, showDisconnected };
