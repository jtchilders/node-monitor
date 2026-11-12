import { ProcessModal } from "./modal.js";
import { renderProcessTable, filterProcesses } from "./list.js";
import { aggregateForGroup, createPieChart, updatePieChart } from "./live_charts.js";

const CATEGORY_COLORS = { ai_agent: "#8b5cf6", editor_server: "#06b6d4", jupyter: "#f59e0b", build: "#ef4444", python: "#3b82f6", shell: "#6b7280", data_transfer: "#10b981", fs_scan: "#a3a3a3", scheduler: "#ec4899", workflow: "#14b8a6", system: "#52525b", other: "#9ca3af" };

let allProcesses = [];
let filtered = [];
let mode = "process"; // process | user
let selectedNode = "__all";
let group = "category";
let pieChart = null;
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

      // health strip update
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
      applyFilter();

      // disconnected banner off on success
      showDisconnected(false);

      // events
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
   // category bar + legend: recompute from filtered
   const bar = document.getElementById("category-mix");
   if (bar) bar.dataset.total = String(filtered.length);
   const counts = {};
   for (const p of filtered) counts[p.category || "other"] = (counts[p.category || "other"] || 0) + 1;
   const legend = document.getElementById("category-legend");
   if (legend) {
      legend.textContent = "";
      const total = filtered.length || 1;
      for (const [cat, cnt] of Object.entries(counts).sort((a,b) => b[1]-a[1])) {
         const span = document.createElement("span");
         span.className = "legend-item";
         const box = document.createElement("i");
         box.className = "color-box";
         box.style.background = CATEGORY_COLORS[cat] || "#9ca3af";
         span.appendChild(box);
         span.appendChild(document.createTextNode(" " + cat + " " + cnt));
         legend.appendChild(span);
      }
      // update category bar segments
      const catBar = bar?.querySelector(".category-bar");
      if (catBar) {
         catBar.textContent = "";
         for (const [cat, cnt] of Object.entries(counts)) {
            const seg = document.createElement("div");
            seg.className = "category-seg";
            seg.dataset.category = cat;
            seg.dataset.count = String(cnt);
            seg.style.width = ((cnt * 100 / total) + "%");
            seg.style.background = CATEGORY_COLORS[cat] || "#9ca3af";
            catBar.appendChild(seg);
         }
      }
   }
   // pie chart
   const agg = aggregateForGroup(filtered, group);
   const canvasEl = document.getElementById("pie-canvas");
   if (canvasEl) {
      if (!pieChart) {
         pieChart = createPieChart(canvasEl, agg, group);
      } else {
         updatePieChart(pieChart, agg, group);
      }
   }
}

function init() {
   // restore group from sessionStorage
   try {
      const saved = sessionStorage.getItem("nm:live:group");
      if (saved) group = saved;
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

   // filter debounced
   const filterInput = document.getElementById("process-filter");
   if (filterInput) {
      filterInput.addEventListener("input", debounce((e) => {
         applyFilter();
      }, 200));
   }

   // toggle user/process — either button switches
   function setMode(newMode) {
      mode = newMode;
      document.querySelectorAll("[data-mode]").forEach(b => b.classList.toggle("active", b.dataset.mode === mode));
      applyFilter();
   }
   document.getElementById("toggle-mode-process")?.addEventListener("click", () => setMode("process"));
   document.getElementById("toggle-mode-user")?.addEventListener("click", () => setMode("user"));
   // node tabs — also link to detail on double-click
   const tabs = document.getElementById("node-tabs");
   if (tabs) {
      tabs.addEventListener("click", (ev) => {
         const btn = ev.target.closest("[data-node]");
         if (!btn) return;
         selectedNode = btn.dataset.node;
         tabs.querySelectorAll(".tab-btn").forEach(b => b.classList.toggle("active", b === btn));
         applyFilter();
      });
      tabs.addEventListener("dblclick", (ev) => {
         const btn = ev.target.closest("[data-node]");
         if (!btn || btn.dataset.node === "__all") return;
         window.location.href = "/node/" + encodeURIComponent(btn.dataset.node);
      });
   }
   // make per-node cards clickable to node detail
   document.getElementById("per-node-cards")?.addEventListener("click", (ev) => {
      const card = ev.target.closest("[data-node]");
      if (card) window.location.href = "/node/" + encodeURIComponent(card.dataset.node);
   });

   // initial from server-rendered initial data if present
   if (window.__INITIAL_DATA__ && window.__INITIAL_DATA__.processes) {
      allProcesses = window.__INITIAL_DATA__.processes;
      applyFilter();
   }

   // poll every 30s
   pollTimer = setInterval(refresh, 30000);
   // also fetch now to hydrate
   refresh();
}

if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
else init();

export { applyFilter, refresh, showDisconnected };
