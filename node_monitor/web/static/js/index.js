import { DotsCanvas } from "./dots.js";
import { ProcessModal } from "./modal.js";
import { renderProcessTable, filterProcesses } from "./list.js";

let allProcesses = [];
let filtered = [];
let mode = "process"; // process | user
let dotsCanvas = null;
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

      // update processes for dots + table (shared filtered set)
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
   filtered = filterProcesses(allProcesses, q);
   const dotsEl = document.getElementById("dots-container");
   if (dotsEl && dotsCanvas) dotsCanvas.setProcesses(filtered);
   const tableEl = document.getElementById("process-table-container");
   if (tableEl) {
      const items = mode === "process" ? filtered : filtered; // toggle changes grouping, but keep same set for now
      renderProcessTable(tableEl, items, {
         onRowClick: (proc) => ProcessModal.open({ system: proc.system || "polaris", node: proc.node, pid: proc.pid, start_time: proc.start_time }),
      });
   }
   // category bar update: sum = filtered length
   const bar = document.getElementById("category-mix");
   if (bar) bar.dataset.total = String(filtered.length);
}

function init() {
   // init dots canvas
   const dotsEl = document.getElementById("dots-container");
   if (dotsEl) {
      dotsCanvas = new DotsCanvas(dotsEl, {
         processes: allProcesses,
         colorBy: "category",
         sizeBy: "rss",
         onHover: () => {},
         onClick: (proc) => ProcessModal.open({ system: proc.system || "polaris", node: proc.node, pid: proc.pid, start_time: proc.start_time }),
      });
   }

   // filter debounced
   const filterInput = document.getElementById("process-filter");
   if (filterInput) {
      filterInput.addEventListener("input", debounce((e) => {
         applyFilter();
      }, 200));
   }

   // toggle user/process
   const toggle = document.getElementById("toggle-mode");
   if (toggle) {
      toggle.addEventListener("click", () => {
         mode = mode === "process" ? "user" : "process";
         toggle.textContent = mode === "process" ? "Toggle to user" : "Toggle to process";
         toggle.dataset.mode = mode;
         applyFilter();
      });
   }

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
