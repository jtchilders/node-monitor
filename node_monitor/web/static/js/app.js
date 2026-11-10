/**
 * app.js — main wiring for the web app.
 *
 * Polls /api/health for the health strip, wires DotsCanvas + ProcessModal + list.
 * All command lines shown are the redacted ones from the API.
 */

import { DotsCanvas } from "./dots.js";
import { ProcessModal } from "./modal.js";
import { renderProcessTable, filterProcesses } from "./list.js";

let dotsCanvas = null;
let allProcesses = [];
let filterQuery = "";

// Debounced filter input
function debounce(fn, ms) {
   let t = null;
   return (...args) => {
      clearTimeout(t);
      t = setTimeout(() => fn(...args), ms);
   };
}

function applyFilter() {
   const filtered = filterProcesses(allProcesses, filterQuery);
   const dotsEl = document.getElementById("dots-container");
   if (dotsEl && dotsCanvas) {
      dotsCanvas.setProcesses(filtered);
   }
   const tableEl = document.getElementById("process-table-container");
   if (tableEl) {
      renderProcessTable(tableEl, filtered, {
         onRowClick: (proc) => {
            ProcessModal.open({
               system: proc.system || document.documentElement.dataset.system || "polaris",
               node: proc.node,
               pid: proc.pid,
               start_time: proc.start_time,
            });
         },
      });
   }
}

async function fetchHealth() {
   const strip = document.getElementById("health-strip");
   if (!strip) return;
   try {
      const resp = await fetch("/api/health");
      const data = await resp.json();
      strip.textContent = "";
      const okEl = document.createElement("span");
      okEl.textContent = data.ok ? "ok" : "degraded";
      okEl.className = data.ok ? "health-ok" : "health-bad";
      strip.appendChild(okEl);
      if (data.heartbeat_age_s != null) {
         const ageEl = document.createElement("span");
         ageEl.textContent = " heartbeat " + data.heartbeat_age_s + "s ago";
         strip.appendChild(ageEl);
      }
      for (const n of data.nodes || []) {
         const nodeEl = document.createElement("span");
         nodeEl.textContent = " " + n.label + ":" + n.state;
         strip.appendChild(nodeEl);
      }
   } catch (e) {
      strip.textContent = "";
      const err = document.createElement("span");
      err.textContent = "health unavailable";
      strip.appendChild(err);
   }
}

async function fetchProcesses() {
   try {
      const resp = await fetch("/api/live/processes");
      if (!resp.ok) return;
      const data = await resp.json();
      allProcesses = data.items || [];
      applyFilter();
   } catch (e) {
      // polling loop will retry
   }
}

function init() {
   const filterInput = document.getElementById("process-filter");
   if (filterInput) {
      filterInput.addEventListener(
         "input",
         debounce((e) => {
            filterQuery = e.target.value;
            applyFilter();
         }, 200)
      );
   }

   const dotsEl = document.getElementById("dots-container");
   if (dotsEl) {
      dotsCanvas = new DotsCanvas(dotsEl, {
         processes: allProcesses,
         colorBy: "category",
         sizeBy: "rss",
         onHover: (payload) => {
            // Hover payload already contains username/activity/cmdline/idle/presence
         },
         onClick: (proc) => {
            ProcessModal.open({
               system: proc.system || document.documentElement.dataset.system || "polaris",
               node: proc.node,
               pid: proc.pid,
               start_time: proc.start_time,
            });
         },
      });
   }

   fetchHealth();
   fetchProcesses();
   setInterval(fetchHealth, 30000);
   setInterval(fetchProcesses, 30000);
}

if (document.readyState === "loading") {
   document.addEventListener("DOMContentLoaded", init);
} else {
   init();
}

export { DotsCanvas, ProcessModal, renderProcessTable, filterProcesses };
