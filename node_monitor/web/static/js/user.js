import { renderProcessTable } from "./list.js";

let currentPage = 1;
const pageSize = 50;
const cap = 2000;
let allProcesses = [];

function renderPage() {
   const el = document.getElementById("user-process-table");
   if (!el) return;
   const end = Math.min(currentPage * pageSize, Math.min(allProcesses.length, cap));
   const slice = allProcesses.slice(0, end);
   renderProcessTable(el, slice);
   const btn = document.querySelector("[data-show-more]");
   if (btn) btn.style.display = end >= Math.min(allProcesses.length, cap) ? "none" : "";
}

async function fetchJson(url) {
   const r = await fetch(url);
   if (!r.ok) throw new Error(r.statusText);
   return r.json();
}

async function loadHistory() {
   const user = (window.__USER__ && window.__USER__.username) || "";
   if (!user) return;
   const now = new Date();
   const from = new Date(now.getTime() - 24 * 3600 * 1000);
   try {
      const data = await fetchJson(`/api/history/user/${encodeURIComponent(user)}?from=${encodeURIComponent(from.toISOString())}&to=${encodeURIComponent(now.toISOString())}&bucket=5m`);
      renderHistory(data.items || []);
   } catch (e) {}
}

function renderHistory(items) {
   const el = document.getElementById("cpu-history");
   if (el) el.dataset.loaded = String(items.length);
   const memEl = document.getElementById("mem-history");
   if (memEl) memEl.dataset.loaded = String(items.length);
}

function renderDormancy() {
   const bar = document.querySelector("[data-dormancy-bar]");
   if (!bar) return;
   // sum example via processes
   bar.dataset.loaded = "1";
}

function init() {
   const procs = window.__PROCESSES__ || [];
   allProcesses = procs.slice(0, cap);
   renderPage();
   const btn = document.querySelector("[data-show-more]");
   if (btn) btn.addEventListener("click", () => { currentPage++; renderPage(); });
   loadHistory();
   renderDormancy();
}

if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
else init();

export { renderPage, loadHistory };
