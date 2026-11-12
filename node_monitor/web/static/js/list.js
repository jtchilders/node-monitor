/**
 * list.js — shared process list/table rendering.
 * Provides filterable, sortable process list synced with dots canvas. All user text via textContent.
 */

export function formatRss(kb) {
   if (kb == null) return "-";
   if (kb < 1024) return kb + " KiB";
   const mib = kb / 1024;
   if (mib < 1024) return mib.toFixed(1) + " MiB";
   return (mib / 1024).toFixed(1) + " GiB";
}

export function formatIdle(sec, lowerBound) {
   if (sec == null) return "-";
   const prefix = lowerBound ? "≥" : "";
   sec = Math.max(0, Math.floor(sec));
   if (sec < 60) return prefix + sec + "s";
   if (sec < 3600) return prefix + Math.floor(sec / 60) + "m";
   if (sec < 86400) return prefix + Math.floor(sec / 3600) + "h" + String(Math.floor((sec % 3600) / 60)).padStart(2, "0") + "m";
   return prefix + Math.floor(sec / 86400) + "d";
}

let _sortKey = "rss";
let _sortDesc = true;

const HEADERS = [
   { label: "Node", key: "node", val: (p) => p.label || p.node || "" },
   { label: "User", key: "user", val: (p) => p.username || "" },
   { label: "PID", key: "pid", val: (p) => p.pid ?? 0 },
   { label: "Category", key: "category", val: (p) => p.category || "" },
   { label: "Activity", key: "activity", val: (p) => p.activity || "" },
   { label: "RSS", key: "rss", val: (p) => p.rss_kb ?? -1 },
   { label: "CPU", key: "cpu", val: (p) => p.cpu_seconds ?? -1 },
   { label: "Idle", key: "idle", val: (p) => p.idle_for_sec ?? -1 },
   { label: "Presence", key: "presence", val: (p) => p.owner_presence || "" },
   { label: "Command", key: null, val: null },
];

const USER_HEADERS = [
   { label: "User", key: "user", val: (r) => r.username || "" },
   { label: "Procs", key: "procs", val: (r) => r.count ?? 0 },
   { label: "RSS", key: "rss", val: (r) => r.rss_kb ?? -1 },
   { label: "CPU", key: "cpu", val: (r) => r.cpu_seconds ?? -1 },
   { label: "Nodes", key: null, val: null },
];

function aggregateByUser(processes) {
   const m = new Map();
   for (const p of processes) {
      const u = p.username || "unknown";
      let a = m.get(u);
      if (!a) m.set(u, a = { username: u, count: 0, rss_kb: 0, cpu_seconds: 0, nodes: new Set() });
      a.count += 1; a.rss_kb += p.rss_kb || 0; a.cpu_seconds += p.cpu_seconds || 0;
      if (p.node || p.label) a.nodes.add(p.node || p.label);
   }
   for (const a of m.values()) a.nodes = [...a.nodes].join(", ");
   return [...m.values()];
}

export function renderProcessTable(container, processes, opts = {}) {
   if (opts.mode === "user") return renderUserTable(container, processes, opts);
   const onRowClick = opts.onRowClick || null;
   container.textContent = "";

   if (!processes || processes.length === 0) {
      const empty = document.createElement("div");
      empty.className = "process-table-empty";
      empty.style.cssText = "padding:1rem; text-align:center; color:var(--text-dim);";
      empty.textContent = "No processes to display";
      container.appendChild(empty);
      return;
   }

   // sort copy
   const sorted = [...processes].sort((a, b) => {
      const h = HEADERS.find(x => x.key === _sortKey);
      if (!h || !h.val) return 0;
      const av = h.val(a), bv = h.val(b);
      let cmp = 0;
      if (typeof av === "number" && typeof bv === "number") cmp = av - bv;
      else cmp = String(av).localeCompare(String(bv));
      return _sortDesc ? -cmp : cmp;
   });

   const table = document.createElement("table");
   table.className = "jobs-table";

   const thead = document.createElement("thead");
   const headRow = document.createElement("tr");
   for (const h of HEADERS) {
      const th = document.createElement("th");
      th.textContent = h.label;
      if (h.key) {
         th.dataset.sort = h.key;
         th.style.cursor = "pointer";
         th.title = "Sort by " + h.label;
         if (h.key === _sortKey) {
            th.textContent += _sortDesc ? " ▼" : " ▲";
            th.style.color = "var(--text-color)";
         }
         th.addEventListener("click", () => {
            if (_sortKey === h.key) _sortDesc = !_sortDesc;
            else { _sortKey = h.key; _sortDesc = true; }
            renderProcessTable(container, processes, opts);
         });
      }
      headRow.appendChild(th);
   }
   thead.appendChild(headRow);
   table.appendChild(thead);

   const tbody = document.createElement("tbody");
   for (const proc of sorted) {
      const tr = document.createElement("tr");
      if (onRowClick) {
         tr.style.cursor = "pointer";
         tr.addEventListener("click", () => onRowClick(proc));
      }
      const cells = [
         proc.label || proc.node || "-",
         proc.username || "-",
         proc.pid != null ? String(proc.pid) : "-",
         proc.category || "-",
         proc.activity || "-",
         formatRss(proc.rss_kb),
         proc.cpu_seconds != null ? String(Math.round(proc.cpu_seconds)) : "-",
         formatIdle(proc.idle_for_sec, proc.idle_for_is_lower_bound),
         proc.owner_presence || "-",
         (proc.cmdline || "").slice(0, 80),
      ];
      for (const val of cells) {
         const td = document.createElement("td");
         td.textContent = val;
         tr.appendChild(td);
      }
      tbody.appendChild(tr);
   }
   table.appendChild(tbody);
   container.appendChild(table);
}

export function filterProcesses(processes, query) {
   if (!query) return processes;
   const q = query.toLowerCase();
   return processes.filter((p) => {
      return (
         (p.username && p.username.toLowerCase().includes(q)) ||
         (p.cmdline && p.cmdline.toLowerCase().includes(q)) ||
         (p.category && p.category.toLowerCase().includes(q)) ||
         (p.activity && p.activity.toLowerCase().includes(q)) ||
         (p.label && p.label.toLowerCase().includes(q)) ||
         (p.node && p.node.toLowerCase().includes(q))
      );
   });
}

function renderUserTable(container, processes, opts = {}) {
   const onRowClick = opts.onRowClick || null;
   container.textContent = "";
   const rows = aggregateByUser(processes);
   if (!rows.length) {
      const empty = document.createElement("div");
      empty.style.cssText = "padding:1rem; text-align:center; color:var(--text-dim);";
      empty.textContent = "No processes to display";
      container.appendChild(empty); return;
   }
   let _uKey = _sortKey;
   if (!USER_HEADERS.some(x => x.key === _uKey)) { _uKey = "rss"; _sortKey = "rss"; _sortDesc = true; }
   const sorted = [...rows].sort((a,b) => {
      const h = USER_HEADERS.find(x => x.key === _uKey);
      if (!h?.val) return 0;
      const av = h.val(a), bv = h.val(b);
      const cmp = typeof av === "number" && typeof bv === "number" ? av-bv : String(av).localeCompare(String(bv));
      return _sortDesc ? -cmp : cmp;
   });
   const table = document.createElement("table");
   table.className = "jobs-table";
   const thead = document.createElement("thead");
   const hr = document.createElement("tr");
   for (const h of USER_HEADERS) {
      const th = document.createElement("th");
      th.textContent = h.label;
      if (h.key) {
         th.style.cursor = "pointer";
         if (h.key === _sortKey) { th.textContent += _sortDesc ? " ▼" : " ▲"; th.style.color = "var(--text-color)"; }
         th.addEventListener("click", () => {
            if (_sortKey === h.key) _sortDesc = !_sortDesc; else { _sortKey = h.key; _sortDesc = true; }
            // pass mode through so next render stays in user view
            renderUserTable(container, processes, { ...opts, mode: "user" });
         });
      }
      hr.appendChild(th);
   }
   thead.appendChild(hr); table.appendChild(thead);
   const tbody = document.createElement("tbody");
   for (const r of sorted) {
      const tr = document.createElement("tr");
      tr.style.cursor = "pointer";
      tr.addEventListener("click", () => { window.location.href = "/user/" + encodeURIComponent(r.username); });
      const cells = [r.username, String(r.count), formatRss(r.rss_kb), String(Math.round(r.cpu_seconds)), r.nodes];
      for (const v of cells) { const td = document.createElement("td"); td.textContent = v; tr.appendChild(td); }
      tbody.appendChild(tr);
   }
   table.appendChild(tbody); container.appendChild(table);
}

export default { renderProcessTable, filterProcesses, formatRss, formatIdle };
