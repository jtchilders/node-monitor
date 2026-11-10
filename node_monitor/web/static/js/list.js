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

export function renderProcessTable(container, processes, opts = {}) {
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

export default { renderProcessTable, filterProcesses, formatRss, formatIdle };
