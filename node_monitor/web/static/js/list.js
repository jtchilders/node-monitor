/**
 * list.js — shared process list/table rendering.
 *
 * Provides helpers to render a filterable, sortable process list that stays
 * in sync with the dots canvas. All user text via textContent.
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
   const prefix = lowerBound ? "\u2265" : "";
   sec = Math.max(0, Math.floor(sec));
   if (sec < 60) return prefix + sec + "s";
   if (sec < 3600) return prefix + Math.floor(sec / 60) + "m";
   if (sec < 86400) return prefix + Math.floor(sec / 3600) + "h" + String(Math.floor((sec % 3600) / 60)).padStart(2, "0") + "m";
   return prefix + Math.floor(sec / 86400) + "d";
}

export function renderProcessTable(container, processes, opts = {}) {
   const onRowClick = opts.onRowClick || null;
   container.textContent = "";

   if (!processes || processes.length === 0) {
      const empty = document.createElement("div");
      empty.className = "process-table-empty";
      empty.textContent = "No processes to display";
      container.appendChild(empty);
      return;
   }

   const table = document.createElement("table");
   table.className = "process-table";

   const thead = document.createElement("thead");
   const headRow = document.createElement("tr");
   for (const h of ["Node", "User", "PID", "Category", "Activity", "RSS", "CPU", "Idle", "Presence", "Command"]) {
      const th = document.createElement("th");
      th.textContent = h;
      headRow.appendChild(th);
   }
   thead.appendChild(headRow);
   table.appendChild(thead);

   const tbody = document.createElement("tbody");
   for (const proc of processes) {
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
         // Literal text, never HTML
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
