/**
 * commands.js — /commands search page.
 * Substring / regex toggle, filters, paged results. All values via textContent.
 */

let currentQuery = "";
let currentMode = "substring";
let currentFilters = { user: "", activity: "", tag: "" };
let currentOffset = 0;
const PAGE_SIZE = 50;
let totalResults = 0;

function getMode() {
   const checked = document.querySelector('input[name="search-mode"]:checked');
   return checked ? checked.value : "substring";
}

function buildParams() {
   const params = new URLSearchParams();
   const mode = getMode();
   if (mode === "regex") {
      if (currentQuery) params.set("regex", currentQuery);
   } else {
      if (currentQuery) params.set("q", currentQuery);
   }
   if (currentFilters.user) params.set("user", currentFilters.user);
   if (currentFilters.activity) params.set("activity", currentFilters.activity);
   if (currentFilters.tag) params.set("tag", currentFilters.tag);
   params.set("limit", String(PAGE_SIZE));
   params.set("offset", String(currentOffset));
   return params;
}

async function doSearch() {
   const params = buildParams();
   let resp;
   try {
      resp = await fetch("/api/commands/search?" + params.toString());
   } catch (e) {
      showMeta("network error");
      return;
   }
   if (resp.status === 400) {
      let detail = "bad request";
      try { const j = await resp.json(); detail = j.detail || detail; } catch {}
      showMeta(detail);
      clearResults();
      return;
   }
   if (!resp.ok) {
      showMeta("error " + resp.status);
      clearResults();
      return;
   }
   const body = await resp.json();
   totalResults = body.total || 0;
   renderResults(body.items || []);
   updateMeta();
   updatePager();
}

function clearResults() {
   const container = document.querySelector("[data-results]");
   if (container) container.replaceChildren();
}

function showMeta(text) {
   const el = document.querySelector("[data-results-meta]");
   if (el) el.textContent = text;
}

function updateMeta() {
   const el = document.querySelector("[data-results-meta]");
   if (!el) return;
   if (totalResults === 0) {
      el.textContent = "no results";
   } else {
      const from = currentOffset + 1;
      const to = Math.min(currentOffset + PAGE_SIZE, totalResults);
      el.textContent = from + "\u2013" + to + " of " + totalResults;
   }
}

function updatePager() {
   const prev = document.querySelector("[data-prev]");
   const next = document.querySelector("[data-next]");
   const info = document.querySelector("[data-pageinfo]");
   if (prev) prev.disabled = currentOffset === 0;
   if (next) next.disabled = currentOffset + PAGE_SIZE >= totalResults;
   if (info) {
      const page = Math.floor(currentOffset / PAGE_SIZE) + 1;
      const totalPages = Math.max(1, Math.ceil(totalResults / PAGE_SIZE));
      info.textContent = "page " + page + " of " + totalPages;
   }
}

function highlightMatch(text, query, isRegex) {
   if (!query) return [document.createTextNode(text)];
   let parts;
   if (isRegex) {
      try {
         const re = new RegExp(query, "i");
         const m = text.match(re);
         if (!m || m[0] === "") return [document.createTextNode(text)];
         const idx = m.index;
         const matched = m[0];
         parts = [];
         if (idx > 0) parts.push(document.createTextNode(text.slice(0, idx)));
         const mark = document.createElement("mark");
         mark.textContent = matched;
         parts.push(mark);
         if (idx + matched.length < text.length) {
            parts.push(document.createTextNode(text.slice(idx + matched.length)));
         }
         return parts;
      } catch {
         return [document.createTextNode(text)];
      }
   } else {
      const lower = text.toLowerCase();
      const ql = query.toLowerCase();
      const idx = lower.indexOf(ql);
      if (idx === -1) return [document.createTextNode(text)];
      parts = [];
      if (idx > 0) parts.push(document.createTextNode(text.slice(0, idx)));
      const mark = document.createElement("mark");
      mark.textContent = text.slice(idx, idx + query.length);
      parts.push(mark);
      if (idx + query.length < text.length) {
         parts.push(document.createTextNode(text.slice(idx + query.length)));
      }
      return parts;
   }
}

function renderResults(items) {
   const container = document.querySelector("[data-results]");
   if (!container) return;
   container.replaceChildren();
   if (items.length === 0) {
      const p = document.createElement("p");
      p.textContent = "No matching commands.";
      p.style.color = "var(--text-dim)";
      p.style.fontSize = "0.85rem";
      container.appendChild(p);
      return;
   }
   const table = document.createElement("table");
   table.className = "cmd-table";
   const thead = document.createElement("thead");
   const hr = document.createElement("tr");
   for (const h of ["Command", "User", "Activity", "Category", "Tags", "First seen", "Last seen", "Nodes", "Count"]) {
      const th = document.createElement("th");
      th.textContent = h;
      hr.appendChild(th);
   }
   thead.appendChild(hr);
   table.appendChild(thead);
   const tbody = document.createElement("tbody");
   const isRegex = getMode() === "regex";
   for (const it of items) {
      const tr = document.createElement("tr");
      // cmdline with highlight as text nodes
      const tdCmd = document.createElement("td");
      tdCmd.className = "cmdline-cell";
      const nodes = highlightMatch(it.cmdline || "", currentQuery, isRegex);
      for (const n of nodes) tdCmd.appendChild(n);
      tr.appendChild(tdCmd);

      const tdUser = document.createElement("td");
      tdUser.textContent = it.username || "";
      tr.appendChild(tdUser);

      const tdAct = document.createElement("td");
      tdAct.textContent = it.activity || "";
      tr.appendChild(tdAct);

      const tdCat = document.createElement("td");
      tdCat.textContent = it.category || "";
      tr.appendChild(tdCat);

      const tdTags = document.createElement("td");
      tdTags.textContent = (it.tags || []).join(", ");
      tr.appendChild(tdTags);

      const tdFirst = document.createElement("td");
      tdFirst.textContent = it.first_seen || "";
      tr.appendChild(tdFirst);

      const tdLast = document.createElement("td");
      tdLast.textContent = it.last_seen || "";
      tr.appendChild(tdLast);

      const tdNodes = document.createElement("td");
      tdNodes.textContent = (it.nodes || []).join(", ");
      tr.appendChild(tdNodes);

      const tdCount = document.createElement("td");
      tdCount.textContent = String(it.sample_count != null ? it.sample_count : "");
      tr.appendChild(tdCount);

      tbody.appendChild(tr);
   }
   table.appendChild(tbody);
   container.appendChild(table);
}

function init() {
   const input = document.querySelector("[data-search-input]");
   const btn = document.querySelector("[data-search-btn]");
   const userIn = document.querySelector("[data-filter-user]");
   const actIn = document.querySelector("[data-filter-activity]");
   const tagIn = document.querySelector("[data-filter-tag]");
   const prevBtn = document.querySelector("[data-prev]");
   const nextBtn = document.querySelector("[data-next]");

   function readInputs() {
      currentQuery = input ? input.value.trim() : "";
      currentMode = getMode();
      currentFilters = {
         user: userIn ? userIn.value.trim() : "",
         activity: actIn ? actIn.value.trim() : "",
         tag: tagIn ? tagIn.value.trim() : "",
      };
   }

   function searchFromStart() {
      readInputs();
      currentOffset = 0;
      doSearch();
   }

   if (btn) btn.addEventListener("click", searchFromStart);
   if (input) input.addEventListener("keydown", (e) => {
      if (e.key === "Enter") searchFromStart();
   });
   if (prevBtn) prevBtn.addEventListener("click", () => {
      if (currentOffset >= PAGE_SIZE) {
         currentOffset -= PAGE_SIZE;
         doSearch();
      }
   });
   if (nextBtn) nextBtn.addEventListener("click", () => {
      if (currentOffset + PAGE_SIZE < totalResults) {
         currentOffset += PAGE_SIZE;
         doSearch();
      }
   });
   // Initial load: show all (no filter) first page
   doSearch();
}

if (document.readyState === "loading") {
   document.addEventListener("DOMContentLoaded", init);
} else {
   init();
}
