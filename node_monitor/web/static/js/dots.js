/**
 * DotsCanvas — one canvas per node, dots colored by CATEGORY_COLORS, sized by rss_kb.
 *
 * Usage: new DotsCanvas(el, {processes, colorBy: 'category', sizeBy: 'rss', onHover, onClick})
 *
 * - el is a container element; DotsCanvas creates per-node sections with a canvas each.
 * - processes is an array of live process rows (must include username, category, activity,
 *   cmdline, rss_kb, idle_for_sec, idle_for_is_lower_bound, owner_presence, pid, node, label).
 * - colorBy: 'category' (default) — future hooks for other modes.
 * - sizeBy: 'rss' (default) — log scale on rss_kb.
 * - onHover(payload|null) — payload contains username, activity, cmdline, idle, presence, etc.
 * - onClick(process) — clicked process row.
 *
 * No extra deps; 2D canvas only. Safe: all user text via textContent.
 */

export const CATEGORY_COLORS = {
   "ai_agent": "#8b5cf6",
   "editor_server": "#06b6d4",
   "jupyter": "#f59e0b",
   "build": "#ef4444",
   "python": "#3b82f6",
   "shell": "#6b7280",
   "data_transfer": "#10b981",
   "fs_scan": "#a3a3a3",
   "scheduler": "#ec4899",
   "workflow": "#14b8a6",
   "system": "#52525b",
   "other": "#9ca3af",
};

const FALLBACK_COLOR = CATEGORY_COLORS["other"];

// log scale: rss_kb 1..16GiB (16777216 KiB) -> radius 2..10 px
const MIN_RSS_KB = 1;
const MAX_RSS_KB = 16 * 1024 * 1024;
const MIN_RADIUS = 2;
const MAX_RADIUS = 10;

function radiusForRss(rssKb) {
   if (rssKb == null || rssKb <= 0) return MIN_RADIUS;
   const clamped = Math.min(Math.max(rssKb, MIN_RSS_KB), MAX_RSS_KB);
   const logMin = Math.log(MIN_RSS_KB + 1);
   const logMax = Math.log(MAX_RSS_KB + 1);
   const logVal = Math.log(clamped + 1);
   const t = (logVal - logMin) / (logMax - logMin);
   return MIN_RADIUS + t * (MAX_RADIUS - MIN_RADIUS);
}

function debounce(fn, ms) {
   let timer = null;
   return function (...args) {
      clearTimeout(timer);
      timer = setTimeout(() => fn.apply(this, args), ms);
   };
}

function formatIdle(row) {
   const sec = row.idle_for_sec;
   if (sec == null) return "-";
   const lower = row.idle_for_is_lower_bound ? "\u2265" : "";
   const s = Math.max(0, Math.floor(sec));
   if (s < 60) return lower + s + "s";
   if (s < 3600) return lower + Math.floor(s / 60) + "m";
   if (s < 86400) return lower + Math.floor(s / 3600) + "h" + String(Math.floor((s % 3600) / 60)).padStart(2, "0") + "m";
   return lower + Math.floor(s / 86400) + "d";
}

function groupByNode(processes) {
   const map = new Map();
   for (const p of processes) {
      const key = p.label || p.node || "unknown";
      if (!map.has(key)) map.set(key, []);
      map.get(key).push(p);
   }
   return map;
}

export class DotsCanvas {
   constructor(el, opts = {}) {
      this.el = el;
      this.processes = opts.processes || [];
      this.colorBy = opts.colorBy || "category";
      this.sizeBy = opts.sizeBy || "rss";
      this.onHover = opts.onHover || null;
      this.onClick = opts.onClick || null;
      this._canvases = [];
      this._tooltip = null;
      this._dots = []; // {x,y,r,process,canvas}
      this._hovered = null;
      this._boundResize = debounce(() => this.render(), 150);
      this._initTooltip();
      window.addEventListener("resize", this._boundResize);
      this.render();
   }

   _initTooltip() {
      let tip = document.getElementById("dots-tooltip");
      if (!tip) {
         tip = document.createElement("div");
         tip.id = "dots-tooltip";
         tip.setAttribute("role", "tooltip");
         tip.style.cssText = "position:fixed;display:none;z-index:1000;background:#1f2937;color:#f9fafb;padding:6px 8px;border-radius:6px;font-size:12px;max-width:320px;pointer-events:none;white-space:pre-wrap;word-break:break-all;";
         document.body.appendChild(tip);
      }
      this._tooltip = tip;
   }

   _showTooltip(process, x, y) {
      const tip = this._tooltip;
      if (!tip) return;
      // Clear previous content safely via textContent
      tip.textContent = "";
      // Build tooltip with safe text nodes
      const lines = [];
      const userEl = document.createElement("div");
      userEl.textContent = "user: " + (process.username || "-");
      tip.appendChild(userEl);
      const actEl = document.createElement("div");
      actEl.textContent = "activity: " + (process.activity || "-");
      tip.appendChild(actEl);
      const catEl = document.createElement("div");
      catEl.textContent = "category: " + (process.category || "-");
      tip.appendChild(catEl);
      const cmdEl = document.createElement("div");
      const snippet = (process.cmdline || "").slice(0, 120);
      cmdEl.textContent = "cmdline: " + (snippet || "-");
      tip.appendChild(cmdEl);
      const idleEl = document.createElement("div");
      idleEl.textContent = "idle: " + formatIdle(process);
      // expose as 'idle' for hover payload test
      tip.appendChild(idleEl);
      const presEl = document.createElement("div");
      presEl.textContent = "presence: " + (process.owner_presence || process.presence || "-");
      tip.appendChild(presEl);

      tip.style.display = "block";
      // Position near cursor, keep on screen
      const pad = 12;
      tip.style.left = (x + pad) + "px";
      tip.style.top = (y + pad) + "px";

      // Also fire onHover callback with required fields
      if (this.onHover) {
         this.onHover({
            username: process.username,
            user: process.username,
            activity: process.activity,
            cmdline: process.cmdline,
            idle: formatIdle(process),
            idle_for_sec: process.idle_for_sec,
            presence: process.owner_presence || process.presence,
            owner_presence: process.owner_presence,
            category: process.category,
            process: process,
         });
      }
   }

   _hideTooltip() {
      if (this._tooltip) this._tooltip.style.display = "none";
      if (this.onHover) this.onHover(null);
   }

   setProcesses(processes) {
      this.processes = processes || [];
      this.render();
   }

   render() {
      // Clean previous canvases
      this.el.textContent = "";
      this._canvases = [];
      this._dots = [];

      if (!this.processes || this.processes.length === 0) {
         const empty = document.createElement("div");
         empty.className = "dots-empty";
         empty.textContent = "No processes to display";
         this.el.appendChild(empty);
         return;
      }

      const groups = groupByNode(this.processes);

      for (const [label, procs] of groups) {
         const section = document.createElement("div");
         section.className = "dots-node";

         const heading = document.createElement("div");
         heading.className = "dots-node-label";
         heading.textContent = label + " (" + procs.length + ")";
         section.appendChild(heading);

         const canvas = document.createElement("canvas");
         // Use container width for canvas
         const width = this.el.clientWidth || 600;
         // Height: try to fit dots in grid-like packing
         const dotArea = Math.max(80, Math.ceil(Math.sqrt(procs.length)) * 20);
         canvas.width = width * (window.devicePixelRatio || 1);
         canvas.height = dotArea * (window.devicePixelRatio || 1);
         canvas.style.width = width + "px";
         canvas.style.height = dotArea + "px";
         canvas.setAttribute("aria-label", "Process dots for " + label);
         section.appendChild(canvas);
         this.el.appendChild(section);
         this._canvases.push(canvas);
         this._drawCanvas(canvas, procs);
      }
   }

   _drawCanvas(canvas, processes) {
      const dpr = window.devicePixelRatio || 1;
      const ctx = canvas.getContext("2d");
      if (!ctx) return;
      ctx.scale(dpr, dpr);
      const width = canvas.clientWidth || canvas.width / dpr;
      const height = canvas.clientHeight || canvas.height / dpr;
      ctx.clearRect(0, 0, width, height);

      // Simple grid packing: place dots in rows
      const cols = Math.max(1, Math.floor(width / 14));
      this._dotsForCanvas = [];

      processes.forEach((proc, idx) => {
         const col = idx % cols;
         const row = Math.floor(idx / cols);
         const x = col * 14 + 7;
         const y = row * 14 + 7;
         const r = this.sizeBy === "rss" ? radiusForRss(proc.rss_kb) : 4;
         const color = (this.colorBy === "category" && proc.category)
            ? (CATEGORY_COLORS[proc.category] || FALLBACK_COLOR)
            : FALLBACK_COLOR;
         ctx.beginPath();
         ctx.arc(x, y, r, 0, Math.PI * 2);
         ctx.fillStyle = color;
         ctx.fill();
         // Store for hit testing
         this._dots.push({ x, y, r, process: proc, canvas });
      });

      // Remove old listeners by cloning pattern: we track per-canvas listeners
      const onMove = (ev) => {
         const rect = canvas.getBoundingClientRect();
         const mx = ev.clientX - rect.left;
         const my = ev.clientY - rect.top;
         let hit = null;
         for (const dot of this._dots) {
            if (dot.canvas !== canvas) continue;
            const dx = mx - dot.x;
            const dy = my - dot.y;
            if (Math.sqrt(dx * dx + dy * dy) <= dot.r + 2) {
               hit = dot;
               break;
            }
         }
         if (hit) {
            canvas.style.cursor = "pointer";
            this._showTooltip(hit.process, ev.clientX, ev.clientY);
         } else {
            canvas.style.cursor = "default";
            this._hideTooltip();
         }
      };
      const onLeave = () => this._hideTooltip();
      const onClick = (ev) => {
         const rect = canvas.getBoundingClientRect();
         const mx = ev.clientX - rect.left;
         const my = ev.clientY - rect.top;
         for (const dot of this._dots) {
            if (dot.canvas !== canvas) continue;
            const dx = mx - dot.x;
            const dy = my - dot.y;
            if (Math.sqrt(dx * dx + dy * dy) <= dot.r + 2) {
               if (this.onClick) this.onClick(dot.process);
               break;
            }
         }
      };
      canvas.addEventListener("mousemove", onMove);
      canvas.addEventListener("mouseleave", onLeave);
      canvas.addEventListener("click", onClick);
   }

   destroy() {
      window.removeEventListener("resize", this._boundResize);
      this._hideTooltip();
      this.el.textContent = "";
      this._dots = [];
      this._canvases = [];
   }
}

// Also support factory function form: DotsCanvas(el, opts) without new
export default DotsCanvas;
