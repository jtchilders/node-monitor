/**
 * ProcessModal — fetches /api/process/{system}/{node}/{pid}/{start_time} and renders.
 *
 * Usage: ProcessModal.open({system, node, pid, start_time})
 * The modal renders redacted cmdline (as stored), parent chain, classification badges,
 * live row details, and handles 404 with a clear message. All user text via textContent.
 */

export const ProcessModal = {
   _overlay: null,

   _ensureOverlay() {
      let overlay = document.getElementById("process-modal-overlay");
      if (overlay) {
         this._overlay = overlay;
         return overlay;
      }
      overlay = document.createElement("div");
      overlay.id = "process-modal-overlay";
      overlay.style.cssText = "position:fixed;inset:0;z-index:2000;display:none;align-items:center;justify-content:center;background:rgba(0,0,0,0.5);";
      overlay.setAttribute("role", "dialog");
      overlay.setAttribute("aria-modal", "true");

      const box = document.createElement("div");
      box.id = "process-modal-box";
      box.style.cssText = "background:#fff;color:#111827;max-width:700px;max-height:80vh;overflow:auto;border-radius:8px;padding:20px;position:relative;min-width:320px;";
      overlay.appendChild(box);

      const closeBtn = document.createElement("button");
      closeBtn.textContent = "Close";
      closeBtn.setAttribute("aria-label", "Close modal");
      closeBtn.style.cssText = "position:absolute;top:8px;right:8px;";
      closeBtn.addEventListener("click", () => this.close());
      box.appendChild(closeBtn);

      const content = document.createElement("div");
      content.id = "process-modal-content";
      box.appendChild(content);

      overlay.addEventListener("click", (e) => {
         if (e.target === overlay) this.close();
      });

      document.body.appendChild(overlay);
      this._overlay = overlay;
      return overlay;
   },

   close() {
      if (this._overlay) this._overlay.style.display = "none";
   },

   _setContent(nodes) {
      const container = document.getElementById("process-modal-content");
      if (!container) return;
      container.textContent = "";
      for (const n of nodes) container.appendChild(n);
   },

   _textRow(label, value) {
      const row = document.createElement("div");
      row.className = "modal-row";
      const labelEl = document.createElement("strong");
      labelEl.textContent = label + ": ";
      row.appendChild(labelEl);
      const valEl = document.createElement("span");
      // Use textContent to render literal text, not HTML. This handles [bold] and ANSI escapes safely.
      valEl.textContent = value == null ? "-" : String(value);
      row.appendChild(valEl);
      return row;
   },

   _badge(text, color) {
      const span = document.createElement("span");
      span.className = "badge";
      span.textContent = text;
      span.style.cssText = "display:inline-block;padding:2px 6px;border-radius:4px;font-size:12px;margin-right:4px;background:" + (color || "#e5e7eb") + ";color:#111827;";
      return span;
   },

   async open({ system, node, pid, start_time }) {
      this._ensureOverlay();
      this._overlay.style.display = "flex";
      const box = document.getElementById("process-modal-content");
      if (box) {
         box.textContent = "";
         const loading = document.createElement("div");
         loading.textContent = "Loading\u2026";
         box.appendChild(loading);
      }

      const url = "/api/process/" + encodeURIComponent(system) + "/" + encodeURIComponent(node) + "/" + encodeURIComponent(pid) + "/" + encodeURIComponent(start_time);

      try {
         const resp = await fetch(url);
         if (resp.status === 404) {
            let detail = "Process not found";
            try {
               const body = await resp.json();
               if (body.detail) detail = body.detail;
            } catch (_) {}
            const err = document.createElement("div");
            err.className = "modal-error";
            // 404 message rendered as text, not HTML
            err.textContent = "404 Not Found: " + detail;
            this._setContent([err]);
            return;
         }
         if (!resp.ok) {
            const err = document.createElement("div");
            err.textContent = "Error " + resp.status + ": failed to load process";
            this._setContent([err]);
            return;
         }
         const data = await resp.json();
         this._renderProcess(data);
      } catch (e) {
         const err = document.createElement("div");
         err.textContent = "Failed to fetch process: " + (e && e.message ? e.message : String(e));
         this._setContent([err]);
      }
   },

   _renderProcess(data) {
      const nodes = [];

      const title = document.createElement("h3");
      title.textContent = "Process " + data.pid + " on " + (data.label || data.node);
      nodes.push(title);

      // Classification badges
      const badges = document.createElement("div");
      badges.className = "modal-badges";
      if (data.category) badges.appendChild(this._badge(data.category, "#dbeafe"));
      if (data.activity) badges.appendChild(this._badge(data.activity, "#fef3c7"));
      if (data.cpu_state) badges.appendChild(this._badge(data.cpu_state, "#dcfce7"));
      nodes.push(badges);

      // Live row details
      const details = document.createElement("div");
      details.className = "modal-details";
      details.appendChild(this._textRow("User", data.username));
      details.appendChild(this._textRow("PID", data.pid));
      details.appendChild(this._textRow("Start time", data.start_time));
      details.appendChild(this._textRow("Category", data.category));
      details.appendChild(this._textRow("Activity", data.activity));
      details.appendChild(this._textRow("State", data.state));
      details.appendChild(this._textRow("RSS", data.rss_kb != null ? (data.rss_kb + " KiB") : "-"));
      details.appendChild(this._textRow("CPU seconds", data.cpu_seconds));
      details.appendChild(this._textRow("Threads", data.threads));
      details.appendChild(this._textRow("Presence", data.owner_presence));
      details.appendChild(this._textRow("Idle", data.idle_for_sec != null ? data.idle_for_sec + "s" + (data.idle_for_is_lower_bound ? " (\u2265)" : "") : "-"));
      nodes.push(details);

      // Redacted cmdline — literal text via textContent
      const cmdSection = document.createElement("div");
      cmdSection.className = "modal-cmdline";
      const cmdLabel = document.createElement("strong");
      cmdLabel.textContent = "Command line:";
      cmdSection.appendChild(cmdLabel);
      const cmdPre = document.createElement("pre");
      cmdPre.style.cssText = "white-space:pre-wrap;word-break:break-all;background:#f3f4f6;padding:8px;border-radius:4px;margin-top:4px;";
      cmdPre.textContent = data.cmdline || "-";
      cmdSection.appendChild(cmdPre);
      nodes.push(cmdSection);

      // Parent chain
      const parentSection = document.createElement("div");
      parentSection.className = "modal-parents";
      const parentLabel = document.createElement("strong");
      parentLabel.textContent = "Parent chain:";
      parentSection.appendChild(parentLabel);
      if (data.parent_chain && data.parent_chain.length > 0) {
         const list = document.createElement("ul");
         for (const anc of data.parent_chain) {
            const li = document.createElement("li");
            // Each ancestor rendered as text
            let line = "pid " + (anc.pid != null ? anc.pid : "?");
            if (anc.comm) line += " (" + anc.comm + ")";
            if (anc.username) line += " user=" + anc.username;
            if (anc.category) line += " [" + anc.category + "]";
            li.textContent = line;
            list.appendChild(li);
         }
         parentSection.appendChild(list);
      } else if (data.parent_comm || data.ppid != null) {
         const p = document.createElement("div");
         let line = "";
         if (data.ppid != null) line += "ppid " + data.ppid + " ";
         if (data.parent_comm) line += data.parent_comm + " ";
         if (data.parent_kind) line += "(" + data.parent_kind + ")";
         p.textContent = line.trim() || "-";
         parentSection.appendChild(p);
      } else {
         const p = document.createElement("div");
         p.textContent = "-";
         parentSection.appendChild(p);
      }
      nodes.push(parentSection);

      this._setContent(nodes);
   },
};

export default ProcessModal;
