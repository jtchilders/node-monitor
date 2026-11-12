export const PALETTE = ["#8b5cf6","#06b6d4","#f59e0b","#ef4444","#3b82f6","#10b981","#ec4899","#14b8a6","#f97316","#a78bfa","#22c55e","#e879f9"];

export const CATEGORY_COLORS = {ai_agent:"#8b5cf6",editor_server:"#06b6d4",jupyter:"#f59e0b",build:"#ef4444",python:"#3b82f6",shell:"#6b7280",data_transfer:"#10b981",fs_scan:"#a3a3a3",scheduler:"#ec4899",workflow:"#14b8a6",system:"#52525b",other:"#9ca3af"};

export const GROUP_DEFS = {category:{label:"Category",key:(p)=>p.category||"other"},activity:{label:"Activity",key:(p)=>p.activity||"other"},user:{label:"User",key:(p)=>p.username||"unknown"},node:{label:"Node",key:(p)=>p.label||p.node||"unknown"},presence:{label:"Presence",key:(p)=>p.owner_presence||"unknown"}};

const MAX_SLICES = 12;
const MAX_BARS = 12;

function djb2(s) {
   let h = 5381;
   for (let i = 0; i < s.length; i++) h = ((h << 5) + h + s.charCodeAt(i)) >>> 0;
   return h;
}

export function colorFor(group, key, index) {
   if (group === "category") return CATEGORY_COLORS[key] || "#9ca3af";
   return PALETTE[djb2(String(key)) % PALETTE.length];
}

export function aggregateForGroup(processes, group) {
   if (!processes || processes.length === 0) return {groups:[],total:0,capped:false};
   const def = GROUP_DEFS[group];
   const keyFn = def ? def.key : (p) => p[group] || "unknown";
   const buckets = new Map();
   for (const p of processes) {
      const k = keyFn(p);
      let b = buckets.get(k);
      if (!b) { b = {key:k,count:0,rss:0,cpu:0}; buckets.set(k, b); }
      b.count += 1;
      b.rss += p.rss_kb || 0;
      b.cpu += p.cpu_seconds || 0;
   }
   let groups = [...buckets.values()].sort((a,b) => b.count - a.count || b.rss - a.rss);
   const total = processes.length;
   const maxItems = MAX_SLICES;
   let capped = false;
   if (groups.length > maxItems) {
      capped = true;
      const head = groups.slice(0, maxItems - 1);
      const tail = groups.slice(maxItems - 1);
      const other = {key:"Other",count:0,rss:0,cpu:0};
      for (const g of tail) { other.count += g.count; other.rss += g.rss; other.cpu += g.cpu; }
      groups = [...head, other];
   }
   void MAX_BARS;
   return {groups, total, capped};
}

export function buildPieDataset(agg, group) {
   const labels = agg.groups.map((g) => String(g.key));
   const data = agg.groups.map((g) => g.count);
   const backgroundColor = agg.groups.map((g, i) => colorFor(group, g.key, i));
   return {labels, data, backgroundColor};
}

export function buildBarDatasets(agg, group) {
   const labels = agg.groups.map((g) => String(g.key));
   const rssData = agg.groups.map((g) => g.rss);
   const cpuData = agg.groups.map((g) => g.cpu);
   const backgroundColor = agg.groups.map((g, i) => colorFor(group, g.key, i));
   return {labels, rssData, cpuData, backgroundColor};
}
