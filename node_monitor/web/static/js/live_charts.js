export const PALETTE = ["#8b5cf6","#06b6d4","#f59e0b","#ef4444","#3b82f6","#10b981","#ec4899","#14b8a6","#f97316","#a78bfa","#22c55e","#e879f9"];

export const CATEGORY_COLORS = {ai_agent:"#8b5cf6",editor_server:"#06b6d4",jupyter:"#f59e0b",build:"#ef4444",python:"#3b82f6",shell:"#6b7280",data_transfer:"#10b981",fs_scan:"#a3a3a3",scheduler:"#ec4899",workflow:"#14b8a6",system:"#52525b",other:"#9ca3af"};

export const GROUP_DEFS = {category:{label:"Category",key:(p)=>p.category||"other"},activity:{label:"Activity",key:(p)=>p.activity||"other"},user:{label:"User",key:(p)=>p.username||"unknown"},node:{label:"Node",key:(p)=>p.label||p.node||"unknown"},presence:{label:"Presence",key:(p)=>p.owner_presence||"unknown"}};

const MAX_SLICES = 15;
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

export function aggregateForGroup(processes, group, weightKey) {
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
   let groups;
   if (weightKey === "rss") groups = [...buckets.values()].sort((a,b) => b.rss - a.rss || b.count - a.count);
   else if (weightKey === "cpu") groups = [...buckets.values()].sort((a,b) => b.cpu - a.cpu || b.count - a.count);
   else groups = [...buckets.values()].sort((a,b) => b.count - a.count || b.rss - a.rss);
   const total = processes.length;
   const maxItems = MAX_SLICES;
   let capped = false;
   if (groups.length > maxItems) {
      capped = true;
      const head = groups.slice(0, maxItems - 1);
      const tail = groups.slice(maxItems - 1);
      // If head already contains an "other" bucket (natural group), merge tail into it
      // to avoid duplicate "other" vs "Other" slices (e.g. activity grouping)
      const existingOther = head.find((g) => String(g.key).toLowerCase() === "other");
      if (existingOther) {
         for (const g of tail) { existingOther.count += g.count; existingOther.rss += g.rss; existingOther.cpu += g.cpu; }
         existingOther.groupCount = (existingOther.groupCount || 0) + tail.length;
         groups = head;
      } else {
         const other = {key:"Other",count:0,rss:0,cpu:0,groupCount:tail.length};
         for (const g of tail) { other.count += g.count; other.rss += g.rss; other.cpu += g.cpu; }
         groups = [...head, other];
      }
   }
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

function otherTooltipLabel(label, value, total, agg) {
   if (label === "Other" && agg && agg.capped) {
      const otherEntry = agg.groups.find((g) => g.key === "Other");
      const groupCount = (otherEntry && otherEntry.groupCount) || (agg.total - (agg.groups.length - 1));
      const rssStr = fmtRss(otherEntry ? otherEntry.rss : 0);
      const cpuStr = String(Math.round(otherEntry ? otherEntry.cpu : 0)) + "s";
      return "Other (" + String(groupCount) + " groups): " + String(value) + " (" + ((value / total) * 100).toFixed(1) + "%) rss " + rssStr + " cpu " + cpuStr;
   }
   return label + ": " + String(value) + " (" + ((value / total) * 100).toFixed(1) + "%)";
}

function otherBarTooltipLabel(label, val, which, agg) {
   if (label === "Other" && agg && agg.capped) {
      const otherEntry = agg.groups.find((g) => g.key === "Other");
      const groupCount = (otherEntry && otherEntry.groupCount) || 0;
      const rssStr = fmtRss(otherEntry ? otherEntry.rss : 0);
      const cpuStr = String(Math.round(otherEntry ? otherEntry.cpu : 0)) + "s";
      let formatted;
      if (which === "rss") formatted = fmtRss(val);
      else formatted = String(Math.round(val)) + "s";
      return "Other (" + String(groupCount) + " groups): " + formatted + " rss " + rssStr + " cpu " + cpuStr;
   }
   let formatted;
   if (which === "rss") formatted = fmtRss(val);
   else formatted = String(Math.round(val)) + "s";
   return label + ": " + formatted;
}

export function createPieChart(canvasEl, agg, group, onSliceClick) {
   const ds = buildPieDataset(agg, group);
   const total = agg.total || ds.data.reduce((a, b) => a + b, 0) || 1;
   const empty = !agg.groups || agg.groups.length === 0;
   const pieCanvas = document.getElementById("pie-canvas");
   const pieEmpty = document.getElementById("pie-empty");
   if (empty) {
      if (pieCanvas) pieCanvas.hidden = true;
      if (pieEmpty) pieEmpty.hidden = false;
   } else {
      if (pieCanvas) pieCanvas.hidden = false;
      if (pieEmpty) pieEmpty.hidden = true;
   }
   const ChartCtor = window.Chart;
   const chart = new ChartCtor(canvasEl, {
      type: "doughnut",
      data: {
         labels: ds.labels,
         datasets: [{data: ds.data, backgroundColor: ds.backgroundColor}],
      },
      options: {
         responsive: true,
         cutout: "55%",
         onClick(_evt, elements) {
            if (!onSliceClick || !elements || elements.length === 0) return;
            const idx = elements[0].index;
            const curAgg = chart._nmAgg || agg;
            const key = (curAgg.groups[idx] && curAgg.groups[idx].key) || (chart.data.labels[idx]);
            onSliceClick(String(key));
         },
         plugins: {
            legend: {
               position: "right",
               labels: {
                  color: "#e2e8f0",
                  generateLabels(chartInstance) {
                     const ds0 = chartInstance.data.datasets[0] || {data: []};
                     const lbs = chartInstance.data.labels || [];
                     return lbs.map((label, i) => ({
                        text: String(label),
                        fillStyle: (ds0.backgroundColor && ds0.backgroundColor[i]) || "#9ca3af",
                        fontColor: "#e2e8f0",
                        strokeStyle: (ds0.backgroundColor && ds0.backgroundColor[i]) || "#9ca3af",
                        hidden: false,
                        index: i,
                     }));
                  },
               },
            },
            tooltip: {
               callbacks: {
                  label(ctx) {
                     const label = String(ctx.label || "");
                     const value = ctx.parsed;
                     return otherTooltipLabel(label, value, total, agg);
                  },
               },
            },
         },
      },
   });
   // keep agg reference for update
   chart._nmAgg = agg;
   chart._nmOnSliceClick = onSliceClick;
   return chart;
}

export function buildResourcePieDataset(agg, group, weightKey) {
   if (!agg || !agg.groups || agg.groups.length === 0) return {labels: [], data: [], backgroundColor: []};
   const labels = agg.groups.map((g) => String(g.key));
   const data = agg.groups.map((g) => {
      if (weightKey === "rss") return g.rss || 0;
      if (weightKey === "cpu") return g.cpu || 0;
      return g.count || 0;
   });
   const backgroundColor = agg.groups.map((g, i) => colorFor(group, g.key, i));
   return {labels, data, backgroundColor};
}

export function formatResourceLabel(totalUsed, totalAvailable, kind) {
   if (kind === "memory") {
      const usedStr = fmtRss(totalUsed || 0);
      const availStr = fmtRss(totalAvailable || 0);
      let pct = 0;
      if (totalAvailable && totalAvailable > 0) pct = Math.round((totalUsed / totalAvailable) * 100);
      return usedStr + " used of " + availStr + " (" + String(pct) + "%)";
   }
   if (kind === "cpu") {
      const secs = Math.round(totalUsed || 0);
      return String(secs) + "s CPU";
   }
   return String(totalUsed || 0);
}

export function createResourcePieChart(canvasEl, agg, group, weightKey, onSliceClick) {
   const ds = buildResourcePieDataset(agg, group, weightKey);
   const empty = !agg.groups || agg.groups.length === 0;
   // hide/show handled by caller via rss/cpu empty divs, but also hide canvas if empty
   const ChartCtor = window.Chart;
   const chart = new ChartCtor(canvasEl, {
      type: "doughnut",
      data: {labels: ds.labels, datasets: [{data: ds.data, backgroundColor: ds.backgroundColor}]},
      options: {
         responsive: true,
         cutout: "55%",
         onClick(_evt, elements) {
            if (!onSliceClick || !elements || elements.length === 0) return;
            const idx = elements[0].index;
            const curAgg = chart._nmAgg || agg;
            const key = (curAgg.groups[idx] && curAgg.groups[idx].key) || chart.data.labels[idx];
            onSliceClick(String(key));
         },
         plugins: {
            legend: {position: "right", labels: {color: "#e2e8f0", generateLabels(chartInstance) {
               const ds0 = chartInstance.data.datasets[0] || {data: []};
               const lbs = chartInstance.data.labels || [];
               return lbs.map((label, i) => ({text: String(label), fillStyle: (ds0.backgroundColor && ds0.backgroundColor[i]) || "#9ca3af", fontColor: "#e2e8f0", strokeStyle: (ds0.backgroundColor && ds0.backgroundColor[i]) || "#9ca3af", hidden: false, index: i}));
            }}},
            tooltip: {callbacks: {label(ctx) {
               const label = String(ctx.label || "");
               const value = ctx.parsed;
               const curAgg = chart._nmAgg || agg;
               const total = ds.data.reduce((a,b) => a+b, 0) || 1;
               // enrich Other tooltip if capped
               if (label === "Other" && curAgg && curAgg.capped) {
                  const otherEntry = curAgg.groups.find((g) => g.key === "Other");
                  const groupCount = (otherEntry && otherEntry.groupCount) || 0;
                  return "Other (" + String(groupCount) + " groups): " + String(value) + " (" + ((value/total)*100).toFixed(1) + "%)";
               }
               return label + ": " + String(value) + " (" + ((value/total)*100).toFixed(1) + "%)";
            }}},
         },
      },
   });
   chart._nmAgg = agg;
   chart._nmOnSliceClick = onSliceClick;
   chart._nmWeightKey = weightKey;
   return chart;
}

export function updateResourcePieChart(chart, agg, group, weightKey) {
   const ds = buildResourcePieDataset(agg, group, weightKey);
   chart._nmAgg = agg;
   chart._nmWeightKey = weightKey;
   chart.data.labels = ds.labels;
   chart.data.datasets[0].data = ds.data;
   chart.data.datasets[0].backgroundColor = ds.backgroundColor;
   chart.update();
}

export function fmtRss(kb) {
   if (kb == null) return "-";
   if (kb < 1024) return kb + " KiB";
   const mib = kb / 1024;
   if (mib < 1024) return mib.toFixed(1) + " MiB";
   return (mib / 1024).toFixed(1) + " GiB";
}

export function createBarChart(canvasEl, which, onSliceClick) {
   const ChartCtor = window.Chart;
   const chart = new ChartCtor(canvasEl, {
      type: "bar",
      data: {
         labels: [],
         datasets: [{data: [], backgroundColor: [], borderWidth: 1, borderColor: "#1e293b"}],
      },
      options: {
         responsive: true,
         indexAxis: "y",
         scales: {
            x: {
               beginAtZero: true,
               ticks: {
                  callback(value) {
                     if (which === "rss") return fmtRss(value);
                     return String(Math.round(value)) + "s";
                  },
               },
            },
            y: {
               ticks: {
                  callback(value, index) {
                     const labels = (this.chart && this.chart.data && this.chart.data.labels) || [];
                     const lbl = String(labels[index] ?? value);
                     return lbl.length > 14 ? lbl.slice(0, 14) + "…" : lbl;
                  },
               },
            },
         },
         plugins: {
            legend: {display: false},
            tooltip: {
               callbacks: {
                  label(ctx) {
                     const curChart = ctx.chart;
                     const curAgg = curChart && curChart._nmAgg;
                     const label = String(ctx.label || "");
                     const val = ctx.parsed.x;
                     if (curAgg) return otherBarTooltipLabel(label, val, which, curAgg);
                     let formatted;
                     if (which === "rss") formatted = fmtRss(val);
                     else formatted = String(Math.round(val)) + "s";
                     return label + ": " + formatted;
                  },
               },
            },
         },
      },
   });
   chart._nmAgg = null;
   if (onSliceClick) {
      chart.options.onClick = function(_evt, elements) {
         if (!elements || elements.length === 0) return;
         const idx = elements[0].index;
         const ch = this; // Chart instance
         const key = ch.data.labels[idx];
         onSliceClick(String(key));
      };
   }
   return chart;
}

export function updateBarChart(chart, agg, group, which) {
   const empty = !agg.groups || agg.groups.length === 0;
   const rssCanvas = document.getElementById("rss-canvas");
   const rssEmpty = document.getElementById("rss-empty");
   const cpuCanvas = document.getElementById("cpu-canvas");
   const cpuEmpty = document.getElementById("cpu-empty");
   let targetCanvas, targetEmpty;
   if (which === "rss") { targetCanvas = rssCanvas; targetEmpty = rssEmpty; }
   else { targetCanvas = cpuCanvas; targetEmpty = cpuEmpty; }
   if (empty) {
      if (targetCanvas) targetCanvas.hidden = true;
      if (targetEmpty) targetEmpty.hidden = false;
   } else {
      if (targetCanvas) targetCanvas.hidden = false;
      if (targetEmpty) targetEmpty.hidden = true;
   }
   const labels = agg.groups.map((g) => String(g.key));
   const data = agg.groups.map((g) => (which === "rss" ? g.rss : g.cpu));
   const backgroundColor = agg.groups.map((g, i) => colorFor(group, g.key, i));
   chart._nmAgg = agg;
   if (chart.options.plugins && chart.options.plugins.tooltip && chart.options.plugins.tooltip.callbacks) {
      chart.options.plugins.tooltip.callbacks.label = function (ctx) {
         const label = String(ctx.label || "");
         const val = ctx.parsed.x;
         return otherBarTooltipLabel(label, val, which, agg);
      };
   }
   chart.data.labels = labels;
   chart.data.datasets[0].data = data;
   chart.data.datasets[0].backgroundColor = backgroundColor;
   chart.update();
}

export function updatePieChart(chart, agg, group) {
   const ds = buildPieDataset(agg, group);
   const total = agg.total || ds.data.reduce((a, b) => a + b, 0) || 1;
   const empty = !agg.groups || agg.groups.length === 0;
   const pieCanvas = document.getElementById("pie-canvas");
   const pieEmpty = document.getElementById("pie-empty");
   if (empty) {
      if (pieCanvas) pieCanvas.hidden = true;
      if (pieEmpty) pieEmpty.hidden = false;
   } else {
      if (pieCanvas) pieCanvas.hidden = false;
      if (pieEmpty) pieEmpty.hidden = true;
   }
   if (chart.options.plugins && chart.options.plugins.tooltip && chart.options.plugins.tooltip.callbacks) {
      chart.options.plugins.tooltip.callbacks.label = function (ctx) {
         const label = String(ctx.label || "");
         const value = ctx.parsed;
         return otherTooltipLabel(label, value, total, agg);
      };
   }
   chart._nmAgg = agg;
   chart.data.labels = ds.labels;
   chart.data.datasets[0].data = ds.data;
   chart.data.datasets[0].backgroundColor = ds.backgroundColor;
   chart.update();
}

export function aggregateByNode(processes, sortBy) {
   if (!processes || processes.length === 0) return [];
   const buckets = new Map();
   for (const p of processes) {
      const k = p.label || p.node || "unknown";
      let b = buckets.get(k);
      if (!b) { b = {key: k, count: 0, rss: 0, cpu: 0}; buckets.set(k, b); }
      b.count += 1;
      b.rss += p.rss_kb || 0;
      b.cpu += p.cpu_seconds || 0;
   }
   let groups = [...buckets.values()];
   if (sortBy === "rss") groups.sort((a,b) => b.rss - a.rss || b.count - a.count);
   else if (sortBy === "cpu") groups.sort((a,b) => b.cpu - a.cpu || b.count - a.count);
   else groups.sort((a,b) => b.count - a.count || b.rss - a.rss);
   return groups;
}

export function buildNodeBarDataset(groups, metric) {
   const labels = groups.map((g) => String(g.key));
   const data = groups.map((g) => {
      if (metric === "rss") return g.rss || 0;
      if (metric === "cpu") return g.cpu || 0;
      return g.count || 0;
   });
   const backgroundColor = groups.map((g) => colorFor("node", g.key, 0));
   return {labels, data, backgroundColor};
}

export function createNodeBarChart(canvasEl, groups, metric, onBarClick) {
   const ds = buildNodeBarDataset(groups, metric);
   const ChartCtor = window.Chart;
   const chart = new ChartCtor(canvasEl, {
      type: "bar",
      data: {labels: ds.labels, datasets: [{data: ds.data, backgroundColor: ds.backgroundColor, borderWidth: 1, borderColor: "#1e293b"}]},
      options: {
         responsive: true,
         maintainAspectRatio: false,
         indexAxis: "y",
         scales: {
            x: {
               beginAtZero: true,
               ticks: {
                  color: "#94a3b8",
                  callback(value) {
                     if (metric === "rss") return fmtRss(value);
                     if (metric === "cpu") return String(Math.round(value)) + "s";
                     return String(value);
                  },
               },
               grid: {color: "#1e293b"},
            },
            y: {
               ticks: {
                  color: "#e2e8f0",
                  callback(value, index) {
                     const labels = (this.chart && this.chart.data && this.chart.data.labels) || [];
                     const lbl = String(labels[index] ?? value);
                     return lbl.length > 16 ? lbl.slice(0, 16) + "…" : lbl;
                  },
               },
               grid: {display: false},
            },
         },
         plugins: {
            legend: {display: false},
            tooltip: {
               callbacks: {
                  label(ctx) {
                     const label = String(ctx.label || "");
                     const val = ctx.parsed.x;
                     if (metric === "rss") return label + ": " + fmtRss(val);
                     if (metric === "cpu") return label + ": " + String(Math.round(val)) + "s";
                     return label + ": " + String(val);
                  },
               },
            },
         },
         onClick(_evt, elements) {
            if (!onBarClick || !elements || elements.length === 0) return;
            const ch = this; // Chart instance at click time
            const key = (ch.data && ch.data.labels ? ch.data.labels[elements[0].index] : elements[0].index);
            onBarClick(String(key));
         },
      },
   });
   chart._nmGroups = groups;
   chart._nmMetric = metric;
   return chart;
}

export function updateNodeBarChart(chart, groups, metric) {
   const ds = buildNodeBarDataset(groups, metric);
   chart._nmGroups = groups;
   chart._nmMetric = metric;
   chart.data.labels = ds.labels;
   chart.data.datasets[0].data = ds.data;
   chart.data.datasets[0].backgroundColor = ds.backgroundColor;
   chart.update();
}
