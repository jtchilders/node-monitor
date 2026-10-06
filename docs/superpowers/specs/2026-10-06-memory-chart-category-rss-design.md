# Memory Chart Category RSS Design

## Goal

Update the **Memory (physical)** chart to show system-used physical memory as a one-minute line with points, total node memory as a dotted reference line, and process-classification p50 RSS as separate 15-minute overlays. Remove the available-memory series.

## Measurement semantics

- **System used** is `MemTotal - MemAvailable`, sampled from counter windows. It includes kernel and cache use.
- **Total memory** comes from node hardware and is repeated across counter timestamps solely as a visual capacity reference.
- **Category RSS p50** comes from closed 15-minute `node_usage_intervals` grains grouped by the existing process `category` classification.
- Category RSS is non-additive: RSS can double-count shared pages and does not account for kernel/cache memory. Category lines must not be stacked, summed, or described as decomposing system-used memory.
- When multiple activity grains exist for one category and interval, choose the largest p50 RSS grain. This preserves the existing hotspot semantics and avoids mathematically invalid addition of percentile values.
- With a username filter active, category lines reflect that filtered username; otherwise they show the per-category hotspot across usernames selected by the service.

## Presentation

- Retain the existing GiB/Percent toggle.
- Render **System used** as a solid line with visible circular points.
- Render **Total memory** as a neutral dotted line without points.
- Remove **Available** entirely.
- Render one distinct-colored line per observed category, labeled `<category> RSS p50 (non-additive)`.
- Keep counter gaps as nulls and do not interpolate across them.
- Keep category series on their native 15-minute timestamps rather than implying one-minute resolution. Use Chart.js `{x, y}` points on a linear/category-compatible shared time-label domain, with nulls where category intervals do not align to counter rows.
- Add a concise chart note explaining system-used and category-RSS semantics.

## Data flow

No database or API change is required. The atomic dashboard response already includes:

- hardware `mem_total_kb`;
- one-minute counter `mem_available_kb` rows;
- usage grains containing `category`, `activity`, `interval_end`, and `rss_p50_kb`.

Frontend chart projection groups p50 RSS grains by `(category, interval_end)`, selects the maximum activity-grain value at each point, and aligns these points to the chart timeline without summing percentiles.

## Verification

- Browser projection test asserts exact system-used values, repeated total-memory values, absence of available-memory data, exact category labels/points, and null alignment.
- Browser rendering test asserts solid used line with points, dotted total line without points, and category labels containing `RSS p50 (non-additive)`.
- Mutation check breaks category grouping or reintroduces Available and confirms the tests fail.
- Run `tests/web tests/browser`, then the full repository suite using the maintained root venv.
- Independently review the exact final diff before merge.
